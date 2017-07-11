# Case Study: Debugging MySQL Deadlocks

Below are some notes on investigating a user-facing deadlock on the `WORKFLOW` table from 06/2017.

### JIRA:
https://broadinstitute.atlassian.net/browse/GAWB-2243

### User-facing error:
- https://files.slack.com/files-pri/T0CMFS7GX-F5XE1MKJN/stack_trace.java
- 500 on submission create
- > Lock wait timeout exceeded; try restarting transaction

### Sentry errors:
- https://sentry.io/broad-institute/firecloud-prod/issues/255118893/events/6011356841/
  - Jun 23, 2017 12:36:54 PM UTC
- https://sentry.io/broad-institute/firecloud-prod/issues/255118893/events/6002193538/
  - Jun 22, 2017 4:56:37 PM UTC
- https://sentry.io/broad-institute/firecloud-prod/issues/255118893/events/6002184900/
  - Jun 22, 2017 4:55:36 PM UTC
- https://sentry.io/broad-institute/firecloud-prod/issues/255118893/events/5991501149/
  - Jun 21, 2017 6:22:20 PM UTC
- https://sentry.io/broad-institute/firecloud-prod/issues/255118893/events/5967509154/
  - Jun 19, 2017 6:58:51 PM UTC

### Database info:
mysql> show engine innodb status;
- Output: https://broadinstitute.slack.com/files/doge/F5ZCZGAQN/-.txt

Scroll to 
```
------------------------
LATEST DETECTED DEADLOCK
------------------------
```
There are two active locks being held:
> RECORD LOCKS space id 5325 page no 8792 n bits 240 index `PRIMARY` of table `rawls`.`WORKFLOW` trx id 203110648 lock_mode X locks rec but not gap waiting

> RECORD LOCKS space id 5325 page no 7198 n bits 600 index `idx_workflow_status` of table `rawls`.`WORKFLOW` trx id 203110647 lock_mode X locks rec but not gap waiting
- https://dev.mysql.com/doc/refman/5.5/en/innodb-locking.html#innodb-record-locks

Let's look at the actual queries.

**Query 1:**
> update WORKFLOW set status = 'Aborted', status_last_changed = '2017-06-23 12:36:54.825', record_version = record_version + 1 where status = 'Queued' and submission_id = x'7B4889EE44A74A10B7CE11E53AE262E7'

**Query 2:**
> update WORKFLOW set status = 'Launching', status_last_changed = '2017-06-23 12:36:54.825', record_version = record_version + 1 where (id, record_version) in ((478207, 0))

- Query 1 locked idx_workflow_status = 'Queued' and is waiting to lock the primary key index
- Query 2 locked the primary key index and is waiting to lock idx_workflow_status = 'Queued'
- It looks like 1 transaction is trying to abort the workflow and the other is trying to launch it.
- **The lock on idx_workflow_status = 'Queued' is blocking the submission POST endpoint!**

### Deadlock handling advice:
https://dev.mysql.com/doc/refman/5.7/en/innodb-deadlocks-handling.html
- At any time, issue the `SHOW ENGINE INNODB STATUS` command to determine the cause of the most recent deadlock. That can help you to tune your application to avoid deadlocks.
- Enable `innodb_print_all_deadlocks flag`. This makes MySQL log all deadlocks instead of just the last detected one.
  - _Ticket open to do this: https://broadinstitute.atlassian.net/browse/GAWB-2264_
  - _This might not be possible due to the flags that CloudSQL allows_
- Always be prepared to re-issue a transaction if it fails due to deadlock. Deadlocks are not dangerous. Just try again.
  - _Not good if it impacts user experience as was the case with creating submissions_
- Keep transactions small and short in duration to make them less prone to collision.
- If you use locking reads (SELECT ... FOR UPDATE or SELECT ... LOCK IN SHARE MODE), try using a lower isolation level such as READ COMMITTED.
  - _This is not really applicable when doing updates_
- When modifying multiple tables within a transaction, or different sets of rows in the same table, do those operations in a consistent order each time. Then transactions form well-defined queues and do not deadlock. For example, organize database operations into functions within your application, or call stored routines, rather than coding multiple similar sequences of INSERT, UPDATE, and DELETE statements in different places.

### Resolution
In this case, resolved in the following way:
- https://github.com/broadinstitute/rawls/pull/718
- `WorkflowSubmissionActor` ignores workflows whose submission is Aborting, Aborted, Done. This removes the conflict between `SubmissionMonitorActor` and `WorkflowSubmissionActor`.

### Unit test reproduction:
1. Start WorkflowSubmissionActor with processInterval = 100 milliseconds
2. Create a submission with 20000 workflows via the API
    - this starts the SubmissionSupervisor/SubmissionMonitorActor for the submission. I reduced the poll interval from 1 minute to 1 second.
    - note: I had to fix some strange issues in the mock Cromwell server (RemoteServicesMockServer) to allow creating this many workflows in a unit test environment
3. Abort the submission via the API
4. Goto 2

The deadlock usually appeared before 30 iterations. After the fix, the deadlock never appears. :success:

### Other deadlocks:

This one in `GoogleGroupSyncMonitorSupervisor` appears in sentry pretty often:
https://broadinstitute.atlassian.net/browse/GAWB-2256

However it is not affecting user experience, so seems to be less critical.


