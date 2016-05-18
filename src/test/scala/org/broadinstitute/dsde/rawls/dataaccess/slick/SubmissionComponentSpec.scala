package org.broadinstitute.dsde.rawls.dataaccess.slick

import java.util.UUID

import org.broadinstitute.dsde.rawls.dataaccess.SlickWorkspaceContext
import org.broadinstitute.dsde.rawls.model._

/**
 * Created by mbemis on 2/22/16.
 */
class SubmissionComponentSpec extends TestDriverComponentWithFlatSpecAndMatchers {
  import driver.api._

  private val submission3 = createTestSubmission(testData.workspace, testData.methodConfig2, testData.indiv1, testData.userOwner,
    Seq(testData.sample1, testData.sample2, testData.sample3), Map(testData.sample1 -> testData.inputResolutions, testData.sample2 -> testData.inputResolutions, testData.sample3 -> testData.inputResolutions),
    Seq(testData.sample4, testData.sample5, testData.sample6), Map(testData.sample4 -> testData.inputResolutions2, testData.sample5 -> testData.inputResolutions2, testData.sample6 -> testData.inputResolutions2))
  private val submission4 = createTestSubmission(testData.workspace, testData.methodConfig2, testData.indiv1, testData.userOwner,
    Seq(testData.sample1, testData.sample2, testData.sample3), Map(testData.sample1 -> testData.inputResolutions, testData.sample2 -> testData.inputResolutions, testData.sample3 -> testData.inputResolutions),
    Seq(testData.sample4, testData.sample5, testData.sample6), Map(testData.sample4 -> testData.inputResolutions2, testData.sample5 -> testData.inputResolutions2, testData.sample6 -> testData.inputResolutions2))


  "SubmissionComponent" should "save, get, list, and delete a submission status" in withDefaultTestDatabase {
    val workspaceContext = SlickWorkspaceContext(testData.workspace)

    runAndWait(submissionQuery.create(workspaceContext, submission3))

    assertResult(Some(submission3)) {
      runAndWait(submissionQuery.get(workspaceContext, submission3.submissionId))
    }

    assert(runAndWait(submissionQuery.list(workspaceContext)).toSet.contains(submission3))

    assert(runAndWait(submissionQuery.delete(workspaceContext, submission3.submissionId)))

    assertResult(None) {
      runAndWait(submissionQuery.get(workspaceContext, submission3.submissionId))
    }

    assert(!runAndWait(submissionQuery.list(workspaceContext)).toSet.contains(submission3))
  }

  it should "save, get, list, and delete two submission statuses" in withDefaultTestDatabase {
    val workspaceContext = SlickWorkspaceContext(testData.workspace)

    runAndWait(submissionQuery.create(workspaceContext, submission3))

    runAndWait(submissionQuery.create(workspaceContext, submission4))


    assertResult(Some(submission3)) {
      runAndWait(submissionQuery.get(workspaceContext, submission3.submissionId))
    }

    assertResult(Some(submission4)) {
      runAndWait(submissionQuery.get(workspaceContext, submission4.submissionId))
    }

    assert(runAndWait(submissionQuery.list(workspaceContext)).toSet.contains(submission3))
    assert(runAndWait(submissionQuery.list(workspaceContext)).toSet.contains(submission4))

    assert(runAndWait(submissionQuery.delete(workspaceContext, submission3.submissionId)))
    assert(runAndWait(submissionQuery.delete(workspaceContext, submission4.submissionId)))

    assert(!runAndWait(submissionQuery.list(workspaceContext)).toSet.contains(submission3))
    assert(!runAndWait(submissionQuery.list(workspaceContext)).toSet.contains(submission4))
  }

  it should "fail to delete submissions that don't exist" in withDefaultTestDatabase {
    val workspaceContext = SlickWorkspaceContext(testData.workspace)

    val randomId = UUID.randomUUID()

    //there's a 1 in 2^128 chance that this succeeds when it shouldn't. if that happens, go buy some powerball tickets
    assert(!runAndWait(submissionQuery.delete(workspaceContext, randomId.toString)))
  }

  it should "update a submission status" in withDefaultTestDatabase {
    val workspaceContext = SlickWorkspaceContext(testData.workspace)

    runAndWait(submissionQuery.updateStatus(UUID.fromString(testData.submission1.submissionId), SubmissionStatuses.Done))

    assertResult(Some(testData.submission1.copy(status = SubmissionStatuses.Done))) {
      runAndWait(submissionQuery.get(workspaceContext, testData.submission1.submissionId))
    }
  }

  it should "count submissions by their statuses" in withDefaultTestDatabase {
    val workspaceContext = SlickWorkspaceContext(testData.workspace)

    // test data contains 5 submissions, all in "Submitted" state
    // update one of the submissions to "Done"
    runAndWait(submissionQuery.updateStatus(UUID.fromString(testData.submission1.submissionId), SubmissionStatuses.Done))
    assertResult(Some(testData.submission1.copy(status = SubmissionStatuses.Done))) {
      runAndWait(submissionQuery.get(workspaceContext, testData.submission1.submissionId))
    }

    // update another of the submissions to "Aborted"
    runAndWait(submissionQuery.updateStatus(UUID.fromString(testData.submission2.submissionId), SubmissionStatuses.Aborted))
    assertResult(Some(testData.submission2.copy(status = SubmissionStatuses.Aborted))) {
      runAndWait(submissionQuery.get(workspaceContext, testData.submission2.submissionId))
    }

    // should return {"Submitted" : 3, "Done" : 1, "Aborted" : 1}
    assert(3 == runAndWait(submissionQuery.countByStatus(workspaceContext)).size)
    assert(Option(3) == runAndWait(submissionQuery.countByStatus(workspaceContext)).get(SubmissionStatuses.Submitted.toString))
    assert(Option(1) == runAndWait(submissionQuery.countByStatus(workspaceContext)).get(SubmissionStatuses.Done.toString))
    assert(Option(1) == runAndWait(submissionQuery.countByStatus(workspaceContext)).get(SubmissionStatuses.Aborted.toString))
  }

  it should "save a submission with workflow failures" in withDefaultTestDatabase {
    val workspaceContext= SlickWorkspaceContext(testData.workspace)

    val submissionWithFailedWorkflows = testData.submission1.copy(submissionId = UUID.randomUUID().toString, notstarted = Seq(WorkflowFailure(testData.indiv1.name, testData.indiv1.entityType, Seq.empty, Seq.empty)))

    runAndWait(submissionQuery.create(workspaceContext, submissionWithFailedWorkflows))

    assertResult(Some(submissionWithFailedWorkflows)) {
      runAndWait(submissionQuery.get(workspaceContext, submissionWithFailedWorkflows.submissionId))
    }
  }

  "WorkflowComponent" should "update the status of a workflow and increment record version" in withDefaultTestDatabase {
    val workflowRecBefore = runAndWait(workflowQuery.listWorkflowRecsForSubmission(UUID.fromString(testData.submission1.submissionId))).head

    assert(workflowRecBefore.status == WorkflowStatuses.Submitted.toString)

    runAndWait(workflowQuery.updateStatus(workflowRecBefore, WorkflowStatuses.Failed))

    val workflowRecAfter = runAndWait(workflowQuery.listWorkflowRecsForSubmission(UUID.fromString(testData.submission1.submissionId))).filter(_.id == workflowRecBefore.id).head

    assert(workflowRecAfter.status == WorkflowStatuses.Failed.toString)
    assert(workflowRecAfter.recordVersion == 1)
  }

  it should "batch update the statuses and record versions of multiple workflows" in withDefaultTestDatabase {
    val workflowRecsBefore = runAndWait(workflowQuery.listWorkflowRecsForSubmission(UUID.fromString(testData.submission1.submissionId)))

    assert(workflowRecsBefore.forall(_.status == WorkflowStatuses.Submitted.toString))

    runAndWait(workflowQuery.batchUpdateStatus(workflowRecsBefore, WorkflowStatuses.Failed))

    val workflowRecsAfter = runAndWait(workflowQuery.listWorkflowRecsForSubmission(UUID.fromString(testData.submission1.submissionId)))

    assert(workflowRecsAfter.forall(_.status == WorkflowStatuses.Failed.toString))
    assert(workflowRecsAfter.forall(_.recordVersion == 1))
  }

  /*
    first, update a workflow status to Succeeded, then using the original (and now stale) workflow record,
    try to update it to status Failed. It should fail because the record version has changed
   */
  it should "throw concurrent modification exception" in withDefaultTestDatabase {
    val workflowRecBefore = runAndWait(workflowQuery.listWorkflowRecsForSubmission(UUID.fromString(testData.submission1.submissionId))).head

    runAndWait(workflowQuery.updateStatus(workflowRecBefore, WorkflowStatuses.Succeeded))
    intercept[RawlsConcurrentModificationException] {
      runAndWait(workflowQuery.updateStatus(workflowRecBefore, WorkflowStatuses.Failed))
    }
  }

  WorkflowStatuses.runningStatuses.foreach { status =>
    it should s"count $status workflows by submitter" in withDefaultTestDatabase {
      val workflowRecs = runAndWait(workflowQuery.result)
      runAndWait(workflowQuery.batchUpdateStatus(workflowRecs, status))
      assertResult(Seq((testData.userOwner.userSubjectId.value, workflowRecs.size))) {
        runAndWait(workflowQuery.listSubmittersWithMoreWorkflowsThan(0, WorkflowStatuses.runningStatuses))
      }
      assertResult(Seq((testData.userOwner.userSubjectId.value, workflowRecs.size))) {
        runAndWait(workflowQuery.listSubmittersWithMoreWorkflowsThan(workflowRecs.size-1, WorkflowStatuses.runningStatuses))
      }
      assertResult(Seq.empty) {
        runAndWait(workflowQuery.listSubmittersWithMoreWorkflowsThan(workflowRecs.size, WorkflowStatuses.runningStatuses))
      }
    }
  }
}
