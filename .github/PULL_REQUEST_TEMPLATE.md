- [ ] **Submitter**: Include the JIRA issue number in the PR description
- [ ] **Submitter**: Make sure Swagger is updated if API changes
- [ ] **Submitter**: If updating admin endpoints, also update [firecloud-admin-cli](https://github.com/broadinstitute/firecloud-admin-cli)
- [ ] **Submitter**: Check documentation and code comments. Add explanatory PR comments if helpful.
- [ ] **Submitter**: JIRA ticket checks:
  * Acceptance criteria exists and is met
  * Note any changes to implementation from the description
  * Add notes on what you've tested
- [ ] **Submitter**: Update RC_XXX release ticket with any config or environment changes necessary
- [ ] **Submitter**: Database checks:
  * If PR includes new or changed db queries, include the explain plans in the description
  * Make sure liquibase is updated if appropriate
  * If doing a migration, take a backup of the dev and alpha DBs in Google Cloud Console
- [ ] **Submitter**: Update FISMA documentation if changes to:
  * Authentication
  * Authorization
  * Encryption
  * Audit trails
- [ ] **Submitter**: If you're adding new libraries, sign us up to security updates for them
- [ ] Tell ![](http://i.imgur.com/9dLzbPd.png) that the PR exists if he wants to look at it **(apply requires_doge label)**
- [ ] Anoint a lead reviewer (LR). **Assign PR to LR**
* Review cycle:
  * LR reviews
  * Rest of team may comment on PR at will
  * **LR assigns to submitter** for feedback fixes
  * Submitter rebases to develop again if necessary
  * Submitter makes further commits. DO NOT SQUASH
  * Submitter updates documentation as needed
  * Submitter **reassigns to LR** for further feedback
- [ ] ![](http://i.imgur.com/9dLzbPd.png) sign off
- [ ] **LR** sign off
- [ ] **Assign to submitter** to finalize
- [ ] **Submitter**: Verify all tests go green, including CI tests
- [ ] **Submitter**: Squash commits and merge to develop
- [ ] **Submitter**: Delete branch after merge
- [ ] **Submitter**: **Test this change works on dev environment after deployment**. YOU own getting it fixed if dev isn't working for ANY reason!
- [ ] **Submitter**: Verify swagger UI on dev environment still works after deployment
- [ ] **Submitter**: Inform other teams of any API changes via hipchat and/or email
- [ ] **Submitter**: Mark JIRA issue as resolved once this checklist is completed
