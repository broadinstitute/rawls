package org.broadinstitute.dsde.rawls.dataaccess.slick

import java.util.UUID

import org.broadinstitute.dsde.rawls.dataaccess.SlickWorkspaceContext
import org.broadinstitute.dsde.rawls.model._
import org.joda.time.DateTime

/**
 * Created by mbemis on 2/22/16.
 */
class SubmissionComponentSpec extends TestDriverComponentWithFlatSpecAndMatchers {
  import driver.api._

  private val submission3 = createTestSubmission(testData.workspace, testData.methodConfig2, testData.indiv1, testData.userOwner, Seq(testData.sample1, testData.sample2, testData.sample3), Map(testData.sample1 -> testData.inputResolutions, testData.sample2 -> testData.inputResolutions, testData.sample3 -> testData.inputResolutions))
  private val submission4 = createTestSubmission(testData.workspace, testData.methodConfig2, testData.indiv1, testData.userOwner, Seq(testData.sample1, testData.sample2, testData.sample3), Map(testData.sample1 -> testData.inputResolutions, testData.sample2 -> testData.inputResolutions, testData.sample3 -> testData.inputResolutions))

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

    runAndWait(submissionQuery.updateStatus(workspaceContext, testData.submission1.submissionId, SubmissionStatuses.Done))

    assertResult(Some(testData.submission1.copy(status = SubmissionStatuses.Done))) {
      runAndWait(submissionQuery.get(workspaceContext, testData.submission1.submissionId))
    }
  }

  it should "count submissions by their statuses" in withDefaultTestDatabase {
    val workspaceContext = SlickWorkspaceContext(testData.workspace)

    // test data contains 5 submissions, all in "Submitted" state
    // update one of the submissions to "Done"
    runAndWait(submissionQuery.updateStatus(workspaceContext, testData.submission1.submissionId, SubmissionStatuses.Done))
    assertResult(Some(testData.submission1.copy(status = SubmissionStatuses.Done))) {
      runAndWait(submissionQuery.get(workspaceContext, testData.submission1.submissionId))
    }

    // update another of the submissions to "Aborted"
    runAndWait(submissionQuery.updateStatus(workspaceContext, testData.submission2.submissionId, SubmissionStatuses.Aborted))
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

  "WorkflowComponent" should "let you modify Workflows within a submission" in withDefaultTestDatabase {
    val workspaceContext = SlickWorkspaceContext(testData.workspace)

    val workflow0 = testData.submission1.workflows(0)
    assertResult(Some(workflow0)) {
      runAndWait(workflowQuery.get(workspaceContext, testData.submission1.submissionId, workflow0.workflowEntity.get.entityType, workflow0.workflowEntity.get.entityName))
    }

    val workflow1 = testData.submission1.workflows(1)
    assertResult(Some(workflow1)) {
      runAndWait(workflowQuery.get(workspaceContext, testData.submission1.submissionId, workflow1.workflowEntity.get.entityType, workflow1.workflowEntity.get.entityName))
    }

    val workflow2 = testData.submission1.workflows(2)
    assertResult(Some(workflow2)) {
      runAndWait(workflowQuery.get(workspaceContext, testData.submission1.submissionId, workflow2.workflowEntity.get.entityType, workflow2.workflowEntity.get.entityName))
    }

    val workflow3 = Workflow(workflow1.workflowId, WorkflowStatuses.Failed, DateTime.now, workflow1.workflowEntity, testData.inputResolutions)

    runAndWait(workflowQuery.update(workspaceContext, UUID.fromString(testData.submission1.submissionId), workflow3))

    assertResult(Some(workflow3)) {
      runAndWait(workflowQuery.get(workspaceContext, testData.submission1.submissionId, workflow3.workflowEntity.get.entityType, workflow3.workflowEntity.get.entityName))
    }

    val workflowsWithIds = runAndWait(workflowQuery.getWithWorkflowIds(workspaceContext, testData.submission1.submissionId))
    val workflow3Id = workflowsWithIds.collect { case (id, workflow) if workflow == workflow3 => id }.head
    assert(runAndWait(workflowQuery.delete(workflow3Id)))

    val submission = testData.submission1.copy(workflows=Seq(workflow0, workflow2))
    assertResult(Some(submission)) {
      runAndWait(submissionQuery.get(workspaceContext, submission.submissionId))
    }
  }


}
