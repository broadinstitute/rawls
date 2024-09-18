package org.broadinstitute.dsde.rawls.dataaccess.slick

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import cats._
import cats.implicits._
import org.broadinstitute.dsde.rawls.RawlsTestUtils
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail

import java.sql.Timestamp
import java.util.UUID

/**
 * Created by mbemis on 2/22/16.
 */
class SubmissionComponentSpec extends TestDriverComponentWithFlatSpecAndMatchers with RawlsTestUtils {
  import driver.api._

  private val submission3 = createTestSubmission(
    testData.workspace,
    testData.methodConfig2,
    testData.indiv1,
    WorkbenchEmail(testData.userOwner.userEmail.value),
    Seq(testData.sample1, testData.sample2, testData.sample3),
    Map(
      testData.sample1 -> Seq(
        SubmissionValidationValue(Option(AttributeString("value1a")), Option("message1a"), "test_input_name"),
        SubmissionValidationValue(Option(AttributeString("value1b")), Option("message1b"), "test_input_name2")
      ),
      testData.sample2 -> Seq(
        SubmissionValidationValue(Option(AttributeString("value2a")), Option("message2a"), "test_input_name"),
        SubmissionValidationValue(Option(AttributeString("value2b")), Option("message2b"), "test_input_name2")
      ),
      testData.sample3 -> Seq(
        SubmissionValidationValue(Option(AttributeString("value3a")), Option("message3a"), "test_input_name"),
        SubmissionValidationValue(Option(AttributeString("value3b")), Option("message3b"), "test_input_name2")
      )
    ),
    Seq(testData.sample4, testData.sample5, testData.sample6),
    Map(testData.sample4 -> testData.inputResolutions2,
        testData.sample5 -> testData.inputResolutions2,
        testData.sample6 -> testData.inputResolutions2
    )
  )
  private val submission4 = createTestSubmission(
    testData.workspace,
    testData.methodConfig2,
    testData.indiv1,
    WorkbenchEmail(testData.userOwner.userEmail.value),
    Seq(testData.sample1, testData.sample2, testData.sample3),
    Map(testData.sample1 -> testData.inputResolutions,
        testData.sample2 -> testData.inputResolutions,
        testData.sample3 -> testData.inputResolutions
    ),
    Seq(testData.sample4, testData.sample5, testData.sample6),
    Map(testData.sample4 -> testData.inputResolutions2,
        testData.sample5 -> testData.inputResolutions2,
        testData.sample6 -> testData.inputResolutions2
    )
  )

  val submissionExternalEntities = Submission(
    submissionId = UUID.randomUUID.toString,
    submissionDate = testDate,
    submitter = WorkbenchEmail(testData.userOwner.userEmail.value),
    methodConfigurationNamespace = testData.methodConfig2.namespace,
    methodConfigurationName = testData.methodConfig2.name,
    submissionEntity = None,
    submissionRoot = "gs://fc-someWorkspaceId/someSubmissionId",
    workflows = Seq(
      Workflow(
        Option(UUID.randomUUID.toString),
        WorkflowStatuses.Running,
        testDate,
        Option(AttributeEntityReference("external", "e1")),
        Seq(SubmissionValidationValue(Option(AttributeString("value1a")), Option("message1a"), "test_input_name"))
      ),
      Workflow(
        Option(UUID.randomUUID.toString),
        WorkflowStatuses.Succeeded,
        testDate,
        Option(AttributeEntityReference("external", "e2")),
        Seq(SubmissionValidationValue(Option(AttributeString("value2a")), Option("message2a"), "test_input_name"))
      ),
      Workflow(
        Option(UUID.randomUUID.toString),
        WorkflowStatuses.Failed,
        testDate,
        Option(AttributeEntityReference("external", "e3")),
        Seq(SubmissionValidationValue(Option(AttributeString("value3a")), Option("message3a"), "test_input_name"))
      )
    ),
    status = SubmissionStatuses.Submitted,
    useCallCache = true,
    externalEntityInfo = Option(ExternalEntityInfo(UUID.randomUUID().toString, "external")),
    deleteIntermediateOutputFiles = true
  )

  val inputResolutionsList = Seq(
    SubmissionValidationValue(
      Option(AttributeValueList(Seq(AttributeString("elem1"), AttributeString("elem2"), AttributeString("elem3")))),
      Option("message3"),
      "test_input_name3"
    )
  )
  private val submissionList = createTestSubmission(
    testData.workspace,
    testData.methodConfigArrayType,
    testData.sset1,
    WorkbenchEmail(testData.userOwner.userEmail.value),
    Seq(testData.sset1),
    Map(testData.sset1 -> inputResolutionsList),
    Seq.empty,
    Map.empty
  )

  val inputResolutionsListEmpty = Seq(
    SubmissionValidationValue(Option(AttributeValueList(Seq())), Option("message4"), "test_input_name4")
  )
  private val submissionListEmpty = createTestSubmission(
    testData.workspace,
    testData.methodConfigArrayType,
    testData.sset1,
    WorkbenchEmail(testData.userOwner.userEmail.value),
    Seq(testData.sset1),
    Map(testData.sset1 -> inputResolutionsListEmpty),
    Seq.empty,
    Map.empty
  )

  val inputResolutionsAttrEmptyList = Seq(
    SubmissionValidationValue(Option(AttributeValueEmptyList), Option("message4"), "test_input_name4")
  )
  private val submissionAttrEmptyList = createTestSubmission(
    testData.workspace,
    testData.methodConfigArrayType,
    testData.sset1,
    WorkbenchEmail(testData.userOwner.userEmail.value),
    Seq(testData.sset1),
    Map(testData.sset1 -> inputResolutionsAttrEmptyList),
    Seq.empty,
    Map.empty
  )

  "SubmissionComponent" should "save, get, list, and delete a submission status" in withDefaultTestDatabase {
    val workspaceContext = testData.workspace

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

  it should "save, get and list a submission with external entities" in withDefaultTestDatabase {
    val workspaceContext = testData.workspace

    assert(submissionExternalEntities.externalEntityInfo.isDefined)

    runAndWait(submissionQuery.create(workspaceContext, submissionExternalEntities))

    assertResult(Some(submissionExternalEntities)) {
      runAndWait(submissionQuery.get(workspaceContext, submissionExternalEntities.submissionId))
    }

    assert(runAndWait(submissionQuery.list(workspaceContext)).toSet.contains(submissionExternalEntities))
  }

  it should "save, get, list, and delete two submission statuses" in withDefaultTestDatabase {
    val workspaceContext = testData.workspace

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

  it should "save and unmarshal listy input resolutions correctly" in withDefaultTestDatabase {
    val workspaceContext = testData.workspace

    runAndWait(submissionQuery.create(workspaceContext, submissionList))

    assertResult(Some(submissionList)) {
      runAndWait(submissionQuery.get(workspaceContext, submissionList.submissionId))
    }
  }

  it should "quietly marshal AttributeList(Seq()) input resolutions into AttributeEmptyList" in withDefaultTestDatabase {
    val workspaceContext = testData.workspace

    val turnedIntoAttrEmptyList = submissionListEmpty.copy(
      workflows = Seq(submissionListEmpty.workflows.head.copy(inputResolutions = inputResolutionsAttrEmptyList))
    )

    runAndWait(submissionQuery.create(workspaceContext, submissionListEmpty))
    assertResult(Some(turnedIntoAttrEmptyList)) {
      runAndWait(submissionQuery.get(workspaceContext, submissionListEmpty.submissionId))
    }
  }

  it should "save and unmarshal AttributeEmptyList input resolutions correctly" in withDefaultTestDatabase {
    // This test passes because saving AttributeEmptyList gives us AttributeEmptyList back
    val workspaceContext = testData.workspace

    runAndWait(submissionQuery.create(workspaceContext, submissionAttrEmptyList))
    assertResult(Some(submissionAttrEmptyList)) {
      runAndWait(submissionQuery.get(workspaceContext, submissionAttrEmptyList.submissionId))
    }
  }

  it should "fail to delete submissions that don't exist" in withDefaultTestDatabase {
    val workspaceContext = testData.workspace

    val randomId = UUID.randomUUID()

    // there's a 1 in 2^128 chance that this succeeds when it shouldn't. if that happens, go buy some powerball tickets
    assert(!runAndWait(submissionQuery.delete(workspaceContext, randomId.toString)))
  }

  it should "update a submission status" in withDefaultTestDatabase {
    val workspaceContext = testData.workspace

    runAndWait(
      submissionQuery.updateStatus(UUID.fromString(testData.submission1.submissionId), SubmissionStatuses.Done)
    )

    assertResult(Some(testData.submission1.copy(status = SubmissionStatuses.Done))) {
      runAndWait(submissionQuery.get(workspaceContext, testData.submission1.submissionId))
    }
  }

  it should "count submissions by their statuses" in withDefaultTestDatabase {
    val workspaceContext = testData.workspace

    // test data contains 5 submissions, all in "Submitted" state
    // update one of the submissions to "Done"
    runAndWait(
      submissionQuery.updateStatus(UUID.fromString(testData.submission1.submissionId), SubmissionStatuses.Done)
    )
    assertResult(Some(testData.submission1.copy(status = SubmissionStatuses.Done))) {
      runAndWait(submissionQuery.get(workspaceContext, testData.submission1.submissionId))
    }

    // update another of the submissions to "Aborted"
    runAndWait(
      submissionQuery.updateStatus(UUID.fromString(testData.submission2.submissionId), SubmissionStatuses.Aborted)
    )
    assertResult(Some(testData.submission2.copy(status = SubmissionStatuses.Aborted))) {
      runAndWait(submissionQuery.get(workspaceContext, testData.submission2.submissionId))
    }

    // should return {"Submitted" : 6, "Done" : 1, "Aborted" : 1}
    assert(3 == runAndWait(submissionQuery.countByStatus(workspaceContext)).size)
    assert(
      Option(6) == runAndWait(submissionQuery.countByStatus(workspaceContext))
        .get(SubmissionStatuses.Submitted.toString)
    )
    assert(
      Option(1) == runAndWait(submissionQuery.countByStatus(workspaceContext)).get(SubmissionStatuses.Done.toString)
    )
    assert(
      Option(1) == runAndWait(submissionQuery.countByStatus(workspaceContext)).get(SubmissionStatuses.Aborted.toString)
    )
  }

  it should "count submissions by their statuses across all workspaces" in withConstantTestDatabase {
    val statusMap = runAndWait(submissionQuery.countAllStatuses)
    statusMap shouldBe Map(SubmissionStatuses.Submitted.toString -> 3)
  }

  it should "unmarshal submission WorkflowFailureModes correctly" in withDefaultTestDatabase {
    val workspaceContext = testData.workspaceWorkflowFailureMode

    assertResult(Some(testData.submissionWorkflowFailureMode)) {
      runAndWait(submissionQuery.get(workspaceContext, testData.submissionWorkflowFailureMode.submissionId))
    }

    assert(runAndWait(submissionQuery.list(workspaceContext)).toSet.contains(testData.submissionWorkflowFailureMode))
  }

  it should "verify submission membership in a workspace" in withDefaultTestDatabase {
    val submissionId = UUID.fromString(testData.submissionSuccessful1.submissionId)
    val yesWorkspaceId = UUID.fromString(testData.workspaceSuccessfulSubmission.workspaceId)
    val noWorkspaceId = UUID.fromString(testData.workspaceNoSubmissions.workspaceId)

    assertResult(Some(())) {
      runAndWait(submissionQuery.confirmInWorkspace(yesWorkspaceId, submissionId))
    }

    assertResult(None) {
      runAndWait(submissionQuery.confirmInWorkspace(noWorkspaceId, submissionId))
    }
  }

  it should "return workflow failure messages even when workflowRec.workflowEntityId is None" in withDefaultTestDatabase {
    // example submission, no workflows as of yet
    val sid = UUID.randomUUID()
    val submissionNoEntitiesYesMessages = Submission(
      submissionId = sid.toString,
      submissionDate = testDate,
      submissionRoot = "gs://fc-someWorkspaceId/someSubmissionId",
      submitter = WorkbenchEmail(testData.userOwner.userEmail.value),
      methodConfigurationNamespace = testData.methodConfigValid.namespace,
      methodConfigurationName = testData.methodConfigValid.name,
      submissionEntity = None,
      workflows = Seq(),
      status = SubmissionStatuses.Done,
      useCallCache = false,
      deleteIntermediateOutputFiles = false
    )

    withWorkspaceContext(testData.workspace) { context =>
      // save the submission
      runAndWait(
        DBIO.seq(submissionQuery.create(context, submissionNoEntitiesYesMessages),
                 updateWorkflowExecutionServiceKey("unittestdefault")
        )
      )

      // save a couple workflow rows to this submission.
      val workflows = Seq(
        WorkflowRecord(11111,
                       Option("workflow1"),
                       sid,
                       WorkflowStatuses.Failed.toString,
                       Timestamp.from(java.time.Instant.now()),
                       None,
                       1,
                       Option("unittestdefault"),
                       None,
                       None
        ),
        WorkflowRecord(22222,
                       Option("workflow2"),
                       sid,
                       WorkflowStatuses.Failed.toString,
                       Timestamp.from(java.time.Instant.now()),
                       None,
                       1,
                       Option("unittestdefault"),
                       None,
                       None
        )
      )
      val saveWorkflows = workflowQuery ++= workflows
      runAndWait(saveWorkflows)
      val actualWorkflows = runAndWait(workflowQuery.findWorkflowsBySubmissionId(sid).result)
      withClue("checking count of saved workflows from findWorkflowsBySubmissionId: ") {
        actualWorkflows.size shouldBe 2
      }

      // save failure messages for these workflows
      actualWorkflows.foreach { w =>
        runAndWait(
          workflowQuery.saveMessages(Seq(AttributeString(s"failure message for ${w.externalId.getOrElse("")}")), w.id)
        )
      }

      // validate the submission and workflows saved properly
      val actual = runAndWait(submissionQuery.loadSubmission(sid))
      withClue("checking existence of submission: ") {
        actual should not be empty
      }
      actual.map { submission =>
        withClue("checking id of submission: ") {
          submission.submissionId shouldBe sid.toString
        }
        withClue("checking count of saved workflows from loadSubmission: ") {
          submission.workflows.size shouldBe 2
        }
      }

      // validate the workflow failure messages
      actual.map(_.workflows.foreach { w =>
        withClue(s"checking count of workflow failure messages returned from loadSubmission for ${w.workflowId}: ") {
          w.messages.size shouldBe 1
        }
        withClue(s"checking content of workflow failure messages returned from loadSubmission for ${w.workflowId}: ") {
          w.messages.head.value shouldBe s"failure message for ${w.workflowId.getOrElse("")}"
        }
      })
    }
  }

  "WorkflowComponent" should "update the status of a workflow and increment record version" in withDefaultTestDatabase {
    val workflowRecBefore =
      runAndWait(workflowQuery.listWorkflowRecsForSubmission(UUID.fromString(testData.submission1.submissionId))).head

    assert(workflowRecBefore.status == WorkflowStatuses.Submitted.toString)

    runAndWait(workflowQuery.updateStatus(workflowRecBefore, WorkflowStatuses.Failed))

    val workflowRecAfter = runAndWait(
      workflowQuery.listWorkflowRecsForSubmission(UUID.fromString(testData.submission1.submissionId))
    ).filter(_.id == workflowRecBefore.id).head

    assert(workflowRecAfter.status == WorkflowStatuses.Failed.toString)
    assert(workflowRecAfter.recordVersion == 1)
  }

  it should "batch update the statuses and record versions of multiple workflows" in withDefaultTestDatabase {
    val workflowRecsBefore =
      runAndWait(workflowQuery.listWorkflowRecsForSubmission(UUID.fromString(testData.submission1.submissionId)))

    assert(workflowRecsBefore.forall(_.status == WorkflowStatuses.Submitted.toString))

    runAndWait(workflowQuery.batchUpdateStatus(workflowRecsBefore, WorkflowStatuses.Failed))

    val workflowRecsAfter =
      runAndWait(workflowQuery.listWorkflowRecsForSubmission(UUID.fromString(testData.submission1.submissionId)))

    assert(workflowRecsAfter.forall(_.status == WorkflowStatuses.Failed.toString))
    assert(workflowRecsAfter.forall(_.recordVersion == 1))
  }

  it should "retrieve the execution service for a workflow" in withDefaultTestDatabase {
    val submissionId = testData.submission1.submissionId
    val otherSubmissionId = testData.submission2.submissionId
    val workflowIds =
      runAndWait(workflowQuery.listWorkflowRecsForSubmission(UUID.fromString(submissionId))) flatMap (_.externalId)

    workflowIds foreach { workflowId =>
      assertResult(Some("unittestdefault")) {
        runAndWait(workflowQuery.getExecutionServiceIdByExternalId(workflowId, submissionId))
      }
      assertResult(None) {
        runAndWait(workflowQuery.getExecutionServiceIdByExternalId(workflowId, otherSubmissionId))
      }
    }
  }

  it should "load workflows input resolutions correctly" in withDefaultTestDatabase {
    // You'd have thought that testing we can save and load a submission would therefore test that we can load workflows!
    // Not so! There's a different code path for "load a workflow because you're loading its submission" to "load a workflow on its own".
    // :(

    withWorkspaceContext(testData.workspace) { ctx =>
      val inputResolutionsList = Seq(
        SubmissionValidationValue(
          Option(AttributeValueList(Seq(AttributeString("elem1"), AttributeString("elem2"), AttributeString("elem3")))),
          Option("message3"),
          "test_input_name3"
        )
      )

      val submissionList = createTestSubmission(
        testData.workspace,
        testData.methodConfigArrayType,
        testData.sset1,
        WorkbenchEmail(testData.userOwner.userEmail.value),
        Seq(testData.sset1),
        Map(testData.sset1 -> inputResolutionsList),
        Seq.empty,
        Map.empty
      )

      runAndWait(submissionQuery.create(ctx, submissionList))

      // This is a bit roundabout, but we need to get the workflow IDs so we can load them individually through the loadWorkflow codepath.
      val workflowIds =
        runAndWait(workflowQuery.listWorkflowRecsForSubmission(UUID.fromString(submissionList.submissionId))).map(_.id)

      assertSameElements(submissionList.workflows, workflowIds map { id => runAndWait(workflowQuery.get(id)).get })
    }
  }

  /*
    first, update a workflow status to Succeeded, then using the original (and now stale) workflow record,
    try to update it to status Failed. It should fail because the record version has changed
   */
  it should "throw concurrent modification exception" in withDefaultTestDatabase {
    val workflowRecBefore =
      runAndWait(workflowQuery.listWorkflowRecsForSubmission(UUID.fromString(testData.submission1.submissionId))).head

    runAndWait(workflowQuery.updateStatus(workflowRecBefore, WorkflowStatuses.Succeeded))
    intercept[RawlsConcurrentModificationException] {
      runAndWait(workflowQuery.updateStatus(workflowRecBefore, WorkflowStatuses.Failed))
    }
  }

  it should "count workflow statuses in aggregate" in withConstantTestDatabase {
    val statusMap = runAndWait(workflowQuery.countAllStatuses)
    statusMap shouldBe Map(WorkflowStatuses.Submitted.toString -> 6)
  }

  it should "count workflows by queue status" in withDefaultTestDatabase {
    // Create some test submissions
    val statusCounts =
      Map(WorkflowStatuses.Submitted -> 1, WorkflowStatuses.Running -> 10, WorkflowStatuses.Aborting -> 100)
    withWorkspaceContext(testData.workspace) { ctx =>
      statusCounts
        .flatMap { case (st, count) =>
          for (_ <- 0 until count)
            yield createTestSubmission(
              testData.workspace,
              testData.methodConfigArrayType,
              testData.sset1,
              WorkbenchEmail(testData.userOwner.userEmail.value),
              Seq(testData.sset1),
              Map(testData.sset1 -> inputResolutionsList),
              Seq.empty,
              Map.empty,
              st
            )
        }
        .foreach { sub =>
          runAndWait(submissionQuery.create(ctx, sub))
        }
    }

    val result = runAndWait(workflowQuery.countWorkflowsByQueueStatus)
    validateCountsByQueueStatus(result)
    statusCounts.foreach { case (st, count) =>
      result(st.toString) should be >= count
    }
  }

  it should "count workflows by queue status by user" in withDefaultTestDatabase {
    // Create a new test user and some test submissions
    val testUserEmail = "testUser"
    val testUserId = "0001"
    val testUserStatusCounts =
      Map(WorkflowStatuses.Submitted -> 1, WorkflowStatuses.Running -> 10, WorkflowStatuses.Aborting -> 100)
    withWorkspaceContext(testData.workspace) { ctx =>
      val testUser = RawlsUser(
        UserInfo(RawlsUserEmail(testUserEmail), OAuth2BearerToken("token"), 123, RawlsUserSubjectId(testUserId))
      )
      testUserStatusCounts
        .flatMap { case (st, count) =>
          for (_ <- 0 until count)
            yield createTestSubmission(
              testData.workspace,
              testData.methodConfigArrayType,
              testData.sset1,
              WorkbenchEmail(testUser.userEmail.value),
              Seq(testData.sset1),
              Map(testData.sset1 -> inputResolutionsList),
              Seq.empty,
              Map.empty,
              st
            )
        }
        .foreach { sub =>
          runAndWait(submissionQuery.create(ctx, sub))
        }
    }

    // Validate testUser counts
    val result: Map[String, Map[String, Int]] = runAndWait(workflowQuery.countWorkflowsByQueueStatusByUser)
//    result should equal(Set())
    result should contain key testUserEmail
    testUserStatusCounts.foreach { case (st, count) =>
      result(testUserEmail)(st.toString) should be(count)
    }

    // Validate all workspace counts by status
    validateCountsByQueueStatus(Monoid.combineAll(result.values))
  }

  /**
    * Validates workflow counts by status against the current contents of the database.
    * @param counts workflow counts by status
    */
  private def validateCountsByQueueStatus(counts: Map[String, Int]): Unit = {
    val workflowRecs = runAndWait(workflowQuery.result)
    val expected = workflowRecs
      .groupBy(_.status)
      .view
      .filterKeys((WorkflowStatuses.queuedStatuses ++ WorkflowStatuses.runningStatuses).map(_.toString).contains)
      .mapValues(_.size)
      .toMap
    counts should equal(expected)
  }

  WorkflowStatuses.runningStatuses.foreach { status =>
    it should s"count $status workflows by submitter" in withDefaultTestDatabase {
      val workflowRecs = runAndWait(workflowQuery.result)
      runAndWait(workflowQuery.batchUpdateStatus(workflowRecs, status))
      assertResult(Seq((testData.userOwner.userEmail.value, workflowRecs.size))) {
        runAndWait(workflowQuery.listSubmittersWithMoreWorkflowsThan(0, WorkflowStatuses.runningStatuses))
      }
      assertResult(Seq((testData.userOwner.userEmail.value, workflowRecs.size))) {
        runAndWait(
          workflowQuery.listSubmittersWithMoreWorkflowsThan(workflowRecs.size - 1, WorkflowStatuses.runningStatuses)
        )
      }
      assertResult(Seq.empty) {
        runAndWait(
          workflowQuery.listSubmittersWithMoreWorkflowsThan(workflowRecs.size, WorkflowStatuses.runningStatuses)
        )
      }
    }
  }

  it should "batch update statuses" in withDefaultTestDatabase {
    val submittedWorkflowRecs =
      runAndWait(workflowQuery.filter(_.status === WorkflowStatuses.Submitted.toString).result)

    assertResult(submittedWorkflowRecs.size) {
      runAndWait(workflowQuery.batchUpdateStatus(WorkflowStatuses.Submitted, WorkflowStatuses.Failed))
    }

    val updatedWorkflowRecs = runAndWait(workflowQuery.findWorkflowByIds(submittedWorkflowRecs.map(_.id)).result)

    assert(updatedWorkflowRecs.forall(_.status == WorkflowStatuses.Failed.toString))
    assertResult(submittedWorkflowRecs.map(r => r.id -> (r.recordVersion + 1)).toMap) {
      updatedWorkflowRecs.map(r => r.id -> r.recordVersion).toMap
    }
  }

  it should "load workflow with extern entities" in withDefaultTestDatabase {
    val workspaceContext = testData.workspace

    assert(submissionExternalEntities.externalEntityInfo.isDefined)

    runAndWait(submissionQuery.create(workspaceContext, submissionExternalEntities))

    val workflow = submissionExternalEntities.workflows.head

    assertResult(Some(workflow)) {
      runAndWait(workflowQuery.getByExternalId(workflow.workflowId.get, submissionExternalEntities.submissionId))
    }

  }
}
