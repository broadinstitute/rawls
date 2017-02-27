package org.broadinstitute.dsde.rawls.webservice

import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.dataaccess.slick.{EntityRecord, WorkflowAuditStatusRecord}
import org.broadinstitute.dsde.rawls.google.MockGooglePubSubDAO
import org.broadinstitute.dsde.rawls.model.ExecutionJsonSupport.{SubmissionReportFormat, SubmissionRequestFormat, SubmissionStatusResponseFormat, SubmissionListResponseFormat, WorkflowQueueStatusResponseFormat, WorkflowOutputsFormat}
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.openam.MockUserInfoDirectives
import spray.http._
import spray.json.{JsString, JsArray}
import spray.json.DefaultJsonProtocol._
import scala.concurrent.ExecutionContext
import java.util.UUID

import org.joda.time.DateTime

/**
 * Created by dvoet on 4/24/15.
 */
class SubmissionApiServiceSpec extends ApiServiceSpec {

  case class TestApiService(dataSource: SlickDataSource, gcsDAO: MockGoogleServicesDAO, gpsDAO: MockGooglePubSubDAO)(implicit val executionContext: ExecutionContext) extends ApiServices with MockUserInfoDirectives

  def withApiServices[T](dataSource: SlickDataSource)(testCode: TestApiService => T): T = {

    val gcsDAO = new MockGoogleServicesDAO("test")
    gcsDAO.storeToken(userInfo, "test_token")

    val apiService = new TestApiService(dataSource, gcsDAO, new MockGooglePubSubDAO)
    try {
      testCode(apiService)
    } finally {
      apiService.cleanupSupervisor
    }
  }

  def withTestDataApiServices[T](testCode: TestApiService => T): T = {
    withDefaultTestDatabase { dataSource: SlickDataSource =>
      withApiServices(dataSource)(testCode)
    }
  }

  "SubmissionApi" should "return 404 Not Found when creating a submission using a MethodConfiguration that doesn't exist in the workspace" in withTestDataApiServices { services =>
    Post(s"/workspaces/${testData.wsName.namespace}/${testData.wsName.name}/submissions", httpJson(SubmissionRequest("dsde","not there","Pattern","pattern1", None))) ~>
      sealRoute(services.submissionRoutes) ~>
      check { assertResult(StatusCodes.NotFound) {status} }
  }

  it should "return 404 Not Found when creating a submission using an Entity that doesn't exist in the workspace" in withTestDataApiServices { services =>
    val mcName = MethodConfigurationName("three_step","dsde", testData.wsName)
    val methodConf = MethodConfiguration(mcName.namespace, mcName.name,"Pattern", Map.empty, Map("three_step.cgrep.pattern"->AttributeString("String")), Map.empty, MethodRepoMethod("dsde","three_step",1))
    Post(s"/workspaces/${testData.wsName.namespace}/${testData.wsName.name}/methodconfigs", httpJson(methodConf)) ~>
      sealRoute(services.methodConfigRoutes) ~>
      check { assertResult(StatusCodes.Created) {status} }
    Post(s"/workspaces/${testData.wsName.namespace}/${testData.wsName.name}/submissions", httpJson(SubmissionRequest(mcName.namespace, mcName.name,"Pattern","pattern1", None))) ~>
      sealRoute(services.submissionRoutes) ~>
      check { assertResult(StatusCodes.NotFound) {status} }
  }

  private def createAndMonitorSubmission(wsName: WorkspaceName, methodConf: MethodConfiguration,
                                         submissionEntity: Entity, submissionExpression: Option[String],
                                         services: TestApiService): SubmissionStatusResponse = {
    Post(s"/workspaces/${wsName.namespace}/${wsName.name}/methodconfigs", httpJson(methodConf)) ~>
      sealRoute(services.methodConfigRoutes) ~>
      check {
        assertResult(StatusCodes.Created) {
          status
        }
      }

    val submissionRq = SubmissionRequest(methodConf.namespace, methodConf.name, submissionEntity.entityType, submissionEntity.name, submissionExpression)
    Post(s"/workspaces/${wsName.namespace}/${wsName.name}/submissions", httpJson(submissionRq)) ~>
      sealRoute(services.submissionRoutes) ~>
      check {
        assertResult(StatusCodes.Created) {
          status
        }
        val submission = responseAs[SubmissionReport]
        Get(s"/workspaces/${wsName.namespace}/${wsName.name}/submissions/${submission.submissionId}") ~>
          sealRoute(services.submissionRoutes) ~>
          check {
            assertResult(StatusCodes.OK) {
              status
            }
            return responseAs[SubmissionStatusResponse]
          }
      }

    fail("Unable to create and monitor submissions")
  }

  it should "return 201 Created when creating and monitoring a submission with no expression" in withTestDataApiServices { services =>
    val wsName = testData.wsName
    val mcName = MethodConfigurationName("no_input", "dsde", wsName)
    val methodConf = MethodConfiguration(mcName.namespace, mcName.name, "Sample", Map.empty, Map.empty, Map.empty, MethodRepoMethod("dsde", "no_input", 1))

    val submission = createAndMonitorSubmission(wsName, methodConf, testData.sample1, None, services)

    assertResult(1) {
      submission.workflows.size
    }
  }
  it should "return 201 Created when creating and monitoring a submission with valid expression" in withTestDataApiServices { services =>
    val wsName = testData.wsName
    val mcName = MethodConfigurationName("no_input", "dsde", wsName)
    val methodConf = MethodConfiguration(mcName.namespace, mcName.name, "Sample", Map.empty, Map.empty, Map.empty, MethodRepoMethod("dsde", "no_input", 1))

    val submission = createAndMonitorSubmission(wsName, methodConf, testData.sset1, Option("this.samples"), services)

    assertResult(3) {
      submission.workflows.size
    }
  }

  it should "update the last modified date on a workspace for submission entries" in withTestDataApiServices { services =>
    val wsName = testData.wsName
    val mcName = MethodConfigurationName("no_input", "dsde", wsName)
    val methodConf = MethodConfiguration(mcName.namespace, mcName.name, "Sample", Map.empty, Map.empty, Map.empty, MethodRepoMethod("dsde", "no_input", 1))

    val submission = createAndMonitorSubmission(wsName, methodConf, testData.sset1, Option("this.samples"), services)

    Get(s"/workspaces/${testData.wsName.namespace}/${testData.wsName.name}") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertWorkspaceModifiedDate(status, responseAs[WorkspaceListResponse].workspace)
      }

  }


  val attributeList = AttributeValueList(Seq(AttributeString("a"), AttributeString("b"), AttributeBoolean(true)))
  val z1 = Entity("z1", "Sample", Map(AttributeName.withDefaultNS("foo") -> AttributeString("x"), AttributeName.withDefaultNS("bar") -> AttributeNumber(3), AttributeName.withDefaultNS("splat") -> attributeList))
  val workspace2Name = new WorkspaceName(testData.wsName.namespace + "2", testData.wsName.name + "2")
  val workspace2Request = WorkspaceRequest(
    workspace2Name.namespace,
    workspace2Name.name,
    None,
    Map.empty
  )

  it should "return 200 on getting a submission" in withTestDataApiServices { services =>
    Get(s"/workspaces/${testData.wsName.namespace}/${testData.wsName.name}/submissions/${testData.submission1.submissionId}") ~>
      sealRoute(services.submissionRoutes) ~>
      check {
        assertResult(StatusCodes.OK, response.entity.asString) {status}
        assertResult(new SubmissionStatusResponse(testData.submission1, testData.userOwner)) {responseAs[SubmissionStatusResponse]}
      }
  }

  it should "return 404 on getting a nonexistent submission" in withTestDataApiServices { services =>
    Get(s"/workspaces/${testData.wsName.namespace}/${testData.wsName.name}/submissions/unrealSubmission42") ~>
      sealRoute(services.submissionRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound) {status}
      }
    Get(s"/workspaces/${testData.wsName.namespace}/${testData.wsName.name}/submissions/${UUID.randomUUID}") ~>
      sealRoute(services.submissionRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound) {status}
      }
  }

  it should "return 200 when listing submissions" in withTestDataApiServices { services =>
    Get(s"/workspaces/${testData.wsName.namespace}/${testData.wsName.name}/submissions") ~>
      sealRoute(services.submissionRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {status}
        assertResult(Set(
          new SubmissionListResponse(testData.submissionTerminateTest, testData.userOwner, Map[String, Int]("Submitted" -> 4)),
          new SubmissionListResponse(testData.submissionNoWorkflows, testData.userOwner, Map[String, Int]()),
          new SubmissionListResponse(testData.submission1, testData.userOwner, Map[String, Int]("Submitted" -> 3)),
          new SubmissionListResponse(testData.submission2, testData.userOwner, Map[String, Int]("Submitted" -> 3)),
          new SubmissionListResponse(testData.submissionUpdateEntity, testData.userOwner, Map[String, Int]("Submitted" -> 1)),
          new SubmissionListResponse(testData.submissionUpdateWorkspace, testData.userOwner, Map[String, Int]("Submitted" -> 1)))) {
          responseAs[Seq[SubmissionListResponse]].toSet
        }
      }
  }

  it should "return 200 when counting submissions" in withTestDataApiServices { services =>
    Get(s"/workspaces/${testData.wsName.namespace}/${testData.wsName.name}/submissionsCount") ~>
      sealRoute(services.submissionRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {status}
        assertResult(Map("Submitted" -> 6)) {
          responseAs[Map[String, Int]]
        }
      }
  }

  // Submissions Queue methods

  def getQueueStatus(route: spray.routing.Route): WorkflowQueueStatusResponse =
    Get("/submissions/queueStatus") ~>
      sealRoute(route) ~>
      check {
        assertResult(StatusCodes.OK) { status }
        responseAs[WorkflowQueueStatusResponse]
      }

  def addWorkflowsToQueue(user: RawlsUser, count: Int) = {
    withWorkspaceContext(testData.workspace) { context =>
      val workflows = Seq.fill(count) {
        Thread.sleep(10) // ensure some time separation

        val ent = Entity(UUID.randomUUID.toString, UUID.randomUUID.toString, Map.empty)
        runAndWait(entityQuery.save(context, ent))
        Workflow(Option(UUID.randomUUID.toString), WorkflowStatuses.Queued, DateTime.now, ent.toReference, testData.inputResolutions)
      }

      val sub = createTestSubmission(testData.workspace, testData.methodConfig, testData.indiv1, user, Seq.empty, Map.empty, Seq.empty, Map.empty).copy(workflows = workflows)
      runAndWait(submissionQuery.create(context, sub))
    }
  }

  it should "return 200 when checking the queue status" in withTestDataApiServices { services =>

    // insert audit records
    val expectedEstimateTime = 12345
    val submittedTime = System.currentTimeMillis()
    val queuedTime = submittedTime - expectedEstimateTime
    runAndWait( workflowAuditStatusQuery.save( WorkflowAuditStatusRecord(0, 321, WorkflowStatuses.Queued.toString, new java.sql.Timestamp(queuedTime)) ) )
    runAndWait( workflowAuditStatusQuery.save( WorkflowAuditStatusRecord(0, 321, WorkflowStatuses.Submitted.toString, new java.sql.Timestamp(submittedTime)) ) )
    // also insert a dummy audit record with a different workflow id to attempt to confuse the code
    runAndWait( workflowAuditStatusQuery.save( WorkflowAuditStatusRecord(0, 42, WorkflowStatuses.Queued.toString, new java.sql.Timestamp(queuedTime-6000)) ) )

    val existingSubmittedWorkflowCount = 15
    val existingWorkflowCounts = Map("Submitted" -> existingSubmittedWorkflowCount)

    val resp = getQueueStatus(services.submissionRoutes)
    assertResult(existingWorkflowCounts) {
      resp.workflowCountsByStatus
    }
    // with nothing in queue, estimated time should be zero
    assertResult(0) {
      resp.estimatedQueueTimeMS
    }

    val newWorkflows = Map(
      WorkflowStatuses.Queued -> 1,
      WorkflowStatuses.Launching -> 2,
      WorkflowStatuses.Submitted -> 4,
      WorkflowStatuses.Running -> 8,
      WorkflowStatuses.Failed -> 16,
      WorkflowStatuses.Succeeded -> 32,
      WorkflowStatuses.Aborting -> 64,
      WorkflowStatuses.Aborted -> 128,
      WorkflowStatuses.Unknown -> 256
    )

    val newWorkflowCounts = Map(
      "Queued" -> 1,
      "Launching" -> 2,
      "Submitted" -> (4 + existingSubmittedWorkflowCount),
      "Running" -> 8,
      "Aborting" -> 64
    )

    withWorkspaceContext(testData.workspace) { context =>
      newWorkflows foreach { case (status, count) =>
        val entityRecs = for (i <- 1 to count) yield EntityRecord(0, i.toString, status.toString, context.workspaceId, 0, None)
        runAndWait(entityQuery.batchInsertEntities(context, entityRecs))
        val workflows = for (i <- 1 to count) yield Workflow(Option(s"workflow${i}_of_$count"), status, testDate, AttributeEntityReference(status.toString, i.toString), testData.inputResolutions)
        runAndWait(workflowQuery.createWorkflows(context, UUID.fromString(testData.submissionUpdateEntity.submissionId), workflows))
      }
    }

    val resp2 = getQueueStatus(services.submissionRoutes)
    assertResult(newWorkflowCounts) {
      resp2.workflowCountsByStatus
    }
    // with items in the queue, estimated time should be calculated from the audit table
    assertResult(expectedEstimateTime) {
      resp2.estimatedQueueTimeMS
    }

  }

  it should "count zero workflows ahead of the user for an empty queue" in withTestDataApiServices { services =>
    assertResult(0) {
      getQueueStatus(services.submissionRoutes).workflowsBeforeNextUserWorkflow
    }
  }

  it should "count zero workflows ahead of the user when the user is the only one in the queue" in withTestDataApiServices { services =>
    addWorkflowsToQueue(RawlsUser(userInfo), 1)

    assertResult(0) {
      getQueueStatus(services.submissionRoutes).workflowsBeforeNextUserWorkflow
    }
  }

  it should "count the whole queue as ahead of the user when the user is not in the queue" in withTestDataApiServices { services =>
    val otherUser1 = RawlsUser(RawlsUserSubjectId("subj-id-1"), RawlsUserEmail("new.email1@example.net"))
    val otherUser2 = RawlsUser(RawlsUserSubjectId("subj-id-2"), RawlsUserEmail("new.email2@example.net"))

    runAndWait(rawlsUserQuery.save(otherUser1))
    runAndWait(rawlsUserQuery.save(otherUser2))

    addWorkflowsToQueue(otherUser1, 5)
    addWorkflowsToQueue(otherUser2, 10)

    val status = getQueueStatus(services.submissionRoutes)
    assertResult(Some(15)) {
      status.workflowCountsByStatus.get("Queued")
    }
    assertResult(15) {
      status.workflowsBeforeNextUserWorkflow
    }
  }

  it should "count workflows ahead of the user when the user is in the queue" in withTestDataApiServices { services =>
    val otherUser1 = RawlsUser(RawlsUserSubjectId("subj-id-1"), RawlsUserEmail("new.email1@example.net"))
    val otherUser2 = RawlsUser(RawlsUserSubjectId("subj-id-2"), RawlsUserEmail("new.email2@example.net"))

    runAndWait(rawlsUserQuery.save(otherUser1))
    runAndWait(rawlsUserQuery.save(otherUser2))

    addWorkflowsToQueue(otherUser1, 5)
    addWorkflowsToQueue(RawlsUser(userInfo), 20)
    addWorkflowsToQueue(otherUser2, 10)

    val status = getQueueStatus(services.submissionRoutes)
    assertResult(Some(35)) {
      status.workflowCountsByStatus.get("Queued")
    }
    assertResult(5) {
      status.workflowsBeforeNextUserWorkflow
    }
  }

  it should "handle unsupported workflow outputs" in withTestDataApiServices { services =>
    import driver.api._

    val workflowId = "8afafe21-2b70-4180-a565-748cb573e10c"
    val workflows = Seq(
      // use the UUID of the workflow that has an output of array(array)
      Workflow(Option(workflowId), WorkflowStatuses.Succeeded, testDate, testData.indiv1.toReference, Seq.empty)
    )

    val testSubmission = Submission(UUID.randomUUID.toString, testDate, testData.userOwner, testData.methodConfig.namespace, testData.methodConfig.name, testData.indiv1.toReference, workflows, SubmissionStatuses.Done)

    runAndWait(submissionQuery.create(SlickWorkspaceContext(testData.workspace), testSubmission))
    runAndWait(workflowQuery.findWorkflowByExternalIdAndSubmissionId(workflowId, UUID.fromString(testSubmission.submissionId)).map(_.executionServiceKey).update(Option("unittestdefault")))

    Get(s"/workspaces/${testData.wsName.namespace}/${testData.wsName.name}/submissions/${testSubmission.submissionId}/workflows/${testSubmission.workflows.head.workflowId.get}/outputs") ~>
      sealRoute(services.submissionRoutes) ~>
      check {
        assertResult(StatusCodes.OK, response.entity.asString) {status}
        val expectedOutputs = WorkflowOutputs(workflowId, Map("aggregate_data_workflow.aggregate_data" -> TaskOutput(None, Option(Map("aggregate_data_workflow.aggregate_data.output_array" -> Left(AttributeValueRawJson(JsArray(Vector(
          JsArray(Vector(JsString("foo"), JsString("bar"))),
          JsArray(Vector(JsString("baz"), JsString("qux"))))))))))))
        assertResult(expectedOutputs) { responseAs[WorkflowOutputs] }
      }
  }
}
