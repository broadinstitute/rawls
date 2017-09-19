package org.broadinstitute.dsde.rawls.webservice

import java.util.UUID

import akka.actor.{ActorRef, PoisonPill}
import akka.testkit.TestProbe
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.dataaccess.slick.{ReadWriteAction, TestData, WorkflowAuditStatusRecord}
import org.broadinstitute.dsde.rawls.google.MockGooglePubSubDAO
import org.broadinstitute.dsde.rawls.jobexec.WorkflowSubmissionActor
import org.broadinstitute.dsde.rawls.model.ExecutionJsonSupport.{ExecutionServiceVersionFormat, SubmissionListResponseFormat, SubmissionReportFormat, SubmissionRequestFormat, SubmissionStatusResponseFormat, WorkflowOutputsFormat, WorkflowQueueStatusResponseFormat}
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.openam.MockUserInfoDirectives
import org.joda.time.DateTime
import org.scalatest.time.{Seconds, Span}
import spray.http._
import spray.json.DefaultJsonProtocol._
import spray.json._

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

/**
 * Created by dvoet on 4/24/15.
 */
class SubmissionApiServiceSpec extends ApiServiceSpec {

  case class TestApiService(dataSource: SlickDataSource, gcsDAO: MockGoogleServicesDAO, gpsDAO: MockGooglePubSubDAO)(implicit val executionContext: ExecutionContext) extends ApiServices with MockUserInfoDirectives

  // increase the route timeout slightly for this test as the "large submission" tests sometimes
  // bump up against the default 5 second timeout.
  implicit override val routeTestTimeout = RouteTestTimeout(30.seconds)

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

  val largeSampleTestData = new LargeSampleSetTestData()

  def withLargeSubmissionApiServices[T](testCode: TestApiService => T): T = {
    withCustomTestDatabase(largeSampleTestData) { dataSource: SlickDataSource =>
      withApiServices(dataSource) { services =>
        try {
          // Simulate a large submission in the mock Cromwell server by making it return
          // numSamples workflows for submission requests.
          mockServer.reset
          mockServer.startServer(largeSampleTestData.numSamples)
          testCode(services)
        } finally {
          mockServer.reset
          mockServer.startServer()
        }
      }
    }
  }

  def withWorkflowSubmissionActor[T](services: TestApiService)(testCode: ActorRef => T): T = {
    val actor = system.actorOf(WorkflowSubmissionActor.props(
      slickDataSource,
      services.methodRepoDAO,
      services.gcsDAO,
      MockShardedExecutionServiceCluster.fromDAO(new HttpExecutionServiceDAO(mockServer.mockServerBaseUrl, mockServer.defaultWorkflowSubmissionTimeout, workbenchMetricBaseName), slickDataSource),
      10,
      services.gcsDAO.getPreparedMockGoogleCredential(),
      50 milliseconds,
      100 milliseconds,
      100000,
      100000,
      None,
      "test"))

    try {
      testCode(actor)
    } finally {
      // stops actor and waits for it to shut down
      val testProbe = TestProbe()
      testProbe watch actor
      actor ! PoisonPill
      testProbe.expectTerminated(actor, 5 seconds)
    }
  }

  "SubmissionApi" should "return 404 Not Found when creating a submission using a MethodConfiguration that doesn't exist in the workspace" in withTestDataApiServices { services =>
    Post(s"${testData.wsName.path}/submissions", httpJson(SubmissionRequest("dsde","not there","Pattern","pattern1", None, false))) ~>
      sealRoute(services.submissionRoutes) ~>
      check { assertResult(StatusCodes.NotFound) {status} }
  }

  it should "return 404 Not Found when creating a submission using an Entity that doesn't exist in the workspace" in withTestDataApiServices { services =>
    val mcName = MethodConfigurationName("three_step","dsde", testData.wsName)
    val methodConf = MethodConfiguration(mcName.namespace, mcName.name,"Pattern", Map.empty, Map("three_step.cgrep.pattern"->AttributeString("this.input_expression")), Map.empty, MethodRepoMethod("dsde","three_step",1))
    Post(s"${testData.wsName.path}/methodconfigs", httpJson(methodConf)) ~>
      sealRoute(services.methodConfigRoutes) ~>
      check { assertResult(StatusCodes.Created) {status} }
    Post(s"${testData.wsName.path}/submissions", httpJson(SubmissionRequest(mcName.namespace, mcName.name,"Pattern","pattern1", None, false))) ~>
      sealRoute(services.submissionRoutes) ~>
      check { assertResult(StatusCodes.NotFound) {status} }
  }

  private def createAndMonitorSubmission(wsName: WorkspaceName, methodConf: MethodConfiguration,
                                         submissionEntity: Entity, submissionExpression: Option[String],
                                         services: TestApiService, workflowFailureMode: Option[String] = None): SubmissionStatusResponse = {

    Get(s"${wsName.path}/methodconfigs/${methodConf.namespace}/${methodConf.name}") ~>
      sealRoute(services.methodConfigRoutes) ~>
      check {
        if (status == StatusCodes.NotFound) {
          Post(s"${wsName.path}/methodconfigs", httpJson(methodConf)) ~>
            sealRoute(services.methodConfigRoutes) ~>
            check {
              assertResult(StatusCodes.Created) {
                status
              }
            }
        } else {
          assertResult(StatusCodes.OK) {
            status
          }
        }
      }

    withStatsD {
      val submissionRq = SubmissionRequest(methodConf.namespace, methodConf.name, submissionEntity.entityType, submissionEntity.name, submissionExpression, false, workflowFailureMode)
      Post(s"${wsName.path}/submissions", httpJson(submissionRq)) ~>
        sealRoute(services.submissionRoutes) ~>
        check {
          assertResult(StatusCodes.Created, responseAs[String]) {
            status
          }
          val submission = responseAs[SubmissionReport]
          Get(s"${wsName.path}/submissions/${submission.submissionId}") ~>
            sealRoute(services.submissionRoutes) ~>
            check {
              assertResult(StatusCodes.OK) {
                status
              }
              responseAs[SubmissionStatusResponse]
            }
        }
    } { capturedMetrics =>
      capturedMetrics should contain (expectedSubmissionStatusMetric(wsName, SubmissionStatuses.Submitted, 1))
    }
  }

  private def abortSubmission(services: TestApiService, wsName: WorkspaceName, submissionId: String, validate: Boolean = true): Unit = {
    import driver.api._
    implicit val patienceConfig = PatienceConfig(timeout = scaled(Span(30, Seconds)))

    withStatsD {
      // Abort the submission
      Delete(s"${wsName.path}/submissions/${submissionId}") ~> services.sealedInstrumentedRoutes ~>
        check {
          assertResult(StatusCodes.NoContent) {
            status
          }
        }

      // The submission should be aborting
      assertResult(SubmissionStatuses.Aborting.toString) {
        runAndWait(submissionQuery.findById(UUID.fromString(submissionId)).result.head).status
      }
    } { capturedMetrics =>
      capturedMetrics should contain (expectedSubmissionStatusMetric(wsName, SubmissionStatuses.Aborting, 1))

      val wsPathForRequestMetrics = s"workspaces.redacted.redacted"
      val expected = expectedHttpRequestMetrics("delete", s"$wsPathForRequestMetrics.submissions.redacted", StatusCodes.NoContent.intValue, 1)
      assertSubsetOf(expected, capturedMetrics)
    }

    // The workflow and submission should be aborted once the SubmissionMonitor runs
    if (validate) {
      eventually {
        assertResult(Set(SubmissionStatuses.Aborted.toString)) {
          runAndWait(workflowQuery.listWorkflowRecsForSubmission(UUID.fromString(submissionId))).map(_.status).toSet
        }
        assertResult(SubmissionStatuses.Aborted.toString) {
          runAndWait(submissionQuery.findById(UUID.fromString(submissionId)).result.head).status
        }
      }
    }
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

    Get(s"${testData.wsName.path}") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertWorkspaceModifiedDate(status, responseAs[WorkspaceListResponse].workspace)
      }

  }

  it should "return 201 Created when creating submission with a workflow_failure_mode" in withTestDataApiServices { services =>
    val wsName = testData.wsName
    val mcName = MethodConfigurationName("no_input", "dsde", wsName)
    val methodConf = MethodConfiguration(mcName.namespace, mcName.name, "Sample", Map.empty, Map.empty, Map.empty, MethodRepoMethod("dsde", "no_input", 1))

    val submission = createAndMonitorSubmission(wsName, methodConf, testData.sample1, None, services, Some(WorkflowFailureModes.ContinueWhilePossible.toString))

    assertResult(1) {
      submission.workflows.size
    }
  }

  it should "return workflow_failure_mode in submission GET calls" in withTestDataApiServices { services =>
    val wsName = testData.wsName
    val mcName = MethodConfigurationName("no_input", "dsde", wsName)
    val methodConf = MethodConfiguration(mcName.namespace, mcName.name, "Sample", Map.empty, Map.empty, Map.empty, MethodRepoMethod("dsde", "no_input", 1))

    // calls GET ../submissions/<sub-id> and returns the SubmissionStatusResponse
    val submissionResponseWithFailureMode = createAndMonitorSubmission(wsName, methodConf, testData.sample1, None, services, Some(WorkflowFailureModes.ContinueWhilePossible.toString))

    // The SubmissionStatusResponse should contain the workflow failure mode
    submissionResponseWithFailureMode.workflows.size should equal (1)
    submissionResponseWithFailureMode.workflowFailureMode should equal (Some(WorkflowFailureModes.ContinueWhilePossible))

    // calls GET ../submissions/<sub-id> and returns the SubmissionStatusResponse
    val submissionResponseWithoutFailureMode = createAndMonitorSubmission(wsName, methodConf, testData.sample1, None, services)

    // The SubmissionStatusResponse should not contain a workflow failure mode
    submissionResponseWithoutFailureMode.workflows.size should equal (1)
    submissionResponseWithoutFailureMode.workflowFailureMode should equal (None)

    def getSubmission(id: String) = runAndWait(submissionQuery.loadSubmission(UUID.fromString(id))).get
    // Build expected SubmissionListResponse objects
    val submissionListResponseWithFailureMode = new SubmissionListResponse(getSubmission(submissionResponseWithFailureMode.submissionId), testData.userOwner, Map("Queued" -> 1))
    val submissionListResponseWithoutFailureMode = new SubmissionListResponse(getSubmission(submissionResponseWithoutFailureMode.submissionId), testData.userOwner, Map("Queued" -> 1))

    // Sanity check the workflow failure modes in the expected SubmissionListResponse objects
    submissionListResponseWithFailureMode.workflowFailureMode should equal (Some(WorkflowFailureModes.ContinueWhilePossible))
    submissionListResponseWithoutFailureMode.workflowFailureMode should equal (None)

    // Listing submissions should return the correct workflow failure mode
    Get(s"${testData.wsName.path}/submissions") ~>
      sealRoute(services.submissionRoutes) ~>
      check {
        assertResult(StatusCodes.OK, response.entity.asString) {
          status
        }
        responseAs[Seq[SubmissionListResponse]] should contain allOf (
          submissionListResponseWithFailureMode,
          submissionListResponseWithoutFailureMode)
      }
  }

  it should "abort a submission" in withTestDataApiServices { services =>
    val wsName = testData.wsName
    val mcName = MethodConfigurationName("no_input", "dsde", wsName)
    val methodConf = MethodConfiguration(mcName.namespace, mcName.name, "Sample", Map.empty, Map.empty, Map.empty, MethodRepoMethod("dsde", "no_input", 1))

    // create a submission
    val submission = createAndMonitorSubmission(wsName, methodConf, testData.sample1, None, services, Some(WorkflowFailureModes.ContinueWhilePossible.toString))

    assertResult(1) {
      submission.workflows.size
    }

    abortSubmission(services, wsName, submission.submissionId)
  }

  val numSamples = 10000

  it should "create and abort a large submission" in withLargeSubmissionApiServices { services =>
    val wsName = largeSampleTestData.wsName
    val mcName = MethodConfigurationName("no_input", "dsde", wsName)
    val methodConf = MethodConfiguration(mcName.namespace, mcName.name, "Sample", Map.empty, Map.empty, Map.empty, MethodRepoMethod("dsde", "no_input", 1))

    // create a submission
    val submission = createAndMonitorSubmission(wsName, methodConf, largeSampleTestData.sampleSet, Option("this.hasSamples"), services)

    assertResult(numSamples) {
      submission.workflows.size
    }

    abortSubmission(services, wsName, submission.submissionId)
  }

  // To test for deadlocks one should run this test, log in to MySQL, run:
  //
  // mysql> show engine innodb status;
  //
  // and look for a section called "LAST DETECTED DEADLOCK".
  it should "not deadlock when aborting a large submission" in withLargeSubmissionApiServices { services =>
    withWorkflowSubmissionActor(services) { _ =>
      val wsName = largeSampleTestData.wsName
      val mcName = MethodConfigurationName("no_input", "dsde", wsName)
      val methodConf = MethodConfiguration(mcName.namespace, mcName.name, "Sample", Map.empty, Map.empty, Map.empty, MethodRepoMethod("dsde", "no_input", 1))
      val numIterations = 20

      (1 to numIterations).map { i =>
        logger.info(s"deadlock test: iteration $i/$numIterations")
        val submission = createAndMonitorSubmission(wsName, methodConf, largeSampleTestData.sampleSet, Option("this.hasSamples"), services)

        assertResult(numSamples) {
          submission.workflows.size
        }

        abortSubmission(services, wsName, submission.submissionId, false)
      }
    }
  }

  it should "return 400 Bad Request when passing an unknown workflow_failure_mode" in withTestDataApiServices { services =>
    val wsName = testData.wsName
    val mcName = MethodConfigurationName("no_input", "dsde", wsName)
    val methodConf = MethodConfiguration(mcName.namespace, mcName.name, "Sample", Map.empty, Map.empty, Map.empty, MethodRepoMethod("dsde", "no_input", 1))

    val submissionRq = SubmissionRequest(methodConf.namespace, methodConf.name, testData.sample1.entityType, testData.sample1.name, None, false, Some(WorkflowFailureModes.ContinueWhilePossible.toString))
    val jsonStr = submissionRq.toJson.toString.replace("ContinueWhilePossible", "Bogus")

    Post(s"${wsName.path}/methodconfigs", httpJson(methodConf)) ~>
      sealRoute(services.methodConfigRoutes) ~>
      check {
        assertResult(StatusCodes.Created) {
          status
        }
      }

    Post(s"${wsName.path}/submissions", httpJsonStr(jsonStr)) ~>
      sealRoute(services.submissionRoutes) ~>
      check {
        assertResult(StatusCodes.BadRequest) {
          status
        }
      }
  }

  val attributeList = AttributeValueList(Seq(AttributeString("a"), AttributeString("b"), AttributeBoolean(true)))
  val z1 = Entity("z1", "Sample", Map(AttributeName.withDefaultNS("foo") -> AttributeString("x"), AttributeName.withDefaultNS("bar") -> AttributeNumber(3), AttributeName.withDefaultNS("splat") -> attributeList))
  val workspace2Name = new WorkspaceName(testData.wsName.namespace + "2", testData.wsName.name + "2")
  val workspace2Request = WorkspaceRequest(
    workspace2Name.namespace,
    workspace2Name.name,
    Map.empty
  )

  it should "return 200 on getting a submission" in withTestDataApiServices { services =>
    Get(s"${testData.wsName.path}/submissions/${testData.submission1.submissionId}") ~>
      sealRoute(services.submissionRoutes) ~>
      check {
        assertResult(StatusCodes.OK, response.entity.asString) {status}
        assertResult(new SubmissionStatusResponse(testData.submission1, testData.userOwner)) {responseAs[SubmissionStatusResponse]}
      }
  }

  it should "return 404 on getting a nonexistent submission" in withTestDataApiServices { services =>
    Get(s"${testData.wsName.path}/submissions/unrealSubmission42") ~>
      sealRoute(services.submissionRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound) {status}
      }
    Get(s"${testData.wsName.path}/submissions/${UUID.randomUUID}") ~>
      sealRoute(services.submissionRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound) {status}
      }
  }

  it should "return 200 when listing submissions" in withTestDataApiServices { services =>
    Get(s"${testData.wsName.path}/submissions") ~>
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
    withStatsD {
      Get(s"${testData.wsName.path}/submissionsCount") ~> services.sealedInstrumentedRoutes ~>
        check {
          assertResult(StatusCodes.OK) {
            status
          }
          assertResult(Map("Submitted" -> 6)) {
            responseAs[Map[String, Int]]
          }
        }
    } { capturedMetrics =>
      val wsPathForRequestMetrics = s"workspaces.redacted.redacted"
      val expected = expectedHttpRequestMetrics("get", s"$wsPathForRequestMetrics.submissionsCount", StatusCodes.OK.intValue, 1)
      assertSubsetOf(expected, capturedMetrics)
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

    val existingSubmittedWorkflowCount = 16
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
        val entities = for (i <- 1 to count) yield Entity(i.toString, status.toString, Map.empty)
        runAndWait(entityQuery.save(context, entities))
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

    runAndWait(rawlsUserQuery.createUser(otherUser1))
    runAndWait(rawlsUserQuery.createUser(otherUser2))

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

    runAndWait(rawlsUserQuery.createUser(otherUser1))
    runAndWait(rawlsUserQuery.createUser(otherUser2))

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

    val testSubmission = Submission(UUID.randomUUID.toString, testDate, testData.userOwner, testData.methodConfig.namespace, testData.methodConfig.name, testData.indiv1.toReference, workflows, SubmissionStatuses.Done, false)

    runAndWait(submissionQuery.create(SlickWorkspaceContext(testData.workspace), testSubmission))
    runAndWait(workflowQuery.findWorkflowByExternalIdAndSubmissionId(workflowId, UUID.fromString(testSubmission.submissionId)).map(_.executionServiceKey).update(Option("unittestdefault")))

    withStatsD {
      Get(s"${testData.wsName.path}/submissions/${testSubmission.submissionId}/workflows/${testSubmission.workflows.head.workflowId.get}/outputs") ~>
        services.sealedInstrumentedRoutes
        check {
          assertResult(StatusCodes.OK, response.entity.asString) {
            status
          }
          val expectedOutputs = WorkflowOutputs(workflowId, Map("aggregate_data_workflow.aggregate_data" -> TaskOutput(None, Option(Map("aggregate_data_workflow.aggregate_data.output_array" -> Left(AttributeValueRawJson(JsArray(Vector(
            JsArray(Vector(JsString("foo"), JsString("bar"))),
            JsArray(Vector(JsString("baz"), JsString("qux"))))))))))))
          assertResult(expectedOutputs) {
            responseAs[WorkflowOutputs]
          }
        }
    } { capturedMetrics =>
      capturedMetrics should contain allElementsOf (expectedHttpRequestMetrics("get",
          s"workspaces.redacted.redacted.submissions.redacted.workflows.redacted.outputs", 200, 1))
    }
  }

  it should "return the cromwell version" in withTestDataApiServices { services =>
    Get("/version/executionEngine") ~>
      sealRoute(services.submissionRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {status}
        responseAs[ExecutionServiceVersion]
      }
  }

  class LargeSampleSetTestData extends TestData {
    val userProjectOwner = RawlsUser(UserInfo(RawlsUserEmail("project-owner-access"), OAuth2BearerToken("token"), 123, RawlsUserSubjectId("123456789876543210101")))
    val userOwner = RawlsUser(UserInfo(RawlsUserEmail("owner-access"), OAuth2BearerToken("token"), 123, RawlsUserSubjectId("123456789876543212345")))
    val wsName = WorkspaceName("myNamespace", "myWorkspaceToTestLargeSubmissions")
    val ownerGroup = makeRawlsGroup(s"${wsName.namespace}-${wsName.name}-OWNER", Set(userOwner))
    val writerGroup = makeRawlsGroup(s"${wsName.namespace}-${wsName.name}-WRITER", Set())
    val readerGroup = makeRawlsGroup(s"${wsName.namespace}-${wsName.name}-READER", Set())
    val billingProject = RawlsBillingProject(RawlsBillingProjectName(wsName.namespace), generateBillingGroups(RawlsBillingProjectName(wsName.namespace), Map(ProjectRoles.Owner -> Set(userProjectOwner, userOwner), ProjectRoles.User -> Set.empty), Map.empty), "testBucketUrl", CreationStatuses.Ready, None, None)

    val workspace = Workspace(wsName.namespace, wsName.name, Set.empty, UUID.randomUUID().toString, "aBucket", currentTime(), currentTime(), "testUser", Map.empty,
      Map(WorkspaceAccessLevels.Owner -> ownerGroup, WorkspaceAccessLevels.Write -> writerGroup, WorkspaceAccessLevels.Read -> readerGroup),
      Map(WorkspaceAccessLevels.Owner -> ownerGroup, WorkspaceAccessLevels.Write -> writerGroup, WorkspaceAccessLevels.Read -> readerGroup))

    val numSamples = 10000

    val lotsOfSamples = (1 to numSamples).map(n => Entity(s"lotsOfSamples$n", s"Sample", Map.empty))
    val sampleSet = Entity("largeSset", "SampleSet", Map(AttributeName.withDefaultNS("hasSamples") -> AttributeEntityReferenceList(lotsOfSamples.map(_.toReference))))

    override def save(): ReadWriteAction[Unit] = {
      import driver.api._

      DBIO.seq(
        rawlsUserQuery.createUser(userProjectOwner),
        rawlsUserQuery.createUser(userOwner),
        rawlsGroupQuery.save(ownerGroup),
        rawlsGroupQuery.save(writerGroup),
        rawlsGroupQuery.save(readerGroup),
        DBIO.sequence(billingProject.groups.values.map(rawlsGroupQuery.save).toSeq),
        rawlsBillingProjectQuery.create(billingProject),
        workspaceQuery.save(workspace),
        entityQuery.save(SlickWorkspaceContext(workspace), lotsOfSamples :+ sampleSet)
      )
    }
  }
}
