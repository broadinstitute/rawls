package org.broadinstitute.dsde.rawls.jobexec

import java.util.UUID

import akka.actor.{ActorRef, ActorSystem, PoisonPill}
import akka.testkit.TestKit
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.dataaccess.slick.{TestData, TestDriverComponent}
import org.broadinstitute.dsde.rawls.genomics.GenomicsService
import org.broadinstitute.dsde.rawls.google.MockGooglePubSubDAO
import org.broadinstitute.dsde.rawls.metrics.StatsDTestUtils
import org.broadinstitute.dsde.rawls.mock.{MockSamDAO, RemoteServicesMockServer}
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.user.UserService
import org.broadinstitute.dsde.rawls.util.MockitoTestUtils
import org.broadinstitute.dsde.rawls.webservice.PerRequest.RequestComplete
import org.broadinstitute.dsde.rawls.workspace.{WorkspaceService, WorkspaceServiceConfig}
import org.broadinstitute.dsde.rawls.{RawlsException, RawlsExceptionWithErrorReport}
import org.broadinstitute.dsde.workbench.google.mock.MockGoogleBigQueryDAO
import org.scalatest.concurrent.Eventually
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}
import akka.http.scaladsl.model.{StatusCode, StatusCodes}
import akka.stream.ActorMaterializer
import com.typesafe.config.ConfigFactory
import org.broadinstitute.dsde.rawls.config.MethodRepoConfig
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.GcsBucketName
import spray.json._

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Try

/**
 * Created with IntelliJ IDEA.
 * User: hussein
 * Date: 07/02/2015
 * Time: 11:06
 */
class SubmissionSpec(_system: ActorSystem) extends TestKit(_system) with FlatSpecLike with Matchers with TestDriverComponent with BeforeAndAfterAll with Eventually with MockitoTestUtils with StatsDTestUtils {
  import driver.api._
  
  def this() = this(ActorSystem("SubmissionSpec"))
  implicit val materializer = ActorMaterializer()

  val testDbName = "SubmissionSpec"
  val submissionSupervisorActorName = "test-subspec-submission-supervisor"

  val mockServer = RemoteServicesMockServer()

  val bigQueryDAO = new MockGoogleBigQueryDAO
  val mockSubmissionCostService = new MockSubmissionCostService("test", "test", bigQueryDAO)

  override def beforeAll(): Unit = {
    super.beforeAll()
    mockServer.startServer()
  }

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    mockServer.stopServer
    super.afterAll()
  }

  var subMissingWorkflow = UUID.randomUUID().toString
  var subMalformedWorkflow = UUID.randomUUID().toString
  var subGoodWorkflow = UUID.randomUUID().toString
  var subTerminalWorkflow = UUID.randomUUID().toString
  var subOneMissingWorkflow = UUID.randomUUID().toString
  var subTwoGoodWorkflows = UUID.randomUUID().toString
  var subCromwellBadWorkflows = UUID.randomUUID().toString

  val subTestData = new SubmissionTestData()

  class SubmissionTestData() extends TestData {
    val wsName = WorkspaceName("myNamespacexxx", "myWorkspace")
    val user = RawlsUser(userInfo)
    val ownerGroup = makeRawlsGroup("workspaceOwnerGroup", Set(user))
    val workspace = Workspace(wsName.namespace, wsName.name, UUID.randomUUID().toString, "aBucket", Some("workflow-collection"), currentTime(), currentTime(), "testUser", Map.empty)

    val sample1 = Entity("sample1", "Sample",
      Map(AttributeName.withDefaultNS("type") -> AttributeString("normal")))
    val sample2 = Entity("sample2", "Sample",
      Map(AttributeName.withDefaultNS("type") -> AttributeString("normal")))

    val refreshToken = UUID.randomUUID.toString

    val existingWorkflowId = Option("69d1d92f-3895-4a7b-880a-82535e9a096e")
    val nonExistingWorkflowId = Option("45def17d-40c2-44cc-89bf-9e77bc2c9999")
    val alreadyTerminatedWorkflowId = Option("45def17d-40c2-44cc-89bf-9e77bc2c8778")
    val badLogsAndMetadataWorkflowId = Option("29b2e816-ecaf-11e6-b006-92361f002671")

    val submissionTestAbortMissingWorkflow = Submission(subMissingWorkflow,testDate, WorkbenchEmail(testData.userOwner.userEmail.value), "std","someMethod",Some(sample1.toReference),
      Seq(Workflow(nonExistingWorkflowId,WorkflowStatuses.Submitted,testDate,Some(sample1.toReference), testData.inputResolutions)), SubmissionStatuses.Submitted, false)

    val submissionTestAbortMalformedWorkflow = Submission(subMalformedWorkflow,testDate, WorkbenchEmail(testData.userOwner.userEmail.value), "std","someMethod",Some(sample1.toReference),
      Seq(Workflow(Option("malformed_workflow"),WorkflowStatuses.Submitted,testDate,Some(sample1.toReference), testData.inputResolutions)), SubmissionStatuses.Submitted, false)

    val submissionTestAbortGoodWorkflow = Submission(subGoodWorkflow,testDate, WorkbenchEmail(testData.userOwner.userEmail.value), "std","someMethod",Some(sample1.toReference),
      Seq(Workflow(existingWorkflowId,WorkflowStatuses.Submitted,testDate,Some(sample1.toReference), testData.inputResolutions)), SubmissionStatuses.Submitted, false)

    val submissionTestAbortTerminalWorkflow = Submission(subTerminalWorkflow,testDate, WorkbenchEmail(testData.userOwner.userEmail.value), "std","someMethod",Some(sample1.toReference),
      Seq(Workflow(alreadyTerminatedWorkflowId,WorkflowStatuses.Submitted,testDate,Some(sample1.toReference), testData.inputResolutions)), SubmissionStatuses.Submitted, false)

    val submissionTestAbortOneMissingWorkflow = Submission(subOneMissingWorkflow,testDate, WorkbenchEmail(testData.userOwner.userEmail.value), "std","someMethod",Some(sample1.toReference),
      Seq(
        Workflow(existingWorkflowId,WorkflowStatuses.Submitted,testDate,Some(sample1.toReference), testData.inputResolutions),
        Workflow(nonExistingWorkflowId,WorkflowStatuses.Submitted,testDate,Some(sample2.toReference), testData.inputResolutions)), SubmissionStatuses.Submitted, false)

    val submissionTestAbortTwoGoodWorkflows = Submission(subTwoGoodWorkflows,testDate, WorkbenchEmail(testData.userOwner.userEmail.value), "std","someMethod",Some(sample1.toReference),
      Seq(
        Workflow(existingWorkflowId,WorkflowStatuses.Submitted,testDate,Some(sample1.toReference), testData.inputResolutions),
        Workflow(alreadyTerminatedWorkflowId,WorkflowStatuses.Submitted,testDate,Some(sample2.toReference), testData.inputResolutions)), SubmissionStatuses.Submitted, false)

    val submissionTestCromwellBadWorkflows = Submission(subCromwellBadWorkflows, testDate, WorkbenchEmail(testData.userOwner.userEmail.value), "std","someMethod",Some(sample1.toReference),
      Seq(
        Workflow(badLogsAndMetadataWorkflowId,WorkflowStatuses.Submitted,testDate,Some(sample1.toReference), testData.inputResolutions)), SubmissionStatuses.Submitted, false)

    val extantWorkflowOutputs = WorkflowOutputs( existingWorkflowId.get,
      Map(
        "wf.y" -> TaskOutput(
          Some(Seq(
            ExecutionServiceCallLogs(
              stdout = "gs://cromwell-dev/cromwell-executions/wf/this_workflow_exists/call-y/job.stdout-1.txt",
              stderr = "gs://cromwell-dev/cromwell-executions/wf/this_workflow_exists/call-y/job.stderr-1.txt",
              backendLogs = Some(Map(
                "log" -> "gs://cromwell-dev/cromwell-executions/wf/this_workflow_exists/call-y/jes.log",
                "stdout" -> "gs://cromwell-dev/cromwell-executions/wf/this_workflow_exists/call-y/jes-stdout.log",
                "stderr" -> "gs://cromwell-dev/cromwell-executions/wf/this_workflow_exists/call-y/jes-stderr.log"
              ))
            ),
            ExecutionServiceCallLogs(
              stdout = "gs://cromwell-dev/cromwell-executions/wf/this_workflow_exists/call-y/job.stdout-2.txt",
              stderr = "gs://cromwell-dev/cromwell-executions/wf/this_workflow_exists/call-y/job.stderr-2.txt")
          )),
          Some(Map("wf.y.six" -> Left(AttributeNumber(4))))),
        "wf.x" -> TaskOutput(
          Some(Seq(ExecutionServiceCallLogs(
            stdout = "gs://cromwell-dev/cromwell-executions/wf/this_workflow_exists/call-x/job.stdout.txt",
            stderr = "gs://cromwell-dev/cromwell-executions/wf/this_workflow_exists/call-x/job.stderr.txt"))),
          Some(Map(
            "wf.x.four" -> Left(AttributeNumber(4)),
            "wf.x.five" -> Left(AttributeNumber(4)))))))

    override def save() = {
      DBIO.seq(
        workspaceQuery.save(workspace),
        withWorkspaceContext(workspace) { context =>
          DBIO.seq(
            entityQuery.save(context, sample1),
            entityQuery.save(context, sample2),
            methodConfigurationQuery.create(context, MethodConfiguration("std", "someMethod", Some("Sample"), Map.empty, Map.empty, Map.empty, AgoraMethod("std", "someMethod", 1))),
            submissionQuery.create(context, submissionTestAbortMissingWorkflow),
            submissionQuery.create(context, submissionTestAbortMalformedWorkflow),
            submissionQuery.create(context, submissionTestAbortGoodWorkflow),
            submissionQuery.create(context, submissionTestAbortTerminalWorkflow),
            submissionQuery.create(context, submissionTestAbortOneMissingWorkflow),
            submissionQuery.create(context, submissionTestAbortTwoGoodWorkflows),
            submissionQuery.create(context, submissionTestCromwellBadWorkflows),
            // update exec key for all test data workflows that have been started.
            updateWorkflowExecutionServiceKey("unittestdefault")
          )
        }
      )
    }
  }

  def withDataAndService[T](
      testCode: WorkspaceService => T,
      withDataOp: (SlickDataSource => T) => T,
      executionServiceDAO: ExecutionServiceDAO = new HttpExecutionServiceDAO(mockServer.mockServerBaseUrl, workbenchMetricBaseName) ): T = {

    withDataOp { dataSource =>
      val execServiceCluster: ExecutionServiceCluster = MockShardedExecutionServiceCluster.fromDAO(executionServiceDAO, dataSource)

      val config = SubmissionMonitorConfig(250 milliseconds, true)
      val gcsDAO: MockGoogleServicesDAO = new MockGoogleServicesDAO("test")
      val samDAO = new MockSamDAO(dataSource)
      val gpsDAO = new MockGooglePubSubDAO
      val submissionSupervisor = system.actorOf(SubmissionSupervisor.props(
        execServiceCluster,
        slickDataSource,
        samDAO,
        gcsDAO,
        gcsDAO.getBucketServiceAccountCredential,
        config,
        workbenchMetricBaseName = workbenchMetricBaseName
      ).withDispatcher("submission-monitor-dispatcher"), submissionSupervisorActorName)

      gcsDAO.storeToken(userInfo, subTestData.refreshToken)

      val testConf = ConfigFactory.load()

      val notificationDAO = new PubSubNotificationDAO(gpsDAO, "test-notification-topic")

      val userServiceConstructor = UserService.constructor(
        slickDataSource,
        gcsDAO,
        notificationDAO,
        samDAO,
        Seq("bigquery.jobUser"),
        "requesterPaysRole",
        testConf.getConfig("gcs.deploymentManager"),
        ProjectTemplate.from(testConf.getConfig("gcs.projectTemplate"), "requesterPaysRole")
      )_

      val genomicsServiceConstructor = GenomicsService.constructor(
        slickDataSource,
        gcsDAO
      )_

      val execServiceBatchSize = 3
      val maxActiveWorkflowsTotal = 10
      val maxActiveWorkflowsPerUser = 2
      val workspaceServiceConfig = WorkspaceServiceConfig(
        true,
        "fc-"
      )
      val workspaceServiceConstructor = WorkspaceService.constructor(
        dataSource,
        new HttpMethodRepoDAO(
          MethodRepoConfig[Agora.type](mockServer.mockServerBaseUrl, ""),
          MethodRepoConfig[Dockstore.type](mockServer.mockServerBaseUrl, ""),
          workbenchMetricBaseName = workbenchMetricBaseName),
        execServiceCluster,
        execServiceBatchSize,
        gcsDAO,
        samDAO,
        notificationDAO,
        userServiceConstructor,
        genomicsServiceConstructor,
        maxActiveWorkflowsTotal,
        maxActiveWorkflowsPerUser,
        workbenchMetricBaseName,
        mockSubmissionCostService,
        workspaceServiceConfig
      )_
      lazy val workspaceService: WorkspaceService = workspaceServiceConstructor(userInfo)
      try {
        testCode(workspaceService)
      }
      finally {
        // for failed tests we also need to poison pill
        submissionSupervisor ! PoisonPill
      }
    }
  }

  def withWorkspaceService[T](testCode: WorkspaceService => T): T = {
    withDataAndService(testCode, withDefaultTestDatabase[T])
  }

  def withWorkspaceServiceMockExecution[T](testCode: (MockExecutionServiceDAO) => (WorkspaceService) => T): T = {
    val execSvcDAO = new MockExecutionServiceDAO()
    withDataAndService(testCode(execSvcDAO), withDefaultTestDatabase[T], execSvcDAO)
  }
  def withWorkspaceServiceMockTimeoutExecution[T](testCode: (MockExecutionServiceDAO) => (WorkspaceService) => T): T = {
    val execSvcDAO = new MockExecutionServiceDAO(true)
    withDataAndService(testCode(execSvcDAO), withDefaultTestDatabase[T], execSvcDAO)
  }

  def withSubmissionTestWorkspaceService[T](testCode: WorkspaceService => T): T = {
    withDataAndService(testCode, withCustomTestDatabase[T](new SubmissionTestData))
  }

  private def checkSubmissionStatus(workspaceService: WorkspaceService, submissionId: String, workspaceName: WorkspaceName = testData.wsName): SubmissionStatusResponse = {
    val submissionStatusRqComplete = workspaceService.getSubmissionStatus(workspaceName, submissionId)

    Await.result(submissionStatusRqComplete, Duration.Inf) match {
      case RequestComplete((submissionStatus: StatusCode, submissionData: Any)) => {
        assertResult(StatusCodes.OK) { submissionStatus }
        val submissionStatusResponse = submissionData.asInstanceOf[SubmissionStatusResponse]
        assertResult(submissionId) { submissionStatusResponse.submissionId }
        submissionStatusResponse
      }
      case _ => fail("Unable to get submission status")
    }
  }

  "Submission requests" should "400 when given an unparseable entity expression" in withWorkspaceService { workspaceService =>
    val submissionRq = SubmissionRequest("dsde", "GoodMethodConfig", Some("Individual"), Some("indiv1"), Some("this.is."), false)
    val rqComplete = intercept[RawlsExceptionWithErrorReport] {
      Await.result(workspaceService.createSubmission( testData.wsName, submissionRq ), Duration.Inf).asInstanceOf[RequestComplete[ErrorReport]]
    }
    assertResult(StatusCodes.BadRequest) {
      rqComplete.errorReport.statusCode.get
    }
  }

  private def waitForSubmissionActor(submissionId: String) = {
    var subActor: Option[ActorRef] = None
    awaitCond({
      val tr = Try(Await.result(system.actorSelection("/user/" + submissionSupervisorActorName + "/" + submissionId).resolveOne(100 milliseconds), Duration.Inf))
      subActor = tr.toOption
      tr.isSuccess
    }, 1 second)
    subActor.get
  }

  it should "return a successful Submission and spawn a submission monitor actor when given an entity expression that evaluates to a single entity" in withWorkspaceServiceMockExecution { mockExecSvc => workspaceService =>
    val submissionRq = SubmissionRequest("dsde", "GoodMethodConfig", Some("Pair"), Some("pair1"), Some("this.case"), false)
    val rqComplete = Await.result(workspaceService.createSubmission(testData.wsName, submissionRq), Duration.Inf).asInstanceOf[RequestComplete[(StatusCode, SubmissionReport)]]
    val (status, newSubmissionReport) = rqComplete.response
    assertResult(StatusCodes.Created) {
      status
    }

    val monitorActor = waitForSubmissionActor(newSubmissionReport.submissionId)
    assert(monitorActor != None) //not really necessary, failing to find the actor above will throw an exception and thus fail this test

    assert(newSubmissionReport.workflows.size == 1)

    checkSubmissionStatus(workspaceService, newSubmissionReport.submissionId)
  }

  it should "continue to monitor a Submission on a deleted entity" in withWorkspaceServiceMockExecution { mockExecSvc => workspaceService =>
    val submissionRq = SubmissionRequest("dsde", "GoodMethodConfig", Some("Pair"), Some("pair1"), Some("this.case"), useCallCache = false)
    val rqComplete = Await.result(workspaceService.createSubmission(testData.wsName, submissionRq), Duration.Inf).asInstanceOf[RequestComplete[(StatusCode, SubmissionReport)]]
    val (status, newSubmissionReport) = rqComplete.response
    assertResult(StatusCodes.Created) {
      status
    }

    runAndWait(entityQuery.hide(SlickWorkspaceContext(testData.workspace), Seq(testData.pair1.toReference)))

    val monitorActor = waitForSubmissionActor(newSubmissionReport.submissionId)
    assert(monitorActor != None) //not really necessary, failing to find the actor above will throw an exception and thus fail this test

    assert(newSubmissionReport.workflows.size == 1)

    checkSubmissionStatus(workspaceService, newSubmissionReport.submissionId)
  }

  it should "fail to submit when given an entity expression that evaluates to a deleted entity" in withWorkspaceServiceMockExecution { mockExecSvc => workspaceService =>

    runAndWait(entityQuery.hide(SlickWorkspaceContext(testData.workspace), Seq(testData.pair1.toReference)))

    val submissionRq = SubmissionRequest("dsde", "GoodMethodConfig", Some("Pair"), Some("pair1"), Some("this.case"), useCallCache = false)
    intercept[RawlsException] {
      Await.result(workspaceService.createSubmission(testData.wsName, submissionRq), Duration.Inf)
    }
  }

  it should "return a successful Submission when given an entity expression that evaluates to a set of entities" in withWorkspaceService { workspaceService =>
    val sset = Entity("testset6", "SampleSet",
      Map(AttributeName.withDefaultNS("samples") -> AttributeEntityReferenceList( Seq(
        AttributeEntityReference("Sample", "sample1"),
        AttributeEntityReference("Sample", "sample2"),
        AttributeEntityReference("Sample", "sample3"),
        AttributeEntityReference("Sample", "sample4"),
        AttributeEntityReference("Sample", "sample5"),
        AttributeEntityReference("Sample", "sample6")))))

    runAndWait(entityQuery.save(SlickWorkspaceContext(testData.workspace), sset))

    val submissionRq = SubmissionRequest("dsde", "GoodMethodConfig", Some(sset.entityType), Some(sset.name), Some("this.samples"), false)
    val rqComplete = Await.result(workspaceService.createSubmission( testData.wsName, submissionRq ), Duration.Inf).asInstanceOf[RequestComplete[(StatusCode, SubmissionReport)]]

    val (status, newSubmissionReport) = rqComplete.response
    assertResult(StatusCodes.Created) {
      status
    }

    assert( newSubmissionReport.workflows.size == 6 )

    checkSubmissionStatus(workspaceService, newSubmissionReport.submissionId)

    val submission = runAndWait(submissionQuery.loadSubmission(UUID.fromString(newSubmissionReport.submissionId))).get
    assert( submission.workflows.forall(_.status == WorkflowStatuses.Queued) )
  }

  it should "400 when given an entity expression that evaluates to an empty set of entities" in withWorkspaceService { workspaceService =>
    val submissionRq = SubmissionRequest("dsde", "GoodMethodConfig", Some("SampleSet"), Some("sset_empty"), Some("this.samples"), false)
    val rqComplete = intercept[RawlsExceptionWithErrorReport] {
      Await.result(workspaceService.createSubmission(testData.wsName, submissionRq), Duration.Inf).asInstanceOf[RequestComplete[ErrorReport]]
    }
    assertResult(StatusCodes.BadRequest) {
      rqComplete.errorReport.statusCode.get
    }
  }

  it should "400 when given a method configuration with unparseable inputs" in withWorkspaceService { workspaceService =>
    val submissionRq = SubmissionRequest("dsde", "UnparseableInputsMethodConfig", Some("Individual"), Some("indiv1"), Some("this.sset.samples"), false)
    val rqComplete = intercept[RawlsExceptionWithErrorReport] {
      Await.result(workspaceService.createSubmission(testData.wsName, submissionRq), Duration.Inf).asInstanceOf[RequestComplete[ErrorReport]]
    }

    assertResult(StatusCodes.BadRequest) {
      rqComplete.errorReport.statusCode.get
    }

    assert {
      rqComplete.errorReport.message.contains("Invalid inputs: three_step.cgrep.pattern")
    }
  }

  it should "400 when given a method configuration with unparseable outputs" in withWorkspaceService { workspaceService =>
    val submissionRq = SubmissionRequest("dsde", "UnparseableOutputsMethodConfig", Some("Individual"), Some("indiv1"), Some("this.sset.samples"), false)
    val rqComplete = intercept[RawlsExceptionWithErrorReport] {
      Await.result(workspaceService.createSubmission(testData.wsName, submissionRq), Duration.Inf).asInstanceOf[RequestComplete[ErrorReport]]
    }

    assertResult(StatusCodes.BadRequest) {
      rqComplete.errorReport.statusCode.get
    }

    assert {
      rqComplete.errorReport.message.contains("Invalid outputs: three_step.cgrep.count")
    }
  }

  it should "400 when given a method configuration with unparseable inputs and outputs" in withWorkspaceService { workspaceService =>
    val submissionRq = SubmissionRequest("dsde", "UnparseableBothMethodConfig", Some("Individual"), Some("indiv1"), Some("this.sset.samples"), false)
    val rqComplete = intercept[RawlsExceptionWithErrorReport] {
      Await.result(workspaceService.createSubmission(testData.wsName, submissionRq), Duration.Inf).asInstanceOf[RequestComplete[ErrorReport]]
    }

    assertResult(StatusCodes.BadRequest) {
      rqComplete.errorReport.statusCode.get
    }

    assert {
      rqComplete.errorReport.message.contains("Invalid inputs: three_step.cgrep.pattern")
    }
    assert {
      rqComplete.errorReport.message.contains("Invalid outputs: three_step.cgrep.count")
    }
  }

  it should "return a successful Submission when given a method configuration with empty outputs" in withWorkspaceService { workspaceService =>
    val submissionRq = SubmissionRequest("dsde", "EmptyOutputsMethodConfig", Some("Individual"), Some("indiv1"), Some("this.sset.samples"), false)
    val rqComplete = Await.result(workspaceService.createSubmission( testData.wsName, submissionRq ), Duration.Inf).asInstanceOf[RequestComplete[(StatusCode, SubmissionReport)]]
    val (status, newSubmissionReport) = rqComplete.response
    assertResult(StatusCodes.Created) {
      status
    }

    assert( newSubmissionReport.workflows.size == 3 )

    checkSubmissionStatus(workspaceService, newSubmissionReport.submissionId)
  }

  it should "return a successful Submission with unstarted workflows where method configuration inputs are missing on some entities" in withWorkspaceService { workspaceService =>
    val submissionRq = SubmissionRequest("dsde", "NotAllSamplesMethodConfig", Some("Individual"), Some("indiv1"), Some("this.sset.samples"), false)
    val rqComplete = Await.result(workspaceService.createSubmission( testData.wsName, submissionRq ), Duration.Inf).asInstanceOf[RequestComplete[(StatusCode, SubmissionReport)]]
    val (status, newSubmissionReport) = rqComplete.response
    assertResult(StatusCodes.Created) {
      status
    }

    assert( newSubmissionReport.workflows.size == 2 )

    checkSubmissionStatus(workspaceService, newSubmissionReport.submissionId)
  }

  it should "400 when given an entity expression that evaluates to an entity of the wrong type" in withWorkspaceService { workspaceService =>
    val submissionRq = SubmissionRequest("dsde", "GoodMethodConfig", Some("PairSet"), Some("ps1"), Some("this.pairs"), false)
    val rqComplete = intercept[RawlsExceptionWithErrorReport] {
      Await.result(workspaceService.createSubmission(testData.wsName, submissionRq), Duration.Inf).asInstanceOf[RequestComplete[ErrorReport]]
    }
    assertResult(StatusCodes.BadRequest) {
      rqComplete.errorReport.statusCode.get
    }
  }

  it should "400 when given no entity expression and an entity of the wrong type" in withWorkspaceService { workspaceService =>
    val submissionRq = SubmissionRequest("dsde", "GoodMethodConfig", Some("PairSet"), Some("ps1"), None, false)
    val rqComplete = intercept[RawlsExceptionWithErrorReport] {
      Await.result(workspaceService.createSubmission(testData.wsName, submissionRq), Duration.Inf).asInstanceOf[RequestComplete[ErrorReport]]
    }
    assertResult(StatusCodes.BadRequest) {
      rqComplete.errorReport.statusCode.get
    }
  }

  it should "fail workflows that evaluate to nonsense and put the rest in Queued" in withWorkspaceServiceMockTimeoutExecution { mockExecSvc => workspaceService =>
    val submissionRq = SubmissionRequest("dsde", "NotAllSamplesMethodConfig", Some("Individual"), Some("indiv1"), Some("this.sset.samples"), false)
    val rqComplete = Await.result(workspaceService.createSubmission(testData.wsName, submissionRq), Duration.Inf).asInstanceOf[RequestComplete[(StatusCode, SubmissionReport)]]
    val (status, newSubmissionReport) = rqComplete.response
    assertResult(StatusCodes.Created) {
      status
    }

    val submissionStatusRq = Await.result(workspaceService.getSubmissionStatus(testData.wsName, newSubmissionReport.submissionId), Duration.Inf).asInstanceOf[RequestComplete[(StatusCode, SubmissionStatusResponse)]]
    val (submissionStatus, submissionStatusResponse) = submissionStatusRq.response
    assertResult(StatusCodes.OK) {
      submissionStatus
    }

    // Only the workflow with the dodgy expression (sample.tumortype on a normal) should fail
    assert(submissionStatusResponse.workflows.size == 3)

    // the rest are in queued
    assert( submissionStatusResponse.workflows.count(_.status == WorkflowStatuses.Queued) == 2 )
  }

  it should "run a submission fine with no root entity" in withWorkspaceService { workspaceService =>
    //Entityless has (duh) no entities and only literals in its outputs
    val submissionRq = SubmissionRequest(testData.methodConfigEntityless.namespace, testData.methodConfigEntityless.name, None, None, None, false)
    val createSub = Await.result(workspaceService.createSubmission(testData.wsName, submissionRq), Duration.Inf).asInstanceOf[RequestComplete[(StatusCode, SubmissionReport)]]

    val (status, newSubmissionReport) = createSub.response
    assertResult(StatusCodes.Created) {
      status
    }
    assert( newSubmissionReport.workflows.size == 1 )

    val submissionData = checkSubmissionStatus(workspaceService, newSubmissionReport.submissionId)
    assert(submissionData.workflows.size == 1)

    val listSubs = Await.result(workspaceService.listSubmissions(testData.wsName), Duration.Inf).asInstanceOf[RequestComplete[(StatusCode, Seq[SubmissionListResponse])]]
    val (_, subList) = listSubs.response

    val oneSub = subList.filter(s => s.submissionId == newSubmissionReport.submissionId)
    assert( oneSub.nonEmpty )
  }

  it should "return BadRequest when running an MC with a root entity without providing one" in withWorkspaceService { workspaceService =>
    //This method config has a root entity, but we've failed to provide one
    val submissionRq = SubmissionRequest("dsde", "GoodMethodConfig", None, None, None, false)
    val rqComplete = intercept[RawlsExceptionWithErrorReport] {
      Await.result(workspaceService.createSubmission( testData.wsName, submissionRq ), Duration.Inf).asInstanceOf[RequestComplete[ErrorReport]]
    }
    assertResult(StatusCodes.BadRequest) {
      rqComplete.errorReport.statusCode.get
    }
  }

  it should "return BadRequest when running against an MC with no root entity and providing one anyway" in withWorkspaceService { workspaceService =>
    //Entityless has (duh) no entities and only literals in its outputs
    val submissionRq = SubmissionRequest(testData.methodConfigEntityless.namespace, testData.methodConfigEntityless.name, Some("Individual"), Some("indiv1"), Some("this.sset.samples"), false)
    val rqComplete = intercept[RawlsExceptionWithErrorReport] {
      Await.result(workspaceService.createSubmission(testData.wsName, submissionRq), Duration.Inf).asInstanceOf[RequestComplete[(StatusCode, SubmissionReport)]]
    }

    assertResult(StatusCodes.BadRequest) {
      rqComplete.errorReport.statusCode.get
    }
  }

  "Submission validation requests" should "report a BadRequest for an unparseable entity expression" in withWorkspaceService { workspaceService =>
    val submissionRq = SubmissionRequest("dsde", "GoodMethodConfig", Some("Individual"), Some("indiv1"), Some("this.is."), false)
    val rqComplete = intercept[RawlsExceptionWithErrorReport] {
      Await.result(workspaceService.validateSubmission(testData.wsName, submissionRq), Duration.Inf).asInstanceOf[RequestComplete[ErrorReport]]
    }
    assertResult(StatusCodes.BadRequest) {
      rqComplete.errorReport.statusCode.get
    }
  }

  it should "report a validated input and runnable workflow when given an entity expression that evaluates to a single entity" in withWorkspaceService { workspaceService =>
    val submissionRq = SubmissionRequest("dsde", "GoodMethodConfig", Some("Pair"), Some("pair1"), Some("this.case"), false)
    val vComplete = Await.result(workspaceService.validateSubmission( testData.wsName, submissionRq ), Duration.Inf).asInstanceOf[RequestComplete[(StatusCode, SubmissionValidationReport)]]
    val (vStatus, vData) = vComplete.response
    assertResult(StatusCodes.OK) {
      vStatus
    }

    assertResult(1) { vData.validEntities.length }
    assert(vData.invalidEntities.isEmpty)
  }

  it should "report validated inputs and runnable workflows when given an entity expression that evaluates to a set of entities" in withWorkspaceService { workspaceService =>
    val submissionRq = SubmissionRequest("dsde", "GoodMethodConfig", Some("SampleSet"), Some("sset1"), Some("this.samples"), false)
    val vComplete = Await.result(workspaceService.validateSubmission( testData.wsName, submissionRq ), Duration.Inf).asInstanceOf[RequestComplete[(StatusCode, SubmissionValidationReport)]]
    val (vStatus, vData) = vComplete.response
    assertResult(StatusCodes.OK) {
      vStatus
    }

    assertResult(testData.sset1.attributes(AttributeName.withDefaultNS("samples")).asInstanceOf[AttributeEntityReferenceList].list.size) { vData.validEntities.length }
    assert(vData.invalidEntities.isEmpty)
  }

  it should "400 when given an entity expression that evaluates to an empty set of entities" in withWorkspaceService { workspaceService =>
    val submissionRq = SubmissionRequest("dsde", "GoodMethodConfig", Some("SampleSet"), Some("sset_empty"), Some("this.samples"), false)
    val rqComplete = intercept[RawlsExceptionWithErrorReport] {
      Await.result(workspaceService.validateSubmission(testData.wsName, submissionRq), Duration.Inf).asInstanceOf[RequestComplete[ErrorReport]]
    }
    assertResult(StatusCodes.BadRequest) {
      rqComplete.errorReport.statusCode.get
    }
  }

  it should "400 when given a method configuration with unparseable inputs" in withWorkspaceService { workspaceService =>
    val submissionRq = SubmissionRequest("dsde", "UnparseableInputsMethodConfig", Some("Individual"), Some("indiv1"), Some("this.sset.samples"), false)
    val rqComplete = intercept[RawlsExceptionWithErrorReport] {
      Await.result(workspaceService.validateSubmission(testData.wsName, submissionRq), Duration.Inf).asInstanceOf[RequestComplete[ErrorReport]]
    }

    assertResult(StatusCodes.BadRequest) {
      rqComplete.errorReport.statusCode.get
    }

    assert {
      rqComplete.errorReport.message.contains("Invalid inputs: three_step.cgrep.pattern")
    }
  }

  it should "400 when given a method configuration with unparseable outputs" in withWorkspaceService { workspaceService =>
    val submissionRq = SubmissionRequest("dsde", "UnparseableOutputsMethodConfig", Some("Individual"), Some("indiv1"), Some("this.sset.samples"), false)
    val rqComplete = intercept[RawlsExceptionWithErrorReport] {
      Await.result(workspaceService.validateSubmission(testData.wsName, submissionRq), Duration.Inf).asInstanceOf[RequestComplete[ErrorReport]]
    }

    assertResult(StatusCodes.BadRequest) {
      rqComplete.errorReport.statusCode.get
    }

    assert {
      rqComplete.errorReport.message.contains("Invalid outputs: three_step.cgrep.count")
    }
  }

  it should "report a successful validation when given a method configuration with empty outputs" in withWorkspaceService { workspaceService =>
    val submissionRq = SubmissionRequest("dsde", "EmptyOutputsMethodConfig", Some("Individual"), Some("indiv1"), Some("this.sset.samples"), false)
    val rqComplete = Await.result(workspaceService.validateSubmission( testData.wsName, submissionRq ), Duration.Inf).asInstanceOf[RequestComplete[(StatusCode, SubmissionValidationReport)]]
    val (status, validation) = rqComplete.response
    assertResult(StatusCodes.OK) {
      status
    }

    assertResult(3) { validation.validEntities.size }
    assert { validation.invalidEntities.isEmpty }
  }

  it should "report validated inputs and a mixture of started and unstarted workflows where method configuration inputs are missing on some entities" in withWorkspaceService { workspaceService =>
    val submissionRq = SubmissionRequest("dsde", "NotAllSamplesMethodConfig", Some("Individual"), Some("indiv1"), Some("this.sset.samples"), false)
    val vComplete = Await.result(workspaceService.validateSubmission( testData.wsName, submissionRq ), Duration.Inf).asInstanceOf[RequestComplete[(StatusCode, SubmissionValidationReport)]]
    val (vStatus, vData) = vComplete.response
    assertResult(StatusCodes.OK) {
      vStatus
    }

    assertResult(testData.sset1.attributes(AttributeName.withDefaultNS("samples")).asInstanceOf[AttributeEntityReferenceList].list.size-1) { vData.validEntities.length }
    assertResult(1) { vData.invalidEntities.length }
  }

  it should "report errors for an entity expression that evaluates to an entity of the wrong type" in withWorkspaceService { workspaceService =>
    val submissionRq = SubmissionRequest("dsde", "GoodMethodConfig", Some("PairSet"), Some("ps1"), Some("this.pairs"), false)
    val rqComplete = intercept[RawlsExceptionWithErrorReport] {
      Await.result(workspaceService.validateSubmission(testData.wsName, submissionRq), Duration.Inf).asInstanceOf[RequestComplete[ErrorReport]]
    }
    assertResult(StatusCodes.BadRequest) {
      rqComplete.errorReport.statusCode.get
    }
  }

  it should "report an error when given no entity expression and the entity is of the wrong type" in withWorkspaceService { workspaceService =>
    val submissionRq = SubmissionRequest("dsde", "GoodMethodConfig", Some("PairSet"), Some("ps1"), None, false)
    val rqComplete = intercept[RawlsExceptionWithErrorReport] {
      Await.result(workspaceService.validateSubmission(testData.wsName, submissionRq), Duration.Inf).asInstanceOf[RequestComplete[ErrorReport]]
    }
    assertResult(StatusCodes.BadRequest) {
      rqComplete.errorReport.statusCode.get
    }
  }

  "Aborting submissions" should "404 if the workspace doesn't exist" in withSubmissionTestWorkspaceService { workspaceService =>
    val rqComplete = intercept[RawlsExceptionWithErrorReport] {
      Await.result(workspaceService.abortSubmission(WorkspaceName(name = "nonexistent", namespace = "workspace"), "12345"), Duration.Inf).asInstanceOf[RequestComplete[ErrorReport]]
    }
    assertResult(StatusCodes.NotFound) {
      rqComplete.errorReport.statusCode.get
    }
  }

  it should "404 if the submission doesn't exist" in withSubmissionTestWorkspaceService { workspaceService =>
    val rqComplete = intercept[RawlsExceptionWithErrorReport] {
      Await.result(workspaceService.abortSubmission(subTestData.wsName, "12345"), Duration.Inf).asInstanceOf[RequestComplete[ErrorReport]]
    }
    assertResult(StatusCodes.NotFound) {
      rqComplete.errorReport.statusCode.get
    }
  }

  it should "204 No Content for a valid submission" in withSubmissionTestWorkspaceService { workspaceService =>
    val rqComplete = workspaceService.abortSubmission(subTestData.wsName, subGoodWorkflow)
    val status = Await.result(rqComplete, Duration.Inf).asInstanceOf[RequestComplete[StatusCode]].response
    assertResult(StatusCodes.NoContent) {
      status
    }
  }

  "Getting workflow outputs" should "return 200 when all is well" in withSubmissionTestWorkspaceService { workspaceService =>
    val rqComplete = workspaceService.workflowOutputs(
      subTestData.wsName,
      subTestData.submissionTestAbortGoodWorkflow.submissionId,
      subTestData.existingWorkflowId.get)
    val (status, data) = Await.result(rqComplete, Duration.Inf).asInstanceOf[RequestComplete[(StatusCode, WorkflowOutputs)]].response

    assertResult(StatusCodes.OK) {status}
    assertResult(subTestData.extantWorkflowOutputs) {data}
  }

  it should "return 404 on getting outputs for a workflow that exists, but not in this submission" in withSubmissionTestWorkspaceService { workspaceService =>
    val rqComplete = workspaceService.workflowOutputs(
      subTestData.wsName,
      subTestData.submissionTestAbortTerminalWorkflow.submissionId,
      subTestData.existingWorkflowId.get)
    val errorReport = intercept[RawlsExceptionWithErrorReport] {
      Await.result(rqComplete, Duration.Inf).asInstanceOf[RequestComplete[ErrorReport]].response
    }

    assertResult(StatusCodes.NotFound) {errorReport.errorReport.statusCode.get}
  }

  "Getting a workflow cost" should "return 200 when all is well" in withSubmissionTestWorkspaceService { workspaceService =>
    val rqComplete = workspaceService.workflowCost(
      subTestData.wsName,
      subTestData.submissionTestAbortGoodWorkflow.submissionId,
      subTestData.existingWorkflowId.get)
    val (status, data) = Await.result(rqComplete, Duration.Inf).asInstanceOf[RequestComplete[(StatusCode, WorkflowCost)]].response

    assertResult(StatusCodes.OK) {status}
    assertResult(WorkflowCost(subTestData.existingWorkflowId.get, Some(mockSubmissionCostService.fixedCost))) {data}
  }

  it should "return 404 on getting the cost for a workflow that exists, but not in this submission" in withSubmissionTestWorkspaceService { workspaceService =>
    val rqComplete = workspaceService.workflowCost(
      subTestData.wsName,
      subTestData.submissionTestAbortTerminalWorkflow.submissionId,
      subTestData.existingWorkflowId.get)
    val errorReport = intercept[RawlsExceptionWithErrorReport] {
      Await.result(rqComplete, Duration.Inf).asInstanceOf[RequestComplete[ErrorReport]].response
    }

    assertResult(StatusCodes.NotFound) {errorReport.errorReport.statusCode.get}
  }

  it should "calculate submission cost as the sum of workflow costs" in withSubmissionTestWorkspaceService { workspaceService =>
    val submissionData = checkSubmissionStatus(workspaceService, subTestData.submissionTestAbortTwoGoodWorkflows.submissionId, subTestData.wsName)
    assertResult(Option(mockSubmissionCostService.fixedCost * 2)) {
      submissionData.cost
    }
  }

  it should "return 502 on getting a workflow if Cromwell barfs" in withSubmissionTestWorkspaceService { workspaceService =>
    val rqComplete = workspaceService.workflowOutputs(
      subTestData.wsName,
      subTestData.submissionTestCromwellBadWorkflows.submissionId,
      subTestData.badLogsAndMetadataWorkflowId.get)
    val errorReport = intercept[RawlsExceptionWithErrorReport] {
      Await.result(rqComplete, Duration.Inf).asInstanceOf[RequestComplete[ErrorReport]].response
    }

    assertResult(StatusCodes.BadGateway) {errorReport.errorReport.statusCode.get}
    assertResult("cromwell"){errorReport.errorReport.causes.head.source}
  }

  "Getting workflow metadata" should "return 200" in withSubmissionTestWorkspaceService { workspaceService =>
    val rqComplete = workspaceService.workflowMetadata(
      subTestData.wsName,
      subTestData.submissionTestAbortGoodWorkflow.submissionId,
      subTestData.existingWorkflowId.get,
      MetadataParams())
    val (status, data) = Await.result(rqComplete, Duration.Inf).asInstanceOf[RequestComplete[(StatusCode, JsObject)]].response

    assertResult(StatusCodes.OK) {
      status
    }
  }

  "ExecutionService" should "parse unsupported output data types" in {
    val workflowId = "8afafe21-2b70-4180-a565-748cb573e10c"
    assertResult(ExecutionServiceOutputs(workflowId, Map("aggregate_data_workflow.aggregate_data.output_array" -> Left(AttributeValueRawJson(JsArray(Vector(
      JsArray(Vector(JsString("foo"), JsString("bar"))),
      JsArray(Vector(JsString("baz"), JsString("qux")))))))))) {

      Await.result(new HttpExecutionServiceDAO(mockServer.mockServerBaseUrl, workbenchMetricBaseName).outputs(workflowId, userInfo), Duration.Inf)
    }

  }
}


