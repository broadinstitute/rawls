package org.broadinstitute.dsde.rawls.jobexec

import java.util.UUID
import java.util.concurrent.TimeUnit
import akka.actor.{ActorRef, PoisonPill, ActorSystem}
import akka.pattern.AskTimeoutException
import akka.testkit.{TestKit, TestActorRef}
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.google.MockGooglePubSubDAO
import org.broadinstitute.dsde.rawls.mock.RemoteServicesMockServer
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.monitor.{GoogleGroupSyncMonitorSupervisor, BucketDeletionMonitor}
import org.broadinstitute.dsde.rawls.user.UserService
import org.broadinstitute.dsde.rawls.webservice.PerRequest.RequestComplete
import org.broadinstitute.dsde.rawls.workspace.WorkspaceService
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}
import spray.http.{StatusCode, StatusCodes}
import spray.json._
import scala.concurrent.duration._
import scala.concurrent.Await
import scala.util.Try
import org.broadinstitute.dsde.rawls.dataaccess.slick.{TestDriverComponent, TestData}

/**
 * Created with IntelliJ IDEA.
 * User: hussein
 * Date: 07/02/2015
 * Time: 11:06
 */
class SubmissionSpec(_system: ActorSystem) extends TestKit(_system) with FlatSpecLike with Matchers with TestDriverComponent with BeforeAndAfterAll {
  import driver.api._
  
  def this() = this(ActorSystem("SubmissionSpec"))

  val testDbName = "SubmissionSpec"
  val submissionSupervisorActorName = "test-subspec-submission-supervisor"

  val mockServer = RemoteServicesMockServer()

  override def beforeAll(): Unit = {
    super.beforeAll()
    mockServer.startServer
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
    val workspace = Workspace(wsName.namespace, wsName.name, None, UUID.randomUUID().toString, "aBucket", currentTime(), currentTime(), "testUser", Map.empty, Map(WorkspaceAccessLevels.Owner -> ownerGroup), Map(WorkspaceAccessLevels.Owner -> ownerGroup))

    val sample1 = Entity("sample1", "Sample",
      Map(AttributeName.withDefaultNS("type") -> AttributeString("normal")))
    val sample2 = Entity("sample2", "Sample",
      Map(AttributeName.withDefaultNS("type") -> AttributeString("normal")))

    val refreshToken = UUID.randomUUID.toString

    val existingWorkflowId = Option("69d1d92f-3895-4a7b-880a-82535e9a096e")
    val nonExistingWorkflowId = Option("45def17d-40c2-44cc-89bf-9e77bc2c9999")
    val alreadyTerminatedWorkflowId = Option("45def17d-40c2-44cc-89bf-9e77bc2c8778")
    val badLogsAndMetadataWorkflowId = Option("29b2e816-ecaf-11e6-b006-92361f002671")
    
    
    
    val submissionTestAbortMissingWorkflow = Submission(subMissingWorkflow,testDate, testData.userOwner, "std","someMethod",sample1.toReference,
      Seq(Workflow(nonExistingWorkflowId,WorkflowStatuses.Submitted,testDate,sample1.toReference, testData.inputResolutions)), SubmissionStatuses.Submitted)

    val submissionTestAbortMalformedWorkflow = Submission(subMalformedWorkflow,testDate, testData.userOwner, "std","someMethod",sample1.toReference,
      Seq(Workflow(Option("malformed_workflow"),WorkflowStatuses.Submitted,testDate,sample1.toReference, testData.inputResolutions)), SubmissionStatuses.Submitted)

    val submissionTestAbortGoodWorkflow = Submission(subGoodWorkflow,testDate, testData.userOwner, "std","someMethod",sample1.toReference,
      Seq(Workflow(existingWorkflowId,WorkflowStatuses.Submitted,testDate,sample1.toReference, testData.inputResolutions)), SubmissionStatuses.Submitted)

    val submissionTestAbortTerminalWorkflow = Submission(subTerminalWorkflow,testDate, testData.userOwner, "std","someMethod",sample1.toReference,
      Seq(Workflow(alreadyTerminatedWorkflowId,WorkflowStatuses.Submitted,testDate,sample1.toReference, testData.inputResolutions)), SubmissionStatuses.Submitted)

    val submissionTestAbortOneMissingWorkflow = Submission(subOneMissingWorkflow,testDate, testData.userOwner, "std","someMethod",sample1.toReference,
      Seq(
        Workflow(existingWorkflowId,WorkflowStatuses.Submitted,testDate,sample1.toReference, testData.inputResolutions),
        Workflow(nonExistingWorkflowId,WorkflowStatuses.Submitted,testDate,sample2.toReference, testData.inputResolutions)), SubmissionStatuses.Submitted)

    val submissionTestAbortTwoGoodWorkflows = Submission(subTwoGoodWorkflows,testDate, testData.userOwner, "std","someMethod",sample1.toReference,
      Seq(
        Workflow(existingWorkflowId,WorkflowStatuses.Submitted,testDate,sample1.toReference, testData.inputResolutions),
        Workflow(alreadyTerminatedWorkflowId,WorkflowStatuses.Submitted,testDate,sample2.toReference, testData.inputResolutions)), SubmissionStatuses.Submitted)

    val submissionTestCromwellBadWorkflows = Submission(subCromwellBadWorkflows, testDate, testData.userOwner, "std","someMethod",sample1.toReference,
      Seq(
        Workflow(badLogsAndMetadataWorkflowId,WorkflowStatuses.Submitted,testDate,sample1.toReference, testData.inputResolutions)), SubmissionStatuses.Submitted)

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
        rawlsUserQuery.save(user),
        rawlsGroupQuery.save(ownerGroup),
        workspaceQuery.save(workspace),
        withWorkspaceContext(workspace) { context =>
          DBIO.seq(
            entityQuery.save(context, sample1),
            entityQuery.save(context, sample2),
            methodConfigurationQuery.save(context, MethodConfiguration("std", "someMethod", "Sample", Map.empty, Map.empty, Map.empty, MethodRepoMethod("std", "someMethod", 1))),
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
      executionServiceDAO: ExecutionServiceDAO = new HttpExecutionServiceDAO(mockServer.mockServerBaseUrl, mockServer.defaultWorkflowSubmissionTimeout) ): T = {

    withDataOp { dataSource =>
      val execServiceCluster: ExecutionServiceCluster = MockShardedExecutionServiceCluster.fromDAO(executionServiceDAO, dataSource)

      val gcsDAO: MockGoogleServicesDAO = new MockGoogleServicesDAO("test")
      val gpsDAO = new MockGooglePubSubDAO
      val submissionSupervisor = system.actorOf(SubmissionSupervisor.props(
        execServiceCluster,
        slickDataSource
      ).withDispatcher("submission-monitor-dispatcher"), submissionSupervisorActorName)
      val bucketDeletionMonitor = system.actorOf(BucketDeletionMonitor.props(slickDataSource, gcsDAO))

      gcsDAO.storeToken(userInfo, subTestData.refreshToken)
      val directoryDAO = new MockUserDirectoryDAO

      val userServiceConstructor = UserService.constructor(
        slickDataSource,
        gcsDAO,
        directoryDAO,
        gpsDAO,
        "test-topic-name"
      )_

      val googleGroupSyncMonitorSupervisor = system.actorOf(GoogleGroupSyncMonitorSupervisor.props(500 milliseconds, 0 seconds, gpsDAO, "test-topic-name", "test-sub-name", 1, userServiceConstructor))

      val execServiceBatchSize = 3
      val workspaceServiceConstructor = WorkspaceService.constructor(
        dataSource,
        new HttpMethodRepoDAO(mockServer.mockServerBaseUrl),
        execServiceCluster,
        execServiceBatchSize,
        gcsDAO,
        submissionSupervisor,
        bucketDeletionMonitor,
        userServiceConstructor
      )_
      lazy val workspaceService: WorkspaceService = TestActorRef(WorkspaceService.props(workspaceServiceConstructor, userInfo)).underlyingActor
      try {
        testCode(workspaceService)
      }
      finally {
        // for failed tests we also need to poison pill
        submissionSupervisor ! PoisonPill
        bucketDeletionMonitor ! PoisonPill
        googleGroupSyncMonitorSupervisor ! PoisonPill
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

  private def checkSubmissionStatus(workspaceService:WorkspaceService, submissionId:String) = {
    val submissionStatusRqComplete = workspaceService.getSubmissionStatus(testData.wsName, submissionId)

    Await.result(submissionStatusRqComplete, Duration.Inf) match {
      case RequestComplete((submissionStatus: StatusCode, submissionData: Any)) => {
        assertResult(StatusCodes.OK) { submissionStatus }
        assertResult(submissionId) { submissionData.asInstanceOf[SubmissionStatusResponse].submissionId }
      }
      case _ => fail("Unable to get submission status")
    }
  }

  "Submission requests" should "400 when given an unparseable entity expression" in withWorkspaceService { workspaceService =>
    val submissionRq = SubmissionRequest("dsde", "GoodMethodConfig", "Individual", "indiv1", Some("this.is."))
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
    }, 250 milliseconds)
    subActor.get
  }

  it should "return a successful Submission and spawn a submission monitor actor when given an entity expression that evaluates to a single entity" in withWorkspaceServiceMockExecution { mockExecSvc => workspaceService =>
    val submissionRq = SubmissionRequest("dsde", "GoodMethodConfig", "Pair", "pair1", Some("this.case"))
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

    val submissionRq = SubmissionRequest("dsde", "GoodMethodConfig", sset.entityType, sset.name, Some("this.samples"))
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

  it should "cause the configurable workflow submissions timeout to trigger" in {
    def wdl = "dummy"
    def wdlInputs = "dummy"
    def workflowOptions = Option("two_second_delay")

    def expectedResponse = ExecutionServiceStatus("69d1d92f-3895-4a7b-880a-82535e9a096e", "Submitted")

    def execWith1SecTimeout = new HttpExecutionServiceDAO(mockServer.mockServerBaseUrl, FiniteDuration(1, TimeUnit.SECONDS))
    def execWith3SecTimeout = new HttpExecutionServiceDAO(mockServer.mockServerBaseUrl, FiniteDuration(3, TimeUnit.SECONDS))

    val execResponse = Await.result(execWith3SecTimeout.submitWorkflows(wdl, Seq(wdlInputs), workflowOptions, userInfo), Duration.Inf)
    assertResult(expectedResponse) { execResponse.head.left.get }

    intercept[AskTimeoutException] {
      Await.result(execWith1SecTimeout.submitWorkflows(wdl, Seq(wdlInputs), workflowOptions, userInfo), Duration.Inf)
    }
  }

  it should "400 when given an entity expression that evaluates to an empty set of entities" in withWorkspaceService { workspaceService =>
    val submissionRq = SubmissionRequest("dsde", "GoodMethodConfig", "SampleSet", "sset_empty", Some("this.samples"))
    val rqComplete = intercept[RawlsExceptionWithErrorReport] {
      Await.result(workspaceService.createSubmission(testData.wsName, submissionRq), Duration.Inf).asInstanceOf[RequestComplete[ErrorReport]]
    }
    assertResult(StatusCodes.BadRequest) {
      rqComplete.errorReport.statusCode.get
    }
  }

  it should "return a successful Submission but no started workflows when given a method configuration with unparseable inputs" in withWorkspaceService { workspaceService =>
    val submissionRq = SubmissionRequest("dsde", "UnparseableMethodConfig", "Individual", "indiv1", Some("this.sset.samples"))
    val rqComplete = Await.result(workspaceService.createSubmission( testData.wsName, submissionRq ), Duration.Inf).asInstanceOf[RequestComplete[(StatusCode, SubmissionReport)]]
    val (status, newSubmissionReport) = rqComplete.response
    assertResult(StatusCodes.Created) {
      status
    }

    assert( newSubmissionReport.workflows.size == 0 )

    checkSubmissionStatus(workspaceService, newSubmissionReport.submissionId)
  }

  it should "return a successful Submission with unstarted workflows where method configuration inputs are missing on some entities" in withWorkspaceService { workspaceService =>
    val submissionRq = SubmissionRequest("dsde", "NotAllSamplesMethodConfig", "Individual", "indiv1", Some("this.sset.samples"))
    val rqComplete = Await.result(workspaceService.createSubmission( testData.wsName, submissionRq ), Duration.Inf).asInstanceOf[RequestComplete[(StatusCode, SubmissionReport)]]
    val (status, newSubmissionReport) = rqComplete.response
    assertResult(StatusCodes.Created) {
      status
    }

    assert( newSubmissionReport.workflows.size == 2 )

    checkSubmissionStatus(workspaceService, newSubmissionReport.submissionId)
  }

  it should "400 when given an entity expression that evaluates to an entity of the wrong type" in withWorkspaceService { workspaceService =>
    val submissionRq = SubmissionRequest("dsde", "GoodMethodConfig", "PairSet", "ps1", Some("this.pairs"))
    val rqComplete = intercept[RawlsExceptionWithErrorReport] {
      Await.result(workspaceService.createSubmission(testData.wsName, submissionRq), Duration.Inf).asInstanceOf[RequestComplete[ErrorReport]]
    }
    assertResult(StatusCodes.BadRequest) {
      rqComplete.errorReport.statusCode.get
    }
  }

  it should "400 when given no entity expression and an entity of the wrong type" in withWorkspaceService { workspaceService =>
    val submissionRq = SubmissionRequest("dsde", "GoodMethodConfig", "PairSet", "ps1", None)
    val rqComplete = intercept[RawlsExceptionWithErrorReport] {
      Await.result(workspaceService.createSubmission(testData.wsName, submissionRq), Duration.Inf).asInstanceOf[RequestComplete[ErrorReport]]
    }
    assertResult(StatusCodes.BadRequest) {
      rqComplete.errorReport.statusCode.get
    }
  }

  it should "fail workflows that evaluate to nonsense and put the rest in Queued" in withWorkspaceServiceMockTimeoutExecution { mockExecSvc => workspaceService =>
    val submissionRq = SubmissionRequest("dsde", "NotAllSamplesMethodConfig", "Individual", "indiv1", Some("this.sset.samples"))
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

  "Submission validation requests" should "report a BadRequest for an unparseable entity expression" in withWorkspaceService { workspaceService =>
    val submissionRq = SubmissionRequest("dsde", "GoodMethodConfig", "Individual", "indiv1", Some("this.is."))
    val rqComplete = intercept[RawlsExceptionWithErrorReport] {
      Await.result(workspaceService.validateSubmission(testData.wsName, submissionRq), Duration.Inf).asInstanceOf[RequestComplete[ErrorReport]]
    }
    assertResult(StatusCodes.BadRequest) {
      rqComplete.errorReport.statusCode.get
    }
  }

  it should "report a validated input and runnable workflow when given an entity expression that evaluates to a single entity" in withWorkspaceService { workspaceService =>
    val submissionRq = SubmissionRequest("dsde", "GoodMethodConfig", "Pair", "pair1", Some("this.case"))
    val vComplete = Await.result(workspaceService.validateSubmission( testData.wsName, submissionRq ), Duration.Inf).asInstanceOf[RequestComplete[(StatusCode, SubmissionValidationReport)]]
    val (vStatus, vData) = vComplete.response
    assertResult(StatusCodes.OK) {
      vStatus
    }

    assertResult(1) { vData.validEntities.length }
    assert(vData.invalidEntities.isEmpty)
  }

  it should "report validated inputs and runnable workflows when given an entity expression that evaluates to a set of entities" in withWorkspaceService { workspaceService =>
    val submissionRq = SubmissionRequest("dsde", "GoodMethodConfig", "SampleSet", "sset1", Some("this.samples"))
    val vComplete = Await.result(workspaceService.validateSubmission( testData.wsName, submissionRq ), Duration.Inf).asInstanceOf[RequestComplete[(StatusCode, SubmissionValidationReport)]]
    val (vStatus, vData) = vComplete.response
    assertResult(StatusCodes.OK) {
      vStatus
    }

    assertResult(testData.sset1.attributes(AttributeName.withDefaultNS("samples")).asInstanceOf[AttributeEntityReferenceList].list.size) { vData.validEntities.length }
    assert(vData.invalidEntities.isEmpty)
  }

  it should "400 when given an entity expression that evaluates to an empty set of entities" in withWorkspaceService { workspaceService =>
    val submissionRq = SubmissionRequest("dsde", "GoodMethodConfig", "SampleSet", "sset_empty", Some("this.samples"))
    val rqComplete = intercept[RawlsExceptionWithErrorReport] {
      Await.result(workspaceService.validateSubmission(testData.wsName, submissionRq), Duration.Inf).asInstanceOf[RequestComplete[ErrorReport]]
    }
    assertResult(StatusCodes.BadRequest) {
      rqComplete.errorReport.statusCode.get
    }
  }

  it should "report validated inputs and unrunnable workflows when given a method configuration with unparseable inputs" in withWorkspaceService { workspaceService =>
    val submissionRq = SubmissionRequest("dsde", "UnparseableMethodConfig", "Individual", "indiv1", Some("this.sset.samples"))
    val vComplete = Await.result(workspaceService.validateSubmission( testData.wsName, submissionRq ), Duration.Inf).asInstanceOf[RequestComplete[(StatusCode, SubmissionValidationReport)]]
    val (vStatus, vData) = vComplete.response
    assertResult(StatusCodes.OK) {
      vStatus
    }

    assert(vData.validEntities.isEmpty)
    assertResult(testData.sset1.attributes(AttributeName.withDefaultNS("samples")).asInstanceOf[AttributeEntityReferenceList].list.size) { vData.invalidEntities.length }
  }

  it should "report validated inputs and a mixture of started and unstarted workflows where method configuration inputs are missing on some entities" in withWorkspaceService { workspaceService =>
    val submissionRq = SubmissionRequest("dsde", "NotAllSamplesMethodConfig", "Individual", "indiv1", Some("this.sset.samples"))
    val vComplete = Await.result(workspaceService.validateSubmission( testData.wsName, submissionRq ), Duration.Inf).asInstanceOf[RequestComplete[(StatusCode, SubmissionValidationReport)]]
    val (vStatus, vData) = vComplete.response
    assertResult(StatusCodes.OK) {
      vStatus
    }

    assertResult(testData.sset1.attributes(AttributeName.withDefaultNS("samples")).asInstanceOf[AttributeEntityReferenceList].list.size-1) { vData.validEntities.length }
    assertResult(1) { vData.invalidEntities.length }
  }

  it should "report errors for an entity expression that evaluates to an entity of the wrong type" in withWorkspaceService { workspaceService =>
    val submissionRq = SubmissionRequest("dsde", "GoodMethodConfig", "PairSet", "ps1", Some("this.pairs"))
    val rqComplete = intercept[RawlsExceptionWithErrorReport] {
      Await.result(workspaceService.validateSubmission(testData.wsName, submissionRq), Duration.Inf).asInstanceOf[RequestComplete[ErrorReport]]
    }
    assertResult(StatusCodes.BadRequest) {
      rqComplete.errorReport.statusCode.get
    }
  }

  it should "report an error when given no entity expression and the entity is of the wrong type" in withWorkspaceService { workspaceService =>
    val submissionRq = SubmissionRequest("dsde", "GoodMethodConfig", "PairSet", "ps1", None)
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

  it should "return 404 on getting a workflow that exists, but not in this submission" in withSubmissionTestWorkspaceService { workspaceService =>
    val rqComplete = workspaceService.workflowOutputs(
      subTestData.wsName,
      subTestData.submissionTestAbortTerminalWorkflow.submissionId,
      subTestData.existingWorkflowId.get)
    val errorReport = intercept[RawlsExceptionWithErrorReport] {
      Await.result(rqComplete, Duration.Inf).asInstanceOf[RequestComplete[ErrorReport]].response
    }

    assertResult(StatusCodes.NotFound) {errorReport.errorReport.statusCode.get}
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
      subTestData.existingWorkflowId.get)
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

      Await.result(new HttpExecutionServiceDAO(mockServer.mockServerBaseUrl, mockServer.defaultWorkflowSubmissionTimeout).outputs(workflowId, userInfo), Duration.Inf)
    }

  }
}


