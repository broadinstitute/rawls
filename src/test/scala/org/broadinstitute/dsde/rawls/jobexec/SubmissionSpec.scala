package org.broadinstitute.dsde.rawls.jobexec

import java.util.UUID
import java.util.concurrent.TimeUnit
import akka.actor.{ActorRef, PoisonPill, ActorSystem}
import akka.pattern.AskTimeoutException
import akka.testkit.{TestKit, TestActorRef}
import com.google.api.client.googleapis.testing.auth.oauth2.MockGoogleCredential.Builder
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.mock.RemoteServicesMockServer
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.monitor.BucketDeletionMonitor
import org.broadinstitute.dsde.rawls.user.UserService
import org.broadinstitute.dsde.rawls.webservice.PerRequest.RequestComplete
import org.broadinstitute.dsde.rawls.workspace.WorkspaceService
import org.scalatest.{FlatSpecLike, Matchers}
import spray.http.{StatusCode, StatusCodes}
import spray.json._
import scala.concurrent.duration._
import scala.concurrent.{Future, Await}
import scala.util.{Try, Success}
import org.broadinstitute.dsde.rawls.dataaccess.slick.TestDriverComponent
import org.scalatest.BeforeAndAfterAll
import org.broadinstitute.dsde.rawls.dataaccess.slick.TestData

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
  override def beforeAll() = {
    super.beforeAll
    mockServer.startServer
  }

  override def afterAll: Unit = {
    TestKit.shutdownActorSystem(system)
    super.afterAll
    mockServer.stopServer
  }

  var subMissingWorkflow = UUID.randomUUID().toString
  var subMalformedWorkflow = UUID.randomUUID().toString
  var subGoodWorkflow = UUID.randomUUID().toString
  var subTerminalWorkflow = UUID.randomUUID().toString
  var subOneMissingWorkflow = UUID.randomUUID().toString
  var subTwoGoodWorkflows = UUID.randomUUID().toString

  val subTestData = new SubmissionTestData()

  class SubmissionTestData() extends TestData {
    val wsName = WorkspaceName("myNamespacexxx", "myWorkspace")
    val user = RawlsUser(userInfo)
    val ownerGroup = makeRawlsGroup("workspaceOwnerGroup", Set(user))
    val workspace = Workspace(wsName.namespace, wsName.name, None, UUID.randomUUID().toString, "aBucket", currentTime(), currentTime(), "testUser", Map.empty, Map(WorkspaceAccessLevels.Owner -> ownerGroup), Map(WorkspaceAccessLevels.Owner -> ownerGroup))

    val sample1 = Entity("sample1", "Sample",
      Map("type" -> AttributeString("normal")))
    val sample2 = Entity("sample2", "Sample",
      Map("type" -> AttributeString("normal")))

    val refreshToken = UUID.randomUUID.toString

    val existingWorkflowId = Option("69d1d92f-3895-4a7b-880a-82535e9a096e")
    val nonExistingWorkflowId = Option("45def17d-40c2-44cc-89bf-9e77bc2c9999")
    val alreadyTerminatedWorkflowId = Option("45def17d-40c2-44cc-89bf-9e77bc2c8778")
    
    
    
    val submissionTestAbortMissingWorkflow = Submission(subMissingWorkflow,testDate, testData.userOwner, "std","someMethod",sample1.toReference,
      Seq(Workflow(nonExistingWorkflowId,WorkflowStatuses.Submitted,testDate,sample1.toReference, testData.inputResolutions)),
      Seq.empty[WorkflowFailure], SubmissionStatuses.Submitted)

    val submissionTestAbortMalformedWorkflow = Submission(subMalformedWorkflow,testDate, testData.userOwner, "std","someMethod",sample1.toReference,
      Seq(Workflow(Option("malformed_workflow"),WorkflowStatuses.Submitted,testDate,sample1.toReference, testData.inputResolutions)),
      Seq.empty[WorkflowFailure], SubmissionStatuses.Submitted)

    val submissionTestAbortGoodWorkflow = Submission(subGoodWorkflow,testDate, testData.userOwner, "std","someMethod",sample1.toReference,
      Seq(Workflow(existingWorkflowId,WorkflowStatuses.Submitted,testDate,sample1.toReference, testData.inputResolutions)),
      Seq.empty[WorkflowFailure], SubmissionStatuses.Submitted)

    val submissionTestAbortTerminalWorkflow = Submission(subTerminalWorkflow,testDate, testData.userOwner, "std","someMethod",sample1.toReference,
      Seq(Workflow(alreadyTerminatedWorkflowId,WorkflowStatuses.Submitted,testDate,sample1.toReference, testData.inputResolutions)),
      Seq.empty[WorkflowFailure], SubmissionStatuses.Submitted)

    val submissionTestAbortOneMissingWorkflow = Submission(subOneMissingWorkflow,testDate, testData.userOwner, "std","someMethod",sample1.toReference,
      Seq(
        Workflow(existingWorkflowId,WorkflowStatuses.Submitted,testDate,sample1.toReference, testData.inputResolutions),
        Workflow(nonExistingWorkflowId,WorkflowStatuses.Submitted,testDate,sample2.toReference, testData.inputResolutions)),
      Seq.empty[WorkflowFailure], SubmissionStatuses.Submitted)

    val submissionTestAbortTwoGoodWorkflows = Submission(subTwoGoodWorkflows,testDate, testData.userOwner, "std","someMethod",sample1.toReference,
      Seq(
        Workflow(existingWorkflowId,WorkflowStatuses.Submitted,testDate,sample1.toReference, testData.inputResolutions),
        Workflow(alreadyTerminatedWorkflowId,WorkflowStatuses.Submitted,testDate,sample2.toReference, testData.inputResolutions)),
      Seq.empty[WorkflowFailure], SubmissionStatuses.Submitted)

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
          Some(Map("wf.y.six" -> AttributeNumber(4)))),
        "wf.x" -> TaskOutput(
          Some(Seq(ExecutionServiceCallLogs(
            stdout = "gs://cromwell-dev/cromwell-executions/wf/this_workflow_exists/call-x/job.stdout.txt",
            stderr = "gs://cromwell-dev/cromwell-executions/wf/this_workflow_exists/call-x/job.stderr.txt"))),
          Some(Map(
            "wf.x.four" -> AttributeNumber(4),
            "wf.x.five" -> AttributeNumber(4))))))

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
            submissionQuery.create(context, submissionTestAbortTwoGoodWorkflows)
          )
        }
      )
    }
  }

  def withDataAndService(
      testCode: WorkspaceService => Any, 
      withDataOp: (SlickDataSource => Any) => Unit, 
      execService: ExecutionServiceDAO = new HttpExecutionServiceDAO(mockServer.mockServerBaseUrl, mockServer.defaultWorkflowSubmissionTimeout)): Unit = {
    withDataOp { dataSource =>
      val gcsDAO: MockGoogleServicesDAO = new MockGoogleServicesDAO("test")
      val submissionSupervisor = system.actorOf(SubmissionSupervisor.props(
        execService,
        slickDataSource
      ).withDispatcher("submission-monitor-dispatcher"), submissionSupervisorActorName)
      val bucketDeletionMonitor = system.actorOf(BucketDeletionMonitor.props(slickDataSource, gcsDAO))

      gcsDAO.storeToken(userInfo, subTestData.refreshToken)
      val directoryDAO = new MockUserDirectoryDAO

      val userServiceConstructor = UserService.constructor(
        slickDataSource,
        gcsDAO,
        directoryDAO
      )_

      val execServiceBatchSize = 3
      val workspaceServiceConstructor = WorkspaceService.constructor(
        dataSource,
        new HttpMethodRepoDAO(mockServer.mockServerBaseUrl),
        execService,
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
      }
    }
  }

  def withWorkspaceService(testCode: WorkspaceService => Any): Unit = {
    withDataAndService(testCode, withDefaultTestDatabase)
  }

  def withWorkspaceServiceMockExecution(testCode: (MockExecutionServiceDAO) => (WorkspaceService) => Any): Unit = {
    val execSvc = new MockExecutionServiceDAO()
    withDataAndService(testCode(execSvc), withDefaultTestDatabase, execSvc)
  }
  def withWorkspaceServiceMockTimeoutExecution(testCode: (MockExecutionServiceDAO) => (WorkspaceService) => Any): Unit = {
    val execSvc = new MockExecutionServiceDAO(true)
    withDataAndService(testCode(execSvc), withDefaultTestDatabase, execSvc)
  }

  def withSubmissionTestWorkspaceService(testCode: WorkspaceService => Any): Unit = {
    withDataAndService(testCode, withCustomTestDatabase(new SubmissionTestData))
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

    assert(newSubmissionReport.notstarted.size == 0)
    assert(newSubmissionReport.workflows.size == 1)

    checkSubmissionStatus(workspaceService, newSubmissionReport.submissionId)
  }

  it should "return a successful Submission when given an entity expression that evaluates to a set of entities" in withWorkspaceService { workspaceService =>
    val sset = Entity("testset6", "SampleSet",
      Map( "samples" -> AttributeEntityReferenceList( Seq(
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

    assert( newSubmissionReport.notstarted.size == 0 )
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

    val execResponse = Await.result(execWith3SecTimeout.submitWorkflow(wdl, wdlInputs, workflowOptions, userInfo), Duration.Inf)
    assertResult(expectedResponse) { execResponse }

    intercept[AskTimeoutException] {
      Await.result(execWith1SecTimeout.submitWorkflow(wdl, wdlInputs, workflowOptions, userInfo), Duration.Inf)
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

    assert( newSubmissionReport.notstarted.size == 3 )
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

    assert( newSubmissionReport.notstarted.size == 1 )
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
    assert(submissionStatusResponse.notstarted.size == 1)
    assert(submissionStatusResponse.workflows.size == 2)

    // the first fails to start for bad expression issues
    assert(submissionStatusResponse.notstarted.head.errors.head.value == "Expected single value for workflow input, but evaluated result set was empty")

    // the rest are in queued
    assert( submissionStatusResponse.workflows.forall(_.status == WorkflowStatuses.Queued) )
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

    assertResult(testData.sset1.attributes("samples").asInstanceOf[AttributeEntityReferenceList].list.size) { vData.validEntities.length }
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
    assertResult(testData.sset1.attributes("samples").asInstanceOf[AttributeEntityReferenceList].list.size) { vData.invalidEntities.length }
  }

  it should "report validated inputs and a mixture of started and unstarted workflows where method configuration inputs are missing on some entities" in withWorkspaceService { workspaceService =>
    val submissionRq = SubmissionRequest("dsde", "NotAllSamplesMethodConfig", "Individual", "indiv1", Some("this.sset.samples"))
    val vComplete = Await.result(workspaceService.validateSubmission( testData.wsName, submissionRq ), Duration.Inf).asInstanceOf[RequestComplete[(StatusCode, SubmissionValidationReport)]]
    val (vStatus, vData) = vComplete.response
    assertResult(StatusCodes.OK) {
      vStatus
    }

    assertResult(testData.sset1.attributes("samples").asInstanceOf[AttributeEntityReferenceList].list.size-1) { vData.validEntities.length }
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

  "Getting workflow metadata" should "return 200" in withSubmissionTestWorkspaceService { workspaceService =>
    val rqComplete = workspaceService.workflowMetadata(
      subTestData.wsName,
      subTestData.submissionTestAbortGoodWorkflow.submissionId,
      subTestData.existingWorkflowId.get)
    val (status, data) = Await.result(rqComplete, Duration.Inf).asInstanceOf[RequestComplete[(StatusCode, ExecutionMetadata)]].response

    assertResult(StatusCodes.OK) {
      status
    }
  }
}


