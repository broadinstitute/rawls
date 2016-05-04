package org.broadinstitute.dsde.rawls.jobexec

import java.util.UUID
import java.util.concurrent.TimeUnit
import akka.actor.{ActorRef, PoisonPill, ActorSystem}
import akka.pattern.AskTimeoutException
import akka.testkit.{TestKit, TestActorRef}
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

    val existingWorkflowId = "69d1d92f-3895-4a7b-880a-82535e9a096e"
    val nonExistingWorkflowId = "45def17d-40c2-44cc-89bf-9e77bc2c9999"
    val alreadyTerminatedWorkflowId = "45def17d-40c2-44cc-89bf-9e77bc2c8778"
    
    
    
    val submissionTestAbortMissingWorkflow = Submission(subMissingWorkflow,testDate, testData.userOwner, "std","someMethod",Option(AttributeEntityReference(sample1.entityType, sample1.name)),
      Seq(Workflow(nonExistingWorkflowId,WorkflowStatuses.Submitted,testDate,Option(AttributeEntityReference(sample1.entityType, sample1.name)), testData.inputResolutions)),
      Seq.empty[WorkflowFailure], SubmissionStatuses.Submitted)

    val submissionTestAbortMalformedWorkflow = Submission(subMalformedWorkflow,testDate, testData.userOwner, "std","someMethod",Option(AttributeEntityReference(sample1.entityType, sample1.name)),
      Seq(Workflow("malformed_workflow",WorkflowStatuses.Submitted,testDate,Option(AttributeEntityReference(sample1.entityType, sample1.name)), testData.inputResolutions)),
      Seq.empty[WorkflowFailure], SubmissionStatuses.Submitted)

    val submissionTestAbortGoodWorkflow = Submission(subGoodWorkflow,testDate, testData.userOwner, "std","someMethod",Option(AttributeEntityReference(sample1.entityType, sample1.name)),
      Seq(Workflow(existingWorkflowId,WorkflowStatuses.Submitted,testDate,Option(AttributeEntityReference(sample1.entityType, sample1.name)), testData.inputResolutions)),
      Seq.empty[WorkflowFailure], SubmissionStatuses.Submitted)

    val submissionTestAbortTerminalWorkflow = Submission(subTerminalWorkflow,testDate, testData.userOwner, "std","someMethod",Option(AttributeEntityReference(sample1.entityType, sample1.name)),
      Seq(Workflow(alreadyTerminatedWorkflowId,WorkflowStatuses.Submitted,testDate,Option(AttributeEntityReference(sample1.entityType, sample1.name)), testData.inputResolutions)),
      Seq.empty[WorkflowFailure], SubmissionStatuses.Submitted)

    val submissionTestAbortOneMissingWorkflow = Submission(subOneMissingWorkflow,testDate, testData.userOwner, "std","someMethod",Option(AttributeEntityReference(sample1.entityType, sample1.name)),
      Seq(
        Workflow(existingWorkflowId,WorkflowStatuses.Submitted,testDate,Option(AttributeEntityReference(sample1.entityType, sample1.name)), testData.inputResolutions),
        Workflow(nonExistingWorkflowId,WorkflowStatuses.Submitted,testDate,Option(AttributeEntityReference(sample2.entityType, sample2.name)), testData.inputResolutions)),
      Seq.empty[WorkflowFailure], SubmissionStatuses.Submitted)

    val submissionTestAbortTwoGoodWorkflows = Submission(subTwoGoodWorkflows,testDate, testData.userOwner, "std","someMethod",Option(AttributeEntityReference(sample1.entityType, sample1.name)),
      Seq(
        Workflow(existingWorkflowId,WorkflowStatuses.Submitted,testDate,Option(AttributeEntityReference(sample1.entityType, sample1.name)), testData.inputResolutions),
        Workflow(alreadyTerminatedWorkflowId,WorkflowStatuses.Submitted,testDate,Option(AttributeEntityReference(sample2.entityType, sample2.name)), testData.inputResolutions)),
      Seq.empty[WorkflowFailure], SubmissionStatuses.Submitted)

    val extantWorkflowOutputs = WorkflowOutputs( existingWorkflowId,
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

      val workspaceServiceConstructor = WorkspaceService.constructor(
        dataSource,
        new HttpMethodRepoDAO(mockServer.mockServerBaseUrl),
        execService,
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
    val (status, newSubmission) = rqComplete.response
    assertResult(StatusCodes.Created) {
      status
    }
    assertResult("{\"three_step.cgrep.pattern\":\"tumor\"}") {
      mockExecSvc.submitInput
    }

    import ExecutionJsonSupport.ExecutionServiceWorkflowOptionsFormat // implicit format make convertTo work below
    assertResult(Some(ExecutionServiceWorkflowOptions(s"gs://${testData.workspace.bucketName}/${newSubmission.submissionId}", testData.wsName.namespace, userInfo.userEmail, subTestData.refreshToken, testData.billingProject.cromwellAuthBucketUrl))) {
      mockExecSvc.submitOptions.map(_.parseJson.convertTo[ExecutionServiceWorkflowOptions])
    }

    val monitorActor = waitForSubmissionActor(newSubmission.submissionId)
    assert(monitorActor != None) //not really necessary, failing to find the actor above will throw an exception and thus fail this test

    assert(newSubmission.notstarted.size == 0)
    assert(newSubmission.workflows.size == 1)

    checkSubmissionStatus(workspaceService, newSubmission.submissionId)
  }

  it should "return a successful Submission when given an entity expression that evaluates to a set of entities" in withWorkspaceService { workspaceService =>
    val submissionRq = SubmissionRequest("dsde", "GoodMethodConfig", "SampleSet", "sset1", Some("this.samples"))
    val rqComplete = Await.result(workspaceService.createSubmission( testData.wsName, submissionRq ), Duration.Inf).asInstanceOf[RequestComplete[(StatusCode, SubmissionReport)]]

    val (status, newSubmission) = rqComplete.response
    assertResult(StatusCodes.Created) {
      status
    }

    assert( newSubmission.notstarted.size == 0 )
    assert( newSubmission.workflows.size == 3 )

    checkSubmissionStatus(workspaceService, newSubmission.submissionId)
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
    val (status, newSubmission) = rqComplete.response
    assertResult(StatusCodes.Created) {
      status
    }

    assert( newSubmission.notstarted.size == 3 )
    assert( newSubmission.workflows.size == 0 )

    checkSubmissionStatus(workspaceService, newSubmission.submissionId)
  }

  it should "return a successful Submission with unstarted workflows where method configuration inputs are missing on some entities" in withWorkspaceService { workspaceService =>
    val submissionRq = SubmissionRequest("dsde", "NotAllSamplesMethodConfig", "Individual", "indiv1", Some("this.sset.samples"))
    val rqComplete = Await.result(workspaceService.createSubmission( testData.wsName, submissionRq ), Duration.Inf).asInstanceOf[RequestComplete[(StatusCode, SubmissionReport)]]
    val (status, newSubmission) = rqComplete.response
    assertResult(StatusCodes.Created) {
      status
    }

    assert( newSubmission.notstarted.size == 1 )
    assert( newSubmission.workflows.size == 2 )

    checkSubmissionStatus(workspaceService, newSubmission.submissionId)
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

  it should "persist WorkflowFailures for each workflow that timeout when submitting to Cromwell" in withWorkspaceServiceMockTimeoutExecution { mockExecSvc => workspaceService =>
    val submissionRq = SubmissionRequest("dsde", "NotAllSamplesMethodConfig", "Individual", "indiv1", Some("this.sset.samples"))
    val rqComplete = Await.result(workspaceService.createSubmission(testData.wsName, submissionRq), Duration.Inf).asInstanceOf[RequestComplete[(StatusCode, SubmissionReport)]]
    val (status, newSubmission) = rqComplete.response
    assertResult(StatusCodes.Created) {
      status
    }

    val submissionStatusRq = Await.result(workspaceService.getSubmissionStatus(testData.wsName, newSubmission.submissionId), Duration.Inf).asInstanceOf[RequestComplete[(StatusCode, SubmissionStatusResponse)]]
    val (submissionStatus, submissionStatusResponse) = submissionStatusRq.response
    assertResult(StatusCodes.OK) {
      submissionStatus
    }

    // all three workflows should fail to start
    assert(submissionStatusResponse.notstarted.size == 3)
    assert(submissionStatusResponse.workflows.size == 0)

    // the first fails to start for bad expression issues, whereas the other two timed out when trying to submit
    assert(submissionStatusResponse.notstarted(0).errors(0).value == "Expected single value for workflow input, but evaluated result set was empty")
    assert(submissionStatusResponse.notstarted(1).errors(0).value == "Unable to submit workflow when creating submission")
    assert(submissionStatusResponse.notstarted(2).errors(0).value == "Unable to submit workflow when creating submission")

    // all of these workflows fail, ensure the submission status is done
    assertResult(SubmissionStatuses.Done) {
      submissionStatusResponse.status
    }
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

  it should "502 if Cromwell can't find the workflow" in withSubmissionTestWorkspaceService { workspaceService =>
    val rqComplete = workspaceService.abortSubmission(subTestData.wsName, subMissingWorkflow)
    val errorReport = Await.result(rqComplete, Duration.Inf).asInstanceOf[RequestComplete[ErrorReport]].response
    assertResult(StatusCodes.BadGateway) {
      errorReport.statusCode.get
    }
  }

  it should "502 if Cromwell says the workflow is malformed" in withSubmissionTestWorkspaceService { workspaceService =>
    val rqComplete = workspaceService.abortSubmission(subTestData.wsName, subMalformedWorkflow)
    val errorReport = Await.result(rqComplete, Duration.Inf).asInstanceOf[RequestComplete[ErrorReport]].response
    assertResult(StatusCodes.BadGateway) {
      errorReport.statusCode.get
    }
  }

  it should "204 No Content for a valid submission with a single workflow" in withSubmissionTestWorkspaceService { workspaceService =>
    val rqComplete = workspaceService.abortSubmission(subTestData.wsName, subGoodWorkflow)
    val status = Await.result(rqComplete, Duration.Inf).asInstanceOf[RequestComplete[StatusCode]].response
    assertResult(StatusCodes.NoContent) {
      status
    }
  }

  it should "204 No Content for a valid submission and change submission status to Aborting" in withSubmissionTestWorkspaceService { workspaceService =>
    val rqComplete = workspaceService.abortSubmission(subTestData.wsName, subGoodWorkflow)
    val status = Await.result(rqComplete, Duration.Inf).asInstanceOf[RequestComplete[StatusCode]].response
    assertResult(StatusCodes.NoContent) {
      status
    }
    val checkStatus = workspaceService.getSubmissionStatus(subTestData.wsName, subGoodWorkflow)
    Await.result(checkStatus, Duration.Inf) match {
      case RequestComplete((submissionStatus: StatusCode, submissionData: Any)) => {
        assertResult(StatusCodes.OK) { submissionStatus }
        assertResult(SubmissionStatuses.Aborting) { submissionData.asInstanceOf[SubmissionStatusResponse].status }
      }
      case _ => fail("Unable to get submission status")
    }
  }

  it should "204 No Content for a valid submission with a workflow that's already terminated" in withSubmissionTestWorkspaceService { workspaceService =>
    val rqComplete = workspaceService.abortSubmission(subTestData.wsName, subTerminalWorkflow)
//    val status = Await.result(rqComplete, Duration.Inf).asInstanceOf[RequestComplete[(StatusCode)]].response
    assertResult(StatusCodes.NoContent) {
      Await.result(rqComplete, Duration.Inf).asInstanceOf[RequestComplete[StatusCode]].response
    }
  }

  it should "502 if Cromwell says one workflow in a multi-workflow submission is missing" in withSubmissionTestWorkspaceService { workspaceService =>
    val rqComplete = workspaceService.abortSubmission(subTestData.wsName, subOneMissingWorkflow)
    val errorReport = Await.result(rqComplete, Duration.Inf).asInstanceOf[RequestComplete[ErrorReport]].response
    assertResult(StatusCodes.BadGateway) {
      errorReport.statusCode.get
    }
  }

  it should "204 No Content for a valid submission with multiple workflows" in withSubmissionTestWorkspaceService { workspaceService =>
    val rqComplete = workspaceService.abortSubmission(subTestData.wsName, subTwoGoodWorkflows)
    val status = Await.result(rqComplete, Duration.Inf).asInstanceOf[RequestComplete[StatusCode]].response
    assertResult(StatusCodes.NoContent) {
      status
    }
  }

  "Getting workflow outputs" should "return 200 when all is well" in withSubmissionTestWorkspaceService { workspaceService =>
    val rqComplete = workspaceService.workflowOutputs(
      subTestData.wsName,
      subTestData.submissionTestAbortGoodWorkflow.submissionId,
      subTestData.existingWorkflowId)
    val (status, data) = Await.result(rqComplete, Duration.Inf).asInstanceOf[RequestComplete[(StatusCode, WorkflowOutputs)]].response

    assertResult(StatusCodes.OK) {status}
    assertResult(subTestData.extantWorkflowOutputs) {data}
  }

  it should "return 404 on getting a workflow that exists, but not in this submission" in withSubmissionTestWorkspaceService { workspaceService =>
    val rqComplete = workspaceService.workflowOutputs(
      subTestData.wsName,
      subTestData.submissionTestAbortTerminalWorkflow.submissionId,
      subTestData.existingWorkflowId)
    val errorReport = intercept[RawlsExceptionWithErrorReport] {
      Await.result(rqComplete, Duration.Inf).asInstanceOf[RequestComplete[ErrorReport]].response
    }

    assertResult(StatusCodes.NotFound) {errorReport.errorReport.statusCode.get}
  }

  "Getting workflow metadata" should "return 200" in withSubmissionTestWorkspaceService { workspaceService =>
    val rqComplete = workspaceService.workflowMetadata(
      subTestData.wsName,
      subTestData.submissionTestAbortGoodWorkflow.submissionId,
      subTestData.existingWorkflowId)
    val (status, data) = Await.result(rqComplete, Duration.Inf).asInstanceOf[RequestComplete[(StatusCode, ExecutionMetadata)]].response

    assertResult(StatusCodes.OK) {
      status
    }
  }
}

class MockExecutionServiceDAO(timeout:Boolean = false) extends ExecutionServiceDAO {
  var submitWdl: String = null
  var submitInput: String = null
  var submitOptions: Option[String] = None

  override def submitWorkflow(wdl: String, inputs: String, options: Option[String], userInfo: UserInfo) = {
    this.submitInput = inputs
    this.submitWdl = wdl
    this.submitOptions = options
    if (timeout) {
      Future.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.GatewayTimeout, s"Failed to submit")))
    }
    else {
      Future.successful(ExecutionServiceStatus("69d1d92f-3895-4a7b-880a-82535e9a096e", "Submitted"))
    }
  }

  override def logs(id: String, userInfo: UserInfo) = Future.successful(ExecutionServiceLogs(id,
    Map("x" -> Seq(ExecutionServiceCallLogs(
      stdout = "gs://cromwell-dev/cromwell-executions/wf/this_workflow_exists/call-x/job.stdout.txt",
      stderr = "gs://cromwell-dev/cromwell-executions/wf/this_workflow_exists/call-x/job.stderr.txt")))))

  override def outputs(id: String, userInfo: UserInfo) = Future.successful(ExecutionServiceOutputs(id, Map("foo" -> AttributeString("bar"))))

  override def abort(id: String, userInfo: UserInfo) = Future.successful(Success(ExecutionServiceStatus(id, "Aborted")))

  override def status(id: String, userInfo: UserInfo) = Future.successful(ExecutionServiceStatus(id, "Submitted"))

  override def callLevelMetadata(id: String, userInfo: UserInfo) = Future.successful(null)
}
