package org.broadinstitute.dsde.rawls.jobexec

import akka.actor.{ActorRef, ActorSystem, PoisonPill}
import akka.http.scaladsl.model.StatusCodes
import akka.stream.ActorMaterializer
import akka.testkit.TestKit
import bio.terra.datarepo.model.{ColumnModel, TableModel}
import bio.terra.workspace.model.CloningInstructionsEnum
import com.google.cloud.PageImpl
import com.google.cloud.bigquery.{Option => _, _}
import com.typesafe.config.ConfigFactory
import org.broadinstitute.dsde.rawls.billing.{BillingProfileManagerClientProvider, BillingProfileManagerDAOImpl}
import org.broadinstitute.dsde.rawls.config._
import org.broadinstitute.dsde.rawls.coordination.UncoordinatedDataSourceAccess
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.dataaccess.datarepo.DataRepoDAO
import org.broadinstitute.dsde.rawls.dataaccess.resourcebuffer.ResourceBufferDAO
import org.broadinstitute.dsde.rawls.dataaccess.slick.{TestData, TestDriverComponent}
import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.WorkspaceManagerDAO
import org.broadinstitute.dsde.rawls.entities.EntityManager
import org.broadinstitute.dsde.rawls.entities.datarepo.DataRepoEntityProviderSpecSupport
import org.broadinstitute.dsde.rawls.fastpass.FastPassService
import org.broadinstitute.dsde.rawls.genomics.GenomicsService
import org.broadinstitute.dsde.rawls.metrics.StatsDTestUtils
import org.broadinstitute.dsde.rawls.mock._
import org.broadinstitute.dsde.rawls.model.SubmissionRetryStatuses.RetryAborted
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.resourcebuffer.ResourceBufferService
import org.broadinstitute.dsde.rawls.serviceperimeter.ServicePerimeterService
import org.broadinstitute.dsde.rawls.user.UserService
import org.broadinstitute.dsde.rawls.util.MockitoTestUtils
import org.broadinstitute.dsde.rawls.workspace.{
  MultiCloudWorkspaceAclManager,
  RawlsWorkspaceAclManager,
  WorkspaceService
}
import org.broadinstitute.dsde.rawls.{RawlsException, RawlsExceptionWithErrorReport, RawlsTestUtils}
import org.broadinstitute.dsde.workbench.dataaccess.{NotificationDAO, PubSubNotificationDAO}
import org.broadinstitute.dsde.workbench.google.mock.{MockGoogleBigQueryDAO, MockGoogleIamDAO, MockGoogleStorageDAO}
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.openTelemetry.FakeOpenTelemetryMetricsInterpreter
import org.mockito.Mockito._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import spray.json._

import java.time.temporal.ChronoUnit
import java.util.UUID
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.language.postfixOps
import scala.util.Try

/**
 * Created with IntelliJ IDEA.
 * User: hussein
 * Date: 07/02/2015
 * Time: 11:06
 */
//noinspection TypeAnnotation,ScalaUnusedSymbol
class SubmissionSpec(_system: ActorSystem)
    extends TestKit(_system)
    with AnyFlatSpecLike
    with Matchers
    with TestDriverComponent
    with BeforeAndAfterAll
    with Eventually
    with MockitoTestUtils
    with StatsDTestUtils
    with RawlsTestUtils
    with DataRepoEntityProviderSpecSupport {
  import driver.api._

  def this() = this(ActorSystem("SubmissionSpec"))
  implicit val materializer = ActorMaterializer()

  val testDbName = "SubmissionSpec"
  val submissionSupervisorActorName = "test-subspec-submission-supervisor"

  val mockServer = RemoteServicesMockServer()

  val bigQueryDAO = new MockGoogleBigQueryDAO
  val mockSubmissionCostService = new MockSubmissionCostService(
    "fakeTableName",
    "fakeDatePartitionColumn",
    "fakeServiceProject",
    31,
    bigQueryDAO
  )

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
  var subToRetry = UUID.randomUUID().toString
  var subCromwellBadWorkflows = UUID.randomUUID().toString

  val subTestData = new SubmissionTestData()

  class SubmissionTestData() extends TestData {
    val billingProject =
      RawlsBillingProject(RawlsBillingProjectName("myNamespacexxx"), CreationStatuses.Ready, None, None)
    val wsName = WorkspaceName(billingProject.projectName.value, "myWorkspace")
    val user = RawlsUser(userInfo)
    val ownerGroup = makeRawlsGroup("workspaceOwnerGroup", Set(user))
    val workspace = Workspace(wsName.namespace,
                              wsName.name,
                              UUID.randomUUID().toString,
                              "aBucket",
                              Some("workflow-collection"),
                              currentTime(),
                              currentTime(),
                              "testUser",
                              Map.empty
    )

    val sample1 = Entity("sample1", "Sample", Map(AttributeName.withDefaultNS("type") -> AttributeString("normal")))
    val sample2 = Entity("sample2", "Sample", Map(AttributeName.withDefaultNS("type") -> AttributeString("normal")))

    val existingWorkflowId = Option("69d1d92f-3895-4a7b-880a-82535e9a096e")
    val nonExistingWorkflowId = Option("45def17d-40c2-44cc-89bf-9e77bc2c9999")
    val alreadyTerminatedWorkflowId = Option("45def17d-40c2-44cc-89bf-9e77bc2c8778")
    val badLogsAndMetadataWorkflowId = Option("29b2e816-ecaf-11e6-b006-92361f002671")

    val submissionTestAbortMissingWorkflow = Submission(
      subMissingWorkflow,
      testDate,
      WorkbenchEmail(testData.userOwner.userEmail.value),
      "std",
      "someMethod",
      Some(sample1.toReference),
      submissionRoot = "gs://fc-someWorkspaceId/someSubmissionId",
      workflows = Seq(
        Workflow(
          workflowId = nonExistingWorkflowId,
          status = WorkflowStatuses.Submitted,
          statusLastChangedDate = testDate,
          workflowEntity = Option(sample1.toReference),
          inputResolutions = testData.inputResolutions
        )
      ),
      status = SubmissionStatuses.Submitted,
      useCallCache = false,
      deleteIntermediateOutputFiles = false
    )

    val submissionTestAbortMalformedWorkflow = Submission(
      subMalformedWorkflow,
      testDate,
      WorkbenchEmail(testData.userOwner.userEmail.value),
      "std",
      "someMethod",
      Some(sample1.toReference),
      submissionRoot = "gs://fc-someWorkspaceId/someSubmissionId",
      Seq(
        Workflow(
          Option("malformed_workflow"),
          WorkflowStatuses.Submitted,
          testDate,
          Option(sample1.toReference),
          testData.inputResolutions
        )
      ),
      SubmissionStatuses.Submitted,
      useCallCache = false,
      deleteIntermediateOutputFiles = false
    )

    val submissionTestAbortGoodWorkflow = Submission(
      subGoodWorkflow,
      testDate,
      WorkbenchEmail(testData.userOwner.userEmail.value),
      "std",
      "someMethod",
      Some(sample1.toReference),
      submissionRoot = "gs://fc-someWorkspaceId/someSubmissionId",
      workflows = Seq(
        Workflow(
          workflowId = existingWorkflowId,
          status = WorkflowStatuses.Submitted,
          statusLastChangedDate = testDate,
          workflowEntity = Option(sample1.toReference),
          inputResolutions = testData.inputResolutions
        )
      ),
      status = SubmissionStatuses.Submitted,
      useCallCache = false,
      deleteIntermediateOutputFiles = false
    )

    val submissionTestAbortTerminalWorkflow = Submission(
      subTerminalWorkflow,
      testDate,
      WorkbenchEmail(testData.userOwner.userEmail.value),
      "std",
      "someMethod",
      Some(sample1.toReference),
      submissionRoot = "gs://fc-someWorkspaceId/someSubmissionId",
      workflows = Seq(
        Workflow(
          workflowId = alreadyTerminatedWorkflowId,
          status = WorkflowStatuses.Submitted,
          statusLastChangedDate = testDate,
          workflowEntity = Option(sample1.toReference),
          inputResolutions = testData.inputResolutions
        )
      ),
      status = SubmissionStatuses.Submitted,
      useCallCache = false,
      deleteIntermediateOutputFiles = false
    )

    val submissionTestAbortOneMissingWorkflow = Submission(
      subOneMissingWorkflow,
      testDate,
      WorkbenchEmail(testData.userOwner.userEmail.value),
      "std",
      "someMethod",
      Some(sample1.toReference),
      submissionRoot = "gs://fc-someWorkspaceId/someSubmissionId",
      workflows = Seq(
        Workflow(
          workflowId = existingWorkflowId,
          status = WorkflowStatuses.Submitted,
          statusLastChangedDate = testDate,
          workflowEntity = Option(sample1.toReference),
          inputResolutions = testData.inputResolutions
        ),
        Workflow(
          workflowId = nonExistingWorkflowId,
          status = WorkflowStatuses.Submitted,
          statusLastChangedDate = testDate,
          workflowEntity = Option(sample2.toReference),
          inputResolutions = testData.inputResolutions
        )
      ),
      status = SubmissionStatuses.Submitted,
      useCallCache = false,
      deleteIntermediateOutputFiles = false
    )

    val submissionTestAbortTwoGoodWorkflows = Submission(
      subTwoGoodWorkflows,
      testDate,
      WorkbenchEmail(testData.userOwner.userEmail.value),
      "std",
      "someMethod",
      Some(sample1.toReference),
      submissionRoot = "gs://fc-someWorkspaceId/someSubmissionId",
      workflows = Seq(
        Workflow(
          workflowId = existingWorkflowId,
          status = WorkflowStatuses.Submitted,
          statusLastChangedDate = testDate,
          workflowEntity = Option(sample1.toReference),
          inputResolutions = testData.inputResolutions
        ),
        Workflow(
          workflowId = alreadyTerminatedWorkflowId,
          status = WorkflowStatuses.Submitted,
          statusLastChangedDate = testDate,
          workflowEntity = Option(sample2.toReference),
          inputResolutions = testData.inputResolutions
        )
      ),
      status = SubmissionStatuses.Submitted,
      useCallCache = false,
      deleteIntermediateOutputFiles = false
    )

    val submissionToRetry = Submission(
      subToRetry,
      testDate,
      WorkbenchEmail("foo-bar"),
      "std",
      "someMethod",
      Some(sample1.toReference),
      submissionRoot = "gs://fc-someWorkspaceId/someSubmissionId",
      workflows = Seq(
        Workflow(
          workflowId = existingWorkflowId,
          status = WorkflowStatuses.Aborted,
          statusLastChangedDate = testDate,
          workflowEntity = Option(sample1.toReference),
          inputResolutions = testData.inputResolutions
        ),
        Workflow(
          workflowId = alreadyTerminatedWorkflowId,
          status = WorkflowStatuses.Aborted,
          statusLastChangedDate = testDate,
          workflowEntity = Option(sample2.toReference),
          inputResolutions = testData.inputResolutions
        )
      ),
      status = SubmissionStatuses.Submitted,
      useCallCache = false,
      deleteIntermediateOutputFiles = false
    )

    val submissionTestCromwellBadWorkflows = Submission(
      subCromwellBadWorkflows,
      testDate,
      WorkbenchEmail(testData.userOwner.userEmail.value),
      "std",
      "someMethod",
      Some(sample1.toReference),
      submissionRoot = "gs://fc-someWorkspaceId/someSubmissionId",
      workflows = Seq(
        Workflow(
          workflowId = badLogsAndMetadataWorkflowId,
          status = WorkflowStatuses.Submitted,
          statusLastChangedDate = testDate,
          workflowEntity = Option(sample1.toReference),
          inputResolutions = testData.inputResolutions
        )
      ),
      status = SubmissionStatuses.Submitted,
      useCallCache = false,
      deleteIntermediateOutputFiles = false
    )

    val extantWorkflowOutputs = WorkflowOutputs(
      existingWorkflowId.get,
      Map(
        "wf.y" -> TaskOutput(
          Some(
            Seq(
              ExecutionServiceCallLogs(
                stdout = "gs://cromwell-dev/cromwell-executions/wf/this_workflow_exists/call-y/job.stdout-1.txt",
                stderr = "gs://cromwell-dev/cromwell-executions/wf/this_workflow_exists/call-y/job.stderr-1.txt",
                backendLogs = Some(
                  Map(
                    "log" -> "gs://cromwell-dev/cromwell-executions/wf/this_workflow_exists/call-y/jes.log",
                    "stdout" -> "gs://cromwell-dev/cromwell-executions/wf/this_workflow_exists/call-y/jes-stdout.log",
                    "stderr" -> "gs://cromwell-dev/cromwell-executions/wf/this_workflow_exists/call-y/jes-stderr.log"
                  )
                )
              ),
              ExecutionServiceCallLogs(
                stdout = "gs://cromwell-dev/cromwell-executions/wf/this_workflow_exists/call-y/job.stdout-2.txt",
                stderr = "gs://cromwell-dev/cromwell-executions/wf/this_workflow_exists/call-y/job.stderr-2.txt"
              )
            )
          ),
          Some(Map("wf.y.six" -> Left(AttributeNumber(4))))
        ),
        "wf.x" -> TaskOutput(
          Some(
            Seq(
              ExecutionServiceCallLogs(
                stdout = "gs://cromwell-dev/cromwell-executions/wf/this_workflow_exists/call-x/job.stdout.txt",
                stderr = "gs://cromwell-dev/cromwell-executions/wf/this_workflow_exists/call-x/job.stderr.txt"
              )
            )
          ),
          Some(Map("wf.x.four" -> Left(AttributeNumber(4)), "wf.x.five" -> Left(AttributeNumber(4))))
        )
      )
    )

    override def save() =
      DBIO.seq(
        rawlsBillingProjectQuery.create(billingProject),
        workspaceQuery.createOrUpdate(workspace),
        withWorkspaceContext(workspace) { context =>
          DBIO.seq(
            entityQuery.save(context, sample1),
            entityQuery.save(context, sample2),
            methodConfigurationQuery.create(context,
                                            MethodConfiguration("std",
                                                                "someMethod",
                                                                Some("Sample"),
                                                                None,
                                                                Map.empty,
                                                                Map.empty,
                                                                AgoraMethod("std", "someMethod", 1)
                                            )
            ),
            submissionQuery.create(context, submissionTestAbortMissingWorkflow),
            submissionQuery.create(context, submissionTestAbortMalformedWorkflow),
            submissionQuery.create(context, submissionTestAbortGoodWorkflow),
            submissionQuery.create(context, submissionTestAbortTerminalWorkflow),
            submissionQuery.create(context, submissionTestAbortOneMissingWorkflow),
            submissionQuery.create(context, submissionTestAbortTwoGoodWorkflows),
            submissionQuery.create(context, submissionTestCromwellBadWorkflows),
            submissionQuery.create(context, submissionToRetry),
            // update exec key for all test data workflows that have been started.
            updateWorkflowExecutionServiceKey("unittestdefault")
          )
        }
      )
  }

  def withDataAndService[T](testCode: WorkspaceService => T,
                            withDataOp: (SlickDataSource => T) => T,
                            executionServiceDAO: ExecutionServiceDAO =
                              new HttpExecutionServiceDAO(mockServer.mockServerBaseUrl, workbenchMetricBaseName),
                            bigQueryServiceFactory: GoogleBigQueryServiceFactory =
                              MockBigQueryServiceFactory.ioFactory(),
                            dataRepoDAO: DataRepoDAO = mock[DataRepoDAO](RETURNS_SMART_NULLS)
  ): T = {

    withDataOp { dataSource =>
      val execServiceCluster: ExecutionServiceCluster =
        MockShardedExecutionServiceCluster.fromDAO(executionServiceDAO, dataSource)
      implicit val openTelemetry = FakeOpenTelemetryMetricsInterpreter

      val config =
        SubmissionMonitorConfig(250.milliseconds, 30 days, trackDetailedSubmissionMetrics = true, 20000, false)
      val gcsDAO: MockGoogleServicesDAO = new MockGoogleServicesDAO("test")
      val mockNotificationDAO: NotificationDAO = mock[NotificationDAO]
      val samDAO = new MockSamDAO(dataSource)
      val gpsDAO = new org.broadinstitute.dsde.workbench.google.mock.MockGooglePubSubDAO
      val submissionSupervisor = system.actorOf(
        SubmissionSupervisor
          .props(
            execServiceCluster,
            new UncoordinatedDataSourceAccess(slickDataSource),
            samDAO,
            gcsDAO,
            mockNotificationDAO,
            gcsDAO.getBucketServiceAccountCredential,
            config,
            workbenchMetricBaseName = workbenchMetricBaseName
          )
          .withDispatcher("submission-monitor-dispatcher"),
        submissionSupervisorActorName
      )

      val testConf = ConfigFactory.load()

      val notificationDAO = new PubSubNotificationDAO(gpsDAO, "test-notification-topic")

      val servicePerimeterServiceConfig = ServicePerimeterServiceConfig(testConf)
      val servicePerimeterService = new ServicePerimeterService(slickDataSource, gcsDAO, servicePerimeterServiceConfig)

      val billingProfileManagerDAO = new BillingProfileManagerDAOImpl(
        mock[BillingProfileManagerClientProvider],
        new MultiCloudWorkspaceConfig(false, None, None)
      )

      val userServiceConstructor = UserService.constructor(
        slickDataSource,
        gcsDAO,
        samDAO,
        MockBigQueryServiceFactory.ioFactory(),
        testConf.getString("gcs.pathToCredentialJson"),
        "requesterPaysRole",
        DeploymentManagerConfig(testConf.getConfig("gcs.deploymentManager")),
        ProjectTemplate.from(testConf.getConfig("gcs.projectTemplate")),
        servicePerimeterService,
        RawlsBillingAccountName("billingAccounts/ABCDE-FGHIJ-KLMNO"),
        billingProfileManagerDAO,
        mock[WorkspaceManagerDAO],
        mock[NotificationDAO]
      ) _

      val genomicsServiceConstructor = GenomicsService.constructor(
        slickDataSource,
        gcsDAO
      ) _

      val execServiceBatchSize = 3
      val maxActiveWorkflowsTotal = 10
      val maxActiveWorkflowsPerUser = 2
      val workspaceServiceConfig = WorkspaceServiceConfig(
        trackDetailedSubmissionMetrics = true,
        "fc-",
        "us-central1"
      )

      val bondApiDAO: BondApiDAO = new MockBondApiDAO(bondBaseUrl = "bondUrl")
      val requesterPaysSetupService =
        new RequesterPaysSetupService(slickDataSource, gcsDAO, bondApiDAO, requesterPaysRole = "requesterPaysRole")

      val workspaceManagerDAO = new MockWorkspaceManagerDAO
      val entityManager = EntityManager.defaultEntityManager(
        dataSource,
        workspaceManagerDAO,
        dataRepoDAO,
        samDAO,
        bigQueryServiceFactory,
        DataRepoEntityProviderConfig(100, 10000, 0),
        testConf.getBoolean("entityStatisticsCache.enabled"),
        workbenchMetricBaseName
      )

      val resourceBufferDAO: ResourceBufferDAO = new MockResourceBufferDAO
      val resourceBufferConfig = ResourceBufferConfig(testConf.getConfig("resourceBuffer"))
      val resourceBufferService = new ResourceBufferService(resourceBufferDAO, resourceBufferConfig)
      val resourceBufferSaEmail = resourceBufferConfig.saEmail

      val fastPassConfig = FastPassConfig.apply(testConf)
      val fastPassServiceConstructor = FastPassService.constructor(
        fastPassConfig,
        new MockGoogleIamDAO,
        new MockGoogleStorageDAO,
        samDAO,
        terraBillingProjectOwnerRole = "fakeTerraBillingProjectOwnerRole",
        terraWorkspaceCanComputeRole = "fakeTerraWorkspaceCanComputeRole",
        terraWorkspaceNextflowRole = "fakeTerraWorkspaceNextflowRole",
        terraBucketReaderRole = "fakeTerraBucketReaderRole",
        terraBucketWriterRole = "fakeTerraBucketWriterRole",
        workbenchMetricBaseName
      ) _

      val workspaceServiceConstructor = WorkspaceService.constructor(
        dataSource,
        new HttpMethodRepoDAO(
          MethodRepoConfig[Agora.type](mockServer.mockServerBaseUrl, ""),
          MethodRepoConfig[Dockstore.type](mockServer.mockServerBaseUrl, ""),
          workbenchMetricBaseName = workbenchMetricBaseName
        ),
        new HttpExecutionServiceDAO(mockServer.mockServerBaseUrl, workbenchMetricBaseName = workbenchMetricBaseName),
        execServiceCluster,
        execServiceBatchSize,
        workspaceManagerDAO,
        methodConfigResolver,
        gcsDAO,
        samDAO,
        notificationDAO,
        userServiceConstructor,
        genomicsServiceConstructor,
        maxActiveWorkflowsTotal,
        maxActiveWorkflowsPerUser,
        workbenchMetricBaseName,
        mockSubmissionCostService,
        workspaceServiceConfig,
        requesterPaysSetupService,
        entityManager,
        resourceBufferService,
        resourceBufferSaEmail,
        servicePerimeterService,
        googleIamDao = new MockGoogleIamDAO,
        terraBillingProjectOwnerRole = "fakeTerraBillingProjectOwnerRole",
        terraWorkspaceCanComputeRole = "fakeTerraWorkspaceCanComputeRole",
        terraWorkspaceNextflowRole = "fakeTerraWorkspaceNextflowRole",
        terraBucketReaderRole = "fakeTerraBucketReaderRole",
        terraBucketWriterRole = "fakeTerraBucketWriterRole",
        new RawlsWorkspaceAclManager(samDAO),
        new MultiCloudWorkspaceAclManager(workspaceManagerDAO, samDAO, billingProfileManagerDAO, dataSource),
        fastPassServiceConstructor
      ) _
      lazy val workspaceService: WorkspaceService = workspaceServiceConstructor(testContext)
      try
        testCode(workspaceService)
      finally
        // for failed tests we also need to poison pill
        submissionSupervisor ! PoisonPill
    }
  }

  def withWorkspaceService[T](testCode: WorkspaceService => T): T =
    withDataAndService(testCode, withDefaultTestDatabase[T])

  def withWorkspaceServiceMockExecution[T](testCode: MockExecutionServiceDAO => WorkspaceService => T): T = {
    val execSvcDAO = new MockExecutionServiceDAO()
    withDataAndService(testCode(execSvcDAO), withDefaultTestDatabase[T], execSvcDAO)
  }
  def withWorkspaceServiceMockTimeoutExecution[T](testCode: MockExecutionServiceDAO => WorkspaceService => T): T = {
    val execSvcDAO = new MockExecutionServiceDAO(true)
    withDataAndService(testCode(execSvcDAO), withDefaultTestDatabase[T], execSvcDAO)
  }

  def withSubmissionTestWorkspaceService[T](testCode: WorkspaceService => T): T =
    withDataAndService(testCode, withCustomTestDatabase[T](new SubmissionTestData))

  private def checkSubmissionStatus(workspaceService: WorkspaceService,
                                    submissionId: String,
                                    workspaceName: WorkspaceName = testData.wsName
  ): Submission = {
    val submissionStatusRqComplete = workspaceService.getSubmissionStatus(workspaceName, submissionId)

    Await.result(submissionStatusRqComplete, Duration.Inf) match {
      case submissionData: Any =>
        val submissionStatusResponse = submissionData.asInstanceOf[Submission]
        assertResult(submissionId)(submissionStatusResponse.submissionId)
        submissionStatusResponse
      case _ => fail("Unable to get submission status")
    }
  }

  "Submission requests" should "400 when given an unparseable entity expression" in withWorkspaceService {
    workspaceService =>
      val submissionRq = SubmissionRequest(
        methodConfigurationNamespace = "dsde",
        methodConfigurationName = "GoodMethodConfig",
        entityType = Option("Individual"),
        entityName = Option("indiv1"),
        expression = Option("this.is."),
        useCallCache = false,
        deleteIntermediateOutputFiles = false
      )
      val rqComplete = intercept[RawlsExceptionWithErrorReport] {
        Await.result(workspaceService.createSubmission(testData.wsName, submissionRq), Duration.Inf)
      }
      assertResult(StatusCodes.BadRequest) {
        rqComplete.errorReport.statusCode.get
      }
  }

  private def waitForSubmissionActor(submissionId: String) = {
    var subActor: Option[ActorRef] = None
    awaitCond(
      {
        val tr = Try(
          Await.result(system
                         .actorSelection("/user/" + submissionSupervisorActorName + "/" + submissionId)
                         .resolveOne(100 milliseconds),
                       Duration.Inf
          )
        )
        subActor = tr.toOption
        tr.isSuccess
      },
      1 second
    )
    subActor.get
  }

  it should "return a successful Submission and spawn a submission monitor actor when given an entity expression that evaluates to a single entity" in withWorkspaceServiceMockExecution {
    mockExecSvc => workspaceService =>
      val submissionRq = SubmissionRequest(
        methodConfigurationNamespace = "dsde",
        methodConfigurationName = "GoodMethodConfig",
        entityType = Option("Pair"),
        entityName = Option("pair1"),
        expression = Option("this.case"),
        useCallCache = false,
        deleteIntermediateOutputFiles = false
      )
      val newSubmissionReport =
        Await.result(workspaceService.createSubmission(testData.wsName, submissionRq), Duration.Inf)

      val monitorActor = waitForSubmissionActor(newSubmissionReport.submissionId)
      // not really necessary, failing to find the actor above will throw an exception and thus fail this test
      assert(monitorActor != ActorRef.noSender)

      assert(newSubmissionReport.workflows.size == 1)

      checkSubmissionStatus(workspaceService, newSubmissionReport.submissionId)
  }

  it should "continue to monitor a Submission on a deleted entity" in withWorkspaceServiceMockExecution {
    mockExecSvc => workspaceService =>
      val submissionRq = SubmissionRequest(
        methodConfigurationNamespace = "dsde",
        methodConfigurationName = "GoodMethodConfig",
        entityType = Option("Pair"),
        entityName = Option("pair1"),
        expression = Option("this.case"),
        useCallCache = false,
        deleteIntermediateOutputFiles = false
      )
      val newSubmissionReport =
        Await.result(workspaceService.createSubmission(testData.wsName, submissionRq), Duration.Inf)

      runAndWait(entityQuery.hide(testData.workspace, Seq(testData.pair1.toReference)))

      val monitorActor = waitForSubmissionActor(newSubmissionReport.submissionId)
      // not really necessary, failing to find the actor above will throw an exception and thus fail this test
      assert(monitorActor != ActorRef.noSender)

      assert(newSubmissionReport.workflows.size == 1)

      checkSubmissionStatus(workspaceService, newSubmissionReport.submissionId)
  }

  it should "fail to submit when given an entity expression that evaluates to a deleted entity" in withWorkspaceServiceMockExecution {
    mockExecSvc => workspaceService =>
      runAndWait(entityQuery.hide(testData.workspace, Seq(testData.pair1.toReference)))

      val submissionRq = SubmissionRequest(
        methodConfigurationNamespace = "dsde",
        methodConfigurationName = "GoodMethodConfig",
        entityType = Option("Pair"),
        entityName = Option("pair1"),
        expression = Option("this.case"),
        useCallCache = false,
        deleteIntermediateOutputFiles = false
      )
      intercept[RawlsException] {
        Await.result(workspaceService.createSubmission(testData.wsName, submissionRq), Duration.Inf)
      }
  }

  it should "return a successful Submission when given an entity expression that evaluates to a set of entities" in withWorkspaceService {
    workspaceService =>
      val sset = Entity(
        "testset6",
        "SampleSet",
        Map(
          AttributeName.withDefaultNS("samples") -> AttributeEntityReferenceList(
            Seq(
              AttributeEntityReference("Sample", "sample1"),
              AttributeEntityReference("Sample", "sample2"),
              AttributeEntityReference("Sample", "sample3"),
              AttributeEntityReference("Sample", "sample4"),
              AttributeEntityReference("Sample", "sample5"),
              AttributeEntityReference("Sample", "sample6")
            )
          )
        )
      )

      runAndWait(entityQuery.save(testData.workspace, sset))

      val submissionRq = SubmissionRequest(
        methodConfigurationNamespace = "dsde",
        methodConfigurationName = "GoodMethodConfig",
        entityType = Option(sset.entityType),
        entityName = Option(sset.name),
        expression = Option("this.samples"),
        useCallCache = false,
        deleteIntermediateOutputFiles = false
      )
      val newSubmissionReport =
        Await.result(workspaceService.createSubmission(testData.wsName, submissionRq), Duration.Inf)

      assert(newSubmissionReport.workflows.size == 6)

      checkSubmissionStatus(workspaceService, newSubmissionReport.submissionId)

      val submission = runAndWait(submissionQuery.loadSubmission(UUID.fromString(newSubmissionReport.submissionId))).get
      assert(submission.workflows.forall(_.status == WorkflowStatuses.Queued))
  }

  it should "return a successful Submission when given an wdl struct entity expression that evaluates to a set of entities" in withWorkspaceService {
    workspaceService =>
      val sample1 = Entity(
        "sample1",
        "Sample",
        Map(
          AttributeName.withDefaultNS("participant_id") -> AttributeNumber(101),
          AttributeName.withDefaultNS("sample_name") -> AttributeString("sample1")
        )
      )
      val sample2 = Entity(
        "sample2",
        "Sample",
        Map(
          AttributeName.withDefaultNS("participant_id") -> AttributeNumber(102),
          AttributeName.withDefaultNS("sample_name") -> AttributeString("sample2")
        )
      )
      val sample3 = Entity(
        "sample3",
        "Sample",
        Map(
          AttributeName.withDefaultNS("participant_id") -> AttributeNumber(103),
          AttributeName.withDefaultNS("sample_name") -> AttributeString("sample3")
        )
      )
      val sample4 = Entity(
        "sample4",
        "Sample",
        Map(
          AttributeName.withDefaultNS("participant_id") -> AttributeNumber(104),
          AttributeName.withDefaultNS("sample_name") -> AttributeString("sample4")
        )
      )

      val sset = Entity(
        "testset6",
        "SampleSet",
        Map(
          AttributeName.withDefaultNS("samples") -> AttributeEntityReferenceList(
            Seq(
              sample1.toReference,
              sample2.toReference,
              sample3.toReference,
              sample4.toReference
            )
          )
        )
      )

      val expectedInputResolutions = Seq(
        AttributeValueRawJson("""{"id":101,"sample_name":"sample1"}"""),
        AttributeValueRawJson("""{"id":102,"sample_name":"sample2"}"""),
        AttributeValueRawJson("""{"id":103,"sample_name":"sample3"}"""),
        AttributeValueRawJson("""{"id":104,"sample_name":"sample4"}""")
      )

      runAndWait(entityQuery.save(testData.workspace, sample1))
      runAndWait(entityQuery.save(testData.workspace, sample2))
      runAndWait(entityQuery.save(testData.workspace, sample3))
      runAndWait(entityQuery.save(testData.workspace, sample4))
      runAndWait(entityQuery.save(testData.workspace, sset))

      val submissionRq = SubmissionRequest(
        methodConfigurationNamespace = "dsde",
        methodConfigurationName = "WdlStructConfig",
        entityType = Option(sset.entityType),
        entityName = Option(sset.name),
        expression = Option("this.samples"),
        useCallCache = false,
        deleteIntermediateOutputFiles = false
      )
      val newSubmissionReport =
        Await.result(workspaceService.createSubmission(testData.wsName, submissionRq), Duration.Inf)

      assert(newSubmissionReport.workflows.size == 4)

      checkSubmissionStatus(workspaceService, newSubmissionReport.submissionId)

      val submission = runAndWait(submissionQuery.loadSubmission(UUID.fromString(newSubmissionReport.submissionId))).get
      val actualInputResolutions = submission.workflows.flatMap(_.inputResolutions.map(_.value.get))
      assert(submission.workflows.forall(_.status == WorkflowStatuses.Queued))
      assertSameElements(expectedInputResolutions, actualInputResolutions)
  }

  it should "400 when given an entity expression that evaluates to an empty set of entities" in withWorkspaceService {
    workspaceService =>
      val submissionRq = SubmissionRequest(
        methodConfigurationNamespace = "dsde",
        methodConfigurationName = "GoodMethodConfig",
        entityType = Option("SampleSet"),
        entityName = Option("sset_empty"),
        expression = Option("this.samples"),
        useCallCache = false,
        deleteIntermediateOutputFiles = false
      )
      val rqComplete = intercept[RawlsExceptionWithErrorReport] {
        Await.result(workspaceService.createSubmission(testData.wsName, submissionRq), Duration.Inf)
      }
      assertResult(StatusCodes.BadRequest) {
        rqComplete.errorReport.statusCode.get
      }
  }

  it should "400 when given a method configuration with unparseable inputs" in withWorkspaceService {
    workspaceService =>
      val submissionRq = SubmissionRequest(
        methodConfigurationNamespace = "dsde",
        methodConfigurationName = "UnparseableInputsMethodConfig",
        entityType = Option("Individual"),
        entityName = Option("indiv1"),
        expression = Option("this.sset.samples"),
        useCallCache = false,
        deleteIntermediateOutputFiles = false
      )
      val rqComplete = intercept[RawlsExceptionWithErrorReport] {
        Await.result(workspaceService.createSubmission(testData.wsName, submissionRq), Duration.Inf)
      }

      assertResult(StatusCodes.BadRequest) {
        rqComplete.errorReport.statusCode.get
      }

      assert {
        rqComplete.errorReport.message.contains("Invalid inputs: three_step.cgrep.pattern")
      }
  }

  it should "400 when given a method configuration with unparseable outputs" in withWorkspaceService {
    workspaceService =>
      val submissionRq = SubmissionRequest(
        methodConfigurationNamespace = "dsde",
        methodConfigurationName = "UnparseableOutputsMethodConfig",
        entityType = Option("Individual"),
        entityName = Option("indiv1"),
        expression = Option("this.sset.samples"),
        useCallCache = false,
        deleteIntermediateOutputFiles = false
      )
      val rqComplete = intercept[RawlsExceptionWithErrorReport] {
        Await.result(workspaceService.createSubmission(testData.wsName, submissionRq), Duration.Inf)
      }

      assertResult(StatusCodes.BadRequest) {
        rqComplete.errorReport.statusCode.get
      }

      assert {
        rqComplete.errorReport.message.contains("Invalid outputs: three_step.cgrep.count")
      }
  }

  it should "400 when given a method configuration with unparseable inputs and outputs" in withWorkspaceService {
    workspaceService =>
      val submissionRq = SubmissionRequest(
        methodConfigurationNamespace = "dsde",
        methodConfigurationName = "UnparseableBothMethodConfig",
        entityType = Option("Individual"),
        entityName = Option("indiv1"),
        expression = Option("this.sset.samples"),
        useCallCache = false,
        deleteIntermediateOutputFiles = false
      )
      val rqComplete = intercept[RawlsExceptionWithErrorReport] {
        Await.result(workspaceService.createSubmission(testData.wsName, submissionRq), Duration.Inf)
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

  it should "return a successful Submission when given a method configuration with empty outputs" in withWorkspaceService {
    workspaceService =>
      val submissionRq = SubmissionRequest(
        methodConfigurationNamespace = "dsde",
        methodConfigurationName = "EmptyOutputsMethodConfig",
        entityType = Option("Individual"),
        entityName = Option("indiv1"),
        expression = Option("this.sset.samples"),
        useCallCache = false,
        deleteIntermediateOutputFiles = false
      )
      val newSubmissionReport =
        Await.result(workspaceService.createSubmission(testData.wsName, submissionRq), Duration.Inf)

      assert(newSubmissionReport.workflows.size == 3)

      checkSubmissionStatus(workspaceService, newSubmissionReport.submissionId)
  }

  it should "return a successful Submission with unstarted workflows where method configuration inputs are missing on some entities" in withWorkspaceService {
    workspaceService =>
      val submissionRq = SubmissionRequest(
        methodConfigurationNamespace = "dsde",
        methodConfigurationName = "NotAllSamplesMethodConfig",
        entityType = Option("Individual"),
        entityName = Option("indiv1"),
        expression = Option("this.sset.samples"),
        useCallCache = false,
        deleteIntermediateOutputFiles = false
      )
      val newSubmissionReport =
        Await.result(workspaceService.createSubmission(testData.wsName, submissionRq), Duration.Inf)

      assert(newSubmissionReport.workflows.size == 2)

      checkSubmissionStatus(workspaceService, newSubmissionReport.submissionId)
  }

  it should "400 when given an entity expression that evaluates to an entity of the wrong type" in withWorkspaceService {
    workspaceService =>
      val submissionRq = SubmissionRequest(
        methodConfigurationNamespace = "dsde",
        methodConfigurationName = "GoodMethodConfig",
        entityType = Option("PairSet"),
        entityName = Option("ps1"),
        expression = Option("this.pairs"),
        useCallCache = false,
        deleteIntermediateOutputFiles = false
      )
      val rqComplete = intercept[RawlsExceptionWithErrorReport] {
        Await.result(workspaceService.createSubmission(testData.wsName, submissionRq), Duration.Inf)
      }
      assertResult(StatusCodes.BadRequest) {
        rqComplete.errorReport.statusCode.get
      }
  }

  it should "400 when given no entity expression and an entity of the wrong type" in withWorkspaceService {
    workspaceService =>
      val submissionRq = SubmissionRequest(
        methodConfigurationNamespace = "dsde",
        methodConfigurationName = "GoodMethodConfig",
        entityType = Option("PairSet"),
        entityName = Option("ps1"),
        expression = None,
        useCallCache = false,
        deleteIntermediateOutputFiles = false
      )
      val rqComplete = intercept[RawlsExceptionWithErrorReport] {
        Await.result(workspaceService.createSubmission(testData.wsName, submissionRq), Duration.Inf)
      }
      assertResult(StatusCodes.BadRequest) {
        rqComplete.errorReport.statusCode.get
      }
  }

  it should "fail workflows that evaluate to nonsense and put the rest in Queued" in withWorkspaceServiceMockTimeoutExecution {
    mockExecSvc => workspaceService =>
      val submissionRq = SubmissionRequest(
        methodConfigurationNamespace = "dsde",
        methodConfigurationName = "NotAllSamplesMethodConfig",
        entityType = Option("Individual"),
        entityName = Option("indiv1"),
        expression = Option("this.sset.samples"),
        useCallCache = false,
        deleteIntermediateOutputFiles = false
      )
      val newSubmissionReport =
        Await.result(workspaceService.createSubmission(testData.wsName, submissionRq), Duration.Inf)

      val submissionStatusResponse =
        Await.result(workspaceService.getSubmissionStatus(testData.wsName, newSubmissionReport.submissionId),
                     Duration.Inf
        )

      // Only the workflow with the dodgy expression (sample.tumortype on a normal) should fail
      assert(submissionStatusResponse.workflows.size == 3)

      // the rest are in queued
      assert(submissionStatusResponse.workflows.count(_.status == WorkflowStatuses.Queued) == 2)
  }

  it should "run a submission fine with no root entity" in withWorkspaceService { workspaceService =>
    // Entityless has (duh) no entities and only literals in its outputs
    val submissionRq = SubmissionRequest(
      methodConfigurationNamespace = testData.methodConfigEntityless.namespace,
      methodConfigurationName = testData.methodConfigEntityless.name,
      entityType = None,
      entityName = None,
      expression = None,
      useCallCache = false,
      deleteIntermediateOutputFiles = false
    )
    val newSubmissionReport =
      Await.result(workspaceService.createSubmission(testData.wsName, submissionRq), Duration.Inf)

    assert(newSubmissionReport.workflows.size == 1)

    val submissionData = checkSubmissionStatus(workspaceService, newSubmissionReport.submissionId)
    assert(submissionData.workflows.size == 1)

    val subList = Await.result(workspaceService.listSubmissions(testData.wsName), Duration.Inf)

    val oneSub = subList.filter(s => s.submissionId == newSubmissionReport.submissionId)
    assert(oneSub.nonEmpty)
  }

  it should "return BadRequest when running an MC with a root entity without providing one" in withWorkspaceService {
    workspaceService =>
      // This method config has a root entity, but we've failed to provide one
      val submissionRq = SubmissionRequest(
        methodConfigurationNamespace = "dsde",
        methodConfigurationName = "GoodMethodConfig",
        entityType = None,
        entityName = None,
        expression = None,
        useCallCache = false,
        deleteIntermediateOutputFiles = false
      )
      val rqComplete = intercept[RawlsExceptionWithErrorReport] {
        Await.result(workspaceService.createSubmission(testData.wsName, submissionRq), Duration.Inf)
      }
      assertResult(StatusCodes.BadRequest) {
        rqComplete.errorReport.statusCode.get
      }
  }

  it should "return BadRequest when running against an MC with no root entity and providing one anyway" in withWorkspaceService {
    workspaceService =>
      // Entityless has (duh) no entities and only literals in its outputs
      val submissionRq = SubmissionRequest(
        methodConfigurationNamespace = testData.methodConfigEntityless.namespace,
        methodConfigurationName = testData.methodConfigEntityless.name,
        entityType = Option("Individual"),
        entityName = Option("indiv1"),
        expression = Option("this.sset.samples"),
        useCallCache = false,
        deleteIntermediateOutputFiles = false
      )
      val rqComplete = intercept[RawlsExceptionWithErrorReport] {
        Await.result(workspaceService.createSubmission(testData.wsName, submissionRq), Duration.Inf)
      }

      assertResult(StatusCodes.BadRequest) {
        rqComplete.errorReport.statusCode.get
      }
  }

  it should "create data repo submission" in {
    val tableData = List.fill(3)(UUID.randomUUID().toString).map(rowId => rowId -> s"value $rowId").toMap
    dataRepoSubmissionTest(tableData) { (workspaceService, methodConfig, snapshotId) =>
      val submissionRq = SubmissionRequest(
        methodConfigurationNamespace = methodConfig.namespace,
        methodConfigurationName = methodConfig.name,
        entityType = None,
        entityName = None,
        expression = None,
        useCallCache = false,
        deleteIntermediateOutputFiles = false
      )

      // change the expression to include both workspace and entity lookups
      val workspaceAttrName = "attr"
      val workspaceAttrValue = "foobar"
      val inputsWithWorkspaceExpression = methodConfig.inputs.map { case (name, expr) =>
        name -> AttributeString(s"""{"entity": ${expr.value}, "workspace": workspace.$workspaceAttrName}""")
      }
      runAndWait(
        methodConfigurationQuery.upsert(minimalTestData.workspace,
                                        methodConfig.copy(inputs = inputsWithWorkspaceExpression)
        )
      )
      runAndWait(
        workspaceQuery.createOrUpdate(
          minimalTestData.workspace.copy(attributes =
            Map(AttributeName.withDefaultNS(workspaceAttrName) -> AttributeString(workspaceAttrValue))
          )
        )
      )

      val resultSubmission =
        Await.result(workspaceService.createSubmission(minimalTestData.wsName, submissionRq), Duration.Inf)

      resultSubmission.header.entityStoreId shouldBe Some(snapshotId.toString)
      resultSubmission.header.entityType shouldBe methodConfig.rootEntityType

      val expectedValidInputs = tableData.map { case (rowId, resultVal) =>
        val expectedRawJson = s"""{"entity": "$resultVal", "workspace": "$workspaceAttrValue"}"""
        SubmissionValidationEntityInputs(rowId,
                                         Set(
                                           SubmissionValidationValue(Option(AttributeValueRawJson(expectedRawJson)),
                                                                     None,
                                                                     methodConfig.inputs.keys.head
                                           )
                                         )
        )
      }
      resultSubmission.workflows should contain theSameElementsAs expectedValidInputs
    }
  }

  "Submission validation requests" should "report a BadRequest for an unparseable entity expression" in withWorkspaceService {
    workspaceService =>
      val submissionRq = SubmissionRequest(
        methodConfigurationNamespace = "dsde",
        methodConfigurationName = "GoodMethodConfig",
        entityType = Option("Individual"),
        entityName = Option("indiv1"),
        expression = Option("this.is."),
        useCallCache = false,
        deleteIntermediateOutputFiles = false
      )
      val rqComplete = intercept[RawlsExceptionWithErrorReport] {
        Await.result(workspaceService.validateSubmission(testData.wsName, submissionRq), Duration.Inf)
      }
      assertResult(StatusCodes.BadRequest) {
        rqComplete.errorReport.statusCode.get
      }
  }

  it should "report a validated input and runnable workflow when given an entity expression that evaluates to a single entity" in withWorkspaceService {
    workspaceService =>
      val submissionRq = SubmissionRequest(
        methodConfigurationNamespace = "dsde",
        methodConfigurationName = "GoodMethodConfig",
        entityType = Option("Pair"),
        entityName = Option("pair1"),
        expression = Option("this.case"),
        useCallCache = false,
        deleteIntermediateOutputFiles = false
      )
      val vData = Await.result(workspaceService.validateSubmission(testData.wsName, submissionRq), Duration.Inf)

      assertResult(1)(vData.validEntities.length)
      assert(vData.invalidEntities.isEmpty)
  }

  it should "report validated inputs and runnable workflows when given an entity expression that evaluates to a set of entities" in withWorkspaceService {
    workspaceService =>
      val submissionRq = SubmissionRequest(
        methodConfigurationNamespace = "dsde",
        methodConfigurationName = "GoodMethodConfig",
        entityType = Option("SampleSet"),
        entityName = Option("sset1"),
        expression = Option("this.samples"),
        useCallCache = false,
        deleteIntermediateOutputFiles = false
      )
      val vData = Await.result(workspaceService.validateSubmission(testData.wsName, submissionRq), Duration.Inf)

      assertResult(
        testData.sset1
          .attributes(AttributeName.withDefaultNS("samples"))
          .asInstanceOf[AttributeEntityReferenceList]
          .list
          .size
      )(vData.validEntities.length)
      assert(vData.invalidEntities.isEmpty)
  }

  it should "400 when given an entity expression that evaluates to an empty set of entities" in withWorkspaceService {
    workspaceService =>
      val submissionRq = SubmissionRequest(
        methodConfigurationNamespace = "dsde",
        methodConfigurationName = "GoodMethodConfig",
        entityType = Option("SampleSet"),
        entityName = Option("sset_empty"),
        expression = Option("this.samples"),
        useCallCache = false,
        deleteIntermediateOutputFiles = false
      )
      val rqComplete = intercept[RawlsExceptionWithErrorReport] {
        Await.result(workspaceService.validateSubmission(testData.wsName, submissionRq), Duration.Inf)
      }
      assertResult(StatusCodes.BadRequest) {
        rqComplete.errorReport.statusCode.get
      }
  }

  it should "400 when given a method configuration with unparseable inputs" in withWorkspaceService {
    workspaceService =>
      val submissionRq = SubmissionRequest(
        methodConfigurationNamespace = "dsde",
        methodConfigurationName = "UnparseableInputsMethodConfig",
        entityType = Option("Individual"),
        entityName = Option("indiv1"),
        expression = Option("this.sset.samples"),
        useCallCache = false,
        deleteIntermediateOutputFiles = false
      )
      val rqComplete = intercept[RawlsExceptionWithErrorReport] {
        Await.result(workspaceService.validateSubmission(testData.wsName, submissionRq), Duration.Inf)
      }

      assertResult(StatusCodes.BadRequest) {
        rqComplete.errorReport.statusCode.get
      }

      assert {
        rqComplete.errorReport.message.contains("Invalid inputs: three_step.cgrep.pattern")
      }
  }

  it should "400 when given a method configuration with unparseable outputs" in withWorkspaceService {
    workspaceService =>
      val submissionRq = SubmissionRequest(
        methodConfigurationNamespace = "dsde",
        methodConfigurationName = "UnparseableOutputsMethodConfig",
        entityType = Option("Individual"),
        entityName = Option("indiv1"),
        expression = Option("this.sset.samples"),
        useCallCache = false,
        deleteIntermediateOutputFiles = false
      )
      val rqComplete = intercept[RawlsExceptionWithErrorReport] {
        Await.result(workspaceService.validateSubmission(testData.wsName, submissionRq), Duration.Inf)
      }

      assertResult(StatusCodes.BadRequest) {
        rqComplete.errorReport.statusCode.get
      }

      assert {
        rqComplete.errorReport.message.contains("Invalid outputs: three_step.cgrep.count")
      }
  }

  it should "report a successful validation when given a method configuration with empty outputs" in withWorkspaceService {
    workspaceService =>
      val submissionRq = SubmissionRequest(
        methodConfigurationNamespace = "dsde",
        methodConfigurationName = "EmptyOutputsMethodConfig",
        entityType = Option("Individual"),
        entityName = Option("indiv1"),
        expression = Option("this.sset.samples"),
        useCallCache = false,
        deleteIntermediateOutputFiles = false
      )
      val validation = Await.result(workspaceService.validateSubmission(testData.wsName, submissionRq), Duration.Inf)

      assertResult(3)(validation.validEntities.size)
      assert(validation.invalidEntities.isEmpty)
  }

  it should "report validated inputs and a mixture of started and unstarted workflows where method configuration inputs are missing on some entities" in withWorkspaceService {
    workspaceService =>
      val submissionRq = SubmissionRequest(
        methodConfigurationNamespace = "dsde",
        methodConfigurationName = "NotAllSamplesMethodConfig",
        entityType = Option("Individual"),
        entityName = Option("indiv1"),
        expression = Option("this.sset.samples"),
        useCallCache = false,
        deleteIntermediateOutputFiles = false
      )
      val vData = Await.result(workspaceService.validateSubmission(testData.wsName, submissionRq), Duration.Inf)

      assertResult(
        testData.sset1
          .attributes(AttributeName.withDefaultNS("samples"))
          .asInstanceOf[AttributeEntityReferenceList]
          .list
          .size - 1
      )(vData.validEntities.length)
      assertResult(1)(vData.invalidEntities.length)
  }

  it should "report errors for an entity expression that evaluates to an entity of the wrong type" in withWorkspaceService {
    workspaceService =>
      val submissionRq = SubmissionRequest(
        methodConfigurationNamespace = "dsde",
        methodConfigurationName = "GoodMethodConfig",
        entityType = Option("PairSet"),
        entityName = Option("ps1"),
        expression = Option("this.pairs"),
        useCallCache = false,
        deleteIntermediateOutputFiles = false
      )
      val rqComplete = intercept[RawlsExceptionWithErrorReport] {
        Await.result(workspaceService.validateSubmission(testData.wsName, submissionRq), Duration.Inf)
      }
      assertResult(StatusCodes.BadRequest) {
        rqComplete.errorReport.statusCode.get
      }
  }

  it should "report an error when given no entity expression and the entity is of the wrong type" in withWorkspaceService {
    workspaceService =>
      val submissionRq = SubmissionRequest(
        methodConfigurationNamespace = "dsde",
        methodConfigurationName = "GoodMethodConfig",
        entityType = Option("PairSet"),
        entityName = Option("ps1"),
        expression = None,
        useCallCache = false,
        deleteIntermediateOutputFiles = false
      )
      val rqComplete = intercept[RawlsExceptionWithErrorReport] {
        Await.result(workspaceService.validateSubmission(minimalTestData.wsName, submissionRq), Duration.Inf)
      }
      assertResult(StatusCodes.BadRequest) {
        rqComplete.errorReport.statusCode.get
      }
  }

  def dataRepoSubmissionTest[T](
    tableData: Map[String, String]
  )(test: (WorkspaceService, MethodConfiguration, UUID) => T) = {
    val tableResult: TableResult = prepareBqData(tableData)

    val dataRepoDAO = mock[DataRepoDAO](RETURNS_SMART_NULLS)

    val snapshotUUID: UUID = UUID.randomUUID()
    val tableName = "table1"
    val columnName = "value"

    when(dataRepoDAO.getSnapshot(snapshotUUID, userInfo.accessToken)).thenReturn(
      createSnapshotModel(
        List(
          new TableModel()
            .name(tableName)
            .primaryKey(null)
            .rowCount(0)
            .columns(List(columnName).map(new ColumnModel().name(_)).asJava)
        )
      ).id(snapshotUUID)
    )
    when(dataRepoDAO.getInstanceName).thenReturn("dataRepoInstance")

    val dataReferenceName = DataReferenceName("dataref")
    val dataReferenceDescription = Option(DataReferenceDescriptionField("description"))

    val methodConfig = MethodConfiguration(
      "dsde",
      "DataRepoMethodConfig",
      Some(tableName),
      prerequisites = None,
      inputs = Map("three_step.cgrep.pattern" -> AttributeString(s"this.$columnName")),
      outputs = Map.empty,
      AgoraMethod("dsde", "three_step", 1),
      dataReferenceName = Option(dataReferenceName)
    )

    withDataAndService(
      { workspaceService =>
        workspaceService.workspaceManagerDAO.createDataRepoSnapshotReference(
          minimalTestData.workspace.workspaceIdAsUUID,
          snapshotUUID,
          dataReferenceName,
          dataReferenceDescription,
          dataRepoDAO.getInstanceName,
          CloningInstructionsEnum.NOTHING,
          testContext
        )
        runAndWait(methodConfigurationQuery.upsert(minimalTestData.workspace, methodConfig))
        test(workspaceService, methodConfig, snapshotUUID)
      },
      withMinimalTestDatabase[Any],
      bigQueryServiceFactory = MockBigQueryServiceFactory.ioFactory(Right(tableResult)),
      dataRepoDAO = dataRepoDAO
    )
  }

  private def prepareBqData(tableData: Map[String, String]) = {
    val rowIdField = Field.of("datarepo_row_id", LegacySQLTypeName.STRING)
    val valueField = Field.of("value", LegacySQLTypeName.STRING)
    val schema: Schema = Schema.of(rowIdField, valueField)

    val results = tableData map { case (rowId, value) =>
      FieldValueList.of(
        List(
          FieldValue.of(com.google.cloud.bigquery.FieldValue.Attribute.PRIMITIVE, rowId),
          FieldValue.of(com.google.cloud.bigquery.FieldValue.Attribute.PRIMITIVE, value)
        ).asJava,
        rowIdField,
        valueField
      )
    }

    val page: PageImpl[FieldValueList] = new PageImpl[FieldValueList](null, null, results.asJava)
    val tableResult: TableResult = new TableResult(schema, 1, page)
    tableResult
  }

  it should "validate data repo submission" in {
    val tableData = List.fill(3)(UUID.randomUUID().toString).map(rowId => rowId -> s"value $rowId").toMap
    dataRepoSubmissionTest(tableData) { (workspaceService, methodConfig, snapshotId) =>
      val submissionRq = SubmissionRequest(
        methodConfigurationNamespace = methodConfig.namespace,
        methodConfigurationName = methodConfig.name,
        entityType = None,
        entityName = None,
        expression = None,
        useCallCache = false,
        deleteIntermediateOutputFiles = false
      )

      val vData = Await.result(workspaceService.validateSubmission(minimalTestData.wsName, submissionRq), Duration.Inf)

      val expectedValidInputs = tableData.map { case (rowId, resultVal) =>
        SubmissionValidationEntityInputs(
          rowId,
          Set(SubmissionValidationValue(Option(AttributeString(resultVal)), None, methodConfig.inputs.keys.head))
        )
      }
      vData.validEntities should contain theSameElementsAs expectedValidInputs
      assert(vData.invalidEntities.isEmpty)
    }
  }

  it should "detect invalid data repo submission" in {
    val tableData = List.fill(3)(UUID.randomUUID().toString).map(rowId => rowId -> null).toMap
    dataRepoSubmissionTest(tableData) { (workspaceService, methodConfig, snapshotId) =>
      val submissionRq = SubmissionRequest(
        methodConfigurationNamespace = methodConfig.namespace,
        methodConfigurationName = methodConfig.name,
        entityType = None,
        entityName = None,
        expression = None,
        useCallCache = false,
        deleteIntermediateOutputFiles = false
      )

      val vData = Await.result(workspaceService.validateSubmission(minimalTestData.wsName, submissionRq), Duration.Inf)

      assert(vData.validEntities.isEmpty)
      val expectedInvalidInputs = tableData.keys.map(rowId =>
        SubmissionValidationEntityInputs(
          rowId,
          Set(
            SubmissionValidationValue(
              None,
              Some("Expected single value for workflow input, but evaluated result set was empty"),
              methodConfig.inputs.keys.head
            )
          )
        )
      )
      vData.invalidEntities should contain theSameElementsAs expectedInvalidInputs
    }
  }

  it should "report error when data reference exists with entity name" in {
    dataRepoSubmissionTest(Map.empty) { (workspaceService, methodConfig, snapshotId) =>
      val submissionRq = SubmissionRequest(
        methodConfigurationNamespace = methodConfig.namespace,
        methodConfigurationName = methodConfig.name,
        entityType = methodConfig.rootEntityType,
        entityName = Option("name"),
        expression = None,
        useCallCache = false,
        deleteIntermediateOutputFiles = false
      )

      val ex = intercept[RawlsExceptionWithErrorReport] {
        Await.result(workspaceService.validateSubmission(minimalTestData.wsName, submissionRq), Duration.Inf)
      }
      ex.errorReport.statusCode shouldBe Option(StatusCodes.BadRequest)
      ex.errorReport.message shouldBe "Your method config defines a data reference and an entity name. Running on a submission on a single entity in a data reference is not yet supported."
    }
  }

  it should "report error when data reference points to unknown snapshot" in {
    dataRepoSubmissionTest(Map.empty) { (workspaceService, methodConfig, snapshotId) =>
      runAndWait(
        methodConfigurationQuery.upsert(minimalTestData.workspace,
                                        methodConfig.copy(dataReferenceName = Some(DataReferenceName("unknown")))
        )
      )

      val submissionRq = SubmissionRequest(
        methodConfigurationNamespace = methodConfig.namespace,
        methodConfigurationName = methodConfig.name,
        entityType = None,
        entityName = None,
        expression = None,
        useCallCache = false,
        deleteIntermediateOutputFiles = false
      )

      val ex = intercept[RawlsExceptionWithErrorReport] {
        Await.result(workspaceService.validateSubmission(minimalTestData.wsName, submissionRq), Duration.Inf)
      }
      ex.errorReport.statusCode shouldBe Option(StatusCodes.NotFound)
      ex.errorReport.message shouldBe "Reference name unknown does not exist in workspace myNamespace/myWorkspace."
    }
  }

  it should "report error when root entity type does not refer to a table in the snapshot" in {
    dataRepoSubmissionTest(Map.empty) { (workspaceService, methodConfig, snapshotId) =>
      runAndWait(
        methodConfigurationQuery.upsert(minimalTestData.workspace, methodConfig.copy(rootEntityType = Some("unknown")))
      )

      val submissionRq = SubmissionRequest(
        methodConfigurationNamespace = methodConfig.namespace,
        methodConfigurationName = methodConfig.name,
        entityType = None,
        entityName = None,
        expression = None,
        useCallCache = false,
        deleteIntermediateOutputFiles = false
      )

      val ex = intercept[RawlsExceptionWithErrorReport] {
        Await.result(workspaceService.validateSubmission(minimalTestData.wsName, submissionRq), Duration.Inf)
      }
      ex.errorReport.statusCode shouldBe Option(StatusCodes.BadRequest)
      ex.errorReport.message shouldBe "Validation errors: Invalid inputs: three_step.cgrep.pattern -> Table `unknown` does not exist in snapshot"
    }
  }

  "Aborting submissions" should "404 if the workspace doesn't exist" in withSubmissionTestWorkspaceService {
    workspaceService =>
      val rqComplete = intercept[RawlsExceptionWithErrorReport] {
        Await.result(
          workspaceService.abortSubmission(WorkspaceName(name = "nonexistent", namespace = "workspace"), "12345"),
          Duration.Inf
        )
      }
      assertResult(StatusCodes.NotFound) {
        rqComplete.errorReport.statusCode.get
      }
  }

  it should "404 if the submission doesn't exist" in withSubmissionTestWorkspaceService { workspaceService =>
    val rqComplete = intercept[RawlsExceptionWithErrorReport] {
      Await.result(workspaceService.abortSubmission(subTestData.wsName, "12345"), Duration.Inf)
    }
    assertResult(StatusCodes.NotFound) {
      rqComplete.errorReport.statusCode.get
    }
  }

  it should "return successfully for a valid submission" in withSubmissionTestWorkspaceService { workspaceService =>
    val rqComplete = Await.result(workspaceService.abortSubmission(subTestData.wsName, subGoodWorkflow), Duration.Inf)
    assertResult(1) {
      rqComplete
    }
  }

  "Retry submission" should "succeed" in withSubmissionTestWorkspaceService { workspaceService =>
    val req = workspaceService.retrySubmission(subTestData.wsName,
                                               SubmissionRetry(RetryAborted),
                                               subTestData.submissionToRetry.submissionId
    )
    val report = Await.result(req, Duration.Inf)
    assert(subTestData.submissionToRetry.submissionId == report.originalSubmissionId,
           "Retried submission should reference original"
    )
    assert(report.submissionId != report.originalSubmissionId, "We should generate a new submission id")
    report.workflows should have size 2
    assert(report.submitter == "owner-access")
    val submission =
      Await.result(workspaceService.getSubmissionStatus(subTestData.wsName, report.submissionId), Duration.Inf)
    assert(submission.userComment.get.contains("retry of submission"))
  }

  "Getting workflow outputs" should "return 200 when all is well" in withSubmissionTestWorkspaceService {
    workspaceService =>
      val rqComplete = workspaceService.workflowOutputs(subTestData.wsName,
                                                        subTestData.submissionTestAbortGoodWorkflow.submissionId,
                                                        subTestData.existingWorkflowId.get
      )
      val data = Await.result(rqComplete, Duration.Inf)

      assertResult(subTestData.extantWorkflowOutputs)(data)
  }

  it should "return 404 on getting outputs for a workflow that exists, but not in this submission" in withSubmissionTestWorkspaceService {
    workspaceService =>
      val rqComplete = workspaceService.workflowOutputs(subTestData.wsName,
                                                        subTestData.submissionTestAbortTerminalWorkflow.submissionId,
                                                        subTestData.existingWorkflowId.get
      )
      val errorReport = intercept[RawlsExceptionWithErrorReport] {
        Await.result(rqComplete, Duration.Inf)
      }

      assertResult(StatusCodes.NotFound)(errorReport.errorReport.statusCode.get)
  }

  "Getting a workflow cost" should "return 200 when all is well" in withSubmissionTestWorkspaceService {
    workspaceService =>
      val rqComplete = workspaceService.workflowCost(subTestData.wsName,
                                                     subTestData.submissionTestAbortGoodWorkflow.submissionId,
                                                     subTestData.existingWorkflowId.get
      )
      val data = Await.result(rqComplete, Duration.Inf)

      assertResult(WorkflowCost(subTestData.existingWorkflowId.get, Some(mockSubmissionCostService.fixedCost)))(data)
  }

  it should "return 404 on getting the cost for a workflow that exists, but not in this submission" in withSubmissionTestWorkspaceService {
    workspaceService =>
      val rqComplete = workspaceService.workflowCost(subTestData.wsName,
                                                     subTestData.submissionTestAbortTerminalWorkflow.submissionId,
                                                     subTestData.existingWorkflowId.get
      )
      val errorReport = intercept[RawlsExceptionWithErrorReport] {
        Await.result(rqComplete, Duration.Inf)
      }

      assertResult(StatusCodes.NotFound)(errorReport.errorReport.statusCode.get)
  }

  it should "calculate submission cost as the sum of workflow costs" in withSubmissionTestWorkspaceService {
    workspaceService =>
      val submissionData = checkSubmissionStatus(workspaceService,
                                                 subTestData.submissionTestAbortTwoGoodWorkflows.submissionId,
                                                 subTestData.wsName
      )
      assertResult(Option(mockSubmissionCostService.fixedCost * 2)) {
        submissionData.cost
      }
  }

  it should "return 502 on getting a workflow if Cromwell barfs" in withSubmissionTestWorkspaceService {
    workspaceService =>
      val rqComplete = workspaceService.workflowOutputs(subTestData.wsName,
                                                        subTestData.submissionTestCromwellBadWorkflows.submissionId,
                                                        subTestData.badLogsAndMetadataWorkflowId.get
      )
      val errorReport = intercept[RawlsExceptionWithErrorReport] {
        Await.result(rqComplete, Duration.Inf)
      }

      assertResult(StatusCodes.BadGateway)(errorReport.errorReport.statusCode.get)
      assertResult("cromwell")(errorReport.errorReport.causes.head.source)
  }

  "Getting workflow metadata" should "return 200" in withSubmissionTestWorkspaceService { workspaceService =>
    val rqComplete = workspaceService.workflowMetadata(subTestData.wsName,
                                                       subTestData.submissionTestAbortGoodWorkflow.submissionId,
                                                       subTestData.existingWorkflowId.get,
                                                       MetadataParams()
    )
    val data = Await.result(rqComplete, Duration.Inf)

    assert(data.fields.nonEmpty)
  }

  "ExecutionService" should "parse unsupported output data types" in {
    val workflowId = "8afafe21-2b70-4180-a565-748cb573e10c"
    assertResult(
      ExecutionServiceOutputs(
        workflowId,
        Map(
          "aggregate_data_workflow.aggregate_data.output_array" -> Left(
            AttributeValueRawJson(
              JsArray(
                Vector(JsArray(Vector(JsString("foo"), JsString("bar"))),
                       JsArray(Vector(JsString("baz"), JsString("qux")))
                )
              )
            )
          )
        )
      )
    ) {

      Await.result(new HttpExecutionServiceDAO(mockServer.mockServerBaseUrl, workbenchMetricBaseName)
                     .outputs(workflowId, userInfo),
                   Duration.Inf
      )
    }

  }
}
