package org.broadinstitute.dsde.rawls.webservice

import akka.actor.PoisonPill
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, StatusCodes}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route.{seal => sealRoute}
import akka.http.scaladsl.server._
import akka.http.scaladsl.testkit.{RouteTestTimeout, ScalatestRouteTest}
import akka.stream.ActorMaterializer
import akka.testkit.TestKitBase
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.RawlsTestUtils
import org.broadinstitute.dsde.rawls.billing.{
  BillingProfileManagerDAO,
  BillingProjectOrchestrator,
  BillingRepository,
  BpmBillingProjectLifecycle,
  GoogleBillingProjectLifecycle
}
import org.broadinstitute.dsde.rawls.config._
import org.broadinstitute.dsde.rawls.coordination.UncoordinatedDataSourceAccess
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.dataaccess.datarepo.DataRepoDAO
import org.broadinstitute.dsde.rawls.dataaccess.drs.DrsHubResolver
import org.broadinstitute.dsde.rawls.dataaccess.resourcebuffer.ResourceBufferDAO
import org.broadinstitute.dsde.rawls.dataaccess.slick.TestDriverComponentWithFlatSpecAndMatchers
import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.WorkspaceManagerDAO
import org.broadinstitute.dsde.rawls.entities.{EntityManager, EntityService}
import org.broadinstitute.dsde.rawls.genomics.GenomicsService
import org.broadinstitute.dsde.rawls.google.MockGooglePubSubDAO
import org.broadinstitute.dsde.rawls.jobexec.{SubmissionMonitorConfig, SubmissionSupervisor}
import org.broadinstitute.dsde.rawls.metrics.{InstrumentationDirectives, RawlsInstrumented, RawlsStatsDTestUtils}
import org.broadinstitute.dsde.rawls.mock._
import org.broadinstitute.dsde.rawls.model.{
  Agora,
  ApplicationVersion,
  Dockstore,
  RawlsBillingAccountName,
  RawlsRequestContext,
  RawlsUser
}
import org.broadinstitute.dsde.rawls.monitor.HealthMonitor
import org.broadinstitute.dsde.rawls.resourcebuffer.ResourceBufferService
import org.broadinstitute.dsde.rawls.serviceperimeter.ServicePerimeterService
import org.broadinstitute.dsde.rawls.snapshot.SnapshotService
import org.broadinstitute.dsde.rawls.spendreporting.SpendReportingService
import org.broadinstitute.dsde.rawls.status.StatusService
import org.broadinstitute.dsde.rawls.user.UserService
import org.broadinstitute.dsde.rawls.util.MockitoTestUtils
import org.broadinstitute.dsde.rawls.workspace.{
  MultiCloudWorkspaceAclManager,
  MultiCloudWorkspaceService,
  RawlsWorkspaceAclManager,
  WorkspaceService
}
import org.broadinstitute.dsde.workbench.dataaccess.{NotificationDAO, PubSubNotificationDAO}
import org.broadinstitute.dsde.workbench.google.mock.{MockGoogleBigQueryDAO, MockGoogleIamDAO}
import org.broadinstitute.dsde.workbench.model.google.GoogleProject
import org.broadinstitute.dsde.workbench.oauth2.mock.FakeOpenIDConnectConfiguration
import org.mockito.Mockito.RETURNS_SMART_NULLS
import org.mockito.ArgumentMatcher
import org.scalatest.concurrent.Eventually
import spray.json._

import java.util.concurrent.TimeUnit
import scala.concurrent.duration._
import scala.language.postfixOps

//noinspection TypeAnnotation
// common trait to be inherited by API service tests
trait ApiServiceSpec
    extends TestDriverComponentWithFlatSpecAndMatchers
    with RawlsTestUtils
    with RawlsInstrumented
    with RawlsStatsDTestUtils
    with InstrumentationDirectives
    with ScalatestRouteTest
    with TestKitBase
    with SprayJsonSupport
    with MockitoTestUtils
    with Eventually
    with LazyLogging {

  def userInfoEq(expectedCtx: RawlsRequestContext): ArgumentMatcher[RawlsRequestContext] = actualCtx =>
    expectedCtx.userInfo == actualCtx.userInfo

  // increase the timeout for ScalatestRouteTest from the default of 1 second, otherwise
  // intermittent failures occur on requests not completing in time
  implicit val routeTestTimeout = RouteTestTimeout(5.seconds)

  // this gets fed into sealRoute so that exceptions are handled the same in tests as in real life
  implicit val exceptionHandler = RawlsApiService.exceptionHandler

  override val workbenchMetricBaseName = "test"

  def actorRefFactory = system

  val mockServer = RemoteServicesMockServer()

  override def beforeAll(): Unit = {
    super.beforeAll()
    mockServer.startServer()
  }

  override def afterAll(): Unit = {
    mockServer.stopServer
    super.afterAll()
  }

  def httpJsonStr(str: String) = HttpEntity(ContentTypes.`application/json`, str)
  def httpJson[T](obj: T)(implicit writer: JsonWriter[T]) = httpJsonStr(obj.toJson.toString())
  val httpJsonEmpty = httpJsonStr("[]")

  def revokeCuratorRole(services: ApiServices, user: RawlsUser = testData.userOwner): Unit = {
    Get("/user/role/curator") ~>
      sealRoute(services.userRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
      }

    Delete(s"/admin/user/role/curator/${user.userEmail.value}") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
      }

    Get("/user/role/curator") ~>
      sealRoute(services.userRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
      }
  }

  // noinspection TypeAnnotation,NameBooleanParameters,ConvertibleToMethodValue,UnitMethodIsParameterless
  trait ApiServices
      extends AdminApiService
      with BillingApiService
      with BillingApiServiceV2
      with EntityApiService
      with NotificationsApiService
      with RawlsApiService
      with SnapshotApiService
      with StatusApiService
      with UserApiService
      with WorkspaceApiService {

    val dataSource: SlickDataSource
    val gcsDAO: MockGoogleServicesDAO
    val gpsDAO: MockGooglePubSubDAO
    val notificationGpsDAO: org.broadinstitute.dsde.workbench.google.mock.MockGooglePubSubDAO =
      new org.broadinstitute.dsde.workbench.google.mock.MockGooglePubSubDAO
    val mockNotificationDAO: NotificationDAO = mock[NotificationDAO]

    def actorRefFactory = system

    implicit override val materializer = ActorMaterializer()
    override val workbenchMetricBaseName: String = "test"
    override val submissionTimeout = FiniteDuration(1, TimeUnit.MINUTES)

    val samDAO: SamDAO = new MockSamDAO(dataSource)

    val workspaceManagerDAO: WorkspaceManagerDAO = new MockWorkspaceManagerDAO()

    val dataRepoDAO: DataRepoDAO = new MockDataRepoDAO(mockServer.mockServerBaseUrl)

    val bigQueryServiceFactory: GoogleBigQueryServiceFactory = MockBigQueryServiceFactory.ioFactory()

    override val executionServiceCluster = MockShardedExecutionServiceCluster.fromDAO(
      new HttpExecutionServiceDAO(mockServer.mockServerBaseUrl, workbenchMetricBaseName = workbenchMetricBaseName),
      slickDataSource
    )

    val config = SubmissionMonitorConfig(5 seconds, 30 days, true, 20000, true)
    val submissionSupervisor = system.actorOf(
      SubmissionSupervisor
        .props(
          executionServiceCluster,
          new UncoordinatedDataSourceAccess(slickDataSource),
          samDAO,
          gcsDAO,
          mockNotificationDAO,
          gcsDAO.getBucketServiceAccountCredential,
          config,
          workbenchMetricBaseName
        )
        .withDispatcher("submission-monitor-dispatcher")
    )

    val testConf = ConfigFactory.load()

    override val batchUpsertMaxBytes = testConf.getLong("entityUpsert.maxContentSizeBytes")

    val googleGroupSyncTopic = "test-topic-name"

    val notificationTopic = "test-notification-topic"
    val notificationDAO = new PubSubNotificationDAO(notificationGpsDAO, notificationTopic)

    val drsResolver = mock[DrsHubResolver](RETURNS_SMART_NULLS)

    val servicePerimeterConfig = ServicePerimeterServiceConfig(testConf)
    val servicePerimeterService = new ServicePerimeterService(slickDataSource, gcsDAO, servicePerimeterConfig)
    val workspaceManagerResourceMonitorRecordDao = mock[WorkspaceManagerResourceMonitorRecordDao](RETURNS_SMART_NULLS)
    val billingProfileManagerDAO = mock[BillingProfileManagerDAO]
    val billingRepository = new BillingRepository(slickDataSource)
    val googleBillingProjectLifecycle = mock[GoogleBillingProjectLifecycle]
    override val billingProjectOrchestratorConstructor = BillingProjectOrchestrator.constructor(
      samDAO,
      mock[NotificationDAO],
      billingRepository,
      googleBillingProjectLifecycle,
      mock[BpmBillingProjectLifecycle],
      workspaceManagerResourceMonitorRecordDao,
      mock[MultiCloudWorkspaceConfig]
    )

    override val userServiceConstructor = UserService.constructor(
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

    override val snapshotServiceConstructor = SnapshotService.constructor(
      slickDataSource,
      samDAO,
      workspaceManagerDAO,
      mockServer.mockServerBaseUrl
    )

    override val genomicsServiceConstructor = GenomicsService.constructor(
      slickDataSource,
      gcsDAO
    ) _

    val spendReportingBigQueryService = bigQueryServiceFactory.getServiceFromJson("json", GoogleProject("test-project"))
    val spendReportingServiceConfig =
      SpendReportingServiceConfig("fakeTableName", "fakeTimePartitionColumn", 90, "test.metrics")
    override val spendReportingConstructor = SpendReportingService.constructor(
      slickDataSource,
      spendReportingBigQueryService,
      mock[BillingRepository],
      mock[BillingProfileManagerDAO],
      samDAO,
      spendReportingServiceConfig
    )

    val methodRepoDAO = new HttpMethodRepoDAO(
      MethodRepoConfig[Agora.type](mockServer.mockServerBaseUrl, ""),
      MethodRepoConfig[Dockstore.type](mockServer.mockServerBaseUrl, ""),
      workbenchMetricBaseName = workbenchMetricBaseName
    )

    val healthMonitor = system.actorOf(
      HealthMonitor.props(
        dataSource,
        gcsDAO,
        gpsDAO,
        methodRepoDAO,
        samDAO,
        billingProfileManagerDAO,
        workspaceManagerDAO,
        executionServiceCluster.readMembers.map(c => c.key -> c.dao).toMap,
        Seq("my-favorite-group"),
        Seq.empty,
        Seq("my-favorite-bucket")
      )
    )
    override val statusServiceConstructor = StatusService.constructor(healthMonitor) _
    val bigQueryDAO = new MockGoogleBigQueryDAO
    val submissionCostService =
      new MockSubmissionCostService("fakeTableName", "fakeDatePartitionColumn", "fakeServiceProject", 31, bigQueryDAO)
    val execServiceBatchSize = 3
    val maxActiveWorkflowsTotal = 10
    val maxActiveWorkflowsPerUser = 2
    val workspaceServiceConfig = WorkspaceServiceConfig(
      true,
      "fc-",
      "us-central1"
    )

    val bondApiDAO: BondApiDAO = new MockBondApiDAO(bondBaseUrl = "bondUrl")
    val requesterPaysSetupService =
      new RequesterPaysSetupService(slickDataSource, gcsDAO, bondApiDAO, requesterPaysRole = "requesterPaysRole")

    val entityManager = EntityManager.defaultEntityManager(
      dataSource,
      workspaceManagerDAO,
      dataRepoDAO,
      samDAO,
      bigQueryServiceFactory,
      DataRepoEntityProviderConfig(100, 10, 0),
      testConf.getBoolean("entityStatisticsCache.enabled"),
      workbenchMetricBaseName
    )

    val resourceBufferDAO: ResourceBufferDAO = new MockResourceBufferDAO
    val resourceBufferConfig = ResourceBufferConfig(testConf.getConfig("resourceBuffer"))
    val resourceBufferService = new ResourceBufferService(resourceBufferDAO, resourceBufferConfig)
    val resourceBufferSaEmail = resourceBufferConfig.saEmail

    val rawlsWorkspaceAclManager = new RawlsWorkspaceAclManager(samDAO)
    val multiCloudWorkspaceAclManager =
      new MultiCloudWorkspaceAclManager(workspaceManagerDAO, samDAO, billingProfileManagerDAO, dataSource)

    override val workspaceServiceConstructor = WorkspaceService.constructor(
      slickDataSource,
      methodRepoDAO,
      new HttpExecutionServiceDAO(mockServer.mockServerBaseUrl, workbenchMetricBaseName = workbenchMetricBaseName),
      executionServiceCluster,
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
      submissionCostService,
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
      rawlsWorkspaceAclManager,
      multiCloudWorkspaceAclManager
    ) _

    override val multiCloudWorkspaceServiceConstructor = MultiCloudWorkspaceService.constructor(
      slickDataSource,
      workspaceManagerDAO,
      billingProfileManagerDAO,
      samDAO,
      MultiCloudWorkspaceConfig(testConf),
      workbenchMetricBaseName
    )

    override val entityServiceConstructor = EntityService.constructor(
      slickDataSource,
      samDAO,
      workbenchMetricBaseName,
      entityManager,
      1000
    ) _

    def cleanupSupervisor =
      submissionSupervisor ! PoisonPill

    val appVersion = ApplicationVersion("dummy", "dummy", "dummy")

    // for metrics testing
    val sealedInstrumentedRoutes: Route = instrumentRequest {
      sealRoute(
        adminRoutes ~ billingRoutesV2 ~ billingRoutes ~ entityRoutes ~ methodConfigRoutes ~ notificationsRoutes ~ statusRoute ~
          submissionRoutes ~ userRoutes ~ workspaceRoutes
      )
    }

    override val openIDConnectConfiguration = FakeOpenIDConnectConfiguration
  }

}
