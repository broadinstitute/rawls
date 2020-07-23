package org.broadinstitute.dsde.rawls.webservice

import java.util.concurrent.TimeUnit

import akka.actor.PoisonPill
import akka.testkit.TestKitBase
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.RawlsTestUtils
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.dataaccess.slick.TestDriverComponentWithFlatSpecAndMatchers
import org.broadinstitute.dsde.rawls.genomics.GenomicsService
import org.broadinstitute.dsde.rawls.google.MockGooglePubSubDAO
import org.broadinstitute.dsde.rawls.jobexec.{SubmissionMonitorConfig, SubmissionSupervisor}
import org.broadinstitute.dsde.rawls.metrics.RawlsStatsDTestUtils
import org.broadinstitute.dsde.rawls.metrics.{InstrumentationDirectives, RawlsInstrumented}
import org.broadinstitute.dsde.rawls.mock.{MockBondApiDAO, MockDataRepoDAO, MockSamDAO, MockWorkspaceManagerDAO, RemoteServicesMockServer}
import org.broadinstitute.dsde.rawls.model.{Agora, ApplicationVersion, Dockstore, RawlsUser}
import org.broadinstitute.dsde.rawls.monitor.HealthMonitor
import org.broadinstitute.dsde.rawls.statistics.StatisticsService
import org.broadinstitute.dsde.rawls.status.StatusService
import org.broadinstitute.dsde.rawls.user.UserService
import org.broadinstitute.dsde.rawls.util.MockitoTestUtils
import org.broadinstitute.dsde.rawls.workspace.{WorkspaceService, WorkspaceServiceConfig}
import org.broadinstitute.dsde.workbench.google.mock.MockGoogleBigQueryDAO
import org.scalatest.concurrent.Eventually
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, StatusCodes}
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.server._
import akka.http.scaladsl.server.Route.{seal => sealRoute}
import spray.json._
import akka.http.scaladsl.testkit.{RouteTestTimeout, ScalatestRouteTest}
import akka.http.scaladsl.server.Directives._
import akka.stream.ActorMaterializer
import com.typesafe.config.ConfigFactory
import org.broadinstitute.dsde.rawls.config.{DataRepoEntityProviderConfig, DeploymentManagerConfig, MethodRepoConfig, SwaggerConfig}
import org.broadinstitute.dsde.rawls.coordination.UncoordinatedDataSourceAccess
import org.broadinstitute.dsde.rawls.dataaccess.datarepo.DataRepoDAO
import org.broadinstitute.dsde.rawls.entities.{EntityManager, EntityService}
import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.WorkspaceManagerDAO
import org.broadinstitute.dsde.rawls.snapshot.SnapshotService

import scala.concurrent.duration._
import scala.language.postfixOps

//noinspection TypeAnnotation
// common trait to be inherited by API service tests
trait ApiServiceSpec extends TestDriverComponentWithFlatSpecAndMatchers with RawlsTestUtils with RawlsInstrumented
  with RawlsStatsDTestUtils with InstrumentationDirectives with ScalatestRouteTest with TestKitBase
  with SprayJsonSupport with MockitoTestUtils with Eventually with LazyLogging {

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

  //noinspection TypeAnnotation,NameBooleanParameters,ConvertibleToMethodValue,UnitMethodIsParameterless
  trait ApiServices extends AdminApiService with BillingApiService with EntityApiService with MethodConfigApiService
    with NotificationsApiService with RawlsApiService with SnapshotApiService with StatusApiService with SubmissionApiService with UserApiService with WorkspaceApiService {

    val dataSource: SlickDataSource
    val gcsDAO: MockGoogleServicesDAO
    val gpsDAO: MockGooglePubSubDAO

    def actorRefFactory = system

    override implicit val materializer = ActorMaterializer()
    override val workbenchMetricBaseName: String = "test"
    override val swaggerConfig: SwaggerConfig = SwaggerConfig("foo", "bar")
    override val submissionTimeout = FiniteDuration(1, TimeUnit.MINUTES)

    val samDAO: SamDAO = new MockSamDAO(dataSource)

    val workspaceManagerDAO: WorkspaceManagerDAO = new MockWorkspaceManagerDAO()

    val dataRepoDAO: DataRepoDAO = new MockDataRepoDAO()

    val bigQueryServiceFactory: GoogleBigQueryServiceFactory = MockBigQueryServiceFactory.ioFactory()

    override val executionServiceCluster = MockShardedExecutionServiceCluster.fromDAO(new HttpExecutionServiceDAO(mockServer.mockServerBaseUrl, workbenchMetricBaseName = workbenchMetricBaseName), slickDataSource)

    val config = SubmissionMonitorConfig(5 seconds, true)
    val submissionSupervisor = system.actorOf(SubmissionSupervisor.props(
      executionServiceCluster,
      new UncoordinatedDataSourceAccess(slickDataSource),
      samDAO,
      gcsDAO,
      gcsDAO.getBucketServiceAccountCredential,
      config,
      workbenchMetricBaseName
    ).withDispatcher("submission-monitor-dispatcher"))

    val testConf = ConfigFactory.load()

    val googleGroupSyncTopic = "test-topic-name"

    val notificationTopic = "test-notification-topic"
    val notificationDAO = new PubSubNotificationDAO(gpsDAO, notificationTopic)

    val dosResolver = new MarthaDosResolver(mockServer.mockServerBaseUrl)

    override val userServiceConstructor = UserService.constructor(
      slickDataSource,
      gcsDAO,
      notificationDAO,
      samDAO,
      Seq("bigquery.jobUser"),
      "requesterPaysRole",
      DeploymentManagerConfig(testConf.getConfig("gcs.deploymentManager")),
      ProjectTemplate.from(testConf.getConfig("gcs.projectTemplate"))
    )_

    override val snapshotServiceConstructor = SnapshotService.constructor(
      slickDataSource,
      samDAO,
      workspaceManagerDAO,
      gcsDAO.getBucketServiceAccountCredential,
      mockServer.mockServerBaseUrl
    )

    override val genomicsServiceConstructor = GenomicsService.constructor(
      slickDataSource,
      gcsDAO
    )_

    override val statisticsServiceConstructor = StatisticsService.constructor(
      slickDataSource,
      gcsDAO
    )_

    val methodRepoDAO = new HttpMethodRepoDAO(
      MethodRepoConfig[Agora.type](mockServer.mockServerBaseUrl, ""),
      MethodRepoConfig[Dockstore.type](mockServer.mockServerBaseUrl, ""),
      workbenchMetricBaseName = workbenchMetricBaseName)

    val healthMonitor = system.actorOf(HealthMonitor.props(
      dataSource, gcsDAO, gpsDAO, methodRepoDAO, samDAO, executionServiceCluster.readMembers.map(c => c.key->c.dao).toMap,
      Seq("my-favorite-group"), Seq.empty, Seq("my-favorite-bucket")))
    override val statusServiceConstructor = StatusService.constructor(healthMonitor)_
    val bigQueryDAO = new MockGoogleBigQueryDAO
    val submissionCostService = new MockSubmissionCostService("test", "test", bigQueryDAO)
    val execServiceBatchSize = 3
    val maxActiveWorkflowsTotal = 10
    val maxActiveWorkflowsPerUser = 2
    val workspaceServiceConfig = WorkspaceServiceConfig(
      true,
      "fc-"
    )

    val bondApiDAO: BondApiDAO = new MockBondApiDAO(bondBaseUrl = "bondUrl")
    val requesterPaysSetupService = new RequesterPaysSetupService(slickDataSource, gcsDAO, bondApiDAO, requesterPaysRole = "requesterPaysRole")

    val entityManager = EntityManager.defaultEntityManager(dataSource, workspaceManagerDAO, dataRepoDAO, samDAO, bigQueryServiceFactory, DataRepoEntityProviderConfig(100, 10))

    override val workspaceServiceConstructor = WorkspaceService.constructor(
      slickDataSource,
      methodRepoDAO,
      new HttpExecutionServiceDAO(mockServer.mockServerBaseUrl, workbenchMetricBaseName = workbenchMetricBaseName),
      executionServiceCluster,
      execServiceBatchSize,
      workspaceManagerDAO,
      dataRepoDAO,
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
      entityManager
    )_

    override val entityServiceConstructor = EntityService.constructor(
      slickDataSource,
      samDAO,
      workbenchMetricBaseName,
      entityManager
    )_

    def cleanupSupervisor = {
      submissionSupervisor ! PoisonPill
    }

    val appVersion = ApplicationVersion("dummy", "dummy", "dummy")
    val googleClientId = "dummy"

    // for metrics testing
    val sealedInstrumentedRoutes: Route = instrumentRequest {
      sealRoute(adminRoutes ~ billingRoutes ~ entityRoutes ~ methodConfigRoutes ~ notificationsRoutes ~ statusRoute ~
        submissionRoutes ~ userRoutes ~ workspaceRoutes)
    }
  }

}
