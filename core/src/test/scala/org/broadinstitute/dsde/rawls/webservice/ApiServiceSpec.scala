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
import org.broadinstitute.dsde.rawls.jobexec.SubmissionSupervisor
import org.broadinstitute.dsde.rawls.metrics.RawlsStatsDTestUtils
import org.broadinstitute.dsde.rawls.metrics.{InstrumentationDirectives, RawlsInstrumented}
import org.broadinstitute.dsde.rawls.mock.RemoteServicesMockServer
import org.broadinstitute.dsde.rawls.model.{Agora, ApplicationVersion, Dockstore, RawlsUser}
import org.broadinstitute.dsde.rawls.monitor.{BucketDeletionMonitor, GoogleGroupSyncMonitorSupervisor, HealthMonitor}
import org.broadinstitute.dsde.rawls.statistics.StatisticsService
import org.broadinstitute.dsde.rawls.status.StatusService
import org.broadinstitute.dsde.rawls.user.UserService
import org.broadinstitute.dsde.rawls.util.MockitoTestUtils
import org.broadinstitute.dsde.rawls.workspace.WorkspaceService
import org.scalatest.concurrent.Eventually
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, StatusCodes}
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.server._
import akka.http.scaladsl.server.Route.{seal => sealRoute}
import spray.json._
import akka.http.scaladsl.testkit.{RouteTestTimeout, ScalatestRouteTest}
import akka.http.scaladsl.server.Directives._
import akka.stream.ActorMaterializer
import org.broadinstitute.dsde.rawls.config.{MethodRepoConfig, SwaggerConfig}

import scala.concurrent.duration._

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

  trait ApiServices extends AdminApiService with BillingApiService with EntityApiService with MethodConfigApiService
    with NotificationsApiService with RawlsApiService with StatusApiService with SubmissionApiService with UserApiService with WorkspaceApiService {

    val dataSource: SlickDataSource
    val gcsDAO: MockGoogleServicesDAO
    val gpsDAO: MockGooglePubSubDAO

    def actorRefFactory = system

    override implicit val materializer = ActorMaterializer()
    override val workbenchMetricBaseName: String = "test"
    override val swaggerConfig: SwaggerConfig = SwaggerConfig("foo", "bar")
    override val submissionTimeout = FiniteDuration(1, TimeUnit.MINUTES)

    override val executionServiceCluster = MockShardedExecutionServiceCluster.fromDAO(new HttpExecutionServiceDAO(mockServer.mockServerBaseUrl, workbenchMetricBaseName = workbenchMetricBaseName), slickDataSource)

    val submissionSupervisor = system.actorOf(SubmissionSupervisor.props(
      executionServiceCluster,
      slickDataSource,
      gcsDAO.getBucketServiceAccountCredential,
      5 seconds,
      trackDetailedSubmissionMetrics = true,
      workbenchMetricBaseName
    ).withDispatcher("submission-monitor-dispatcher"))

    val googleGroupSyncTopic = "test-topic-name"

    val notificationTopic = "test-notification-topic"
    val notificationDAO = new PubSubNotificationDAO(gpsDAO, notificationTopic)

    override val userServiceConstructor = UserService.constructor(
      slickDataSource,
      gcsDAO,
      gpsDAO,
      googleGroupSyncTopic,
      notificationDAO,
      samDAO,
      Seq("bigquery.jobUser")
    )_

    val googleGroupSyncMonitorSupervisor = system.actorOf(GoogleGroupSyncMonitorSupervisor.props(500 milliseconds, 0 seconds, gpsDAO, googleGroupSyncTopic, "test-sub-name", 1, userServiceConstructor))

    override val genomicsServiceConstructor = GenomicsService.constructor(
      slickDataSource,
      gcsDAO
    )_

    override val statisticsServiceConstructor = StatisticsService.constructor(
      slickDataSource,
      gcsDAO
    )_

    val methodRepoDAO = new HttpMethodRepoDAO(
      MethodRepoConfig[Agora](mockServer.mockServerBaseUrl, ""),
      MethodRepoConfig[Dockstore](mockServer.mockServerBaseUrl, ""),
      workbenchMetricBaseName = workbenchMetricBaseName)

    val samDAO = new HttpSamDAO(mockServer.mockServerBaseUrl, gcsDAO.getBucketServiceAccountCredential)

    val healthMonitor = system.actorOf(HealthMonitor.props(
      dataSource, gcsDAO, gpsDAO, methodRepoDAO, samDAO, executionServiceCluster.readMembers,
      Seq.empty, Seq.empty, Seq("my-favorite-bucket")))
    override val statusServiceConstructor = StatusService.constructor(healthMonitor)_

    val execServiceBatchSize = 3
    val maxActiveWorkflowsTotal = 10
    val maxActiveWorkflowsPerUser = 2
    override val workspaceServiceConstructor = WorkspaceService.constructor(
      slickDataSource,
      methodRepoDAO,
      executionServiceCluster,
      execServiceBatchSize,
      gcsDAO,
      samDAO,
      notificationDAO,
      userServiceConstructor,
      genomicsServiceConstructor,
      maxActiveWorkflowsTotal,
      maxActiveWorkflowsPerUser,
      workbenchMetricBaseName
    )_

    def cleanupSupervisor = {
      submissionSupervisor ! PoisonPill
      googleGroupSyncMonitorSupervisor ! PoisonPill
    }

    val appVersion = ApplicationVersion("dummy", "dummy", "dummy")
    val googleClientId = "dummy"

    // for metrics testing
    val sealedInstrumentedRoutes: Route = instrumentRequest {
      sealRoute(adminRoutes ~ billingRoutes ~ entityRoutes ~ methodConfigRoutes ~ notificationsRoutes ~ statusRoute ~
        submissionRoutes ~ userRoutes ~ createUserRoute ~ workspaceRoutes)
    }
  }

}
