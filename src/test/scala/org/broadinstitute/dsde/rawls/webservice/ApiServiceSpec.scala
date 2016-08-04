package org.broadinstitute.dsde.rawls.webservice

import java.util.concurrent.TimeUnit

import akka.actor.PoisonPill
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.genomics.GenomicsService
import org.broadinstitute.dsde.rawls.statistics.StatisticsService
import org.broadinstitute.dsde.rawls.jobexec.SubmissionSupervisor
import org.broadinstitute.dsde.rawls.mock.RemoteServicesMockServer
import org.broadinstitute.dsde.rawls.monitor.BucketDeletionMonitor
import org.broadinstitute.dsde.rawls.user.UserService
import org.broadinstitute.dsde.rawls.workspace.WorkspaceService
import spray.http.{ContentTypes, HttpEntity}
import spray.httpx.SprayJsonSupport
import spray.json._
import spray.routing._
import spray.testkit.ScalatestRouteTest
import scala.concurrent.duration._
import org.broadinstitute.dsde.rawls.dataaccess.slick.TestDriverComponentWithFlatSpecAndMatchers

// common trait to be inherited by API service tests
trait ApiServiceSpec extends TestDriverComponentWithFlatSpecAndMatchers with HttpService with ScalatestRouteTest with SprayJsonSupport {
  // increate the timeout for ScalatestRouteTest from the default of 1 second, otherwise
  // intermittent failures occur on requests not completing in time
  implicit val routeTestTimeout = RouteTestTimeout(5.seconds)

  def actorRefFactory = system

  val mockServer = RemoteServicesMockServer()

  override def beforeAll(): Unit = {
    super.beforeAll()
    mockServer.startServer
  }

  override def afterAll(): Unit = {
    mockServer.stopServer
    super.afterAll()
  }

  def httpJson[T](obj: T)(implicit writer: JsonWriter[T]) = HttpEntity(ContentTypes.`application/json`, obj.toJson.toString())

  trait ApiServices extends AdminApiService with EntityApiService with MethodConfigApiService with SubmissionApiService with UserApiService with WorkspaceApiService with BillingApiService {
    val dataSource: SlickDataSource
    val gcsDAO: MockGoogleServicesDAO

    def actorRefFactory = system

    val submissionTimeout = FiniteDuration(1, TimeUnit.MINUTES)

    val executionServiceCluster = MockShardedExecutionServiceCluster.fromDAO(new HttpExecutionServiceDAO(mockServer.mockServerBaseUrl, mockServer.defaultWorkflowSubmissionTimeout), slickDataSource)

    val submissionSupervisor = system.actorOf(SubmissionSupervisor.props(
      executionServiceCluster,
      slickDataSource
    ).withDispatcher("submission-monitor-dispatcher"))

    val bucketDeletionMonitor = system.actorOf(BucketDeletionMonitor.props(
      slickDataSource,
      gcsDAO
    ))

    val directoryDAO = new MockUserDirectoryDAO

    val userServiceConstructor = UserService.constructor(
      slickDataSource,
      gcsDAO,
      directoryDAO,
      ProjectTemplate(Map.empty, Seq.empty)
    )_

    val genomicsServiceConstructor = GenomicsService.constructor(
      slickDataSource,
      gcsDAO,
      directoryDAO
    )_

    val statisticsServiceConstructor = StatisticsService.constructor(
      slickDataSource,
      gcsDAO,
      directoryDAO
    )_

    val execServiceBatchSize = 3
    val workspaceServiceConstructor = WorkspaceService.constructor(
      slickDataSource,
      new HttpMethodRepoDAO(mockServer.mockServerBaseUrl),
      executionServiceCluster,
      execServiceBatchSize,
      gcsDAO,
      submissionSupervisor,
      bucketDeletionMonitor,
      userServiceConstructor
    )_

    def cleanupSupervisor = {
      submissionSupervisor ! PoisonPill
    }
  }

}
