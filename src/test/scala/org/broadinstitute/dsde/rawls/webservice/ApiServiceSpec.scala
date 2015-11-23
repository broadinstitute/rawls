package org.broadinstitute.dsde.rawls.webservice

import akka.actor.PoisonPill
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.graph.OrientDbTestFixture
import org.broadinstitute.dsde.rawls.jobexec.SubmissionSupervisor
import org.broadinstitute.dsde.rawls.mock.RemoteServicesMockServer
import org.broadinstitute.dsde.rawls.monitor.BucketDeletionMonitor
import org.broadinstitute.dsde.rawls.user.UserService
import org.broadinstitute.dsde.rawls.workspace.WorkspaceService
import org.scalatest.{Matchers, FlatSpec}
import spray.http.{ContentTypes, HttpEntity}
import spray.httpx.SprayJsonSupport
import spray.json._
import spray.routing._
import spray.testkit.ScalatestRouteTest
import scala.concurrent.duration._

// common trait to be inherited by API service tests
trait ApiServiceSpec extends FlatSpec with HttpService with ScalatestRouteTest with Matchers with OrientDbTestFixture with SprayJsonSupport {
  // increate the timeout for ScalatestRouteTest from the default of 1 second, otherwise
  // intermittent failures occur on requests not completing in time
  implicit val routeTestTimeout = RouteTestTimeout(5.seconds)

  def actorRefFactory = system

  val mockServer = RemoteServicesMockServer()

  override def beforeAll = {
    super.beforeAll
    mockServer.startServer
  }

  override def afterAll = {
    super.afterAll
    mockServer.stopServer
  }

  def httpJson[T](obj: T)(implicit writer: JsonWriter[T]) = HttpEntity(ContentTypes.`application/json`, obj.toJson.toString())

  trait ApiServices extends AdminApiService with EntityApiService with MethodConfigApiService with SubmissionApiService with UserApiService with WorkspaceApiService {
    val dataSource: DataSource
    val gcsDAO: MockGoogleServicesDAO

    def actorRefFactory = system

    val submissionSupervisor = system.actorOf(SubmissionSupervisor.props(
      containerDAO,
      new HttpExecutionServiceDAO(mockServer.mockServerBaseUrl),
      dataSource
    ).withDispatcher("submission-monitor-dispatcher"), "test-wsapi-submission-supervisor")

    val bucketDeletionMonitor = system.actorOf(BucketDeletionMonitor.props(
      dataSource,
      containerDAO,
      gcsDAO
    ))

    val workspaceServiceConstructor = WorkspaceService.constructor(
      dataSource,
      containerDAO,
      new HttpMethodRepoDAO(mockServer.mockServerBaseUrl),
      new HttpExecutionServiceDAO(mockServer.mockServerBaseUrl),
      gcsDAO,
      submissionSupervisor,
      bucketDeletionMonitor
    )_

    val directoryDAO = new MockUserDirectoryDAO

    val userServiceConstructor = UserService.constructor(
      dataSource,
      gcsDAO,
      containerDAO,
      directoryDAO
    )_

    def cleanupSupervisor = {
      submissionSupervisor ! PoisonPill
    }
  }

}
