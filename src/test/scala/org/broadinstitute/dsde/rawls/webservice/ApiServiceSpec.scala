package org.broadinstitute.dsde.rawls.webservice

import akka.actor.PoisonPill
import org.broadinstitute.dsde.rawls.dataaccess._
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
    val dataSource: SlickDataSource
    val gcsDAO: MockGoogleServicesDAO

    def actorRefFactory = system

    val executionServiceDAO = new HttpExecutionServiceDAO(mockServer.mockServerBaseUrl, mockServer.defaultWorkflowSubmissionTimeout)

    val submissionSupervisor = system.actorOf(SubmissionSupervisor.props(
      executionServiceDAO,
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
      directoryDAO
    )_

    val execServiceBatchSize = 3
    val workspaceServiceConstructor = WorkspaceService.constructor(
      slickDataSource,
      new HttpMethodRepoDAO(mockServer.mockServerBaseUrl),
      executionServiceDAO,
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
