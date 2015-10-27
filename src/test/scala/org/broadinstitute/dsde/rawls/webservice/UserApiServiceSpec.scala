package org.broadinstitute.dsde.rawls.webservice

import akka.actor.PoisonPill
import org.broadinstitute.dsde.rawls.dataaccess.{DataSource, HttpExecutionServiceDAO, HttpMethodRepoDAO, _}
import org.broadinstitute.dsde.rawls.graph.OrientDbTestFixture
import org.broadinstitute.dsde.rawls.jobexec.SubmissionSupervisor
import org.broadinstitute.dsde.rawls.mock.RemoteServicesMockServer
import org.broadinstitute.dsde.rawls.model.ExecutionJsonSupport.{SubmissionFormat, SubmissionReportFormat, SubmissionRequestFormat}
import org.broadinstitute.dsde.rawls.model.UserJsonSupport._
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.openam.MockUserInfoDirectives
import org.broadinstitute.dsde.rawls.user.UserService
import org.broadinstitute.dsde.rawls.workspace.WorkspaceService
import org.scalatest.{FlatSpec, Matchers}
import spray.http._
import spray.httpx.SprayJsonSupport._
import spray.json._
import spray.routing.HttpService
import spray.testkit.ScalatestRouteTest

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

/**
 * Created by dvoet on 4/24/15.
 */
class UserApiServiceSpec extends FlatSpec with HttpService with ScalatestRouteTest with Matchers with OrientDbTestFixture {
  // increate the timeout for ScalatestRouteTest from the default of 1 second, otherwise
  // intermittent failures occur on requests not completing in time
  implicit val routeTestTimeout = RouteTestTimeout(5.seconds)

  def actorRefFactory = system

  val mockServer = RemoteServicesMockServer()

  override def beforeAll() = {
    super.beforeAll
    mockServer.startServer
  }

  override def afterAll() = {
    super.afterAll
    mockServer.stopServer
  }

  case class TestApiService(dataSource: DataSource, user: String)(implicit val executionContext: ExecutionContext) extends WorkspaceApiService with EntityApiService with MethodConfigApiService with SubmissionApiService with UserApiService with MockUserInfoDirectives {
    def actorRefFactory = system

    val submissionSupervisor = system.actorOf(SubmissionSupervisor.props(
      containerDAO,
      new HttpExecutionServiceDAO(mockServer.mockServerBaseUrl),
      dataSource
    ).withDispatcher("submission-monitor-dispatcher"), "test-wsapi-submission-supervisor")
    val workspaceServiceConstructor = WorkspaceService.constructor(dataSource, containerDAO, new HttpMethodRepoDAO(mockServer.mockServerBaseUrl), new HttpExecutionServiceDAO(mockServer.mockServerBaseUrl), new MockGoogleServicesDAO, submissionSupervisor)_
    val userServiceConstructor = UserService.constructor(dataSource, new MockGoogleServicesDAO)_

    def cleanupSupervisor = {
      submissionSupervisor ! PoisonPill
    }
  }

  def withApiServices(dataSource: DataSource, user: String = "test_token")(testCode: TestApiService => Any): Unit = {
    val apiService = new TestApiService(dataSource, user)
    try {
      testCode(apiService)
    } finally {
      apiService.cleanupSupervisor
    }
  }

  def withTestDataApiServices(testCode: TestApiService => Any): Unit = {
    withDefaultTestDatabase { dataSource =>
      withApiServices(dataSource)(testCode)
    }
  }

  "UserApi" should "put token and get date" in withTestDataApiServices { services =>
    Put("/user/refreshToken", HttpEntity(ContentTypes.`application/json`, UserRefreshToken("gobblegobble").toJson.toString)) ~>
      sealRoute(services.userRoutes) ~>
      check { assertResult(StatusCodes.Created) {status} }

    Get("/user/refreshTokenDate") ~>
      sealRoute(services.userRoutes) ~>
      check { assertResult(StatusCodes.OK) {status} }
  }

  "UserApi" should "get 404 when token is not set" in withTestDataApiServices { services =>
    Get("/user/refreshTokenDate") ~>
      sealRoute(services.userRoutes) ~>
      check { assertResult(StatusCodes.NotFound) {status} }
  }

}
