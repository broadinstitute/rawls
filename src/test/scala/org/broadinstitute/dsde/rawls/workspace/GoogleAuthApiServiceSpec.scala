package org.broadinstitute.dsde.rawls.workspace

import akka.actor.PoisonPill
import akka.testkit.TestKit
import org.broadinstitute.dsde.rawls.graph.OrientDbTestFixture
import org.broadinstitute.dsde.rawls.jobexec.SubmissionSupervisor
import org.broadinstitute.dsde.rawls.webservice._
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.openam._
import org.broadinstitute.dsde.rawls.mock._
import org.scalatest.{FlatSpec, Matchers}
import spray.http.HttpHeaders.Cookie
import spray.http._
import spray.routing.HttpService
import spray.testkit.ScalatestRouteTest

/**
 * Created by dvoet on 4/24/15.
 */
class GoogleAuthApiServiceSpec extends FlatSpec with HttpService with ScalatestRouteTest with Matchers with OrientDbTestFixture {

  def addMockOpenAmCookie = addHeader(Cookie(HttpCookie("iPlanetDirectoryPro", "test_token")))

  def actorRefFactory = system

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

  case class TestApiService(dataSource: DataSource) extends GoogleAuthApiService with MockOpenAmDirectives {
    def actorRefFactory = system
    val submissionSupervisor = system.actorOf(SubmissionSupervisor.props(
      new GraphSubmissionDAO(new GraphWorkflowDAO()),
      new HttpExecutionServiceDAO(mockServer.mockServerBaseUrl),
      new GraphWorkflowDAO(),
      dataSource
    ).withDispatcher("submission-monitor-dispatcher"), "test-gauth-submission-supervisor")
    val workspaceServiceConstructor = WorkspaceService.constructor(dataSource, workspaceDAO, entityDAO, methodConfigDAO, new HttpMethodRepoDAO(mockServer.mockServerBaseUrl), new HttpExecutionServiceDAO(mockServer.mockServerBaseUrl), MockGoogleCloudStorageDAO, submissionSupervisor, submissionDAO)_

    def cleanupSupervisor = {
      submissionSupervisor ! PoisonPill
    }
  }

  def withApiServices(dataSource: DataSource)(testCode: TestApiService => Any): Unit = {
    val apiService = new TestApiService(dataSource)
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

  "GoogleAuthApi" should "return 303 for post register" in withTestDataApiServices { services =>
    Get("/authentication/register") ~>
      addMockOpenAmCookie ~>
      sealRoute(services.authRoutes) ~>
      check {
        assertResult(StatusCodes.SeeOther) {
          status
        }
      }
  }

  it should "return 201 for get on register_callback" in withTestDataApiServices { services =>
    Get("/authentication/register_callback?code=authCode&state=test") ~>
      addMockOpenAmCookie ~>
      sealRoute(services.authRoutes) ~>
      check {
        assertResult(StatusCodes.Created) {
          status
        }
      }
  }
}
