package org.broadinstitute.dsde.rawls.webservice

import akka.actor.PoisonPill
import org.broadinstitute.dsde.rawls.dataaccess.{DataSource, HttpExecutionServiceDAO, HttpMethodRepoDAO, _}
import org.broadinstitute.dsde.rawls.graph.OrientDbTestFixture
import org.broadinstitute.dsde.rawls.jobexec.SubmissionSupervisor
import org.broadinstitute.dsde.rawls.mock.RemoteServicesMockServer
import org.broadinstitute.dsde.rawls.model.UserJsonSupport._
import org.broadinstitute.dsde.rawls.model.UserAuthJsonSupport.RawlsBillingProjectNameFormat
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.openam.MockUserInfoDirectives
import org.broadinstitute.dsde.rawls.user.UserService
import org.broadinstitute.dsde.rawls.workspace.WorkspaceService
import org.scalatest.{FlatSpec, Matchers}
import spray.http._
import spray.json._
import spray.routing.HttpService
import spray.testkit.ScalatestRouteTest
import spray.httpx.SprayJsonSupport._

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

import com.tinkerpop.blueprints.{Vertex, Graph}
import com.tinkerpop.blueprints.impls.orient.OrientVertex

import scala.collection.JavaConversions._


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

  case class TestApiService(dataSource: DataSource, user: String)(implicit val executionContext: ExecutionContext) extends WorkspaceApiService with EntityApiService with MethodConfigApiService with SubmissionApiService with UserApiService with AdminApiService with MockUserInfoDirectives {
    def actorRefFactory = system

    val submissionSupervisor = system.actorOf(SubmissionSupervisor.props(
      containerDAO,
      new HttpExecutionServiceDAO(mockServer.mockServerBaseUrl),
      dataSource
    ).withDispatcher("submission-monitor-dispatcher"), "test-wsapi-submission-supervisor")
    val workspaceServiceConstructor = WorkspaceService.constructor(dataSource, containerDAO, new HttpMethodRepoDAO(mockServer.mockServerBaseUrl), new HttpExecutionServiceDAO(mockServer.mockServerBaseUrl), new MockGoogleServicesDAO, submissionSupervisor)_
    val gcsDAO: MockGoogleServicesDAO = new MockGoogleServicesDAO
    val directoryDAO = new MockUserDirectoryDAO
    val userServiceConstructor = UserService.constructor(dataSource, gcsDAO, containerDAO, directoryDAO)_

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

  def userFromId(subjectId: String) =
    RawlsUser(RawlsUserSubjectId(subjectId), RawlsUserEmail("dummy@example.com"))

  def getMatchingUserVertices(graph: Graph, user: RawlsUser): Iterable[Vertex] =
    graph.getVertices.filter(v => {
      v.asInstanceOf[OrientVertex].getRecord.getClassName.equalsIgnoreCase(VertexSchema.User) &&
        v.getProperty[String]("userSubjectId") == user.userSubjectId.toString
    })


  "UserApi" should "put token and get date" in withTestDataApiServices { services =>
    Put("/user/refreshToken", HttpEntity(ContentTypes.`application/json`, UserRefreshToken("gobblegobble").toJson.toString)) ~>
      sealRoute(services.userRoutes) ~>
      check { assertResult(StatusCodes.Created) {status} }

    Get("/user/refreshTokenDate") ~>
      sealRoute(services.userRoutes) ~>
      check { assertResult(StatusCodes.OK) {status} }
  }

  it should "get 404 when token is not set" in withTestDataApiServices { services =>
    Get("/user/refreshTokenDate") ~>
      sealRoute(services.userRoutes) ~>
      check { assertResult(StatusCodes.NotFound) {status} }
  }

  it should "create a graph user, user proxy group and ldap entry" in withEmptyTestDatabase { dataSource =>
    withApiServices(dataSource) { services =>

      // values from MockUserInfoDirectives
      val user = RawlsUser(RawlsUserSubjectId("123456789876543212345"), RawlsUserEmail("test_token"))

      dataSource.inTransaction() { txn =>
        txn.withGraph { graph =>
          assert {
            getMatchingUserVertices(graph, user).isEmpty
          }
        }
      }

      assert {
        ! services.gcsDAO.containsProxyGroup(user)
      }
      assert {
        ! services.directoryDAO.exists(user)
      }

      Post("/user") ~>
        sealRoute(services.createUserRoute) ~>
        check {
          assertResult(StatusCodes.Created) {
            status
          }
        }

      dataSource.inTransaction() { txn =>
        txn.withGraph { graph =>
          assert {
            getMatchingUserVertices(graph, user).nonEmpty
          }
        }
      }

      assert {
        services.gcsDAO.containsProxyGroup(user)
      }
      assert {
        services.directoryDAO.exists(user)
      }
    }
  }

  it should "enable/disable user" in withEmptyTestDatabase { dataSource =>
    withApiServices(dataSource) { services =>

      // values from MockUserInfoDirectives
      val user = RawlsUser(RawlsUserSubjectId("123456789876543212345"), RawlsUserEmail("test_token"))

      Post("/user") ~>
        sealRoute(services.createUserRoute) ~>
        check {
          assertResult(StatusCodes.Created) {
            status
          }
        }
      Get(s"/user/${user.userSubjectId}") ~>
        sealRoute(services.userRoutes) ~>
        check {
          assertResult(StatusCodes.OK) {
            status
          }
          assertResult(UserStatus(user, Map("google" -> false, "ldap" -> false))) {
            responseAs[UserStatus]
          }
        }
      Post(s"/user/${user.userSubjectId}/enable") ~>
        sealRoute(services.userRoutes) ~>
        check {
          assertResult(StatusCodes.NoContent) {
            status
          }
        }
      Get(s"/user/${user.userSubjectId}") ~>
        sealRoute(services.userRoutes) ~>
        check {
          assertResult(StatusCodes.OK) {
            status
          }
          assertResult(UserStatus(user, Map("google" -> true, "ldap" -> true))) {
            responseAs[UserStatus]
          }
        }
      Post(s"/user/${user.userSubjectId}/disable") ~>
        sealRoute(services.userRoutes) ~>
        check {
          assertResult(StatusCodes.NoContent) {
            status
          }
        }
      Get(s"/user/${user.userSubjectId}") ~>
        sealRoute(services.userRoutes) ~>
        check {
          assertResult(StatusCodes.OK) {
            status
          }
          assertResult(UserStatus(user, Map("google" -> false, "ldap" -> false))) {
            responseAs[UserStatus]
          }
        }
    }
  }

  it should "list a user's billing projects" in withTestDataApiServices { services =>

    // first add the project and user to the graph

    val billingUser = RawlsUser(RawlsUserSubjectId("nothing"), RawlsUserEmail("test_token"))
    val project1 = RawlsBillingProject(RawlsBillingProjectName("project1"), Set.empty)

    services.dataSource.inTransaction() { txn =>
      containerDAO.authDAO.saveUser(billingUser, txn)
    }

    Put(s"/admin/billing/${project1.projectName.value}") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.Created) {
          status
        }
      }
    Put(s"/admin/billing/${project1.projectName.value}/${billingUser.userEmail.value}") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
      }

    Get("/user/billing") ~>
      sealRoute(services.userRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
        assertResult(Set(project1.projectName)) {
          responseAs[Seq[RawlsBillingProjectName]].toSet
        }
      }
  }



}
