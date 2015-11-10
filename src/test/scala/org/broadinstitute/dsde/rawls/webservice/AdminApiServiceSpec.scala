package org.broadinstitute.dsde.rawls.webservice

import akka.actor.PoisonPill
import org.broadinstitute.dsde.rawls.dataaccess.{MockGoogleServicesDAO, HttpMethodRepoDAO, HttpExecutionServiceDAO, DataSource}
import org.broadinstitute.dsde.rawls.graph.OrientDbTestFixture
import org.broadinstitute.dsde.rawls.jobexec.SubmissionSupervisor
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.model.ExecutionJsonSupport.ActiveSubmissionFormat
import org.broadinstitute.dsde.rawls.mock.RemoteServicesMockServer
import org.broadinstitute.dsde.rawls.openam.MockUserInfoDirectives
import org.broadinstitute.dsde.rawls.workspace.WorkspaceService
import org.scalatest.{Matchers, FlatSpec}
import spray.http.{ContentTypes, HttpEntity, StatusCodes}
import spray.httpx.SprayJsonSupport
import spray.json.DefaultJsonProtocol._
import spray.routing.HttpService
import spray.testkit.ScalatestRouteTest
import spray.json._
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

/**
 * Created by tsharpe on 9/28/15.
 */
class AdminApiServiceSpec extends FlatSpec with HttpService with ScalatestRouteTest with Matchers with OrientDbTestFixture with SprayJsonSupport {
  implicit val routeTestTimeout = RouteTestTimeout(5.seconds)

  import org.broadinstitute.dsde.rawls.model.UserAuthJsonSupport._
  import spray.httpx.SprayJsonSupport._

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

  case class TestApiService(dataSource: DataSource)(implicit val executionContext: ExecutionContext) extends AdminApiService with MockUserInfoDirectives {
    def actorRefFactory = system

    val submissionSupervisor = system.actorOf(SubmissionSupervisor.props(
      containerDAO,
      new HttpExecutionServiceDAO(mockServer.mockServerBaseUrl),
      dataSource
    ).withDispatcher("submission-monitor-dispatcher"), "test-wsapi-submission-supervisor")

    val gcsDAO: MockGoogleServicesDAO = new MockGoogleServicesDAO
    val workspaceServiceConstructor = WorkspaceService.constructor(dataSource, containerDAO, new HttpMethodRepoDAO(mockServer.mockServerBaseUrl), new HttpExecutionServiceDAO(mockServer.mockServerBaseUrl), gcsDAO, submissionSupervisor)_

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

  "AdminApi" should "return 204 when checking for the seeded user" in withTestDataApiServices { services =>
    Get(s"/admin/users/test_token") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.NoContent) { status }
      }
  }

  it should "return 404 when checking for a bogus user" in withTestDataApiServices { services =>
    Get(s"/admin/users/fred") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound) { status }
      }
  }

  it should "return 200 and the seeded user as a single-element list when listing users" in withTestDataApiServices { services =>
    Get(s"/admin/users") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.OK) { status }
        assertResult(Array("test_token")) { responseAs[Array[String]]}
      }
  }

  it should "return 201 when adding an new user to the admin list" in withTestDataApiServices { services =>
    Put(s"/admin/users/bob") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.Created) { status }
      }
  }

  it should "return 204 when adding an existing user to the admin list" in withTestDataApiServices { services =>
    services.gcsDAO.addAdmin("bob")
    Put(s"/admin/users/bob") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.NoContent) { status }
      }
  }

  it should "return 204 when checking on the new admin user" in withTestDataApiServices { services =>
    services.gcsDAO.addAdmin("bob")
    Get(s"/admin/users/bob") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.NoContent) { status }
      }
  }

  it should "return 200 and a two-member list when asked for the current list" in withTestDataApiServices { services =>
    services.gcsDAO.addAdmin("bob")
    services.gcsDAO.addAdmin("test_token")
    Get(s"/admin/users") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.OK) { status }
        responseAs[Array[String]] should contain theSameElementsAs(Array("bob","test_token"))
      }
  }

  it should "return 204 when removing an existing user from the admin list" in withTestDataApiServices { services =>
    services.gcsDAO.addAdmin("bob")
    Delete(s"/admin/users/bob") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.NoContent) { status }
      }
  }

  it should "return 404 when removing a bogus user from the admin list" in withTestDataApiServices { services =>
    Delete(s"/admin/users/jimmy") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound) { status }
      }
  }

  it should "return 200 when listing active submissions" in withTestDataApiServices { services =>
    Get(s"/admin/submissions") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.OK) { status }
        responseAs[Array[ActiveSubmission]] should contain
          theSameElementsAs(Array(ActiveSubmission(testData.wsName.namespace,testData.wsName.name,testData.submission1),
                                  ActiveSubmission(testData.wsName.namespace,testData.wsName.name,testData.submission2),
                                  ActiveSubmission(testData.wsName.namespace,testData.wsName.name,testData.submissionTerminateTest)))
      }
  }

  it should "return 204 when aborting an active submission" in withTestDataApiServices { services =>
    Delete(s"/admin/submissions/${testData.wsName.namespace}/${testData.wsName.name}/${testData.submissionTerminateTest.submissionId}") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.NoContent) { status }
      }
  }

  it should "return 404 when aborting a bogus active submission" in withTestDataApiServices { services =>
    Delete(s"/admin/submissions/${testData.wsName.namespace}/${testData.wsName.name}/fake") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound) { status }
      }
  }

  it should "return 403 when making an admin call as a non-admin user" in withTestDataApiServices { services =>
    Delete(s"/admin/users/test_token") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.NoContent) { status }
      }
    Get(s"/admin/users") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.Forbidden) { status }
      }
    }


  it should "return 201 when creating a new group" in withTestDataApiServices { services =>
    val group = new RawlsGroupRef(RawlsGroupName("test_group"))

    Post(s"/admin/groups", HttpEntity(ContentTypes.`application/json`, group.toJson.toString)) ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.Created) { status }
      }
  }

  it should "return 409 when trying to create a group that already exists" in withTestDataApiServices { services =>
    val group = new RawlsGroupRef(RawlsGroupName("test_group"))

    Post(s"/admin/groups", HttpEntity(ContentTypes.`application/json`, group.toJson.toString)) ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.Created) { status }
      }
    Post(s"/admin/groups", HttpEntity(ContentTypes.`application/json`, group.toJson.toString)) ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.Conflict) { status }
      }
  }

  it should "return 200 when deleting a group that exists" in withTestDataApiServices { services =>
    val group = new RawlsGroupRef(RawlsGroupName("test_group"))

    Post(s"/admin/groups", HttpEntity(ContentTypes.`application/json`, group.toJson.toString)) ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.Created) { status }
      }
    Delete(s"/admin/groups/${group.groupName.value}") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.OK) { status }
      }
  }

  it should "return 404 when trying to delete a group that does not exist" in withTestDataApiServices { services =>
    Delete(s"/admin/groups/doesnotexist") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound) { status }
      }
  }

  //probably check contents too once we can reliably add members
  it should "return 200 when listing the members of a group" in withTestDataApiServices { services =>
    val group = new RawlsGroupRef(RawlsGroupName("test_group"))
    val subGroup = new RawlsGroupRef(RawlsGroupName("dbgap"))

    Post(s"/admin/groups", HttpEntity(ContentTypes.`application/json`, group.toJson.toString)) ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.Created) { status }
      }
    Post(s"/admin/groups", HttpEntity(ContentTypes.`application/json`, subGroup.toJson.toString)) ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.Created) { status }
      }
    Get(s"/admin/groups/${group.groupName.value}/members") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.OK) { status }
      }
    Put(s"/admin/groups/${group.groupName.value}/members/GROUP_${subGroup.groupName.value}@dev.firecloud.org") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.OK) { status }
      }
    Get(s"/admin/groups/${group.groupName.value}/members") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(List(s"GROUP_${subGroup.groupName.value}@dev.firecloud.org")) {
          responseAs[Array[String]]
        }
      }
  }

  it should "return 404 when adding a member that doesn't exist" in withTestDataApiServices { services =>
    val group = new RawlsGroupRef(RawlsGroupName("test_group"))

    Post(s"/admin/groups", HttpEntity(ContentTypes.`application/json`, group.toJson.toString)) ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.Created) { status }
      }
    Put(s"/admin/groups/${group.groupName.value}/members/thisuserdoesntexist@example.com") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound) { status }
      }
  }

  it should "return 200 when adding a member to a group" in withTestDataApiServices { services =>
    val group = new RawlsGroupRef(RawlsGroupName("test_group"))
    val subGroup = new RawlsGroupRef(RawlsGroupName("test_subGroup"))

    Post(s"/admin/groups", HttpEntity(ContentTypes.`application/json`, group.toJson.toString)) ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.Created) { status }
      }
    Post(s"/admin/groups", HttpEntity(ContentTypes.`application/json`, subGroup.toJson.toString)) ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.Created) { status }
      }
    Put(s"/admin/groups/${group.groupName.value}/members/GROUP_${subGroup.groupName.value}@dev.firecloud.org") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.OK) { status }
      }
    Get(s"/admin/groups/${group.groupName.value}/members") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(List(s"GROUP_${subGroup.groupName.value}@dev.firecloud.org")) {
          responseAs[Array[String]]
        }
      }
  }

  it should "return 200 when removing a member from a group" in withTestDataApiServices { services =>
    val group = new RawlsGroupRef(RawlsGroupName("test_group"))
    val subGroup = new RawlsGroupRef(RawlsGroupName("test_subGroup"))

    //make main group
    Post(s"/admin/groups", HttpEntity(ContentTypes.`application/json`, group.toJson.toString)) ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.Created) { status }
      }
    //make subgroup
    Post(s"/admin/groups", HttpEntity(ContentTypes.`application/json`, subGroup.toJson.toString)) ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.Created) { status }
      }
    //put subgroup into main group
    Put(s"/admin/groups/${group.groupName.value}/members/GROUP_${subGroup.groupName.value}@dev.firecloud.org") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.OK) { status }
      }
    //verify subgroup was put into subgroup
    Get(s"/admin/groups/${group.groupName.value}/members") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(List(s"GROUP_${subGroup.groupName.value}@dev.firecloud.org")) {
          responseAs[Array[String]]
        }
      }
    //remove subgroup from main group
    Delete(s"/admin/groups/${group.groupName.value}/members/GROUP_${subGroup.groupName.value}@dev.firecloud.org") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.OK) { status }
      }
    //verify that subgroup was removed from maingroup
    Get(s"/admin/groups/${group.groupName.value}/members") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(List.empty) {
          responseAs[Array[String]]
        }
      }
  }
}
