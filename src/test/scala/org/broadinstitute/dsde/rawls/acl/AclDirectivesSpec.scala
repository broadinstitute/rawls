package org.broadinstitute.dsde.rawls.acl

import java.util.UUID

import akka.actor.PoisonPill
import akka.testkit.TestActorRef
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.graph.OrientDbTestFixture
import org.broadinstitute.dsde.rawls.jobexec.SubmissionSupervisor
import org.broadinstitute.dsde.rawls.mock.RemoteServicesMockServer
import org.broadinstitute.dsde.rawls.openam.MockOpenAmDirectives
import org.broadinstitute.dsde.rawls.webservice._
import org.broadinstitute.dsde.rawls.workspace.WorkspaceService
import org.scalatest.{Matchers, FlatSpec}
import spray.http.{StatusCodes, HttpCookie}
import spray.routing.{Directives, HttpService}
import spray.testkit.ScalatestRouteTest

trait AclDirectivesTestFixture extends Directives with ScalatestRouteTest with OrientDbTestFixture {
  this: org.scalatest.BeforeAndAfterAll with org.scalatest.Suite =>

  case class MockUser(id: String, workspaceNamespace: String, workspace: String, id_route: String, access: AccessLevel.Value)

  object WriteAccessUser extends MockUser(
    id = UUID.randomUUID.toString,
    workspaceNamespace = "myNamespace",   // matches OrientDbTestFixture
    workspace = "myWorkspace",            // matches OrientDbTestFixture
    id_route = "write_id_route",
    access = AccessLevel.Write
  )

  val readRequired = "read_required"
  val ownerRequired = "owner_required"

  case class TestRoutes(dataSource: DataSource) extends WorkspaceApiService with EntityApiService with MethodConfigApiService with SubmissionApiService with GoogleAuthApiService with MockOpenAmDirectives {
    def actorRefFactory = system
    lazy val workspaceService: WorkspaceService = TestActorRef(WorkspaceService.props(workspaceServiceConstructor)).underlyingActor
    val mockServer = RemoteServicesMockServer()

    val submissionSupervisor = system.actorOf(SubmissionSupervisor.props(
      new GraphSubmissionDAO(new GraphWorkflowDAO()),
      new HttpExecutionServiceDAO(mockServer.mockServerBaseUrl),
      new GraphWorkflowDAO(),
      dataSource
    ).withDispatcher("submission-monitor-dispatcher"), "test-acl-directives-submission-supervisor")

    def shutdownSupervisor = {
      submissionSupervisor ! PoisonPill
    }

    val workspaceServiceConstructor = WorkspaceService.constructor(dataSource, workspaceDAO, entityDAO, methodConfigDAO, new HttpMethodRepoDAO(mockServer.mockServerBaseUrl), new HttpExecutionServiceDAO(mockServer.mockServerBaseUrl), MockGoogleCloudStorageDAO, submissionSupervisor, submissionDAO)

    val TestAclDirectives = new AclDirectives(workspaceService)

    def writeIdRoute =
      path(WriteAccessUser.id_route) {
        get {
          TestAclDirectives.withAccess(WriteAccessUser.id, WriteAccessUser.workspaceNamespace, WriteAccessUser.workspace) { access =>
            complete(access.toString)
          }
        }
      }

    def readRequiredRoute =
      path(readRequired) {
        get {
          TestAclDirectives.requireAccess(AccessLevel.Read, WriteAccessUser.id, WriteAccessUser.workspaceNamespace, WriteAccessUser.workspace) {
            complete("success")
          }
        }
      }

    def ownerRequiredRoute =
      path(ownerRequired) {
        get {
          TestAclDirectives.requireAccess(AccessLevel.Owner, WriteAccessUser.id, WriteAccessUser.workspaceNamespace, WriteAccessUser.workspace) {
            complete("success")
          }
        }
      }

    val fixtureRoutes = writeIdRoute ~ readRequiredRoute ~ ownerRequiredRoute
  }

  def withTestDataApiServices(testCode: TestRoutes => Any): Unit = {
    withDefaultTestDatabase { dataSource =>
      val routes = new TestRoutes(dataSource)
      testCode(routes)
      routes.shutdownSupervisor
    }
  }
}

class AclDirectivesSpec extends FlatSpec with AclDirectivesTestFixture with HttpService with ScalatestRouteTest with Matchers {

  def actorRefFactory = system

  "AclDirectives" should "return the Write access level" in withTestDataApiServices { testRoutes =>
    Get("/" + WriteAccessUser.id_route) ~> sealRoute(testRoutes.fixtureRoutes) ~> check {
      assertResult(StatusCodes.OK) { status }
      assertResult(WriteAccessUser.access.toString) { responseAs[String] }
    }
  }

  it should "pass when read access is required" in withTestDataApiServices { testRoutes =>
    Get("/" + readRequired) ~> sealRoute(testRoutes.fixtureRoutes) ~> check {
      assertResult(StatusCodes.OK) { status }
      assertResult("success") { responseAs[String] }
    }
  }

  it should "fail when owner access is required" in withTestDataApiServices { testRoutes =>
    Get("/" + ownerRequired) ~> sealRoute(testRoutes.fixtureRoutes) ~> check {
      assertResult(StatusCodes.Forbidden) { status }
     }
  }

}
