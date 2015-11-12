package org.broadinstitute.dsde.rawls.webservice

/**
 * Created by tsharpe on 9/25/15.
 */

import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.openam.UserInfoDirectives
import org.broadinstitute.dsde.rawls.user.UserService
import org.broadinstitute.dsde.rawls.workspace.WorkspaceService
import spray.routing._

import scala.concurrent.ExecutionContext

trait AdminApiService extends HttpService with PerRequestCreator with UserInfoDirectives {
  implicit val executionContext: ExecutionContext

  import org.broadinstitute.dsde.rawls.model.UserAuthJsonSupport._
  import spray.httpx.SprayJsonSupport._

  val userServiceConstructor: UserInfo => UserService
  val workspaceServiceConstructor: UserInfo => WorkspaceService

  val adminRoutes = requireUserInfo() { userInfo =>
    path("admin" / "users") {
      get {
        requestContext => perRequest(requestContext,
          UserService.props(userServiceConstructor, userInfo),
          UserService.ListAdmins)
      }
    } ~
    path("admin" / "users" / Segment) { (userId) =>
      get {
        requestContext => perRequest(requestContext,
          UserService.props(userServiceConstructor, userInfo),
          UserService.IsAdmin(userId))
      }
    } ~
    path("admin" / "users" / Segment) { (userId) =>
      put {
        requestContext => perRequest(requestContext,
          UserService.props(userServiceConstructor, userInfo),
          UserService.AddAdmin(userId))
      }
    } ~
    path("admin" / "users" / Segment) { (userId) =>
      delete {
        requestContext => perRequest(requestContext,
          UserService.props(userServiceConstructor, userInfo),
          UserService.DeleteAdmin(userId))
      }
    } ~
    path("admin" / "submissions") {
      get {
        requestContext => perRequest(requestContext,
          WorkspaceService.props(workspaceServiceConstructor, userInfo),
          WorkspaceService.ListAllActiveSubmissions)
      }
    } ~
    path("admin" / "submissions" / Segment / Segment / Segment) { (workspaceNamespace, workspaceName, submissionId) =>
      delete {
        requestContext => perRequest(requestContext,
          WorkspaceService.props(workspaceServiceConstructor, userInfo),
          WorkspaceService.AdminAbortSubmission(workspaceNamespace,workspaceName,submissionId))
      }
    } ~
    path("admin" / "groups") { //create group
      post {
        entity(as[RawlsGroupRef]) { groupRef =>
          requestContext => perRequest(requestContext,
            UserService.props(userServiceConstructor, userInfo),
            UserService.CreateGroup(groupRef))
        }
      }
    } ~
    path("admin" / "groups") { //delete group
      delete {
        entity(as[RawlsGroupRef]) { groupRef =>
          requestContext => perRequest(requestContext,
            UserService.props(userServiceConstructor, userInfo),
            UserService.DeleteGroup(groupRef))
        }
      }
    } ~
    path("admin" / "groups" / Segment / "members") { (groupName) => //add members to group
      post {
        entity(as[RawlsGroupMemberList]) { memberList =>
          requestContext => perRequest(requestContext,
            UserService.props(userServiceConstructor, userInfo),
            UserService.AddGroupMembers(RawlsGroupRef(RawlsGroupName(groupName)), memberList))
        }
      }
    } ~
    path("admin" / "groups" / Segment / "members") { (groupName) => //remove member from group
      delete {
        entity(as[RawlsGroupMemberList]) { memberList =>
          requestContext => perRequest(requestContext,
            UserService.props(userServiceConstructor, userInfo),
            UserService.RemoveGroupMembers(RawlsGroupRef(RawlsGroupName(groupName)), memberList))
        }
      }
    } ~
    path("admin" / "groups" / Segment / "members") { (groupName) => //list group members
      get {
        requestContext => perRequest(requestContext,
          UserService.props(userServiceConstructor, userInfo),
          UserService.ListGroupMembers(groupName))
      }
    }
  }
}
