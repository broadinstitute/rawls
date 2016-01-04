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

  val workspaceServiceConstructor: UserInfo => WorkspaceService
  val userServiceConstructor: UserInfo => UserService

  val adminRoutes = requireUserInfo() { userInfo =>
    path("admin" / "billing" / "list" / Segment) { (userEmail) =>
      get {
        requestContext => perRequest(requestContext,
          UserService.props(userServiceConstructor, userInfo),
          UserService.ListBillingProjectsForUser(RawlsUserEmail(userEmail)))
      }
    } ~
    path("admin" / "billing" / Segment) { (projectId) =>
      put {
        requestContext => perRequest(requestContext,
          UserService.props(userServiceConstructor, userInfo),
          UserService.CreateBillingProject(RawlsBillingProjectName(projectId)))
      } ~
      delete {
        requestContext => perRequest(requestContext,
          UserService.props(userServiceConstructor, userInfo),
          UserService.DeleteBillingProject(RawlsBillingProjectName(projectId)))
      }
    } ~
    path("admin" / "billing" / Segment / Segment) { (projectId, userEmail) =>
      put {
        requestContext => perRequest(requestContext,
          UserService.props(userServiceConstructor, userInfo),
          UserService.AddUserToBillingProject(RawlsBillingProjectName(projectId), RawlsUserEmail(userEmail)))
      } ~
      delete {
        requestContext => perRequest(requestContext,
          UserService.props(userServiceConstructor, userInfo),
          UserService.RemoveUserFromBillingProject(RawlsBillingProjectName(projectId), RawlsUserEmail(userEmail)))
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
          WorkspaceService.AdminAbortSubmission(workspaceNamespace, workspaceName, submissionId))
      }
    } ~
    path("admin" / "groups") { //create group
      post {
        entity(as[RawlsGroupRef]) { groupRef =>
          requestContext => perRequest(requestContext,
            UserService.props(userServiceConstructor, userInfo),
            UserService.CreateGroup(groupRef))
        }
      } ~
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
      } ~
      delete {
        entity(as[RawlsGroupMemberList]) { memberList =>
          requestContext => perRequest(requestContext,
            UserService.props(userServiceConstructor, userInfo),
            UserService.RemoveGroupMembers(RawlsGroupRef(RawlsGroupName(groupName)), memberList))
        }
      } ~
      get {
        requestContext => perRequest(requestContext,
          UserService.props(userServiceConstructor, userInfo),
          UserService.ListGroupMembers(groupName))
      }
    } ~
    path("admin" / "users") {
      get {
        requestContext => perRequest(requestContext,
          UserService.props(userServiceConstructor, userInfo),
          UserService.ListUsers)
      } ~
      post {
        entity(as[RawlsUserInfoList]) { userInfoList =>
          requestContext => perRequest(requestContext,
            UserService.props(userServiceConstructor, userInfo),
            UserService.ImportUsers(userInfoList))
        }
      }
    } ~
    path("admin" / "allUserReadAccess" / Segment / Segment) { (workspaceNamespace, workspaceName) =>
      get {
        requestContext => perRequest(requestContext,
          WorkspaceService.props(workspaceServiceConstructor, userInfo),
          WorkspaceService.HasAllUserReadAccess(WorkspaceName(workspaceNamespace, workspaceName)))
      } ~
      put {
        requestContext => perRequest(requestContext,
          WorkspaceService.props(workspaceServiceConstructor, userInfo),
          WorkspaceService.GrantAllUserReadAccess(WorkspaceName(workspaceNamespace, workspaceName)))
      } ~
      delete {
        requestContext => perRequest(requestContext,
          WorkspaceService.props(workspaceServiceConstructor, userInfo),
          WorkspaceService.RevokeAllUserReadAccess(WorkspaceName(workspaceNamespace, workspaceName)))
      }
    }
  }
}
