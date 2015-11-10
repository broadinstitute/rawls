package org.broadinstitute.dsde.rawls.webservice

/**
 * Created by tsharpe on 9/25/15.
 */

import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.openam.UserInfoDirectives
import org.broadinstitute.dsde.rawls.workspace.WorkspaceService
import spray.routing._

import scala.concurrent.ExecutionContext

trait AdminApiService extends HttpService with PerRequestCreator with UserInfoDirectives {
  implicit val executionContext: ExecutionContext

  import org.broadinstitute.dsde.rawls.model.UserAuthJsonSupport._
  import spray.httpx.SprayJsonSupport._

  val workspaceServiceConstructor: UserInfo => WorkspaceService

  val adminRoutes = requireUserInfo() { userInfo =>
    path("admin" / "users") {
      get {
        requestContext => perRequest(requestContext,
          WorkspaceService.props(workspaceServiceConstructor, userInfo),
          WorkspaceService.ListAdmins)
      }
    } ~
    path("admin" / "users" / Segment) { (userId) =>
      get {
        requestContext => perRequest(requestContext,
          WorkspaceService.props(workspaceServiceConstructor, userInfo),
          WorkspaceService.IsAdmin(userId))
      }
    } ~
    path("admin" / "users" / Segment) { (userId) =>
      put {
        requestContext => perRequest(requestContext,
          WorkspaceService.props(workspaceServiceConstructor, userInfo),
          WorkspaceService.AddAdmin(userId))
      }
    } ~
    path("admin" / "users" / Segment) { (userId) =>
      delete {
        requestContext => perRequest(requestContext,
          WorkspaceService.props(workspaceServiceConstructor, userInfo),
          WorkspaceService.DeleteAdmin(userId))
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
            WorkspaceService.props(workspaceServiceConstructor, userInfo),
            WorkspaceService.CreateGroup(groupRef))
        }
      }
    } ~
    path("admin" / "groups" / Segment) { (groupName) => //delete group
      delete {
        requestContext => perRequest(requestContext,
          WorkspaceService.props(workspaceServiceConstructor, userInfo),
          WorkspaceService.DeleteGroup(groupName))
      }
    } ~
    path("admin" / "groups" / Segment / "members" / Segment) { (groupName, memberEmail) => //add member to group
      put { //should probably be a POST
        requestContext => perRequest(requestContext,
          WorkspaceService.props(workspaceServiceConstructor, userInfo),
          WorkspaceService.AddGroupMember(groupName, memberEmail))
      }
    } ~
    path("admin" / "groups" / Segment / "members" / Segment) { (groupName, memberEmail) => //remove member from group
      delete {
        requestContext => perRequest(requestContext,
          WorkspaceService.props(workspaceServiceConstructor, userInfo),
          WorkspaceService.RemoveGroupMember(groupName, memberEmail))
      }
    } ~
    path("admin" / "groups" / Segment / "members") { (groupName) => //list group members
      get {
        requestContext => perRequest(requestContext,
          WorkspaceService.props(workspaceServiceConstructor, userInfo),
          WorkspaceService.ListGroupMembers(groupName))
      }
    }
  }
}
