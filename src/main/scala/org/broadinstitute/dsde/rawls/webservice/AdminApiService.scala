package org.broadinstitute.dsde.rawls.webservice

/**
 * Created by tsharpe on 9/25/15.
 */

import org.broadinstitute.dsde.rawls.model.UserInfo
import org.broadinstitute.dsde.rawls.openam.UserInfoDirectives
import org.broadinstitute.dsde.rawls.workspace.WorkspaceService
import spray.routing._

trait AdminApiService extends HttpService with PerRequestCreator with UserInfoDirectives {
  lazy private implicit val executionContext = actorRefFactory.dispatcher
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
    }
  }
}
