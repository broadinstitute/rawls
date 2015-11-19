package org.broadinstitute.dsde.rawls.webservice

/**
 * Created by tsharpe on 9/25/15.
 */

import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.openam.UserInfoDirectives
import org.broadinstitute.dsde.rawls.user.UserService
import org.broadinstitute.dsde.rawls.workspace.WorkspaceService
import spray.routing._
import kamon.spray.KamonTraceDirectives._

import scala.concurrent.ExecutionContext

trait AdminApiService extends HttpService with PerRequestCreator with UserInfoDirectives {
  implicit val executionContext: ExecutionContext
  val workspaceServiceConstructor: UserInfo => WorkspaceService
  val userServiceConstructor: UserInfo => UserService

  val adminRoutes = requireUserInfo() { userInfo =>
    path("admin" / "billing" / "list" / Segment) { (userEmail) =>
      get {
        traceName("ListBillingProjectsForUser") {
          requestContext => perRequest(requestContext,
            UserService.props(userServiceConstructor, userInfo),
            UserService.ListBillingProjectsForUser(RawlsUserEmail(userEmail)))
        }
      }
    } ~
    path("admin" / "billing" / Segment) { (projectId) =>
      put {
        traceName("CreateBillingProject") {
          requestContext => perRequest(requestContext,
            UserService.props(userServiceConstructor, userInfo),
            UserService.CreateBillingProject(RawlsBillingProjectName(projectId)))
        }
      } ~
      delete {
        traceName("DeleteBillingProject") {
          requestContext => perRequest(requestContext,
            UserService.props(userServiceConstructor, userInfo),
            UserService.DeleteBillingProject(RawlsBillingProjectName(projectId)))
        }
      }
    } ~
    path("admin" / "billing" / Segment / Segment) { (projectId, userEmail) =>
      put {
        traceName("AddUserToBillingProject") {
          requestContext => perRequest(requestContext,
            UserService.props(userServiceConstructor, userInfo),
            UserService.AddUserToBillingProject(RawlsBillingProjectName(projectId), RawlsUserEmail(userEmail)))
        }
      } ~
      delete {
        traceName("RemoveUserFromBillingProject") {
          requestContext => perRequest(requestContext,
            UserService.props(userServiceConstructor, userInfo),
            UserService.RemoveUserFromBillingProject(RawlsBillingProjectName(projectId), RawlsUserEmail(userEmail)))
        }
      }
    } ~
    path("admin" / "submissions") {
      get {
        traceName("ListAllActiveSubmissions") {
          requestContext => perRequest(requestContext,
            WorkspaceService.props(workspaceServiceConstructor, userInfo),
            WorkspaceService.ListAllActiveSubmissions)
        }
      }
    } ~
    path("admin" / "submissions" / Segment / Segment / Segment) { (workspaceNamespace, workspaceName, submissionId) =>
      delete {
        traceName("AdminAbortSubmission") {
          requestContext => perRequest(requestContext,
            WorkspaceService.props(workspaceServiceConstructor, userInfo),
            WorkspaceService.AdminAbortSubmission(workspaceNamespace, workspaceName, submissionId))
        }
      }
    }
  }
}
