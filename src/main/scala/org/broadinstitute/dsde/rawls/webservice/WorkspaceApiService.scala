package org.broadinstitute.dsde.rawls.webservice

import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.model.WorkspaceACLJsonSupport._
import org.broadinstitute.dsde.rawls.openam.UserInfoDirectives
import AttributeUpdateOperations.AttributeUpdateOperation
import org.broadinstitute.dsde.rawls.workspace.WorkspaceService
import spray.routing.Directive.pimpApply
import spray.routing._

import scala.concurrent.ExecutionContext

/**
 * Created by dvoet on 6/4/15.
 */

trait WorkspaceApiService extends HttpService with PerRequestCreator with UserInfoDirectives {
  implicit val executionContext: ExecutionContext

  import spray.httpx.SprayJsonSupport._
  import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._

  val workspaceServiceConstructor: UserInfo => WorkspaceService
  val workspaceRoutes = requireUserInfo() { userInfo =>
    path("workspaces") {
      post {
        entity(as[WorkspaceRequest]) { workspace =>
          requestContext => perRequest(requestContext,
            WorkspaceService.props(workspaceServiceConstructor, userInfo),
            WorkspaceService.CreateWorkspace(workspace))
        }
      }
    } ~
    path("workspaces" / Segment / Segment) { (workspaceNamespace, workspaceName) =>
      patch {
        entity(as[Array[AttributeUpdateOperation]]) { operations =>
          requestContext => perRequest(requestContext, WorkspaceService.props(workspaceServiceConstructor, userInfo),
            WorkspaceService.UpdateWorkspace(WorkspaceName(workspaceNamespace, workspaceName), operations))
        }
      }
    } ~
    path("workspaces" / Segment / Segment) { (workspaceNamespace, workspaceName) =>
      get {
        requestContext => perRequest(requestContext, WorkspaceService.props(workspaceServiceConstructor, userInfo), WorkspaceService.GetWorkspace(WorkspaceName(workspaceNamespace, workspaceName)))
      }
    } ~
      path("workspaces" / Segment / Segment) { (workspaceNamespace, workspaceName) =>
        delete {
          requestContext => perRequest(requestContext, WorkspaceService.props(workspaceServiceConstructor, userInfo), WorkspaceService.DeleteWorkspace(WorkspaceName(workspaceNamespace, workspaceName)))
        }
      } ~
    path("workspaces") {
      get {
        requestContext => perRequest(requestContext, WorkspaceService.props(workspaceServiceConstructor, userInfo), WorkspaceService.ListWorkspaces)
      }
    } ~
    path("workspaces" / Segment / Segment / "clone" ) { (sourceNamespace, sourceWorkspace) =>
      post {
        entity(as[WorkspaceName]) { destWorkspace =>
          requestContext => perRequest(requestContext, WorkspaceService.props(workspaceServiceConstructor, userInfo),
            WorkspaceService.CloneWorkspace(WorkspaceName(sourceNamespace, sourceWorkspace), destWorkspace))
        }
      }
    } ~
    path("workspaces" / Segment / Segment / "acl" ) { (workspaceNamespace, workspaceName) =>
      get {
        requestContext => perRequest(requestContext, WorkspaceService.props(workspaceServiceConstructor, userInfo),
                                WorkspaceService.GetACL(WorkspaceName(workspaceNamespace, workspaceName)))
      }
    } ~
    path("workspaces" / Segment / Segment / "acl" ) { (workspaceNamespace, workspaceName) =>
      patch {
        requireUserInfo() { userInfo =>
          entity(as[Array[WorkspaceACLUpdate]]) { aclUpdate =>
            requestContext => perRequest(requestContext, WorkspaceService.props(workspaceServiceConstructor, userInfo),
                                      WorkspaceService.UpdateACL(WorkspaceName(workspaceNamespace, workspaceName), aclUpdate))
          }
        }
      }
    } ~
    path("workspaces" / Segment / Segment / "lock" ) { (workspaceNamespace, workspaceName) =>
      put {
        requireUserInfo() { userInfo =>
          requestContext => perRequest(requestContext, WorkspaceService.props(workspaceServiceConstructor, userInfo),
            WorkspaceService.LockWorkspace(WorkspaceName(workspaceNamespace, workspaceName)))
        }
      }
    } ~
    path("workspaces" / Segment / Segment / "unlock" ) { (workspaceNamespace, workspaceName) =>
      put {
        requireUserInfo() { userInfo =>
          requestContext => perRequest(requestContext, WorkspaceService.props(workspaceServiceConstructor, userInfo),
            WorkspaceService.UnlockWorkspace(WorkspaceName(workspaceNamespace, workspaceName)))
        }
      }
    }
  }
}
