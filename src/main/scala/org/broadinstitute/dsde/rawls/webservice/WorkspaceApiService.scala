package org.broadinstitute.dsde.rawls.webservice

import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.openam.OpenAmDirectives
import AttributeUpdateOperations.AttributeUpdateOperation
import org.broadinstitute.dsde.rawls.workspace.WorkspaceService
import spray.routing.Directive.pimpApply
import spray.routing._

/**
 * Created by dvoet on 6/4/15.
 */

trait WorkspaceApiService extends HttpService with PerRequestCreator with OpenAmDirectives {
  lazy private implicit val executionContext = actorRefFactory.dispatcher

  import spray.httpx.SprayJsonSupport._
  import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._

  val workspaceServiceConstructor: UserInfo => WorkspaceService
  val workspaceRoutes = userInfoFromCookie() { userInfo =>
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
      put {
        userInfoFromCookie() { userInfo =>
          entity(as[String]) { acl =>
            requestContext => perRequest(requestContext, WorkspaceService.props(workspaceServiceConstructor, userInfo),
                                      WorkspaceService.PutACL(WorkspaceName(workspaceNamespace, workspaceName), acl))
          }
        }
      }
    }
  }
}
