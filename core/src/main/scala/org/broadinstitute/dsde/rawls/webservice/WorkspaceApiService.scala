package org.broadinstitute.dsde.rawls.webservice

import org.broadinstitute.dsde.rawls.model.AttributeUpdateOperations.AttributeUpdateOperation
import org.broadinstitute.dsde.rawls.model.WorkspaceACLJsonSupport._
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.openam.UserInfoDirectives
import org.broadinstitute.dsde.rawls.workspace.WorkspaceService
import spray.httpx.SprayJsonSupport._
import spray.json.DefaultJsonProtocol._
import spray.routing.Directive.pimpApply
import spray.routing._

import scala.concurrent.ExecutionContext

/**
  * Created by dvoet on 6/4/15.
  */

trait WorkspaceApiService extends HttpService with PerRequestCreator with UserInfoDirectives {
  implicit val executionContext: ExecutionContext

  val workspaceServiceConstructor: UserInfo => WorkspaceService

  val workspaceRoutes = requireUserInfo() { userInfo =>
    path("workspaces") {
      post {
        entity(as[WorkspaceRequest]) { workspace =>
          requestContext =>
            perRequest(requestContext,
              WorkspaceService.props(workspaceServiceConstructor, userInfo),
              WorkspaceService.CreateWorkspace(workspace))
        }
      } ~
        get {
          requestContext => perRequest(requestContext, WorkspaceService.props(workspaceServiceConstructor, userInfo), WorkspaceService.ListWorkspaces)
        }
    } ~
      path("workspaces" / Segment / Segment) { (workspaceNamespace, workspaceName) =>
        patch {
          entity(as[Array[AttributeUpdateOperation]]) { operations =>
            requestContext =>
              perRequest(requestContext, WorkspaceService.props(workspaceServiceConstructor, userInfo),
                WorkspaceService.UpdateWorkspace(WorkspaceName(workspaceNamespace, workspaceName), operations))
          }
        } ~
          get {
            requestContext => perRequest(requestContext, WorkspaceService.props(workspaceServiceConstructor, userInfo), WorkspaceService.GetWorkspace(WorkspaceName(workspaceNamespace, workspaceName)))
          } ~
          delete {
            requestContext => perRequest(requestContext, WorkspaceService.props(workspaceServiceConstructor, userInfo), WorkspaceService.DeleteWorkspace(WorkspaceName(workspaceNamespace, workspaceName)))
          }
      } ~
      path("workspaces" / Segment / Segment / "accessInstructions") { (workspaceNamespace, workspaceName) =>
        get {
          requestContext =>
            perRequest(requestContext, WorkspaceService.props(workspaceServiceConstructor, userInfo),
              WorkspaceService.GetAccessInstructions(WorkspaceName(workspaceNamespace, workspaceName)))
        }
      } ~
      path("workspaces" / Segment / Segment / "clone") { (sourceNamespace, sourceWorkspace) =>
        post {
          entity(as[WorkspaceRequest]) { destWorkspace =>
            requestContext =>
              perRequest(requestContext, WorkspaceService.props(workspaceServiceConstructor, userInfo),
                WorkspaceService.CloneWorkspace(WorkspaceName(sourceNamespace, sourceWorkspace), destWorkspace))
          }
        }
      } ~
      path("workspaces" / Segment / Segment / "acl") { (workspaceNamespace, workspaceName) =>
        get {
          requestContext =>
            perRequest(requestContext, WorkspaceService.props(workspaceServiceConstructor, userInfo),
              WorkspaceService.GetACL(WorkspaceName(workspaceNamespace, workspaceName)))
        } ~
          patch {
            parameter('inviteUsersNotFound.?) { inviteUsersNotFound =>
              entity(as[Array[WorkspaceACLUpdate]]) { aclUpdate =>
                requestContext =>
                  perRequest(requestContext, WorkspaceService.props(workspaceServiceConstructor, userInfo),
                    WorkspaceService.UpdateACL(WorkspaceName(workspaceNamespace, workspaceName), aclUpdate, inviteUsersNotFound.getOrElse("false").toBoolean))
              }
            }
          }
      } ~
      path("workspaces" / Segment / Segment / "library") { (workspaceNamespace, workspaceName) =>
        patch {
          entity(as[Array[AttributeUpdateOperation]]) { operations =>
            requestContext =>
              perRequest(requestContext,
                WorkspaceService.props(workspaceServiceConstructor, userInfo),
                WorkspaceService.UpdateLibraryAttributes(WorkspaceName(workspaceNamespace, workspaceName), operations)
              )
          }
        }
      } ~
      path("workspaces" / Segment / Segment / "catalog") { (workspaceNamespace, workspaceName) =>
        get {
          requestContext =>
            perRequest(requestContext, WorkspaceService.props(workspaceServiceConstructor, userInfo),
              WorkspaceService.GetCatalog(WorkspaceName(workspaceNamespace, workspaceName)))
        } ~
          patch {
            entity(as[Array[WorkspaceCatalog]]) { catalogUpdate =>
              requestContext =>
                perRequest(requestContext, WorkspaceService.props(workspaceServiceConstructor, userInfo),
                  WorkspaceService.UpdateCatalog(WorkspaceName(workspaceNamespace, workspaceName), catalogUpdate))
            }
          }
      } ~
      path("workspaces" / Segment / Segment / "checkBucketReadAccess") { (workspaceNamespace, workspaceName) =>
        get {
          requestContext =>
            perRequest(requestContext, WorkspaceService.props(workspaceServiceConstructor, userInfo),
              WorkspaceService.CheckBucketReadAccess(WorkspaceName(workspaceNamespace, workspaceName)))
        }
      } ~
      path("workspaces" / Segment / Segment / "lock") { (workspaceNamespace, workspaceName) =>
        put {
          requestContext =>
            perRequest(requestContext, WorkspaceService.props(workspaceServiceConstructor, userInfo),
              WorkspaceService.LockWorkspace(WorkspaceName(workspaceNamespace, workspaceName)))
        }
      } ~
      path("workspaces" / Segment / Segment / "unlock") { (workspaceNamespace, workspaceName) =>
        put {
          requestContext =>
            perRequest(requestContext, WorkspaceService.props(workspaceServiceConstructor, userInfo),
              WorkspaceService.UnlockWorkspace(WorkspaceName(workspaceNamespace, workspaceName)))
        }
      } ~
      path("workspaces" / Segment / Segment / "bucketUsage") { (workspaceNamespace, workspaceName) =>
        get {
          requestContext =>
            perRequest(requestContext, WorkspaceService.props(workspaceServiceConstructor, userInfo),
              WorkspaceService.GetBucketUsage(WorkspaceName(workspaceNamespace, workspaceName)))
        }
      } ~
      path("workspaces" / "tags") {
        parameter('q.?) { queryString =>
          get {
            requestContext => perRequest(requestContext, WorkspaceService.props(workspaceServiceConstructor, userInfo), WorkspaceService.GetTags(queryString))
          }
        }
      } ~
      path("workspaces" / Segment / Segment / "sendChangeNotification") { (namespace, name) =>
        post {
          requestContext =>
            perRequest(requestContext, WorkspaceService.props(workspaceServiceConstructor, userInfo),
              WorkspaceService.SendChangeNotifications(WorkspaceName(namespace, name)))
        }
      } ~
      path("workspaces" / Segment / Segment / "genomics" / "operations" / Segment) { (namespace, name, jobId) =>
        get {
          requestContext =>
            perRequest(requestContext, WorkspaceService.props(workspaceServiceConstructor, userInfo),
              WorkspaceService.GetGenomicsOperation(WorkspaceName(namespace, name), jobId))
        }
      }
  }
}
