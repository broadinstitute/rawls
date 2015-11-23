package org.broadinstitute.dsde.rawls.webservice

import kamon.spray.KamonTraceDirectives._
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.openam.UserInfoDirectives
import org.broadinstitute.dsde.rawls.model.AttributeUpdateOperations.{EntityUpdateDefinition, AttributeUpdateOperation}
import org.broadinstitute.dsde.rawls.workspace.WorkspaceService
import spray.routing.Directive.pimpApply
import spray.routing._

import scala.concurrent.ExecutionContext

/**
 * Created by dvoet on 6/4/15.
 */

trait EntityApiService extends HttpService with PerRequestCreator with UserInfoDirectives {
  implicit val executionContext: ExecutionContext

  import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._
  import spray.httpx.SprayJsonSupport._

  val workspaceServiceConstructor: UserInfo => WorkspaceService

  val entityRoutes = requireUserInfo() { userInfo =>
    path("workspaces" / Segment / Segment / "entities") { (workspaceNamespace, workspaceName) =>
      post {
        traceName("CreateEntity") {
          entity(as[Entity]) { entity =>
            requestContext => perRequest(requestContext, WorkspaceService.props(workspaceServiceConstructor, userInfo),
              WorkspaceService.CreateEntity(WorkspaceName(workspaceNamespace, workspaceName), entity))
          }
        }
      }
    } ~
    path("workspaces" / Segment / Segment / "entities" / Segment / Segment) { (workspaceNamespace, workspaceName, entityType, entityName) =>
      get {
        traceName("GetEntity") {
          requestContext => perRequest(requestContext, WorkspaceService.props(workspaceServiceConstructor, userInfo),
            WorkspaceService.GetEntity(WorkspaceName(workspaceNamespace, workspaceName), entityType, entityName))
        }
      }
    } ~
    path("workspaces" / Segment / Segment / "entities" / Segment / Segment) { (workspaceNamespace, workspaceName, entityType, entityName) =>
      patch {
        traceName("UpdateEntity") {
          entity(as[Array[AttributeUpdateOperation]]) { operations =>
            requestContext => perRequest(requestContext, WorkspaceService.props(workspaceServiceConstructor, userInfo),
              WorkspaceService.UpdateEntity(WorkspaceName(workspaceNamespace, workspaceName), entityType, entityName, operations))
          }
        }
      }
    } ~
    path("workspaces" / Segment / Segment / "entities" / "batchUpsert") { (workspaceNamespace, workspaceName) =>
      post {
        traceName("BatchUpsertEntities") {
          entity(as[Array[EntityUpdateDefinition]]) { operations =>
            requestContext => perRequest(requestContext, WorkspaceService.props(workspaceServiceConstructor, userInfo),
              WorkspaceService.BatchUpsertEntities(WorkspaceName(workspaceNamespace, workspaceName), operations))
          }
        }
      }
    } ~
      path("workspaces" / Segment / Segment / "entities" / "batchUpdate") { (workspaceNamespace, workspaceName) =>
        post {
          traceName("BatchUpdateEntities") {
            entity(as[Array[EntityUpdateDefinition]]) { operations =>
              requestContext => perRequest(requestContext, WorkspaceService.props(workspaceServiceConstructor, userInfo),
                WorkspaceService.BatchUpdateEntities(WorkspaceName(workspaceNamespace, workspaceName), operations))
            }
          }
        }
      } ~
    path("workspaces" / Segment / Segment / "entities" / Segment / Segment) { (workspaceNamespace, workspaceName, entityType, entityName) =>
      delete {
        traceName("DeleteEntity") {
          requestContext => perRequest(requestContext, WorkspaceService.props(workspaceServiceConstructor, userInfo),
            WorkspaceService.DeleteEntity(WorkspaceName(workspaceNamespace, workspaceName), entityType, entityName))
        }
      }
    } ~
    path("workspaces" / Segment / Segment / "entities" / Segment / Segment / "rename") { (workspaceNamespace, workspaceName, entityType, entityName) =>
      post {
        traceName("RenameEntity") {
          entity(as[EntityName]) { newEntityName =>
            requestContext => perRequest(requestContext, WorkspaceService.props(workspaceServiceConstructor, userInfo),
              WorkspaceService.RenameEntity(WorkspaceName(workspaceNamespace, workspaceName), entityType, entityName, newEntityName.name))
          }
        }
      }
    } ~
    path("workspaces" / Segment / Segment / "entities" / Segment / Segment / "evaluate") { (workspaceNamespace, workspaceName, entityType, entityName) =>
      post {
        traceName("EvaluateExpression") {
          entity(as[String]) { expression =>
            requestContext => perRequest(requestContext, WorkspaceService.props(workspaceServiceConstructor, userInfo),
              WorkspaceService.EvaluateExpression(WorkspaceName(workspaceNamespace, workspaceName), entityType, entityName, expression))
          }
        }
      }
    } ~
    path("workspaces" / Segment / Segment / "entities") { (workspaceNamespace, workspaceName) =>
      get {
        traceName("ListEntityTypes") {
          requestContext => perRequest(requestContext, WorkspaceService.props(workspaceServiceConstructor, userInfo),
            WorkspaceService.ListEntityTypes(WorkspaceName(workspaceNamespace, workspaceName)))
        }
      }
    } ~
    path("workspaces" / Segment / Segment / "entities" / Segment) { (workspaceNamespace, workspaceName, entityType) =>
      get {
        traceName("ListEntities") {
          requestContext => perRequest(requestContext, WorkspaceService.props(workspaceServiceConstructor, userInfo),
            WorkspaceService.ListEntities(WorkspaceName(workspaceNamespace, workspaceName), entityType))
        }
      }
    } ~
    path("workspaces" / "entities" / "copy" ) {
      post {
        traceName("CopyEntities") {
          entity(as[EntityCopyDefinition]) { copyDefinition =>
            requestContext => perRequest(requestContext, WorkspaceService.props(workspaceServiceConstructor, userInfo),
              WorkspaceService.CopyEntities(copyDefinition, requestContext.request.uri))
          }
        }
      }
    }
  }
}
