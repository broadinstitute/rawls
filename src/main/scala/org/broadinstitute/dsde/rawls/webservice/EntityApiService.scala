package org.broadinstitute.dsde.rawls.webservice

import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.openam.UserInfoDirectives
import org.broadinstitute.dsde.rawls.model.AttributeUpdateOperations.{EntityUpdateDefinition, AttributeUpdateOperation}
import org.broadinstitute.dsde.rawls.workspace.WorkspaceService
import spray.routing.Directive.pimpApply
import spray.routing._

import scala.concurrent.ExecutionContext
import scala.util.Try

/**
 * Created by dvoet on 6/4/15.
 */

trait EntityApiService extends HttpService with PerRequestCreator with UserInfoDirectives {
  implicit val executionContext: ExecutionContext

  import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._
  import spray.httpx.SprayJsonSupport._

  val workspaceServiceConstructor: UserInfo => WorkspaceService

  val entityRoutes = requireUserInfo() { userInfo =>
    path("workspaces" / Segment / Segment / "entityQuery" / Segment) { (workspaceNamespace, workspaceName, entityType) =>
      get {
        parameters('page.?, 'pageSize.?, 'sortField.?, 'sortDirection.?, 'query.?) { (page, pageSize, sortField, sortDirection, query) =>
          validate(page.forall(p => Try(p.toInt).isSuccess && p.toInt > 0), "page must be a positive integer") {
            validate(pageSize.forall(p => Try(p.toInt).isSuccess && p.toInt > 0), "pageSize must be a positive integer") {

              val entityQuery = EntityQuery(page.map(_.toInt), pageSize.map(_.toInt), sortField, sortDirection, query)
              requestContext => perRequest(requestContext, WorkspaceService.props(workspaceServiceConstructor, userInfo),
                WorkspaceService.QueryEntities(WorkspaceName(workspaceNamespace, workspaceName), entityType, entityQuery))
            }
          }
        }
      }
    } ~
    path("workspaces" / Segment / Segment / "entities") { (workspaceNamespace, workspaceName) =>
      get {
        requestContext => perRequest(requestContext, WorkspaceService.props(workspaceServiceConstructor, userInfo),
          WorkspaceService.ListEntityTypes(WorkspaceName(workspaceNamespace, workspaceName)))
      }
    } ~
    path("workspaces" / Segment / Segment / "entities") { (workspaceNamespace, workspaceName) =>
      post {
        entity(as[Entity]) { entity =>
          requestContext => perRequest(requestContext, WorkspaceService.props(workspaceServiceConstructor, userInfo),
            WorkspaceService.CreateEntity(WorkspaceName(workspaceNamespace, workspaceName), entity))
        }
      }
    } ~
    path("workspaces" / Segment / Segment / "entities" / Segment / Segment) { (workspaceNamespace, workspaceName, entityType, entityName) =>
      get {
        requestContext => perRequest(requestContext, WorkspaceService.props(workspaceServiceConstructor, userInfo),
          WorkspaceService.GetEntity(WorkspaceName(workspaceNamespace, workspaceName), entityType, entityName))
      }
    } ~
    path("workspaces" / Segment / Segment / "entities" / Segment / Segment) { (workspaceNamespace, workspaceName, entityType, entityName) =>
      patch {
        entity(as[Array[AttributeUpdateOperation]]) { operations =>
          requestContext => perRequest(requestContext, WorkspaceService.props(workspaceServiceConstructor, userInfo),
            WorkspaceService.UpdateEntity(WorkspaceName(workspaceNamespace, workspaceName), entityType, entityName, operations))
        }
      }
    } ~
/*  This endpoint has been disabled as part of GAWB-423 and will return when GAWB-422 is complete
    path("workspaces" / Segment / Segment / "entities" / Segment / Segment) { (workspaceNamespace, workspaceName, entityType, entityName) =>
      delete {
        requestContext => perRequest(requestContext, WorkspaceService.props(workspaceServiceConstructor, userInfo),
          WorkspaceService.DeleteEntity(WorkspaceName(workspaceNamespace, workspaceName), entityType, entityName))
      }
    } ~ */
    path("workspaces" / Segment / Segment / "entities" / "batchUpsert") { (workspaceNamespace, workspaceName) =>
      post {
        entity(as[Array[EntityUpdateDefinition]]) { operations =>
          requestContext => perRequest(requestContext, WorkspaceService.props(workspaceServiceConstructor, userInfo),
            WorkspaceService.BatchUpsertEntities(WorkspaceName(workspaceNamespace, workspaceName), operations))
        }
      }
    } ~
    path("workspaces" / Segment / Segment / "entities" / "batchUpdate") { (workspaceNamespace, workspaceName) =>
      post {
        entity(as[Array[EntityUpdateDefinition]]) { operations =>
          requestContext => perRequest(requestContext, WorkspaceService.props(workspaceServiceConstructor, userInfo),
            WorkspaceService.BatchUpdateEntities(WorkspaceName(workspaceNamespace, workspaceName), operations))
        }
      }
    } ~
    path("workspaces" / Segment / Segment / "entities" / Segment / Segment / "rename") { (workspaceNamespace, workspaceName, entityType, entityName) =>
      post {
        entity(as[EntityName]) { newEntityName =>
          requestContext => perRequest(requestContext, WorkspaceService.props(workspaceServiceConstructor, userInfo),
            WorkspaceService.RenameEntity(WorkspaceName(workspaceNamespace, workspaceName), entityType, entityName, newEntityName.name))
        }
      }
    } ~
    path("workspaces" / Segment / Segment / "entities" / Segment / Segment / "evaluate") { (workspaceNamespace, workspaceName, entityType, entityName) =>
      post {
        entity(as[String]) { expression =>
          requestContext => perRequest(requestContext, WorkspaceService.props(workspaceServiceConstructor, userInfo),
            WorkspaceService.EvaluateExpression(WorkspaceName(workspaceNamespace, workspaceName), entityType, entityName, expression))
        }
      }
    } ~
    path("workspaces" / Segment / Segment / "entities" / Segment) { (workspaceNamespace, workspaceName, entityType) =>
      get {
        requestContext => perRequest(requestContext, WorkspaceService.props(workspaceServiceConstructor, userInfo),
          WorkspaceService.ListEntities(WorkspaceName(workspaceNamespace, workspaceName), entityType))
      }
    } ~
    path("workspaces" / "entities" / "copy" ) {
      post {
        entity(as[EntityCopyDefinition]) { copyDefinition =>
          requestContext => perRequest(requestContext, WorkspaceService.props(workspaceServiceConstructor, userInfo),
            WorkspaceService.CopyEntities(copyDefinition, requestContext.request.uri))
        }
      }
    }
  }
}
