package org.broadinstitute.dsde.rawls.webservice

import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.openam.OpenAmDirectives
import org.broadinstitute.dsde.rawls.model.AttributeUpdateOperations.{EntityUpdateDefinition, AttributeUpdateOperation}
import org.broadinstitute.dsde.rawls.workspace.WorkspaceService
import spray.routing.Directive.pimpApply
import spray.routing._

/**
 * Created by dvoet on 6/4/15.
 */

trait EntityApiService extends HttpService with PerRequestCreator with OpenAmDirectives {
  lazy private implicit val executionContext = actorRefFactory.dispatcher

  import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._
  import spray.httpx.SprayJsonSupport._

  val workspaceServiceConstructor: UserInfo => WorkspaceService

  val entityRoutes = userInfoFromCookie() { userInfo =>
    path("workspaces" / Segment / Segment / "entities") { (workspaceNamespace, workspaceName) =>
      post {
        entity(as[Entity]) { entity =>
          requestContext => perRequest(requestContext, WorkspaceService.props(workspaceServiceConstructor, userInfo),
            WorkspaceService.CreateEntity(workspaceNamespace, workspaceName, entity))
        }
      }
    } ~
    path("workspaces" / Segment / Segment / "entities" / Segment / Segment) { (workspaceNamespace, workspaceName, entityType, entityName) =>
      get {
        requestContext => perRequest(requestContext, WorkspaceService.props(workspaceServiceConstructor, userInfo),
          WorkspaceService.GetEntity(workspaceNamespace, workspaceName, entityType, entityName))
      }
    } ~
    path("workspaces" / Segment / Segment / "entities" / Segment / Segment) { (workspaceNamespace, workspaceName, entityType, entityName) =>
      patch {
        entity(as[Array[AttributeUpdateOperation]]) { operations =>
          requestContext => perRequest(requestContext, WorkspaceService.props(workspaceServiceConstructor, userInfo),
            WorkspaceService.UpdateEntity(workspaceNamespace, workspaceName, entityType, entityName, operations))
        }
      }
    } ~
    path("workspaces" / Segment / Segment / "entities" / "batchUpsert") { (workspaceNamespace, workspaceName) =>
      post {
        entity(as[Array[EntityUpdateDefinition]]) { operations =>
          requestContext => perRequest(requestContext, WorkspaceService.props(workspaceServiceConstructor, userInfo),
            WorkspaceService.BatchUpsertEntities(workspaceNamespace, workspaceName, operations))
        }
      }
    } ~
    path("workspaces" / Segment / Segment / "entities" / Segment / Segment) { (workspaceNamespace, workspaceName, entityType, entityName) =>
      delete {
        requestContext => perRequest(requestContext, WorkspaceService.props(workspaceServiceConstructor, userInfo),
          WorkspaceService.DeleteEntity(workspaceNamespace, workspaceName, entityType, entityName))
      }
    } ~
    path("workspaces" / Segment / Segment / "entities" / Segment / Segment / "rename") { (workspaceNamespace, workspaceName, entityType, entityName) =>
      post {
        entity(as[EntityName]) { newEntityName =>
          requestContext => perRequest(requestContext, WorkspaceService.props(workspaceServiceConstructor, userInfo),
            WorkspaceService.RenameEntity(workspaceNamespace, workspaceName, entityType, entityName, newEntityName.name))
        }
      }
    } ~
    path("workspaces" / Segment / Segment / "entities" / Segment / Segment / "evaluate") { (workspaceNamespace, workspaceName, entityType, entityName) =>
      post {
        entity(as[String]) { expression =>
          requestContext => perRequest(requestContext, WorkspaceService.props(workspaceServiceConstructor, userInfo),
            WorkspaceService.EvaluateExpression(workspaceNamespace, workspaceName, entityType, entityName, expression))
        }
      }
    } ~
    path("workspaces" / Segment / Segment / "entities") { (workspaceNamespace, workspaceName) =>
      get {
        requestContext => perRequest(requestContext, WorkspaceService.props(workspaceServiceConstructor, userInfo),
          WorkspaceService.ListEntityTypes(workspaceNamespace, workspaceName))
      }
    } ~
    path("workspaces" / Segment / Segment / "entities" / Segment) { (workspaceNamespace, workspaceName, entityType) =>
      get {
        requestContext => perRequest(requestContext, WorkspaceService.props(workspaceServiceConstructor, userInfo),
          WorkspaceService.ListEntities(workspaceNamespace, workspaceName, entityType))
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
