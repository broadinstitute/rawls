package org.broadinstitute.dsde.rawls.webservice

import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.model.SortDirections.Ascending
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._
import org.broadinstitute.dsde.rawls.openam.UserInfoDirectives
import org.broadinstitute.dsde.rawls.model.AttributeUpdateOperations.{EntityUpdateDefinition, AttributeUpdateOperation}
import org.broadinstitute.dsde.rawls.workspace.WorkspaceService
import spray.http.StatusCodes
import spray.httpx.SprayJsonSupport._
import spray.routing.Directive.pimpApply
import spray.routing._
import spray.json.DefaultJsonProtocol._

import scala.concurrent.ExecutionContext
import scala.util.{Success, Failure, Try}

/**
 * Created by dvoet on 6/4/15.
 */

trait EntityApiService extends HttpService with PerRequestCreator with UserInfoDirectives {
  implicit val executionContext: ExecutionContext

  val workspaceServiceConstructor: UserInfo => WorkspaceService

  val entityRoutes = requireUserInfo() { userInfo =>
    path("workspaces" / Segment / Segment / "entityQuery" / Segment) { (workspaceNamespace, workspaceName, entityType) =>
      get {
        parameters('page.?, 'pageSize.?, 'sortField.?, 'sortDirection.?, 'filterTerms.?) { (page, pageSize, sortField, sortDirection, filterTerms) =>
          val toIntTries = Map("page" -> page, "pageSize" -> pageSize).map { case (k,s) => k -> Try(s.map(_.toInt)) }
          val sortDirectionTry = sortDirection.map(dir => Try(SortDirections.fromString(dir))).getOrElse(Success(Ascending))

          val errors = toIntTries.collect {
            case (k, Failure(t)) => s"$k must be a positive integer"
            case (k, Success(Some(i))) if i <= 0 => s"$k must be a positive integer"
          } ++ (if (sortDirectionTry.isFailure) Seq(sortDirectionTry.failed.get.getMessage) else Seq.empty)

          if (errors.isEmpty) {
            val entityQuery = EntityQuery(toIntTries("page").get.getOrElse(1), toIntTries("pageSize").get.getOrElse(10), sortField.getOrElse("name"), sortDirectionTry.get, filterTerms)
            requestContext =>
              perRequest(requestContext, WorkspaceService.props(workspaceServiceConstructor, userInfo),
                WorkspaceService.QueryEntities(WorkspaceName(workspaceNamespace, workspaceName), entityType, entityQuery))
          } else {
            complete(StatusCodes.BadRequest, ErrorReport(StatusCodes.BadRequest, errors.mkString(", ")))
          }
        }
      }
    } ~
    path("workspaces" / Segment / Segment / "entities") { (workspaceNamespace, workspaceName) =>
      get {
        requestContext => perRequest(requestContext, WorkspaceService.props(workspaceServiceConstructor, userInfo),
          WorkspaceService.GetEntityTypeMetadata(WorkspaceName(workspaceNamespace, workspaceName)))
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
    path("workspaces" / Segment / Segment / "entities" / "delete") { (workspaceNamespace, workspaceName) =>
      post {
        entity(as[Array[AttributeEntityReference]]) { entities =>
          requestContext =>
            perRequest(requestContext, WorkspaceService.props(workspaceServiceConstructor, userInfo),
              WorkspaceService.DeleteEntities(WorkspaceName(workspaceNamespace, workspaceName), entities))
        }
      }
    } ~
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
        parameters('linkExistingEntities.?) { (linkExistingEntities) =>
          val linkExistingEntitiesBool = Try(linkExistingEntities.getOrElse("false").toBoolean).getOrElse(false)
          entity(as[EntityCopyDefinition]) { copyDefinition =>
            requestContext =>
              perRequest(requestContext, WorkspaceService.props(workspaceServiceConstructor, userInfo),
                WorkspaceService.CopyEntities(copyDefinition, requestContext.request.uri, linkExistingEntitiesBool))
          }
        }
      }
    }
  }
}
