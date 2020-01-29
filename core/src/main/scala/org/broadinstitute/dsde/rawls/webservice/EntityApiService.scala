package org.broadinstitute.dsde.rawls.webservice

import org.broadinstitute.dsde.rawls.model.SortDirections.Ascending
import org.broadinstitute.dsde.rawls.model._
import spray.json.DefaultJsonProtocol._
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._
import org.broadinstitute.dsde.rawls.openam.UserInfoDirectives
import org.broadinstitute.dsde.rawls.model.AttributeUpdateOperations.{EntityUpdateDefinition, AttributeUpdateOperation, AttributeUpdateOperationFormat}
import org.broadinstitute.dsde.rawls.workspace.WorkspaceService
import akka.http.scaladsl.server
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.StatusCodes
import CustomDirectives._

import scala.concurrent.ExecutionContext
import scala.util.{Success, Failure, Try}

/**
 * Created by dvoet on 6/4/15.
 */

trait EntityApiService extends UserInfoDirectives {
  import PerRequest.requestCompleteMarshaller
  implicit val executionContext: ExecutionContext

  val workspaceServiceConstructor: UserInfo => WorkspaceService

  val entityRoutes: server.Route = requireUserInfo() { userInfo =>
      path("workspaces" / Segment / Segment / "entityQuery" / Segment) { (workspaceNamespace, workspaceName, entityType) =>
        get {
          parameters('page.?, 'pageSize.?, 'sortField.?, 'sortDirection.?, 'filterTerms.?) { (page, pageSize, sortField, sortDirection, filterTerms) =>
            val toIntTries = Map("page" -> page, "pageSize" -> pageSize).map { case (k, s) => k -> Try(s.map(_.toInt)) }
            val sortDirectionTry = sortDirection.map(dir => Try(SortDirections.fromString(dir))).getOrElse(Success(Ascending))

            val errors = toIntTries.collect {
              case (k, Failure(t)) => s"$k must be a positive integer"
              case (k, Success(Some(i))) if i <= 0 => s"$k must be a positive integer"
            } ++ (if (sortDirectionTry.isFailure) Seq(sortDirectionTry.failed.get.getMessage) else Seq.empty)

            if (errors.isEmpty) {
              val entityQuery = EntityQuery(toIntTries("page").get.getOrElse(1), toIntTries("pageSize").get.getOrElse(10), sortField.getOrElse("name"), sortDirectionTry.get, filterTerms)
              complete { workspaceServiceConstructor(userInfo).QueryEntities(WorkspaceName(workspaceNamespace, workspaceName), entityType, entityQuery) }
            } else {
              complete(StatusCodes.BadRequest, ErrorReport(StatusCodes.BadRequest, errors.mkString(", ")))
            }
          }
        }
      } ~
        path("workspaces" / Segment / Segment / "entities") { (workspaceNamespace, workspaceName) =>
          get {
            complete { workspaceServiceConstructor(userInfo).GetEntityTypeMetadata(WorkspaceName(workspaceNamespace, workspaceName)) }
          }
        } ~
        path("workspaces" / Segment / Segment / "entities") { (workspaceNamespace, workspaceName) =>
          post {
            entity(as[Entity]) { entity =>
              addLocationHeader(entity.path(WorkspaceName(workspaceNamespace, workspaceName))) {
                complete {
                  workspaceServiceConstructor(userInfo).CreateEntity(WorkspaceName(workspaceNamespace, workspaceName), entity).map(StatusCodes.Created -> _)
                }
              }
            }
          }
        } ~
        path("workspaces" / Segment / Segment / "entities" / Segment / Segment) { (workspaceNamespace, workspaceName, entityType, entityName) =>
          get {
            complete { workspaceServiceConstructor(userInfo).GetEntity(WorkspaceName(workspaceNamespace, workspaceName), entityType, entityName) }
          }
        } ~
        path("workspaces" / Segment / Segment / "entities" / Segment / Segment) { (workspaceNamespace, workspaceName, entityType, entityName) =>
          patch {
            entity(as[Array[AttributeUpdateOperation]]) { operations =>
              complete { workspaceServiceConstructor(userInfo).UpdateEntity(WorkspaceName(workspaceNamespace, workspaceName), entityType, entityName, operations) }
            }
          }
        } ~
        path("workspaces" / Segment / Segment / "entities" / "delete") { (workspaceNamespace, workspaceName) =>
          post {
            entity(as[Array[AttributeEntityReference]]) { entities =>
              complete { workspaceServiceConstructor(userInfo).DeleteEntities(WorkspaceName(workspaceNamespace, workspaceName), entities) }
            }
          }
        } ~
        path("workspaces" / Segment / Segment / "entities" / "batchUpsert") { (workspaceNamespace, workspaceName) =>
          post {
            entity(as[Array[EntityUpdateDefinition]]) { operations =>
              complete { workspaceServiceConstructor(userInfo).BatchUpsertEntities(WorkspaceName(workspaceNamespace, workspaceName), operations) }
            }
          }
        } ~
        path("workspaces" / Segment / Segment / "entities" / "batchUpdate") { (workspaceNamespace, workspaceName) =>
          post {
            entity(as[Array[EntityUpdateDefinition]]) { operations =>
              complete { workspaceServiceConstructor(userInfo).BatchUpdateEntities(WorkspaceName(workspaceNamespace, workspaceName), operations) }
            }
          }
        } ~
        path("workspaces" / Segment / Segment / "entities" / Segment / Segment / "rename") { (workspaceNamespace, workspaceName, entityType, entityName) =>
          post {
            entity(as[EntityName]) { newEntityName =>
              complete { workspaceServiceConstructor(userInfo).RenameEntity(WorkspaceName(workspaceNamespace, workspaceName), entityType, entityName, newEntityName.name) }
            }
          }
        } ~
        path("workspaces" / Segment / Segment / "entities" / Segment / Segment / "evaluate") { (workspaceNamespace, workspaceName, entityType, entityName) =>
          post {
            entity(as[String]) { expression =>
              complete { workspaceServiceConstructor(userInfo).EvaluateExpression(WorkspaceName(workspaceNamespace, workspaceName), entityType, entityName, expression) }
            }
          }
        } ~
        path("workspaces" / Segment / Segment / "entities" / Segment) { (workspaceNamespace, workspaceName, entityType) =>
          get {
            complete { workspaceServiceConstructor(userInfo).ListEntities(WorkspaceName(workspaceNamespace, workspaceName), entityType) }
          }
        } ~
        path("workspaces" / "entities" / "copy") {
          post {
            parameters('linkExistingEntities.?) { (linkExistingEntities) =>
              extractRequest { request =>
                val linkExistingEntitiesBool = Try(linkExistingEntities.getOrElse("false").toBoolean).getOrElse(false)
                entity(as[EntityCopyDefinition]) { copyDefinition =>
                  complete {
                    workspaceServiceConstructor(userInfo).CopyEntities(copyDefinition, request.uri, linkExistingEntitiesBool)
                  }
                }
              }
            }
          }
        }
    }
}
