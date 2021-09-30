package org.broadinstitute.dsde.rawls.webservice

import org.broadinstitute.dsde.rawls.model.SortDirections.Ascending
import org.broadinstitute.dsde.rawls.model._
import spray.json.DefaultJsonProtocol._
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._
import org.broadinstitute.dsde.rawls.openam.UserInfoDirectives
import org.broadinstitute.dsde.rawls.model.AttributeUpdateOperations.{AttributeUpdateOperation, AttributeUpdateOperationFormat, EntityUpdateDefinition}
import akka.http.scaladsl.server
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.StatusCodes
import CustomDirectives._
import akka.http.scaladsl.common.{EntityStreamingSupport, JsonEntityStreamingSupport}
import org.broadinstitute.dsde.rawls.entities.EntityService

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success, Try}

/**
 * Created by dvoet on 6/4/15.
 */

trait EntityApiService extends UserInfoDirectives {
  import PerRequest.requestCompleteMarshaller
  implicit val executionContext: ExecutionContext

  val entityServiceConstructor: UserInfo => EntityService
  val batchUpsertMaxBytes: Long

  val entityRoutes: server.Route = requireUserInfo() { userInfo =>
    parameters("dataReference".?, "billingProject".?) { (dataReferenceString, billingProjectString) =>
      val dataReference = dataReferenceString.map(DataReferenceName)
      val billingProject = billingProjectString.map(GoogleProjectId)
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
              complete { entityServiceConstructor(userInfo).queryEntities(WorkspaceName(workspaceNamespace, workspaceName), dataReference, entityType, entityQuery, billingProject) }
            } else {
              complete(StatusCodes.BadRequest, ErrorReport(StatusCodes.BadRequest, errors.mkString(", ")))
            }
          }
        }
      } ~
        path("workspaces" / Segment / Segment / "entities") { (workspaceNamespace, workspaceName) =>
          get {
            //if useCache param is unset or set to a value that won't coerce to a boolean, default to true
            parameters('useCache.?) { (useCache) =>
              val useCacheBool = Try(useCache.getOrElse("true").toBoolean).getOrElse(true)
              complete {
                entityServiceConstructor(userInfo).entityTypeMetadata(WorkspaceName(workspaceNamespace, workspaceName), dataReference, None, useCacheBool)
              }
            }
          }
        } ~
        path("workspaces" / Segment / Segment / "entities") { (workspaceNamespace, workspaceName) =>
          post {
            entity(as[Entity]) { entity =>
              addLocationHeader(entity.path(WorkspaceName(workspaceNamespace, workspaceName))) {
                complete {
                  entityServiceConstructor(userInfo).createEntity(WorkspaceName(workspaceNamespace, workspaceName), entity).map(StatusCodes.Created -> _)
                }
              }
            }
          }
        } ~
        path("workspaces" / Segment / Segment / "entities" / Segment / Segment) { (workspaceNamespace, workspaceName, entityType, entityName) =>
          get {
            complete { entityServiceConstructor(userInfo).getEntity(WorkspaceName(workspaceNamespace, workspaceName), entityType, entityName, dataReference, billingProject) }
          }
        } ~
        path("workspaces" / Segment / Segment / "entities" / Segment / Segment) { (workspaceNamespace, workspaceName, entityType, entityName) =>
          patch {
            entity(as[Array[AttributeUpdateOperation]]) { operations =>
              complete { entityServiceConstructor(userInfo).updateEntity(WorkspaceName(workspaceNamespace, workspaceName), entityType, entityName, operations) }
            }
          }
        } ~
        path("workspaces" / Segment / Segment / "entities" / "delete") { (workspaceNamespace, workspaceName) =>
          post {
            entity(as[Array[AttributeEntityReference]]) { entities =>
              complete { entityServiceConstructor(userInfo).deleteEntities(WorkspaceName(workspaceNamespace, workspaceName), entities, None, None) }
            }
          }
        } ~
        path("workspaces" / Segment / Segment / "entities" / "batchUpsert") { (workspaceNamespace, workspaceName) =>
          post {
            withSizeLimit(batchUpsertMaxBytes) {
              entity(as[Array[EntityUpdateDefinition]]) { operations =>
                complete {
                  entityServiceConstructor(userInfo).batchUpsertEntities(WorkspaceName(workspaceNamespace, workspaceName), operations, dataReference, billingProject)
                }
              }
            }
          }
        } ~
        path("workspaces" / Segment / Segment / "entities" / "batchUpdate") { (workspaceNamespace, workspaceName) =>
          post {
            entity(as[Array[EntityUpdateDefinition]]) { operations =>
              complete { entityServiceConstructor(userInfo).batchUpdateEntities(WorkspaceName(workspaceNamespace, workspaceName), operations, dataReference, billingProject) }
            }
          }
        } ~
        path("workspaces" / Segment / Segment / "entities" / Segment / Segment / "rename") { (workspaceNamespace, workspaceName, entityType, entityName) =>
          post {
            entity(as[EntityName]) { newEntityName =>
              complete { entityServiceConstructor(userInfo).renameEntity(WorkspaceName(workspaceNamespace, workspaceName), entityType, entityName, newEntityName.name) }
            }
          }
        } ~
        path("workspaces" / Segment / Segment / "entities" / Segment / Segment / "evaluate") { (workspaceNamespace, workspaceName, entityType, entityName) =>
          post {
            entity(as[String]) { expression =>
              complete { entityServiceConstructor(userInfo).evaluateExpression(WorkspaceName(workspaceNamespace, workspaceName), entityType, entityName, expression) }
            }
          }
        } ~
        path("workspaces" / Segment / Segment / "entities" / Segment) { (workspaceNamespace, workspaceName, entityType) =>
          get {
            // if any other APIs adopt streaming, move this implicit val higher up in the EntityApiService trait
            implicit val jsonStreamingSupport: JsonEntityStreamingSupport = EntityStreamingSupport.json()
            complete {
              entityServiceConstructor(userInfo).listEntities(WorkspaceName(workspaceNamespace, workspaceName), entityType)
            }
          }
        } ~
        path("workspaces" / "entities" / "copy") {
          post {
            parameters('linkExistingEntities.?) { (linkExistingEntities) =>
              extractRequest { request =>
                val linkExistingEntitiesBool = Try(linkExistingEntities.getOrElse("false").toBoolean).getOrElse(false)
                entity(as[EntityCopyDefinition]) { copyDefinition =>
                  complete {
                    entityServiceConstructor(userInfo).copyEntities(copyDefinition, request.uri, linkExistingEntitiesBool)
                  }
                }
              }
            }
          }
        }
      }
    }
}
