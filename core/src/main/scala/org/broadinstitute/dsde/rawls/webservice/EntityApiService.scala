package org.broadinstitute.dsde.rawls.webservice

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.StatusCodes.BadRequest
import akka.http.scaladsl.server
import akka.http.scaladsl.server.Directives._
import io.opencensus.scala.akka.http.TracingDirective.traceRequest
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.entities.EntityService
import org.broadinstitute.dsde.rawls.model.AttributeUpdateOperations.{AttributeUpdateOperation, AttributeUpdateOperationFormat, EntityUpdateDefinition}
import org.broadinstitute.dsde.rawls.model.SortDirections.Ascending
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._
import org.broadinstitute.dsde.rawls.model.{AttributeName, _}
import org.broadinstitute.dsde.rawls.openam.UserInfoDirectives
import org.broadinstitute.dsde.rawls.webservice.CustomDirectives._
import spray.json.DefaultJsonProtocol._

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success, Try}

/**
 * Created by dvoet on 6/4/15.
 */

trait EntityApiService extends UserInfoDirectives {
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
            traceRequest { span =>
              parameterSeq { allParams =>
                val toIntTries = Map("page" -> page, "pageSize" -> pageSize).map { case (k, s) => k -> Try(s.map(_.toInt)) }
                val sortDirectionTry = sortDirection.map(dir => Try(SortDirections.fromString(dir))).getOrElse(Success(Ascending))

                val errors = toIntTries.collect {
                  case (k, Failure(t)) => s"$k must be a positive integer"
                  case (k, Success(Some(i))) if i <= 0 => s"$k must be a positive integer"
                } ++ (if (sortDirectionTry.isFailure) Seq(sortDirectionTry.failed.get.getMessage) else Seq.empty)

                if (errors.isEmpty) {
                  val entityQuery = EntityQuery(toIntTries("page").get.getOrElse(1), toIntTries("pageSize").get.getOrElse(10),
                    sortField.getOrElse("name"), sortDirectionTry.get,
                    filterTerms,
                    WorkspaceFieldSpecs.fromQueryParams(allParams, "fields"))
                  complete {
                    entityServiceConstructor(userInfo).queryEntities(WorkspaceName(workspaceNamespace, workspaceName), dataReference, entityType, entityQuery, billingProject, span)
                  }
                } else {
                  complete(StatusCodes.BadRequest, ErrorReport(StatusCodes.BadRequest, errors.mkString(", ")))
                }
              }
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
              complete {
                entityServiceConstructor(userInfo).deleteEntities(WorkspaceName(workspaceNamespace, workspaceName), entities, None, None).map {
                  case entities if entities.isEmpty => StatusCodes.NoContent -> None
                  case entities => StatusCodes.Conflict -> Option(entities)
                }
              }
            }
          }
        } ~
        path("workspaces" / Segment / Segment / "entities" / "batchUpsert") { (workspaceNamespace, workspaceName) =>
          post {
            withSizeLimit(batchUpsertMaxBytes) {
              entity(as[Array[EntityUpdateDefinition]]) { operations =>
                complete {
                  entityServiceConstructor(userInfo).batchUpsertEntities(WorkspaceName(workspaceNamespace, workspaceName), operations, dataReference, billingProject).map(_ => StatusCodes.NoContent)
                }
              }
            }
          }
        } ~
        path("workspaces" / Segment / Segment / "entities" / "batchUpdate") { (workspaceNamespace, workspaceName) =>
          post {
            entity(as[Array[EntityUpdateDefinition]]) { operations =>
              complete {
                entityServiceConstructor(userInfo).batchUpdateEntities(WorkspaceName(workspaceNamespace, workspaceName), operations, dataReference, billingProject).map(_ => StatusCodes.NoContent)
              }
            }
          }
        } ~
        path("workspaces" / Segment / Segment / "entities" / Segment / Segment / "rename") { (workspaceNamespace, workspaceName, entityType, entityName) =>
          post {
            entity(as[EntityName]) { newEntityName =>
              complete { entityServiceConstructor(userInfo).renameEntity(WorkspaceName(workspaceNamespace, workspaceName), entityType, entityName, newEntityName.name).map(_ => StatusCodes.NoContent) }
            }
          }
        } ~
        path("workspaces" / Segment / Segment / "entities" / "renameEntityType") { (workspaceNamespace, workspaceName) =>
          post {
            entity(as[EntityTypeRename]) { rename =>
              complete { entityServiceConstructor(userInfo).renameEntityType(WorkspaceName(workspaceNamespace, workspaceName), rename).map(_ => StatusCodes.NoContent) }
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
            traceRequest { span =>
              complete {
                entityServiceConstructor(userInfo).listEntities(WorkspaceName(workspaceNamespace, workspaceName), entityType, span)
              }
            }
          } ~
          delete {
            parameterSeq { allParams =>
              def parseAttributeNames() = {
                val paramName = "attributeNames"
                WorkspaceFieldSpecs.fromQueryParams(allParams, paramName).fields match {
                  case None => throw new RawlsExceptionWithErrorReport(ErrorReport(BadRequest, s"Parameter '$paramName' must be included.")(ErrorReportSource("rawls")))
                  case Some(atts) => atts.toSet.map{ (value: String) => AttributeName.fromDelimitedName(value.trim) }
                }
              }
              complete {
                entityServiceConstructor(userInfo).deleteEntityAttributes(WorkspaceName(workspaceNamespace, workspaceName), entityType, parseAttributeNames()).map(_ => StatusCodes.NoContent)
              }
            }
          }
        } ~
        path("workspaces" / "entities" / "copy") {
          post {
            parameters('linkExistingEntities.?) { (linkExistingEntities) =>
              extractRequest { request =>
                val linkExistingEntitiesBool = Try(linkExistingEntities.getOrElse("false").toBoolean).getOrElse(false)
                entity(as[EntityCopyDefinition]) { copyDefinition =>
                  traceRequest { span =>
                    complete {
                      entityServiceConstructor(userInfo).copyEntities(copyDefinition, request.uri, linkExistingEntitiesBool).map { response =>
                        if (response.hardConflicts.isEmpty && (response.softConflicts.isEmpty || linkExistingEntitiesBool)) StatusCodes.Created -> response
                        else StatusCodes.Conflict -> response
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
}
