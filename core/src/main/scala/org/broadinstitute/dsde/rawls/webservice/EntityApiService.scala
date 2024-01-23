package org.broadinstitute.dsde.rawls.webservice

import akka.http.scaladsl.common.{EntityStreamingSupport, JsonEntityStreamingSupport}
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.StatusCodes.BadRequest
import akka.http.scaladsl.server
import akka.http.scaladsl.server.Directives._
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.entities.EntityService
import org.broadinstitute.dsde.rawls.metrics.TracingDirectives
import org.broadinstitute.dsde.rawls.model.AttributeUpdateOperations.{AttributeUpdateOperation, AttributeUpdateOperationFormat, EntityUpdateDefinition}
import org.broadinstitute.dsde.rawls.model.FilterOperators.And
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

trait EntityApiService extends UserInfoDirectives with TracingDirectives {
  implicit val executionContext: ExecutionContext

  val entityServiceConstructor: RawlsRequestContext => EntityService
  val batchUpsertMaxBytes: Long

  val entityRoutes: server.Route = traceRequest { otelContext =>
    requireUserInfo(Option(otelContext)) { userInfo =>
      val ctx = RawlsRequestContext(userInfo, Option(otelContext))
      parameters("dataReference".?, "billingProject".?) { (dataReferenceString, billingProjectString) =>
        val dataReference = dataReferenceString.map(DataReferenceName)
        val billingProject = billingProjectString.map(GoogleProjectId)
        path("workspaces" / Segment / Segment / "entityQuery" / Segment) {
          (workspaceNamespace, workspaceName, entityType) =>
            get {
              parameters('page.?,
                         'pageSize.?,
                         'sortField.?,
                         'sortDirection.?,
                         'filterTerms.?,
                         'filterOperator.?,
                         'columnFilter.?
              ) { (page, pageSize, sortField, sortDirection, filterTerms, filterOperator, columnFilterStringOpt) =>
                parameterSeq { allParams =>
                  val toIntTries = Map("page" -> page, "pageSize" -> pageSize).map { case (k, s) =>
                    k -> Try(s.map(_.toInt))
                  }
                  val sortDirectionTry =
                    sortDirection.map(dir => Try(SortDirections.fromString(dir))).getOrElse(Success(Ascending))
                  val operatorTry =
                    filterOperator.map(op => Try(FilterOperators.fromString(op))).getOrElse(Success(And))

                  val filterValidation =
                    if (Seq(filterTerms, columnFilterStringOpt).count(_.isDefined) > 1) {
                      Seq(
                        "filterTerms and columnFilter are mutually exclusive; you may specify only one of these parameters."
                      )
                    } else Seq.empty

                  val columnFilter: Option[Either[Seq[String], EntityColumnFilter]] =
                    EntityApiService.createColumnFilter(columnFilterStringOpt)
                  val errors = Seq(
                    toIntTries.collect {
                      case (k, Failure(t))                 => s"$k must be a positive integer"
                      case (k, Success(Some(i))) if i <= 0 => s"$k must be a positive integer"
                    },
                    if (sortDirectionTry.isFailure) Seq(sortDirectionTry.failed.get.getMessage) else Seq.empty,
                    filterValidation,
                    columnFilter.flatMap(_.swap.toOption).getOrElse(Seq.empty)
                  ).flatten

                  if (errors.isEmpty) {
                    val entityQuery = EntityQuery(
                      toIntTries("page").get.getOrElse(1),
                      toIntTries("pageSize").get.getOrElse(10),
                      sortField.getOrElse("name"),
                      sortDirectionTry.get,
                      filterTerms,
                      operatorTry.get,
                      WorkspaceFieldSpecs.fromQueryParams(allParams, "fields"),
                      columnFilter.flatMap(_.toOption)
                    )
                    complete {
                      entityServiceConstructor(ctx).queryEntities(WorkspaceName(workspaceNamespace, workspaceName),
                                                                  dataReference,
                                                                  entityType,
                                                                  entityQuery,
                                                                  billingProject
                      )
                    }
                  } else {
                    complete(StatusCodes.BadRequest, ErrorReport(StatusCodes.BadRequest, errors.mkString(", ")))
                  }
                }
              }
            }
        } ~
          path("workspaces" / Segment / Segment / "entities") { (workspaceNamespace, workspaceName) =>
            get {
              // if useCache param is unset or set to a value that won't coerce to a boolean, default to true
              parameters('useCache.?) { useCache =>
                val useCacheBool = Try(useCache.getOrElse("true").toBoolean).getOrElse(true)
                complete {
                  entityServiceConstructor(ctx).entityTypeMetadata(WorkspaceName(workspaceNamespace, workspaceName),
                                                                   dataReference,
                                                                   None,
                                                                   useCacheBool
                  )
                }
              }
            }
          } ~
          path("workspaces" / Segment / Segment / "entities") { (workspaceNamespace, workspaceName) =>
            post {
              entity(as[Entity]) { entity =>
                addLocationHeader(entity.path(WorkspaceName(workspaceNamespace, workspaceName))) {
                  complete {
                    entityServiceConstructor(ctx)
                      .createEntity(WorkspaceName(workspaceNamespace, workspaceName), entity)
                      .map(StatusCodes.Created -> _)
                  }
                }
              }
            }
          } ~
          path("workspaces" / Segment / Segment / "entities" / Segment / Segment) {
            (workspaceNamespace, workspaceName, entityType, entityName) =>
              get {
                complete {
                  entityServiceConstructor(ctx).getEntity(WorkspaceName(workspaceNamespace, workspaceName),
                                                          entityType,
                                                          entityName,
                                                          dataReference,
                                                          billingProject
                  )
                }
              }
          } ~
          path("workspaces" / Segment / Segment / "entities" / Segment / Segment) {
            (workspaceNamespace, workspaceName, entityType, entityName) =>
              patch {
                entity(as[Array[AttributeUpdateOperation]]) { operations =>
                  complete {
                    entityServiceConstructor(ctx).updateEntity(WorkspaceName(workspaceNamespace, workspaceName),
                                                               entityType,
                                                               entityName,
                                                               operations
                    )
                  }
                }
              }
          } ~
          path("workspaces" / Segment / Segment / "entities" / "delete") { (workspaceNamespace, workspaceName) =>
            post {
              entity(as[Array[AttributeEntityReference]]) { entities =>
                complete {
                  entityServiceConstructor(ctx)
                    .deleteEntities(WorkspaceName(workspaceNamespace, workspaceName), entities, None, None)
                    .map {
                      case entities if entities.isEmpty => StatusCodes.NoContent -> None
                      case entities                     => StatusCodes.Conflict -> Option(entities)
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
                    entityServiceConstructor(ctx)
                      .batchUpsertEntities(WorkspaceName(workspaceNamespace, workspaceName),
                                           operations,
                                           dataReference,
                                           billingProject
                      )
                      .map(_ => StatusCodes.NoContent)
                  }
                }
              }
            }
          } ~
          path("workspaces" / Segment / Segment / "entities" / "batchUpdate") { (workspaceNamespace, workspaceName) =>
            post {
              entity(as[Array[EntityUpdateDefinition]]) { operations =>
                complete {
                  entityServiceConstructor(ctx)
                    .batchUpdateEntities(WorkspaceName(workspaceNamespace, workspaceName),
                                         operations,
                                         dataReference,
                                         billingProject
                    )
                    .map(_ => StatusCodes.NoContent)
                }
              }
            }
          } ~
          path("workspaces" / Segment / Segment / "entities" / Segment / Segment / "rename") {
            (workspaceNamespace, workspaceName, entityType, entityName) =>
              post {
                entity(as[EntityName]) { newEntityName =>
                  complete {
                    entityServiceConstructor(ctx)
                      .renameEntity(WorkspaceName(workspaceNamespace, workspaceName),
                                    entityType,
                                    entityName,
                                    newEntityName.name
                      )
                      .map(_ => StatusCodes.NoContent)
                  }
                }
              }
          } ~
          path("workspaces" / Segment / Segment / "entityTypes" / Segment) {
            (workspaceNamespace, workspaceName, entityType) =>
              patch {
                entity(as[EntityTypeRename]) { rename =>
                  complete {
                    entityServiceConstructor(ctx)
                      .renameEntityType(WorkspaceName(workspaceNamespace, workspaceName), entityType, rename)
                      .map(_ => StatusCodes.NoContent)
                  }
                }
              } ~
                delete {
                  complete {
                    entityServiceConstructor(ctx)
                      .deleteEntitiesOfType(WorkspaceName(workspaceNamespace, workspaceName), entityType, None, None)
                      .map(_ => StatusCodes.NoContent)
                  }
                }
          } ~
          path("workspaces" / Segment / Segment / "entities" / Segment / Segment / "evaluate") {
            (workspaceNamespace, workspaceName, entityType, entityName) =>
              post {
                entity(as[String]) { expression =>
                  complete {
                    entityServiceConstructor(ctx).evaluateExpression(WorkspaceName(workspaceNamespace, workspaceName),
                                                                     entityType,
                                                                     entityName,
                                                                     expression
                    )
                  }
                }
              }
          } ~
          path("workspaces" / Segment / Segment / "entities" / Segment) {
            (workspaceNamespace, workspaceName, entityType) =>
              get {
                // if any other APIs adopt streaming, move this implicit val higher up in the EntityApiService trait
                implicit val jsonStreamingSupport: JsonEntityStreamingSupport = EntityStreamingSupport.json()
                import spray.json._

                // listEntities returns a source of Entity. We will stream-output each successful entity to the user,
                // and if an exception occurs midstream we want to output the exception (wrapped in an ErrorReport)
                // as the final element. Akka and Spray have a hard time with the mixed element types in the stream,
                // so we manually convert them all to JsValue here and then output the stream of JsValue.
                val jsValueSource = entityServiceConstructor(ctx)
                  .listEntities(WorkspaceName(workspaceNamespace, workspaceName), entityType)
                  .map(source =>
                    source
                      .map(entity => entity.toJson)
                      .recover {
                        case rex: RawlsExceptionWithErrorReport =>
                          rex.errorReport.copy(stackTrace = Seq.empty).toJson
                        case ex: Exception =>
                          ErrorReport(ex).copy(message = ex.getMessage, stackTrace = Seq.empty).toJson
                      }
                  )

                complete(jsValueSource)
              } ~
                delete {
                  parameterSeq { allParams =>
                    def parseAttributeNames() = {
                      val paramName = "attributeNames"
                      WorkspaceFieldSpecs.fromQueryParams(allParams, paramName).fields match {
                        case None =>
                          throw new RawlsExceptionWithErrorReport(
                            ErrorReport(BadRequest, s"Parameter '$paramName' must be included.")(
                              ErrorReportSource("rawls")
                            )
                          )
                        case Some(atts) =>
                          atts.toSet.map((value: String) => AttributeName.fromDelimitedName(value.trim))
                      }
                    }

                    complete {
                      entityServiceConstructor(ctx)
                        .deleteEntityAttributes(WorkspaceName(workspaceNamespace, workspaceName),
                                                entityType,
                                                parseAttributeNames()
                        )
                        .map(_ => StatusCodes.NoContent)
                    }
                  }
                }
          } ~
          path("workspaces" / "entities" / "copy") {
            post {
              parameters('linkExistingEntities.?) { linkExistingEntities =>
                extractRequest { request =>
                  val linkExistingEntitiesBool = Try(linkExistingEntities.getOrElse("false").toBoolean).getOrElse(false)
                  entity(as[EntityCopyDefinition]) { copyDefinition =>
                    complete {
                      entityServiceConstructor(ctx)
                        .copyEntities(copyDefinition, linkExistingEntitiesBool)
                        .map { response =>
                          if (
                            response.hardConflicts.isEmpty && (response.softConflicts.isEmpty || linkExistingEntitiesBool)
                          ) StatusCodes.Created -> response
                          else StatusCodes.Conflict -> response
                        }
                    }
                  }
                }
              }
            }
          } ~
          path("workspaces" / Segment / Segment / "entityTypes" / Segment / "attributes" / Segment) {
            (workspaceNamespace, workspaceName, entityType, attributeName) =>
              patch {
                entity(as[AttributeRename]) { attributeRenameRequest =>
                  complete {
                    entityServiceConstructor(ctx)
                      .renameAttribute(WorkspaceName(workspaceNamespace, workspaceName),
                                       entityType,
                                       AttributeName.fromDelimitedName(attributeName),
                                       attributeRenameRequest
                      )
                      .map(_ => StatusCodes.NoContent)
                  }
                }
              }
          }
      }
    }
  }

}

object EntityApiService {
  // create the column filter. separated out for testing
  def createColumnFilter(
    columnFilterStringOpt: Option[String]
  ): Option[Either[Seq[String], EntityColumnFilter]] =
    columnFilterStringOpt.map { str =>
      val parts = str.split('=')
      if (parts.length == 2) {
        val attributeNameTry = Try(AttributeName.fromDelimitedName(parts(0)))
        attributeNameTry match {
          case Success(attributeName) =>
            val term = parts(1)
            Right(EntityColumnFilter(attributeName, term))
          case Failure(ex) => Left(Seq(ex.getMessage))
        }

      } else {
        Left(Seq("invalid input to the columnFilter parameter"))
      }
    }
}
