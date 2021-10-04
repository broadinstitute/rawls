package org.broadinstitute.dsde.rawls.entities

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.{StatusCodes, Uri}
import com.google.api.client.googleapis.json.GoogleJsonResponseException
import com.google.cloud.bigquery.BigQueryException
import com.typesafe.scalalogging.LazyLogging
import io.opencensus.scala.Tracing.traceWithParent
import io.opencensus.trace.Span
import org.broadinstitute.dsde.rawls.dataaccess.{AttributeTempTableType, SamDAO, SlickDataSource}
import org.broadinstitute.dsde.rawls.deltalayer.DeltaLayerException
import org.broadinstitute.dsde.rawls.entities.exceptions.{DataEntityException, DeleteEntitiesConflictException, EntityNotFoundException}
import org.broadinstitute.dsde.rawls.expressions.ExpressionEvaluator
import org.broadinstitute.dsde.rawls.metrics.RawlsInstrumented
import org.broadinstitute.dsde.rawls.model.AttributeUpdateOperations.{AttributeUpdateOperation, EntityUpdateDefinition}
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._
import org.broadinstitute.dsde.rawls.model.{AttributeEntityReference, Entity, EntityCopyDefinition, EntityQuery, ErrorReport, SamResourceTypeNames, SamWorkspaceActions, UserInfo, WorkspaceName, _}
import org.broadinstitute.dsde.rawls.util.OpenCensusDBIOUtils.traceDBIOWithParent
import org.broadinstitute.dsde.rawls.util.{AttributeSupport, EntitySupport, JsonFilterUtils, WorkspaceSupport}
import org.broadinstitute.dsde.rawls.webservice.PerRequest
import org.broadinstitute.dsde.rawls.webservice.PerRequest.{PerRequestMessage, RequestComplete}
import org.broadinstitute.dsde.rawls.workspace.AttributeUpdateOperationException
import org.broadinstitute.dsde.rawls.{RawlsException, RawlsExceptionWithErrorReport}
import spray.json.DefaultJsonProtocol._

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

object EntityService {
  def constructor(dataSource: SlickDataSource, samDAO: SamDAO, workbenchMetricBaseName: String, entityManager: EntityManager)
                 (userInfo: UserInfo)
                 (implicit executionContext: ExecutionContext): EntityService = {

    new EntityService(userInfo, dataSource, samDAO, entityManager, workbenchMetricBaseName)
  }
}

class EntityService(protected val userInfo: UserInfo, val dataSource: SlickDataSource, val samDAO: SamDAO, entityManager: EntityManager, override val workbenchMetricBaseName: String)(implicit protected val executionContext: ExecutionContext)
  extends WorkspaceSupport with EntitySupport with AttributeSupport with LazyLogging with RawlsInstrumented with JsonFilterUtils {

  import dataSource.dataAccess.driver.api._

  def createEntity(workspaceName: WorkspaceName, entity: Entity): Future[Entity] = {
    withAttributeNamespaceCheck(entity) {
      for {
        workspaceContext <- getWorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.write, Some(WorkspaceAttributeSpecs(all = false)))
        entityManager <- entityManager.resolveProviderFuture(EntityRequestArguments(workspaceContext, userInfo))
        result <- entityManager.createEntity(entity)
      } yield result
    }
  }

  def getEntity(workspaceName: WorkspaceName, entityType: String, entityName: String, dataReference: Option[DataReferenceName], billingProject: Option[GoogleProjectId]): Future[PerRequestMessage] =
    getWorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.read, Some(WorkspaceAttributeSpecs(all = false))) flatMap { workspaceContext =>

      val entityRequestArguments = EntityRequestArguments(workspaceContext, userInfo, dataReference, billingProject)

      val entityFuture = for {
        entityProvider <- entityManager.resolveProviderFuture(entityRequestArguments)
        entity <- entityProvider.getEntity(entityType, entityName)
      } yield {
        PerRequest.RequestComplete(StatusCodes.OK, entity)
      }

      entityFuture.recover {
        case _: EntityNotFoundException =>
          // could move this error message into EntityNotFoundException and allow it to bubble up
          RequestComplete(ErrorReport(StatusCodes.NotFound, s"${entityType} ${entityName} does not exist in $workspaceName"))
      }.recover(bigQueryRecover)
    }

  def updateEntity(workspaceName: WorkspaceName, entityType: String, entityName: String, operations: Seq[AttributeUpdateOperation]): Future[PerRequestMessage] =
    withAttributeNamespaceCheck(operations.map(_.name)) {
      getWorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.write, Some(WorkspaceAttributeSpecs(all = false))) flatMap { workspaceContext =>
        dataSource.inTransactionWithAttrTempTable(Set(AttributeTempTableType.Entity)) { dataAccess =>
          withEntity(workspaceContext, entityType, entityName, dataAccess) { entity =>
            val updateAction = Try {
              val updatedEntity = applyOperationsToEntity(entity, operations)
              dataAccess.entityQuery.save(workspaceContext, updatedEntity)
            } match {
              case Success(result) => result
              case Failure(e: AttributeUpdateOperationException) =>
                DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.BadRequest, s"Unable to update entity ${entityType}/${entityName} in ${workspaceName}", ErrorReport(e))))
              case Failure(regrets) => DBIO.failed(regrets)
            }
            updateAction.map(RequestComplete(StatusCodes.OK, _))
          }
        }
      }
    }

  def deleteEntities(workspaceName: WorkspaceName, entRefs: Seq[AttributeEntityReference], dataReference: Option[DataReferenceName], billingProject: Option[GoogleProjectId]): Future[PerRequestMessage] =
    getWorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.write, Some(WorkspaceAttributeSpecs(all = false))) flatMap { workspaceContext =>

      val entityRequestArguments = EntityRequestArguments(workspaceContext, userInfo, dataReference, billingProject)

      val deleteFuture = for {
        entityProvider <- entityManager.resolveProviderFuture(entityRequestArguments)
        _ <- entityProvider.deleteEntities(entRefs)
      } yield {
        PerRequest.RequestComplete(StatusCodes.NoContent)
      }

      deleteFuture.recover {
        case delEx: DeleteEntitiesConflictException => RequestComplete(StatusCodes.Conflict, delEx.referringEntities)
      }.recover(bigQueryRecover)
    }

  def deleteEntityAttributes(workspaceName: WorkspaceName, entityType: String, attributeNames: Set[AttributeName]): Future[PerRequestMessage] =
    getWorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.write, Some(WorkspaceAttributeSpecs(all = false))) flatMap { workspaceContext =>
      dataSource.inTransaction { dataAccess =>
        dataAccess.entityQuery.deleteAttributes(workspaceContext, entityType, attributeNames) flatMap {
          case Vector(0) => throw new RawlsExceptionWithErrorReport(errorReport = ErrorRepot(StatusCodes.BadRequest, s"Could not find any of the given attribute names."))
          case _ => DBIO.successful(())
        } map (_ => RequestComplete(StatusCodes.NoContent))
      }
    }

  def renameEntity(workspaceName: WorkspaceName, entityType: String, entityName: String, newName: String): Future[PerRequestMessage] =
    getWorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.write, Some(WorkspaceAttributeSpecs(all = false))) flatMap { workspaceContext =>
      dataSource.inTransaction { dataAccess =>
        withEntity(workspaceContext, entityType, entityName, dataAccess) { entity =>
          dataAccess.entityQuery.get(workspaceContext, entity.entityType, newName) flatMap {
            case None => dataAccess.entityQuery.rename(workspaceContext, entity.entityType, entity.name, newName)
            case Some(_) => throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.Conflict, s"Destination ${entity.entityType} ${newName} already exists"))
          } map(_ => RequestComplete(StatusCodes.NoContent))
        }
      }
    }

  def evaluateExpression(workspaceName: WorkspaceName, entityType: String, entityName: String, expression: String): Future[PerRequestMessage] =
    getWorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.read, Some(WorkspaceAttributeSpecs(all = false))) flatMap { workspaceContext =>
      dataSource.inTransaction { dataAccess =>
        withSingleEntityRec(entityType, entityName, workspaceContext, dataAccess) { entities =>
          ExpressionEvaluator.withNewExpressionEvaluator(dataAccess, Some(entities)) { evaluator =>
            evaluator.evalFinalAttribute(workspaceContext, expression).asTry map { tryValuesByEntity => tryValuesByEntity match {
              //parsing failure
              case Failure(regret) => throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.BadRequest, regret))
              case Success(valuesByEntity) =>
                if (valuesByEntity.size != 1) {
                  //wrong number of entities?!
                  throw new RawlsException(s"Expression parsing should have returned a single entity for ${entityType}/$entityName $expression, but returned ${valuesByEntity.size} entities instead")
                } else {
                  assert(valuesByEntity.head._1 == entityName)
                  valuesByEntity.head match {
                    case (_, Success(result)) => RequestComplete(StatusCodes.OK, result.toSeq)
                    case (_, Failure(regret)) =>
                      throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.BadRequest, "Unable to evaluate expression '${expression}' on ${entityType}/${entityName} in ${workspaceName}", ErrorReport(regret)))
                  }
                }
            }
            }
          }
        }
      }
    }

  def entityTypeMetadata(workspaceName: WorkspaceName, dataReference: Option[DataReferenceName], billingProject: Option[GoogleProjectId], useCache: Boolean): Future[PerRequestMessage] =
    getWorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.read, Some(WorkspaceAttributeSpecs(all = false))) flatMap { workspaceContext =>

      val entityRequestArguments = EntityRequestArguments(workspaceContext, userInfo, dataReference, billingProject)

      val metadataFuture = for {
        entityProvider <- entityManager.resolveProviderFuture(entityRequestArguments)
        metadata <- entityProvider.entityTypeMetadata(useCache)
      } yield {
        PerRequest.RequestComplete(StatusCodes.OK, metadata)
      }

      metadataFuture.recover(bigQueryRecover)
    }

  def listEntities(workspaceName: WorkspaceName, entityType: String, parentSpan: Span = null): Future[PerRequestMessage] =
    getWorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.read, Some(WorkspaceAttributeSpecs(all = false))) flatMap { workspaceContext =>
      dataSource.inTransaction { dataAccess =>
        traceDBIOWithParent("listActiveEntitiesOfType", parentSpan) { _ =>
          dataAccess.entityQuery.listActiveEntitiesOfType(workspaceContext, entityType)
        }.map { r =>
          RequestComplete(StatusCodes.OK, r.toSeq)
        }
      }
    }

  def queryEntities(workspaceName: WorkspaceName, dataReference: Option[DataReferenceName], entityType: String, query: EntityQuery, billingProject: Option[GoogleProjectId], parentSpan: Span = null): Future[PerRequestMessage] = {
    getWorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.read, Some(WorkspaceAttributeSpecs(all = false))) flatMap { workspaceContext =>

      val entityRequestArguments = EntityRequestArguments(workspaceContext, userInfo, dataReference, billingProject)

      val queryFuture = for {
        entityProvider <- entityManager.resolveProviderFuture(entityRequestArguments)
        entities <- entityProvider.queryEntities(entityType, query, parentSpan)
      } yield {
        PerRequest.RequestComplete(StatusCodes.OK, entities)
      }

      queryFuture.recover(bigQueryRecover)
    }
  }

  def copyEntities(entityCopyDef: EntityCopyDefinition, uri: Uri, linkExistingEntities: Boolean, parentSpan: Span = null): Future[PerRequestMessage] =

    getWorkspaceContextAndPermissions(entityCopyDef.destinationWorkspace, SamWorkspaceActions.write, Some(WorkspaceAttributeSpecs(all = false))) flatMap { destWorkspaceContext =>
      getWorkspaceContextAndPermissions(entityCopyDef.sourceWorkspace,SamWorkspaceActions.read, Some(WorkspaceAttributeSpecs(all = false))) flatMap { sourceWorkspaceContext =>
        dataSource.inTransaction { dataAccess =>
          for {
            sourceAD <- DBIO.from(samDAO.getResourceAuthDomain(SamResourceTypeNames.workspace, sourceWorkspaceContext.workspaceId, userInfo))
            destAD <- DBIO.from(samDAO.getResourceAuthDomain(SamResourceTypeNames.workspace, destWorkspaceContext.workspaceId, userInfo))
            result <- authDomainCheck(sourceAD.toSet, destAD.toSet) flatMap { _ =>
              val entityNames = entityCopyDef.entityNames
              val entityType = entityCopyDef.entityType
              val copyResults = traceDBIOWithParent("checkAndCopyEntities", parentSpan)( s1 => dataAccess.entityQuery.checkAndCopyEntities(sourceWorkspaceContext, destWorkspaceContext, entityType, entityNames, linkExistingEntities, s1))
              copyResults.map { response =>
                if (response.hardConflicts.isEmpty && (response.softConflicts.isEmpty || linkExistingEntities)) RequestComplete(StatusCodes.Created, response)
                else RequestComplete(StatusCodes.Conflict, response)
              }
            }
          } yield result
        }
      }
    }

  def batchUpdateEntitiesInternal(workspaceName: WorkspaceName, entityUpdates: Seq[EntityUpdateDefinition], upsert: Boolean, dataReference: Option[DataReferenceName], billingProject: Option[GoogleProjectId]): Future[Traversable[Entity]] =
    getWorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.write, Some(WorkspaceAttributeSpecs(all = false))) flatMap { workspaceContext =>
      val entityRequestArguments = EntityRequestArguments(workspaceContext, userInfo, dataReference, billingProject)
      (for {
        entityProvider <- entityManager.resolveProviderFuture(entityRequestArguments)
        entities       <- if (upsert) {
                            entityProvider.batchUpsertEntities(entityUpdates)
                          } else {
                            entityProvider.batchUpdateEntities(entityUpdates)
                          }
      } yield {
        entities
      }) recover {
        case dle: DeltaLayerException =>
          throw new RawlsExceptionWithErrorReport(ErrorReport(dle.code, dle.getMessage, dle))
      }
    }

  def batchUpdateEntities(workspaceName: WorkspaceName, entityUpdates: Seq[EntityUpdateDefinition], dataReference: Option[DataReferenceName], billingProject: Option[GoogleProjectId]): Future[PerRequestMessage] =
    batchUpdateEntitiesInternal(workspaceName, entityUpdates, upsert = false, dataReference, billingProject).map (_ =>
      RequestComplete(StatusCodes.NoContent))

  def batchUpsertEntities(workspaceName: WorkspaceName, entityUpdates: Seq[EntityUpdateDefinition], dataReference: Option[DataReferenceName], billingProject: Option[GoogleProjectId]): Future[PerRequestMessage] =
    batchUpdateEntitiesInternal(workspaceName, entityUpdates, upsert = true, dataReference, billingProject).map (_ =>
      RequestComplete(StatusCodes.NoContent))

  private def bigQueryRecover: PartialFunction[Throwable, PerRequestMessage] = {
    case dee:DataEntityException =>
      RequestComplete(ErrorReport(dee.code, dee.getMessage))
    case bqe:BigQueryException =>
      RequestComplete(ErrorReport(StatusCodes.getForKey(bqe.getCode).getOrElse(StatusCodes.InternalServerError), bqe.getMessage))
    case gjre:GoogleJsonResponseException =>
      // unlikely to hit this case; we should see BigQueryExceptions instead of GoogleJsonResponseExceptions
      RequestComplete(ErrorReport(StatusCodes.getForKey(gjre.getStatusCode).getOrElse(StatusCodes.InternalServerError), gjre.getMessage))
    case report:RawlsExceptionWithErrorReport =>
      throw report // don't rewrap these, just rethrow
    case ex:Exception =>
      RequestComplete(ErrorReport(StatusCodes.InternalServerError, s"Unexpected error: ${ex.getMessage}", ex))
  }

}
