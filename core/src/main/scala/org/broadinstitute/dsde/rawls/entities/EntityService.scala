package org.broadinstitute.dsde.rawls.entities

import akka.http.scaladsl.model.{StatusCodes, Uri}
import com.google.api.client.googleapis.json.GoogleJsonResponseException
import com.google.cloud.bigquery.BigQueryException
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.dataaccess.{SamDAO, SlickDataSource}
import org.broadinstitute.dsde.rawls.entities.exceptions.{DataEntityException, DeleteEntitiesConflictException, EntityNotFoundException}
import org.broadinstitute.dsde.rawls.expressions.ExpressionEvaluator
import org.broadinstitute.dsde.rawls.metrics.RawlsInstrumented
import org.broadinstitute.dsde.rawls.model.AttributeUpdateOperations.{AttributeUpdateOperation, EntityUpdateDefinition}
import org.broadinstitute.dsde.rawls.model.{AttributeEntityReference, Entity, EntityCopyDefinition, EntityQuery, ErrorReport, SamResourceTypeNames, SamWorkspaceActions, UserInfo, WorkspaceName, _}
import org.broadinstitute.dsde.rawls.util.{AttributeSupport, EntitySupport, JsonFilterUtils, WorkspaceSupport}
import org.broadinstitute.dsde.rawls.workspace.AttributeUpdateOperationException
import org.broadinstitute.dsde.rawls.{RawlsException, RawlsExceptionWithErrorReport}

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

  def getEntity(workspaceName: WorkspaceName, entityType: String, entityName: String, dataReference: Option[DataReferenceName], billingProject: Option[GoogleProjectId]): Future[Entity] =
    getWorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.read, Some(WorkspaceAttributeSpecs(all = false))) flatMap { workspaceContext =>

      val entityRequestArguments = EntityRequestArguments(workspaceContext, userInfo, dataReference, billingProject)

      val entityFuture = for {
        entityProvider <- entityManager.resolveProviderFuture(entityRequestArguments)
        entity <- entityProvider.getEntity(entityType, entityName)
      } yield {
        entity
      }

      entityFuture.recover {
        case _: EntityNotFoundException =>
          // could move this error message into EntityNotFoundException and allow it to bubble up
          throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.NotFound, s"${entityType} ${entityName} does not exist in $workspaceName"))
      }.recover(bigQueryRecover)
    }

  def updateEntity(workspaceName: WorkspaceName, entityType: String, entityName: String, operations: Seq[AttributeUpdateOperation]): Future[Entity] =
    withAttributeNamespaceCheck(operations.map(_.name)) {
      getWorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.write, Some(WorkspaceAttributeSpecs(all = false))) flatMap { workspaceContext =>
        dataSource.inTransaction { dataAccess =>
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
            updateAction
          }
        }
      }
    }

  def deleteEntities(workspaceName: WorkspaceName, entRefs: Seq[AttributeEntityReference], dataReference: Option[DataReferenceName], billingProject: Option[GoogleProjectId]): Future[Set[AttributeEntityReference]] =
    getWorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.write, Some(WorkspaceAttributeSpecs(all = false))) flatMap { workspaceContext =>

      val entityRequestArguments = EntityRequestArguments(workspaceContext, userInfo, dataReference, billingProject)

      val deleteFuture = for {
        entityProvider <- entityManager.resolveProviderFuture(entityRequestArguments)
        _ <- entityProvider.deleteEntities(entRefs)
      } yield {
        Set[AttributeEntityReference]()
      }

      deleteFuture.recover {
        case delEx: DeleteEntitiesConflictException => delEx.referringEntities
      }.recover(bigQueryRecover)
    }

  def renameEntity(workspaceName: WorkspaceName, entityType: String, entityName: String, newName: String): Future[Int] =
    getWorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.write, Some(WorkspaceAttributeSpecs(all = false))) flatMap { workspaceContext =>
      dataSource.inTransaction { dataAccess =>
        withEntity(workspaceContext, entityType, entityName, dataAccess) { entity =>
          dataAccess.entityQuery.get(workspaceContext, entity.entityType, newName) flatMap {
            case None => dataAccess.entityQuery.rename(workspaceContext, entity.entityType, entity.name, newName)
            case Some(_) => throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.Conflict, s"Destination ${entity.entityType} ${newName} already exists"))
          }
        }
      }
    }

  def evaluateExpression(workspaceName: WorkspaceName, entityType: String, entityName: String, expression: String): Future[Seq[AttributeValue]] =
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
                    case (_, Success(result)) => result.toSeq
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

  def entityTypeMetadata(workspaceName: WorkspaceName, dataReference: Option[DataReferenceName], billingProject: Option[GoogleProjectId], useCache: Boolean): Future[Map[String, EntityTypeMetadata]] =
    getWorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.read, Some(WorkspaceAttributeSpecs(all = false))) flatMap { workspaceContext =>

      val entityRequestArguments = EntityRequestArguments(workspaceContext, userInfo, dataReference, billingProject)

      val metadataFuture = for {
        entityProvider <- entityManager.resolveProviderFuture(entityRequestArguments)
        metadata <- entityProvider.entityTypeMetadata(useCache)
      } yield {
        metadata
      }

      metadataFuture.recover(bigQueryRecover)
    }

  def listEntities(workspaceName: WorkspaceName, entityType: String): Future[Seq[Entity]] =
    getWorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.read, Some(WorkspaceAttributeSpecs(all = false))) flatMap { workspaceContext =>
      dataSource.inTransaction { dataAccess =>
        dataAccess.entityQuery.listActiveEntitiesOfType(workspaceContext, entityType).map(r => r.toSeq)
      }
    }

  def queryEntities(workspaceName: WorkspaceName, dataReference: Option[DataReferenceName], entityType: String, query: EntityQuery, billingProject: Option[GoogleProjectId]): Future[EntityQueryResponse] = {
    getWorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.read, Some(WorkspaceAttributeSpecs(all = false))) flatMap { workspaceContext =>

      val entityRequestArguments = EntityRequestArguments(workspaceContext, userInfo, dataReference, billingProject)

      val queryFuture = for {
        entityProvider <- entityManager.resolveProviderFuture(entityRequestArguments)
        entities <- entityProvider.queryEntities(entityType, query)
      } yield {
        entities
      }

      queryFuture.recover(bigQueryRecover)
    }
  }

  def copyEntities(entityCopyDef: EntityCopyDefinition, uri: Uri, linkExistingEntities: Boolean): Future[EntityCopyResponse] =

    getWorkspaceContextAndPermissions(entityCopyDef.destinationWorkspace, SamWorkspaceActions.write, Some(WorkspaceAttributeSpecs(all = false))) flatMap { destWorkspaceContext =>
      getWorkspaceContextAndPermissions(entityCopyDef.sourceWorkspace,SamWorkspaceActions.read, Some(WorkspaceAttributeSpecs(all = false))) flatMap { sourceWorkspaceContext =>
        dataSource.inTransaction { dataAccess =>
          for {
            sourceAD <- DBIO.from(samDAO.getResourceAuthDomain(SamResourceTypeNames.workspace, sourceWorkspaceContext.workspaceId, userInfo))
            destAD <- DBIO.from(samDAO.getResourceAuthDomain(SamResourceTypeNames.workspace, destWorkspaceContext.workspaceId, userInfo))
            result <- authDomainCheck(sourceAD.toSet, destAD.toSet) flatMap { _ =>
              val entityNames = entityCopyDef.entityNames
              val entityType = entityCopyDef.entityType
              val copyResults = dataAccess.entityQuery.checkAndCopyEntities(sourceWorkspaceContext, destWorkspaceContext, entityType, entityNames, linkExistingEntities)
              copyResults
            }
          } yield result
        }
      }
    }

  def batchUpdateEntitiesInternal(workspaceName: WorkspaceName, entityUpdates: Seq[EntityUpdateDefinition], upsert: Boolean = false): Future[Traversable[Entity]] = {
    val namesToCheck = for {
      update <- entityUpdates
      operation <- update.operations
    } yield operation.name

    withAttributeNamespaceCheck(namesToCheck) {
      getWorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.write, Some(WorkspaceAttributeSpecs(all = false))) flatMap { workspaceContext =>
        dataSource.inTransaction { dataAccess =>
          val updateTrialsAction = dataAccess.entityQuery.getActiveEntities(workspaceContext, entityUpdates.map(eu => AttributeEntityReference(eu.entityType, eu.name))) map { entities =>
            val entitiesByName = entities.map(e => (e.entityType, e.name) -> e).toMap
            entityUpdates.map { entityUpdate =>
              entityUpdate -> (entitiesByName.get((entityUpdate.entityType, entityUpdate.name)) match {
                case Some(e) =>
                  Try(applyOperationsToEntity(e, entityUpdate.operations))
                case None =>
                  if (upsert) {
                    Try(applyOperationsToEntity(Entity(entityUpdate.name, entityUpdate.entityType, Map.empty), entityUpdate.operations))
                  } else {
                    Failure(new RuntimeException("Entity does not exist"))
                  }
              })
            }
          }

          val saveAction = updateTrialsAction flatMap { updateTrials =>
            val errorReports = updateTrials.collect { case (entityUpdate, Failure(regrets)) =>
              ErrorReport(s"Could not update ${entityUpdate.entityType} ${entityUpdate.name}", ErrorReport(regrets))
            }
            if (!errorReports.isEmpty) {
              DBIO.failed(new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.BadRequest, "Some entities could not be updated.", errorReports)))
            } else {
              val t = updateTrials.collect { case (entityUpdate, Success(entity)) => entity }

              dataAccess.entityQuery.save(workspaceContext, t)
            }
          }

          saveAction
        }
      }
    }
  }

  def batchUpdateEntities(workspaceName: WorkspaceName, entityUpdates: Seq[EntityUpdateDefinition]): Future[Traversable[Entity]] = {
    batchUpdateEntitiesInternal(workspaceName, entityUpdates, upsert = false)
  }

  def batchUpsertEntities(workspaceName: WorkspaceName, entityUpdates: Seq[EntityUpdateDefinition]): Future[Traversable[Entity]] = {
    batchUpdateEntitiesInternal(workspaceName, entityUpdates, upsert = true)
  }


  /**
    * Applies the sequence of operations in order to the entity.
    *
    * @param entity to update
    * @param operations sequence of operations
    * @throws org.broadinstitute.dsde.rawls.workspace.AttributeNotFoundException when removing from a list attribute that does not exist
    * @throws AttributeUpdateOperationException when adding or removing from an attribute that is not a list
    * @return the updated entity
    */
  def applyOperationsToEntity(entity: Entity, operations: Seq[AttributeUpdateOperation]): Entity = {
    entity.copy(attributes = applyAttributeUpdateOperations(entity, operations))
  }

  private def bigQueryRecover[U]: PartialFunction[Throwable, U] = {
    case dee:DataEntityException =>
      throw new RawlsExceptionWithErrorReport(ErrorReport(dee.code, dee.getMessage))
    case bqe:BigQueryException =>
      throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.getForKey(bqe.getCode).getOrElse(StatusCodes.InternalServerError), bqe.getMessage))
    case gjre:GoogleJsonResponseException =>
      // unlikely to hit this case; we should see BigQueryExceptions instead of GoogleJsonResponseExceptions
      throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.getForKey(gjre.getStatusCode).getOrElse(StatusCodes.InternalServerError), gjre.getMessage))
    case report:RawlsExceptionWithErrorReport =>
      throw report // don't rewrap these, just rethrow
    case ex:Exception =>
      throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.InternalServerError, s"Unexpected error: ${ex.getMessage}", ex))
  }

}
