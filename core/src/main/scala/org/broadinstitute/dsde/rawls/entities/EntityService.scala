package org.broadinstitute.dsde.rawls.entities

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.{StatusCodes, Uri}
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.dataaccess.{SamDAO, SlickDataSource}
import org.broadinstitute.dsde.rawls.entities.exceptions.{DeleteEntitiesConflictException, EntityNotFoundException}
import org.broadinstitute.dsde.rawls.expressions.ExpressionEvaluator
import org.broadinstitute.dsde.rawls.metrics.RawlsInstrumented
import org.broadinstitute.dsde.rawls.model.AttributeUpdateOperations.{AttributeUpdateOperation, EntityUpdateDefinition}
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._
import org.broadinstitute.dsde.rawls.model.{AttributeEntityReference, Entity, EntityCopyDefinition, EntityQuery, EntityQueryResponse, EntityQueryResultMetadata, ErrorReport, SamResourceTypeNames, SamWorkspaceActions, UserInfo, WorkspaceName, _}
import org.broadinstitute.dsde.rawls.util.{AttributeSupport, EntitySupport, JsonFilterUtils, WorkspaceSupport}
import org.broadinstitute.dsde.rawls.webservice.PerRequest
import org.broadinstitute.dsde.rawls.webservice.PerRequest.{PerRequestMessage, RequestComplete}
import org.broadinstitute.dsde.rawls.workspace.AttributeUpdateOperationException
import org.broadinstitute.dsde.rawls.{RawlsException, RawlsExceptionWithErrorReport}
import spray.json.DefaultJsonProtocol._

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

object EntityService {
  // TODO: constructor should accept userInfo, dataReferenceName, billingProject, anything else needed to resolve provider
  def constructor(dataSource: SlickDataSource, samDAO: SamDAO, workbenchMetricBaseName: String, entityManager: EntityManager)
                 (userInfo: UserInfo)
                 (implicit executionContext: ExecutionContext): EntityService = {

    // TODO: resolve the provider here, pass to EntityService so implementations never have to resolve
    new EntityService(userInfo, dataSource, samDAO, entityManager, workbenchMetricBaseName)
  }
}

class EntityService(protected val userInfo: UserInfo, val dataSource: SlickDataSource, val samDAO: SamDAO, entityManager: EntityManager, override val workbenchMetricBaseName: String)(implicit protected val executionContext: ExecutionContext)
  extends WorkspaceSupport with EntitySupport with AttributeSupport with LazyLogging with RawlsInstrumented with JsonFilterUtils {

  import dataSource.dataAccess.driver.api._

  def CreateEntity(workspaceName: WorkspaceName, entity: Entity) = createEntity(workspaceName, entity)
  def GetEntity(workspaceName: WorkspaceName, entityType: String, entityName: String, dataReference: Option[String]) = getEntity(workspaceName, entityType, entityName, dataReference)
  def UpdateEntity(workspaceName: WorkspaceName, entityType: String, entityName: String, operations: Seq[AttributeUpdateOperation]) = updateEntity(workspaceName, entityType, entityName, operations)
  def DeleteEntities(workspaceName: WorkspaceName, entities: Seq[AttributeEntityReference], dataReference: Option[String]) = deleteEntities(workspaceName, entities, dataReference)
  def RenameEntity(workspaceName: WorkspaceName, entityType: String, entityName: String, newName: String) = renameEntity(workspaceName, entityType, entityName, newName)
  def EvaluateExpression(workspaceName: WorkspaceName, entityType: String, entityName: String, expression: String) = evaluateExpression(workspaceName, entityType, entityName, expression)
  def GetEntityTypeMetadata(workspaceName: WorkspaceName, dataReference: Option[String]) = entityTypeMetadata(workspaceName, dataReference)
  def ListEntities(workspaceName: WorkspaceName, entityType: String) = listEntities(workspaceName, entityType)
  def QueryEntities(workspaceName: WorkspaceName, entityType: String, query: EntityQuery) = queryEntities(workspaceName, entityType, query)
  def CopyEntities(entityCopyDefinition: EntityCopyDefinition, uri:Uri, linkExistingEntities: Boolean) = copyEntities(entityCopyDefinition, uri, linkExistingEntities)
  def BatchUpsertEntities(workspaceName: WorkspaceName, entityUpdates: Seq[EntityUpdateDefinition]) = batchUpdateEntities(workspaceName, entityUpdates, true)
  def BatchUpdateEntities(workspaceName: WorkspaceName, entityUpdates: Seq[EntityUpdateDefinition]) = batchUpdateEntities(workspaceName, entityUpdates, false)


  def createEntity(workspaceName: WorkspaceName, entity: Entity): Future[Entity] =
    withAttributeNamespaceCheck(entity) {
      getWorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.write, Some(WorkspaceAttributeSpecs(all = false))) flatMap { workspaceContext =>
        entityManager.resolveProvider(workspaceContext, userInfo).createEntity(entity)
      }
    }

  def getEntity(workspaceName: WorkspaceName, entityType: String, entityName: String, dataReference: Option[String]): Future[PerRequestMessage] =
    getWorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.read, Some(WorkspaceAttributeSpecs(all = false))) flatMap { workspaceContext =>

      // TODO: insert the billing project, if present. May want to use EntityRequestArguments or other container class.
      // we haven't done this yet because we don't know the business logic around which billing project to use for each user.
      // TODO: now with two methods building EntityRequestArguments, we probably want to factor that out into the
      // EntityService constructor, or other higher-level method
      val entityRequestArguments = EntityRequestArguments(workspaceContext, userInfo, dataReference)

      entityManager.resolveProvider(entityRequestArguments).getEntity(entityType, entityName)
        .map { entity => PerRequest.RequestComplete(StatusCodes.OK, entity) }
        .recover {
          case _:EntityNotFoundException => RequestComplete(StatusCodes.NotFound, s"${entityType} ${entityName} does not exist in $workspaceName")
          case ex => RequestComplete(StatusCodes.InternalServerError, ex)
        }
    }

  def updateEntity(workspaceName: WorkspaceName, entityType: String, entityName: String, operations: Seq[AttributeUpdateOperation]): Future[PerRequestMessage] =
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
            updateAction.map(RequestComplete(StatusCodes.OK, _))
          }
        }
      }
    }

  def deleteEntities(workspaceName: WorkspaceName, entRefs: Seq[AttributeEntityReference], dataReference: Option[String]): Future[PerRequestMessage] =
    getWorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.write, Some(WorkspaceAttributeSpecs(all = false))) flatMap { workspaceContext =>

      // TODO: insert the billing project, if present. May want to use EntityRequestArguments or other container class.
      // TODO: now with two methods building EntityRequestArguments, we probably want to factor that out into the
      // EntityService constructor, or other higher-level method
      val entityRequestArguments = EntityRequestArguments(workspaceContext, userInfo, dataReference)

      entityManager.resolveProvider(entityRequestArguments).deleteEntities(entRefs)
        .map(r => RequestComplete(StatusCodes.NoContent))
        .recover {
          case delEx: DeleteEntitiesConflictException => RequestComplete(StatusCodes.Conflict, delEx.referringEntities)
          case ex => RequestComplete(StatusCodes.InternalServerError, ex)
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

  def entityTypeMetadata(workspaceName: WorkspaceName, dataReference: Option[String]): Future[PerRequestMessage] =
    getWorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.read, Some(WorkspaceAttributeSpecs(all = false))) flatMap { workspaceContext =>

      // TODO: AS-321 insert the billing project, if present. May want to use EntityRequestArguments or other container class.
      val entityRequestArguments = EntityRequestArguments(workspaceContext, userInfo, dataReference)

      entityManager.resolveProvider(entityRequestArguments).entityTypeMetadata()
        .map(r => RequestComplete(StatusCodes.OK, r))
    }

  def listEntities(workspaceName: WorkspaceName, entityType: String): Future[PerRequestMessage] =
    getWorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.read, Some(WorkspaceAttributeSpecs(all = false))) flatMap { workspaceContext =>
      dataSource.inTransaction { dataAccess =>
        dataAccess.entityQuery.listActiveEntitiesOfType(workspaceContext, entityType).map(r => RequestComplete(StatusCodes.OK, r.toSeq))
      }
    }

  def queryEntities(workspaceName: WorkspaceName, entityType: String, query: EntityQuery): Future[PerRequestMessage] = {
    getWorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.read, Some(WorkspaceAttributeSpecs(all = false))) flatMap { workspaceContext =>
      dataSource.inTransaction { dataAccess =>
        dataAccess.entityQuery.loadEntityPage(workspaceContext, entityType, query) map { case (unfilteredCount, filteredCount, entities) =>
          createEntityQueryResponse(query, unfilteredCount, filteredCount, entities.toSeq).get
        }
      }
    }
  }

  def createEntityQueryResponse(query: EntityQuery, unfilteredCount: Int, filteredCount: Int, page: Seq[Entity]): Try[RequestComplete[(StatusCodes.Success, EntityQueryResponse)]] = {
    val pageCount = Math.ceil(filteredCount.toFloat / query.pageSize).toInt
    if (filteredCount > 0 && query.page > pageCount) {
      Failure(new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.BadRequest, s"requested page ${query.page} is greater than the number of pages $pageCount")))

    } else {
      val response = EntityQueryResponse(query, EntityQueryResultMetadata(unfilteredCount, filteredCount, pageCount), page)

      Success(RequestComplete(StatusCodes.OK, response))
    }
  }

  def copyEntities(entityCopyDef: EntityCopyDefinition, uri: Uri, linkExistingEntities: Boolean): Future[PerRequestMessage] =

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
              copyResults.map { response =>
                if (response.hardConflicts.isEmpty && (response.softConflicts.isEmpty || linkExistingEntities)) RequestComplete(StatusCodes.Created, response)
                else RequestComplete(StatusCodes.Conflict, response)
              }
            }
          } yield result
        }
      }
    }

  def batchUpdateEntities(workspaceName: WorkspaceName, entityUpdates: Seq[EntityUpdateDefinition], upsert: Boolean = false): Future[PerRequestMessage] = {
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

          saveAction.map(_ => RequestComplete(StatusCodes.NoContent))
        }
      }
    }
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

}
