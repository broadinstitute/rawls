package org.broadinstitute.dsde.rawls.entities.compact

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import akka.stream.scaladsl.Source
import com.google.common.annotations.VisibleForTesting
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.dataaccess.slick._
import org.broadinstitute.dsde.rawls.entities.base.ExpressionEvaluationSupport.LookupExpression
import org.broadinstitute.dsde.rawls.entities.base.{EntityProvider, ExpressionEvaluationContext, ExpressionValidator}
import org.broadinstitute.dsde.rawls.entities.compact.batch.BatchHandling
import org.broadinstitute.dsde.rawls.entities.compact.entityQuery.{CountAndSource, EntityQueryStrategy}
import org.broadinstitute.dsde.rawls.entities.exceptions.{
  AttributeException,
  DataEntityException,
  DeleteEntitiesConflictException,
  DeleteEntitiesOfTypeConflictException,
  EntityNotFoundException,
  EntityReferenceNotFoundException,
  UnsupportedEntityOperationException
}
import org.broadinstitute.dsde.rawls.entities.{EntityRequestArguments, EntityUtils}
import org.broadinstitute.dsde.rawls.jobexec.MethodConfigResolver
import org.broadinstitute.dsde.rawls.model.AttributeUpdateOperations.{AttributeUpdateOperation, EntityUpdateDefinition}
import org.broadinstitute.dsde.rawls.model.{
  Attributable,
  AttributeEntityReference,
  AttributeEntityReferenceList,
  AttributeName,
  AttributeRename,
  AttributeValue,
  Entity,
  EntityCopyResponse,
  EntityHardConflict,
  EntityPointer,
  EntityQuery,
  EntityQueryResponse,
  EntityQueryResultMetadata,
  EntitySoftConflict,
  EntityTypeMetadata,
  EntityTypeRename,
  ErrorReport,
  RawlsRequestContext,
  SubmissionValidationEntityInputs,
  Workspace
}
import org.broadinstitute.dsde.rawls.util.TracingUtils.{trace, traceDBIOWithParent}
import slick.dbio.{DBIO, DBIOAction, Effect, NoStream}
import slick.jdbc.ResultSetConcurrency.ReadOnly
import slick.jdbc.{ResultSetConcurrency, ResultSetType}
import slick.jdbc.TransactionIsolation.ReadCommitted

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}
import scala.language.postfixOps
import scala.util.Try

/**
  * Implementation logic for compact data tables. Compact data tables store all an entity's attributes in a
  * single JSON packet within the database. Compare this to the implementation in LocalEntityProvider, which
  * requires at least one row per attribute.
  *
  * @param executionContext scala concurrency context
  */
class CompactEntityProvider(requestArguments: EntityRequestArguments,
                            val repository: CompactEntityRepository,
                            config: CompactEntityProviderConfig = CompactEntityProviderConfig()
)(implicit
  protected val executionContext: ExecutionContext,
  implicit val actorSystem: ActorSystem
) extends EntityProvider
    with BatchHandling {
  override def entityStoreId: Option[String] = None // unused

  val workspaceId: UUID = requestArguments.workspace.workspaceIdAsUUID // shorthand for methods below
  val workspaceContext: Workspace = requestArguments.workspace

  override def batchUpdateEntities(
    entityUpdates: Source[EntityUpdateDefinition, _],
    parentContext: RawlsRequestContext
  ): Future[Int] = {
    // perform the updates
    val dbResults: Future[Int] = handleUpdates(entityUpdates, allowInsert = false, config, parentContext)
    // Fire-and-forget an update to the workspace's last-modified date; no need to wait for it to complete
    withWorkspaceLastModified(dbResults)
    // and return
    dbResults
  }

  override def batchUpsertEntities(
    entityUpdates: Source[EntityUpdateDefinition, _],
    parentContext: RawlsRequestContext
  ): Future[Int] = {
    // perform the upserts
    val dbResults: Future[Int] = handleUpdates(entityUpdates, allowInsert = true, config, parentContext)
    // Fire-and-forget an update to the workspace's last-modified date; no need to wait for it to complete
    withWorkspaceLastModified(dbResults)
    // and return
    dbResults
  }

  def saveWorkflowOutputEntities(
    dataAccess: DataAccess,
    workspace: Workspace,
    updatedEntities: Seq[Entity]
  ): ReadWriteAction[Traversable[Entity]] = DBIO.successful(Seq()) // TODO CORE-483: implement this

  def listWorkflowEntities(dataAccess: DataAccess,
                           workspace: Workspace,
                           entityIds: Seq[Long]
  ): ReadAction[Map[Long, Entity]] = DBIO.successful(Map()) // TODO CORE-483: implement this

  override def copyEntities(sourceWorkspaceContext: Workspace,
                            destWorkspaceContext: Workspace,
                            entityType: String,
                            entityNames: Seq[String],
                            linkExistingEntities: Boolean,
                            parentContext: RawlsRequestContext
  ): Future[EntityCopyResponse] = {
    val entitiesToCopyRefs = entityNames.map(name => EntityPointer(entityType, name)).toSet
    val copyResult = repository.dataSource.inTransaction { _ =>
      for {
        hardConflicts <- repository.queries.getEntityRefs(destWorkspaceContext.workspaceIdAsUUID, entitiesToCopyRefs)
        result <-
          if (hardConflicts.nonEmpty) {
            DBIO.successful(
              EntityCopyResponse(
                Seq.empty,
                hardConflicts.map(c => EntityHardConflict(c.entityType, c.name)),
                Seq.empty
              )
            )
          } else {
            repository.queries
              .recursiveGetEntityReferences(sourceWorkspaceContext.workspaceIdAsUUID,
                                            entitiesToCopyRefs,
                                            config.batchCopyBatchSize
              )
              .flatMap { entityReferenceMap =>
                val entities = entityReferenceMap.map(_.from)
                val entityReferences = entityReferenceMap.flatMap(_.to)
                repository.queries.getEntityRefs(destWorkspaceContext.workspaceIdAsUUID, entityReferences).flatMap {
                  conflicts =>
                    val softConflicts = conflicts.map(_.toPointer).toSet
                    if (softConflicts.isEmpty || linkExistingEntities) {
                      copyEntitiesExcludingAnySoftConflicts(
                        entities,
                        entityReferences,
                        softConflicts,
                        sourceWorkspaceContext.workspaceIdAsUUID,
                        destWorkspaceContext.workspaceIdAsUUID
                      )
                    } else {
                      unmergedSoftConflicts(entityReferenceMap, softConflicts)
                    }
                }
              }
          }
      } yield result
    }
    withWorkspaceLastModified(copyResult)
    copyResult
  }

  /**
   * Copy all entities from sourceWorkspaceId to destWorkspaceId, excluding any entities that
   * are in the set of soft conflicts. Return an EntityCopyResponse containing a Seq of entities
   * that were copied.
   */
  private def copyEntitiesExcludingAnySoftConflicts(entities: Set[EntityPointer],
                                                    entityReferences: Set[EntityPointer],
                                                    softConflicts: Set[EntityPointer],
                                                    sourceWorkspaceId: UUID,
                                                    destWorkspaceId: UUID
  ) = {
    val allEntityRefs: Set[EntityPointer] = entities ++ entityReferences
    val entitiesToCopy: Set[EntityPointer] = allEntityRefs diff softConflicts
    repository.queries
      .copyEntitiesToNewWorkspace(
        sourceWorkspaceId,
        destWorkspaceId,
        entitiesToCopy,
        config.batchCopyBatchSize
      )
      .map { _ =>
        EntityCopyResponse(
          entitiesToCopy.map(_.toAttributeEntityReference).toSeq,
          Seq.empty,
          Seq.empty
        )
      }
  }

  /**
   * For each entity in the entityReferenceMap, check any of the entites that it references
   * are in the set of soft conflicts. If so, create an EntitySoftConflict for that entity
   */
  def unmergedSoftConflicts(entityReferenceMap: Set[RefMapping],
                            softConflicts: Set[EntityPointer]
  ): DBIOAction[EntityCopyResponse, NoStream, Effect] = {
    val unmergedSoftConflicts = entityReferenceMap.flatMap { refMapping =>
      val conflicts = refMapping.to
        .intersect(softConflicts)
        .map(conflict => EntitySoftConflict(conflict.entityType, conflict.entityName, Seq.empty))
        .toSeq
      if (conflicts.nonEmpty) {
        Some(EntitySoftConflict(refMapping.from.entityType, refMapping.from.entityName, conflicts))
      } else {
        None
      }
    }.toSeq
    DBIO.successful(EntityCopyResponse(Seq.empty, Seq.empty, unmergedSoftConflicts))
  }

  override def createEntity(entity: Entity, parentContext: RawlsRequestContext): Future[Entity] = {
    EntityUtils.validateEntity(entity)
    val createFuture = repository.dataSource.inTransaction { _ =>
      for {
        // does this entity already exist?
        preExisting <- repository.queries.getEntity(workspaceId, entity.entityType, entity.name)
        _ = if (preExisting.nonEmpty)
          throw new RawlsExceptionWithErrorReport(
            errorReport = ErrorReport(
              StatusCodes.Conflict,
              s"${entity.entityType} ${entity.name} already exists in ${requestArguments.workspace.toWorkspaceName}"
            )
          )
        // find all references in this entity
        refs: Set[RefMapping] = findAllReferences(entity)

        // verify that all references in the entity-to-be-saved actually exist
        referencesExist <- repository.queries.existsAll(workspaceId, refs.flatMap(_.to))
        _ = if (!referencesExist)
          throw new EntityReferenceNotFoundException("Some entity references do not exist")
        // save the entity
        _ <- repository.queries.createEntity(workspaceId, entity)
        // Did it save correctly? Re-retrieve it. By re-retrieving it, we can 1) get its id, and 2) get the actual,
        // normalized JSON that was persisted to the db. When we return the entity to the user, we return the
        // normalized version.
        savedEntityRecordOption <- repository.queries.getEntity(workspaceId, entity.entityType, entity.name)
        savedEntityRecord = savedEntityRecordOption.getOrElse(throw new DataEntityException("Could not save entity"))
        // verify that nobody else saved this same record in the meantime
        _ = if (savedEntityRecord.recordVersion != 0L)
          throw new RawlsConcurrentModificationException(
            s"Detected concurrent modifications to entity ${savedEntityRecord.toPointer}."
          )
      } yield savedEntityRecord.toEntity
    }
    // fire-and-forget an update to the workspace's last-modified date; no need to wait for it to complete
    withWorkspaceLastModified(createFuture)

    createFuture
  }

  override def deleteEntities(pointers: Seq[EntityPointer], parentContext: RawlsRequestContext): Future[Int] =
    repository.dataSource.inTransaction { _ =>
      for {
        // check if any of these entities are referenced by someone else
        referencingEntities: Seq[EntityPointer] <- repository.queries.getReferencesTo(workspaceId, pointers)
        // getReferencesTo already excludes the entities that are being deleted
        _ = if (referencingEntities.nonEmpty) {
          throw new DeleteEntitiesConflictException(referencingEntities.map(_.toAttributeEntityReference).toSet)
        }
        // hard-delete everything we can
        _ <- repository.queries.deleteEntities(workspaceId, pointers)
        // soft-delete (i.e. hide) everything that could not be hard-deleted
        res <- repository.queries.batchHide(workspaceId, pointers)
      } yield res
    }

  override def deleteEntitiesOfType(entityType: String, parentContext: RawlsRequestContext): Future[Int] =
    repository.dataSource.inTransaction { _ =>
      for {
        // check if any of these entities are referenced by someone else
        referencingEntities: Seq[EntityPointer] <- repository.queries.getReferencesToType(workspaceId, entityType)
        // The getReferencesToType query already disregards references of the type to be deleted
        _ = if (referencingEntities.nonEmpty) {
          throw new DeleteEntitiesOfTypeConflictException(referencingEntities.size)
        }
        // hard-delete everything we can
        _ <- repository.queries.deleteEntitiesOfType(workspaceId, entityType)
        // soft-delete (i.e. hide) everything that could not be hard-deleted
        res <- repository.queries.batchHideType(
          workspaceId,
          entityType
        )
      } yield res
    }

  override def deleteEntityAttributes(entityType: String,
                                      attributeNames: Set[AttributeName],
                                      parentContext: RawlsRequestContext
  ): Future[Unit] = ???

  override def entityTypeMetadata(useCache: Boolean,
                                  parentContext: RawlsRequestContext
  ): Future[Map[String, EntityTypeMetadata]] =
    repository.dataSource.inTransaction(ReadOnly) { _ =>
      for {
        entityTypeAndKeys <- traceDBIOWithParent("listEntityKeys", parentContext) { _ =>
          repository.queries.listEntityKeys(workspaceId)
        }
        entityTypeAndCounts <- traceDBIOWithParent("countEntitiesGroupedByType", parentContext) { _ =>
          repository.queries.countEntitiesGroupedByType(workspaceId)
        }
      } yield trace("resultCalculation", parentContext) { _ =>
        // note that entityTypeAndKeys only contains entity types that have at least one key
        // and that entityTypeAndCounts contains all entity types, even those with zero keys
        val keysByType = entityTypeAndKeys.groupMap(_.entityType)(_.attributeKey)
        entityTypeAndCounts.map { case EntityTypeAndCount(entityType, count) =>
          entityType -> EntityTypeMetadata(
            count,
            entityType + Attributable.entityIdAttributeSuffix,
            keysByType.getOrElse(entityType, Seq.empty).map(AttributeName.toDelimitedName).sortBy(_.toLowerCase)
          )
        }.toMap
      }
    }

  override def evaluateExpression(entityType: String,
                                  entityName: String,
                                  expression: String,
                                  parentContext: RawlsRequestContext
  ): Future[Seq[AttributeValue]] = ???

  override def evaluateExpressions(expressionEvaluationContext: ExpressionEvaluationContext,
                                   gatherInputsResult: MethodConfigResolver.GatherInputsResult,
                                   workspaceExpressionResults: Map[LookupExpression, Try[Iterable[AttributeValue]]]
  ): Future[LazyList[SubmissionValidationEntityInputs]] = ???

  override def expressionValidator: ExpressionValidator = ???

  override def getEntity(entityType: String, entityName: String, parentContext: RawlsRequestContext): Future[Entity] = {
    val queryResult = repository.dataSource.inTransaction(ReadOnly) { _ =>
      repository.queries.getEntity(workspaceId, entityType, entityName)
    }
    queryResult map {
      case Some(entityRec) => entityRec.toEntity
      case None            => throw new EntityNotFoundException()
    }
  }

  override def listEntities(entityType: String): Source[Entity, NotUsed] = {
    import repository.dataSource.dataAccess.driver.api._

    Source
      .fromPublisher(
        repository.dataSource.database.stream(
          repository.queries
            .listEntities(workspaceId, entityType)
            .transactionally
            .withTransactionIsolation(ReadCommitted)
            .withStatementParameters(rsType = ResultSetType.ForwardOnly,
                                     rsConcurrency = ResultSetConcurrency.ReadOnly,
                                     fetchSize = repository.dataSource.fetchSize
            )
        )
      )
      .map(_.toEntity)
  }

  override def queryEntities(entityType: String,
                             query: EntityQuery,
                             parentContext: RawlsRequestContext
  ): Future[EntityQueryResponse] = ???

  /**
   * Returns the components needed to stream a EntityQueryResponse to an end user in response to the entityQuery API.
   * This method returns fully materialized metadata (row counts, page size, etc) as EntityQueryResultMetadata, and
   * also returns a streaming Source of Entity objects. We avoid materializing the full set of Entity objects for
   * performance and memory reasons.
   *
   * @param entityType the type of entities to return in the result set
   * @param entityQuery criteria for filtering and paginating the result set
   * @param parentContext tracing context into which this method will add traces
   * @return a tuple of 1) the fully materialized metadata, and 2) a streaming Source of Entity objects
   */
  override def queryEntitiesSource(entityType: String,
                                   entityQuery: EntityQuery,
                                   parentContext: RawlsRequestContext = requestArguments.ctx
  ): Future[(EntityQueryResultMetadata, Source[Entity, _])] =
    countEntitiesOfType(entityType).flatMap { unfilteredCount =>
      if (unfilteredCount == 0) {
        // if there are no entities, we can just return an empty source
        Future.successful((EntityQueryResultMetadata(0, 0, 0), Source.empty))
      } else {
        EntityQueryStrategy
          .choose(repository, workspaceId, entityType, entityQuery, unfilteredCount)
          .getCountAndSource
          .map(prepareQueryEntitiesResult(entityQuery, unfilteredCount, _))
      }
    }

  @VisibleForTesting
  private[compact] def prepareQueryEntitiesResult(entityQuery: EntityQuery,
                                                  unfilteredCount: Int,
                                                  filteredCountAndSource: CountAndSource
  ): (EntityQueryResultMetadata, Source[Entity, _]) = {
    val pageCount: Int = Math.ceil(filteredCountAndSource.count.toFloat / entityQuery.pageSize).toInt
    if (filteredCountAndSource.count > 0 && entityQuery.page > pageCount) {
      throw new DataEntityException(
        code = StatusCodes.BadRequest,
        message = s"requested page ${entityQuery.page} is greater than the number of pages $pageCount"
      )
    }
    (EntityQueryResultMetadata(unfilteredCount, filteredCountAndSource.count, pageCount), filteredCountAndSource.source)
  }

  private def countEntitiesOfType(entityType: LookupExpression) =
    repository.dataSource
      .inTransaction(ReadCommitted) { _ =>
        repository.queries.countEntities(workspaceId, entityType)
      }

  override def renameAttribute(entityType: String,
                               oldName: AttributeName,
                               attributeRenameRequest: AttributeRename,
                               parentContext: RawlsRequestContext
  ): Future[Int] = {
    val newName = attributeRenameRequest.newAttributeName

    // nested helper function to validate and perform the renaming all in one transaction
    def renameInTransaction: Future[Int] = repository.dataSource.inTransaction { _ =>
      for {
        // does new name already exist? fail if it does.
        newNameExists <- repository.queries.attributeExists(workspaceId, entityType, newName)
        _ = if (newNameExists)
          throw new AttributeException(
            message = s"${AttributeName.toDelimitedName(newName)} already exists.",
            code = StatusCodes.BadRequest
          )
        // does old name already exist? fail if it does not.
        oldNameExists <- repository.queries.attributeExists(workspaceId, entityType, oldName)
        _ = if (!oldNameExists)
          throw new AttributeException(
            message = s"${AttributeName.toDelimitedName(oldName)} does not exist.",
            code = StatusCodes.BadRequest
          )
        // perform the rename
        numEntitiesAffected <- repository.queries.renameAttribute(workspaceId,
                                                                  entityType,
                                                                  oldName,
                                                                  attributeRenameRequest
        )
      } yield numEntitiesAffected
    }

    val renameFuture = for {
      // validate both old and new names for syntax
      _ <- Future(EntityUtils.validateAttrName(oldName, entityType))
      _ <- Future(EntityUtils.validateAttrName(newName, entityType))
      _ = if (oldName == newName) {
        throw new AttributeException(
          message = s"Old and new names are the same: ${AttributeName.toDelimitedName(oldName)}",
          code = StatusCodes.BadRequest
        )
      }
      // perform the rename in a transaction
      numEntitiesAffected <- renameInTransaction
    } yield numEntitiesAffected

    // Fire-and-forget an update to the workspace's last-modified date; no need to wait for it to complete
    withWorkspaceLastModified(renameFuture)

    // return the future
    renameFuture
  }

  override def renameEntity(entityType: String,
                            entityName: String,
                            newName: String,
                            parentContext: RawlsRequestContext
  ): Future[Int] = {
    // Check if the newName is the same as the entityName
    if (newName == entityName) {
      throw new RawlsExceptionWithErrorReport(
        errorReport = ErrorReport(StatusCodes.BadRequest, "New name is the same as the entity name")
      )
    }
    // Validate the newName
    EntityUtils.validateEntityName(newName)
    // Perform the rename in a transaction
    val renameFuture = repository.dataSource.inTransaction { _ =>
      for {
        // Check if the newName already exists
        newNameExists <- repository.queries.existsAll(workspaceId, Set(EntityPointer(entityType, newName)))
        _ = if (newNameExists) {
          throw new DataEntityException(
            code = StatusCodes.Conflict,
            message = s"Destination $entityType $newName already exists"
          )
        }
        // Check if the Entity exists, throw an error if it does not
        entityExists <- repository.queries.existsAll(workspaceId, Set(EntityPointer(entityType, entityName)))
        _ = if (!entityExists) {
          throw new EntityNotFoundException("Can't find entity name!")
        }
        // Perform the rename
        entityRowsUpdated <- repository.queries.renameEntity(workspaceId, entityType, entityName, newName)
      } yield entityRowsUpdated
    }
    // Fire-and-forget an update to the workspace's last-modified date; no need to wait for it to complete
    withWorkspaceLastModified(renameFuture)
    // return the future
    renameFuture
  }

  override def renameEntityType(oldName: String,
                                renameInfo: EntityTypeRename,
                                parentContext: RawlsRequestContext
  ): Future[Int] = {
    // Extract the new entity type name from the rename info
    val newName = renameInfo.newName

    // Perform the rename in a transaction
    val renameFuture = repository.dataSource.inTransaction { _ =>
      for {
        // First check if the old entity type exists
        entityTypeCount <- repository.queries.countEntities(workspaceId, oldName)
        _ = if (entityTypeCount == 0) {
          throw new RawlsExceptionWithErrorReport(
            errorReport = ErrorReport(StatusCodes.NotFound, s"Can't find entity type $oldName")
          )
        }

        // Check if the new entity type already exists
        newTypeCount <- repository.queries.countEntities(workspaceId, newName)
        _ = if (newTypeCount > 0) {
          throw new RawlsExceptionWithErrorReport(
            errorReport = ErrorReport(StatusCodes.Conflict, s"$newName already exists as an entity type")
          )
        }

        // Call the renameEntityType method in CompactEntityComponent to update entity types and references
        entityRowsUpdated <- repository.queries.renameEntityType(workspaceId, oldName, newName)

      } yield entityRowsUpdated // Return the number of entities that were renamed
    }

    // Fire-and-forget an update to the workspace's last-modified date; no need to wait for it to complete
    withWorkspaceLastModified(renameFuture)

    // Return the future
    renameFuture
  }

  override def updateEntity(entityType: String,
                            entityName: String,
                            operations: Seq[AttributeUpdateOperation],
                            parentContext: RawlsRequestContext
  ): Future[Entity] =
    // validate
    if (operations.isEmpty) {
      Future.failed(
        new UnsupportedEntityOperationException(message = "No operations provided", code = StatusCodes.BadRequest)
      )
    } else {
      // translate the input arguments to the batchUpdate arguments
      val updateDefinition = Source.single(EntityUpdateDefinition(entityName, entityType, operations))
      // perform a batchUpdate for just this one entity. batchUpdate will throw an error if the entity does not exist.
      batchUpdateEntities(updateDefinition, parentContext) flatMap { _ =>
        repository.dataSource.inTransaction { _ =>
          // on batchUpsert completion, re-retrieve the entity and return it
          repository.queries.getEntity(workspaceId, entityType, entityName) map {
            case Some(entityRec) => entityRec.toEntity
            case None            => throw new EntityNotFoundException()
          }
        }
      }
    }

  // ====================================================================================================
  //  helper methods
  // ====================================================================================================

  /** Given an entity, finds all references in that entity. Returns a set of RefMappings; the set will have one entry. */
  protected[compact] def findAllReferences(entity: Entity): Set[RefMapping] =
    findAllReferences(Seq(entity))

  /**
    * Given a Seq of entities, finds all references in those entities. Returns a set of RefMappings
    * representing all references. If the same entity is repeated multiple times in the input argument, only the last
    * occurrence of that entity is included in the response.
    */
  protected[compact] def findAllReferences(entities: Seq[Entity]): Set[RefMapping] = {
    // use a map here, generated by Seq.toMap, to automatically respect only the last occurrence
    // of any given entity
    val referenceMap: Map[EntityPointer, Seq[EntityPointer]] = entities
      .map { entity =>
        val referenceTargets =
          entity.attributes
            .collect {
              case (_, ref: AttributeEntityReference)         => Seq(ref.toPointer)
              case (_, refList: AttributeEntityReferenceList) => refList.list.map(_.toPointer)
            }
            .flatten
            .toSeq
        entity.toPointer -> referenceTargets
      }
      .filter(_._2.nonEmpty)
      .toMap
    referenceMap.map { case (key, value) => RefMapping(key, value.toSet) }.toSet
  }

  /**
    * Update the workspace's last-modified timestamp - in a separate transaction - upon successful
    * completion of a given Future.
    *
    * @param func the original function
    * @tparam T return type of original function
    */
  protected[compact] def withWorkspaceLastModified[T](func: Future[T]): Unit =
    func.foreach(_ =>
      repository.dataSource
        .inTransaction { _ =>
          repository.updateLastModified(workspaceId)
        }
        .recover { case t: Throwable =>
          logger.warn(
            s"Failed to update workspace last-modified timestamp. Workspace $workspaceId; message: ${t.getMessage}"
          )
        }
    )

}
