package org.broadinstitute.dsde.rawls.entities.compact

import akka.NotUsed
import akka.http.scaladsl.model.StatusCodes
import akka.stream.scaladsl.Source
import com.google.common.annotations.VisibleForTesting
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.dataaccess.slick.{CompactEntityRecord, EntityTypeAndCount, ReadWriteAction}
import org.broadinstitute.dsde.rawls.entities.base.ExpressionEvaluationSupport.LookupExpression
import org.broadinstitute.dsde.rawls.entities.base.{EntityProvider, ExpressionEvaluationContext, ExpressionValidator}
import org.broadinstitute.dsde.rawls.entities.compact.entityQuery._
import org.broadinstitute.dsde.rawls.entities.exceptions.{
  DataEntityException,
  DeleteEntitiesConflictException,
  DeleteEntitiesOfTypeConflictException,
  EntityNotFoundException,
  EntityReferenceNotFoundException
}
import org.broadinstitute.dsde.rawls.entities.{EntityRequestArguments, EntityUtils}
import org.broadinstitute.dsde.rawls.jobexec.MethodConfigResolver
import org.broadinstitute.dsde.rawls.model.AttributeUpdateOperations.EntityUpdateDefinition
import org.broadinstitute.dsde.rawls.model.{
  Attributable,
  AttributeEntityReference,
  AttributeEntityReferenceList,
  AttributeName,
  AttributeRename,
  AttributeUpdateOperations,
  AttributeValue,
  Entity,
  EntityCopyResponse,
  EntityQuery,
  EntityQueryResponse,
  EntityQueryResultMetadata,
  EntityTypeMetadata,
  EntityTypeRename,
  ErrorReport,
  RawlsRequestContext,
  SubmissionValidationEntityInputs,
  Workspace
}
import slick.dbio.DBIO
import slick.jdbc.ResultSetConcurrency.ReadOnly
import slick.jdbc.TransactionIsolation.ReadCommitted

import java.util
import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

/**
  * Implementation logic for compact data tables. Compact data tables store all an entity's attributes in a
  * single JSON packet within the database. Compare this to the implementation in LocalEntityProvider, which
  * requires at least one row per attribute.
  *
  * @param executionContext scala concurrency context
  */
class CompactEntityProvider(requestArguments: EntityRequestArguments, repository: CompactEntityRepository)(implicit
  protected val executionContext: ExecutionContext
) extends EntityProvider
    with LazyLogging {
  override def entityStoreId: Option[String] = None // unused

  val workspaceId: UUID = requestArguments.workspace.workspaceIdAsUUID // shorthand for methods below
  val workspaceContext = requestArguments.workspace

  override def batchUpdateEntities(
    entityUpdates: Source[EntityUpdateDefinition, _],
    parentContext: RawlsRequestContext
  ): Future[Int] = ???

  override def batchUpsertEntities(
    entityUpdates: Source[EntityUpdateDefinition, _],
    parentContext: RawlsRequestContext
  ): Future[Int] = ???

  override def copyEntities(sourceWorkspaceContext: Workspace,
                            destWorkspaceContext: Workspace,
                            entityType: String,
                            entityNames: Seq[String],
                            linkExistingEntities: Boolean,
                            parentContext: RawlsRequestContext
  ): Future[EntityCopyResponse] = ???

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
        refs: Map[AttributeName, Seq[AttributeEntityReference]] = findAllReferences(entity)
        // find all unique references in this entity
        uniqueRefs: Set[AttributeEntityReference] = refs.values.flatten.toSet
        // verify that all references in the entity-to-be-saved actually exist
        referencedIds <- repository.queries.getReferencedIds(workspaceId, uniqueRefs)
        _ = if (uniqueRefs.size != referencedIds.size)
          throw new EntityReferenceNotFoundException("Some entity references do not exist")
        // save the entity
        _ <- repository.queries.createEntity(workspaceId, entity)
        // did it save correctly? re-retrieve it. By re-retrieving it, we can 1) get its id, and 2) get the actual,
        // normalized JSON that was persisted to the db. When we return the entity to the user, we return the
        // normalized version.
        savedEntityRecordOption <- repository.queries.getEntity(workspaceId, entity.entityType, entity.name)
        savedEntityRecord = savedEntityRecordOption.getOrElse(throw new DataEntityException("Could not save entity"))
        // save all references from this entity to other entities
        _ <- replaceReferences(savedEntityRecord.id, referencedIds.toSet, isInsert = true)
      } yield savedEntityRecord.toEntity
    }
    // fire-and-forget an update to the workspace's last-modified date; no need to wait for it to complete
    withWorkspaceLastModified(createFuture)

    createFuture
  }

  override def deleteEntities(entityRefs: Seq[AttributeEntityReference],
                              parentContext: RawlsRequestContext
  ): Future[Int] =
    repository.dataSource.inTransaction { _ =>
      for {
        // check if any of these entities are referenced by someone else
        referencingEntities: Seq[AttributeEntityReference] <- repository.queries.getReferencesTo(workspaceId,
                                                                                                 entityRefs
        )
        // getReferencesTo already excludes the entities that are being deleted
        _ = if (referencingEntities.size != 0) {
          throw new DeleteEntitiesConflictException(referencingEntities.toSet)
        }
        // remove all references from these entities
        _ <- repository.queries.deleteAllReferencesFrom(workspaceId, entityRefs.toSet)
        res <- repository.queries.batchHide(workspaceId, entityRefs)
      } yield res
    }

  override def deleteEntitiesOfType(entityType: String, parentContext: RawlsRequestContext): Future[Int] =
    repository.dataSource.inTransaction { _ =>
      for {
        // check if any of these entities are referenced by someone else
        referencingEntities: Seq[AttributeEntityReference] <- repository.queries.getReferencesToType(workspaceId,
                                                                                                     entityType
        )
        // The getReferencesToType query already disregards references of the type to be deleted
        _ = if (referencingEntities.size > 0) {
          throw new DeleteEntitiesOfTypeConflictException(referencingEntities.size)
        }
        // remove all references from these entities
        _ <- repository.queries.deleteAllReferencesFromType(workspaceId, entityType)
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
        entityTypeAndKeys <- repository.queries.listEntityKeys(workspaceId)
        entityTypeAndCounts <- repository.queries.countEntitiesGroupedByType(workspaceId)
      } yield {
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

  override def listEntities(entityType: String): Source[Entity, NotUsed] =
    throw new DataEntityException("list all entities not supported for compact data tables.",
                                  code = StatusCodes.NotImplemented
    )

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
   * @param query criteria for filtering and paginating the result set
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
    (EntityQueryResultMetadata(unfilteredCount, filteredCountAndSource.count, pageCount),
     filteredCountAndSource.source.map(_.toEntity)
    )
  }

  private def countEntitiesOfType(entityType: LookupExpression) =
    repository.dataSource
      .inTransaction(ReadCommitted) { _ =>
        repository.queries.countEntities(workspaceId, entityType)
      }

  override def renameAttribute(entityType: String,
                               oldAttributeName: AttributeName,
                               attributeRenameRequest: AttributeRename,
                               parentContext: RawlsRequestContext
  ): Future[Int] = ???

  override def renameEntity(entityType: String,
                            entityName: String,
                            newName: String,
                            parentContext: RawlsRequestContext
  ): Future[Int] = ???

  override def renameEntityType(oldName: String,
                                renameInfo: EntityTypeRename,
                                parentContext: RawlsRequestContext
  ): Future[Int] = ???

  override def updateEntity(entityType: String,
                            entityName: String,
                            operations: Seq[AttributeUpdateOperations.AttributeUpdateOperation],
                            parentContext: RawlsRequestContext
  ): Future[Entity] = ???

  // ====================================================================================================
  //  helper methods
  // ====================================================================================================

  // given an entity, finds all references in that entity, grouped by their attribute names
  protected[compact] def findAllReferences(entity: Entity): Map[AttributeName, Seq[AttributeEntityReference]] =
    entity.attributes
      .collect {
        case (name: AttributeName, ref: AttributeEntityReference)         => Seq((name, ref))
        case (name: AttributeName, refList: AttributeEntityReferenceList) => refList.list.map(ref => (name, ref))
      }
      .flatten
      .toSeq
      .groupMap(_._1)(_._2)

  // given already-validated references, represented as target ids, update the ENTITY_REFS table for a given source
  // entity
  protected[compact] def replaceReferences(fromId: Long,
                                           toIds: Set[Long],
                                           isInsert: Boolean
  ): ReadWriteAction[(Int, Int)] = {
    // short-circuit
    if (isInsert && toIds.isEmpty) {
      DBIO.successful((0, 0))
    }

    for {
      // delete any reference pointers that should no longer exist
      deletes <-
        if (isInsert) {
          DBIO.successful(0)
        } else {
          repository.queries.deleteReferences(fromId, toIds)
        }
      // upsert all reference pointers that do exist
      upserts <-
        if (toIds.isEmpty) {
          DBIO.successful(0)
        } else {
          repository.queries.upsertReferences(fromId, toIds)
        }
    } yield (deletes, upserts)
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
