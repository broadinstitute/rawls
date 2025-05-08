package org.broadinstitute.dsde.rawls.entities.compact

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import akka.stream.scaladsl.{Sink, Source}
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.dataaccess.slick.{EntityTypeAndCount, ReadWriteAction, RefPointers}
import org.broadinstitute.dsde.rawls.entities.base.ExpressionEvaluationSupport.LookupExpression
import org.broadinstitute.dsde.rawls.entities.base.{EntityProvider, ExpressionEvaluationContext, ExpressionValidator}
import org.broadinstitute.dsde.rawls.entities.exceptions.{
  DataEntityException,
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
import org.broadinstitute.dsde.rawls.util.AttributeSupport
import slick.dbio.DBIO
import slick.jdbc.ResultSetConcurrency.ReadOnly

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
class CompactEntityProvider(requestArguments: EntityRequestArguments,
                            repository: CompactEntityRepository,
                            config: CompactEntityProviderConfig = CompactEntityProviderConfig()
)(implicit
  protected val executionContext: ExecutionContext,
  actorSystem: ActorSystem
) extends EntityProvider
    with AttributeSupport
    with LazyLogging {
  override def entityStoreId: Option[String] = None // unused

  val workspaceId: UUID = requestArguments.workspace.workspaceIdAsUUID // shorthand for methods below

  override def batchUpdateEntities(
    entityUpdates: Source[EntityUpdateDefinition, _],
    parentContext: RawlsRequestContext
  ): Future[Int] = ???

  override def batchUpsertEntities(
    entityUpdates: Source[EntityUpdateDefinition, _],
    parentContext: RawlsRequestContext
  ): Future[Int] = {

    // Translate the input stream entityUpdates to a stream of Entity by applying the updates to
    // a pre-existing entity (for updates) or a blank entity (for inserts)
    val entitySource: Source[Entity, _] = entityUpdates.map { updateDefinition =>
      // TODO CORE-428: for updates, look ahead (some quantity) in the update definitions and
      //   retrieve the existing entities from the db
      // for inserts, start with an empty entity
      val baseEntity = Entity(updateDefinition.name, updateDefinition.entityType, Map())
      // update the starting entity with the user's operations
      applyOperationsToEntity(baseEntity, updateDefinition.operations)
    }

    // Group the entities-to-be-saved into batches to optimize our SQL interactions
    val batches: Source[Seq[Entity], _] = entitySource.groupedWeighted(config.maxSqlBatchSizeBytes)(calculateEntitySize)

    // For each batch, generate the db action to write it to the database
    val batchActionsSource: Source[ReadWriteAction[Int], _] = batches
      .map { batch =>
        logger.info(s"batch upsert: batch of ${batch.size} entities")
        insertBatch(batch)
      }

    // Materialize the batch actions into a sequence and convert to a single DBIO action
    val batchActionsF: Future[ReadWriteAction[Int]] = batchActionsSource
      .runWith(Sink.seq)
      .map(DBIO.sequence(_).map(_.sum))

    // Convert the Future[ReadWriteAction] to a Source[Int] by executing the db actions.
    // Note that since all the ReadWriteActions are fused via DBIO.sequence above,
    // there is only one action to execute inside a transaction.
    val dbResultsSource: Source[Int, _] = Source.futureSource(batchActionsF.map { dbAction =>
      Source.future(repository.dataSource.inTransaction(_ => dbAction))
    })

    // Finally, run the Source. Ignore the results from the db; they are not necessary.
    val dbResults: Future[Int] = dbResultsSource.runWith(Sink.head)

    // Fire-and-forget an update to the workspace's last-modified date; no need to wait for it to complete
    withWorkspaceLastModified(dbResults)

    // and return
    dbResults
  }

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
        refs: Map[AttributeEntityReference, Seq[AttributeEntityReference]] = findAllReferences(entity)
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
  ): Future[Int] = ???

  override def deleteEntitiesOfType(entityType: String, parentContext: RawlsRequestContext): Future[Int] = ???

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

  override def queryEntitiesSource(entityType: String,
                                   query: EntityQuery,
                                   parentContext: RawlsRequestContext
  ): Future[(EntityQueryResultMetadata, Source[Entity, _])] = ???

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

  // Given an entity, finds all references in that entity. Returns a map of source entity -> target entities
  protected[compact] def findAllReferences(
    entity: Entity
  ): Map[AttributeEntityReference, Seq[AttributeEntityReference]] =
    findAllReferences(Seq(entity))

  // Given a Seq of entities, finds all references in those entities. Returns a map of source entity -> target entities
  // representing all references.
  protected[compact] def findAllReferences(
    entities: Seq[Entity]
  ): Map[AttributeEntityReference, Seq[AttributeEntityReference]] =
    entities
      .map { entity =>
        val referenceTargets =
          entity.attributes
            .collect {
              case (_, ref: AttributeEntityReference)         => Seq(ref)
              case (_, refList: AttributeEntityReferenceList) => refList.list
            }
            .flatten
            .toSeq
        entity.toReference -> referenceTargets
      }
      .filter(_._2.nonEmpty)
      .toMap

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
          repository.queries.deleteReferencesWithFilter(fromId, toIds)
        }
      // upsert all reference pointers that do exist
      upserts <-
        if (toIds.isEmpty) {
          DBIO.successful(0)
        } else {
          repository.queries.upsertReferences(Set(RefPointers(fromId, toIds)))
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

  // approximate the byte size of this entity by looking at the character length of its JSONized attributes
  private def calculateEntitySize(entity: Entity): Int =
    CompactEntitySerialization.toSql(entity.attributes).compactPrint.length

  private def insertBatch(batch: Seq[Entity]): ReadWriteAction[Int] =
    for {
      // batch insert to ENTITY table. Save the whole batch first to handle cases where an entity in this batch
      // has a reference to another entity in the same batch.
      entitiesCreated <- repository.queries.batchCreateEntities(workspaceId, batch)

      // find all requested references within this batch
      allReferences = findAllReferences(batch)

      // generate a combined list of entity type/name pairs for both reference sources and targets
      lookupCriteria: Set[AttributeEntityReference] = allReferences.keys.toSet ++ allReferences.values.flatten.toSet

      // look up the ids for both reference sources and targets
      foundIds <- repository.queries.getEntityRefs(workspaceId, lookupCriteria)

      // did we find all the reference sources and targets?
      _ = if (foundIds.size != lookupCriteria.size) {
        // here's what the query actually returned; turn this into a Set
        val actuallyFound = foundIds.map(_.toAttributeEntityReference).toSet

        // did we find all the reference targets?
        val notFoundReferenceTargets = allReferences.values.flatten.toSet diff actuallyFound
        if (notFoundReferenceTargets.nonEmpty)
          throw new RawlsExceptionWithErrorReport(
            ErrorReport(
              StatusCodes.BadRequest,
              "Could not resolve some entity references",
              notFoundReferenceTargets.map { missingRef =>
                ErrorReport(s"${missingRef.entityType} ${missingRef.entityName} not found", Seq.empty)
              }.toSeq
            )
          )

        // did we find all the reference sources? This should never happen, but let's be defensive
        val notFoundReferenceSources = allReferences.keys.toSet diff actuallyFound
        if (notFoundReferenceSources.nonEmpty)
          throw new RawlsExceptionWithErrorReport(
            ErrorReport(
              StatusCodes.BadRequest,
              "Could not resolve some entity reference sources",
              notFoundReferenceSources.map { missingRef =>
                ErrorReport(s"${missingRef.entityType} ${missingRef.entityName} not found", Seq.empty)
              }.toSeq
            )
          )
      }

      // build a lookup table for the ids we found
      idLookup: Map[AttributeEntityReference, Long] = foundIds.map { rec =>
        rec.toAttributeEntityReference -> rec.id
      }.toMap

      // rehydrate the looked-up ids into sources and targets (from_id, to_id)
      referencesToInsert: Set[RefPointers] = allReferences.map { case (from, tos) =>
        val toIds = tos.map(idLookup).toSet
        RefPointers(idLookup(from), toIds)
      }.toSet
      // insert the references into the ENTITY_REFS table.
      _ <- repository.queries.upsertReferences(referencesToInsert)
    } yield entitiesCreated

}
