package org.broadinstitute.dsde.rawls.entities.compact.batch

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Flow, Sink, Source}
import com.google.common.annotations.VisibleForTesting
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.dataaccess.slick.{
  RawlsConcurrentModificationException,
  ReadWriteAction,
  RefMapping
}
import org.broadinstitute.dsde.rawls.entities.EntityUtils
import org.broadinstitute.dsde.rawls.entities.compact.{
  CompactEntityProvider,
  CompactEntityProviderConfig,
  CompactEntityRepository
}
import org.broadinstitute.dsde.rawls.entities.exceptions.{EntityNotFoundException, EntityReferenceNotFoundException}
import org.broadinstitute.dsde.rawls.model.AttributeUpdateOperations.EntityUpdateDefinition
import org.broadinstitute.dsde.rawls.model.{AttributeName, Entity, EntityPointer, RawlsRequestContext}
import org.broadinstitute.dsde.rawls.util.AttributeSupport
import slick.dbio.DBIO

import scala.annotation.tailrec
import scala.concurrent.Future

/**
  * Support for batchUpsert/batchUpdate; to be mixed in to CompactEntityProvider
  */
trait BatchHandling extends LazyLogging with AttributeSupport {

  // this trait is only extended by CompactEntityProvider
  this: CompactEntityProvider =>

  implicit val actorSystem: ActorSystem
  val repository: CompactEntityRepository

  /**
    * process the incoming EntityUpdateDefinitions and persist to the database
    *
    * @param entityUpdates the entity operations to process
    * @param allowInsert are both inserts and updates allowed? When false, only updates are allowed.
    * @param config options for processing the operations
    * @param parentContext parent tracing span and userinfo
    */
  def handleUpdates(entityUpdates: Source[EntityUpdateDefinition, _],
                    allowInsert: Boolean,
                    config: CompactEntityProviderConfig,
                    parentContext: RawlsRequestContext
  ): Future[Int] = {

    // Group the updates into batches for efficiency
    val batchedUpdates: Source[Seq[EntityUpdateDefinition], _] = entityUpdates.grouped(config.batchUpsertBatchSize)

    // Apply the incoming operations to a pre-existing entity (for updates) or a blank entity (for inserts)
    // and create the DBIO actions to persist the results.
    val batchActionsSource: Source[ReadWriteAction[Int], _] =
      batchedUpdates.via(flowOperationsToEntities(allowInsert))

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

    // Finally, run the Source.
    val dbResults: Future[Int] = dbResultsSource.runWith(Sink.head)

    dbResults
  }

  /** Stream component to accept a seq of batch updates, look for any pre-existing entities targeted by those updates,
    * apply the updates to those entities, then persist those entities. Emits the count of rows written as a DBIO.
    *
    * @param allowInsert are both inserts and updates allowed? When false, only updates are allowed.
    */
  private def flowOperationsToEntities(
    allowInsert: Boolean
  ): Flow[Seq[EntityUpdateDefinition], ReadWriteAction[Int], _] =
    Flow[Seq[EntityUpdateDefinition]].map { updates =>
      // validate entity type, entity name, and attribute names
      updates foreach { update =>
        EntityUtils.validateEntityType(update.entityType)
        EntityUtils.validateEntityName(update.name)
      }

      val attributeNamesToCheck: Seq[AttributeName] = for {
        update <- updates
        operation <- update.operations
      } yield operation.name
      // noop function to validate attribute names
      withAttributeNamespaceCheck(attributeNamesToCheck) {}

      // Extract the entity type and name from each update
      val updateIdentifiers = updates.map(update => EntityPointer(update.entityType, update.name))
      val uniqueUpdateIdentifiers = updateIdentifiers.toSet

      for {
        // Query the database for any pre-existing entities being updated
        existingEntities <- repository.queries.getEntities(workspaceId, uniqueUpdateIdentifiers)
        // If this invocation does NOT allow inserts, validate that we found all entities being updated
        _ = if (!allowInsert) {
          val actualPointers = existingEntities.map(_.toPointer).toSet
          if (
            existingEntities.size != uniqueUpdateIdentifiers.size || (uniqueUpdateIdentifiers diff actualPointers).nonEmpty
          )
            throw new EntityNotFoundException(
              s"expected ${uniqueUpdateIdentifiers.size} entities to be updated, but found ${existingEntities.size}"
            )
        }

        // Massage the existing entities so they're easier to look up later
        existingEntitiesByIdentifier = existingEntities
          .map(rec => rec.toPointer -> rec.toEntity)
          .toMap

        // Apply the incoming operations to the existing entities (or to an empty entity if none pre-existed).
        // This skips unchanged entities
        updatedEntities = applyAll(updates, existingEntitiesByIdentifier)

        // How many updates do we have for each entity being updated?
        updateCounts = updatedEntities
          .groupBy(_.toPointer)
          .map { case (updateIdentifier, updates) =>
            updateIdentifier -> updates.size
          }
        // what are the existing record_versions?
        existingVersionsByIdentifier = existingEntities
          .map(rec => rec.toPointer -> rec.recordVersion)
          .toMap
        // Increment the existing record_version values with the number of updates for each entity.
        // This gives us the final record_version we should expect for each entity.
        expectedRecordVersions: Map[EntityPointer, Long] = updateCounts.map { case (identifier, updateCount) =>
          val existingVersion = existingVersionsByIdentifier.getOrElse(identifier, -1L)
          identifier -> (existingVersion + updateCount)
        }

        // Persist the updated entities to the database
        _ <- insertOrUpdateBatch(updatedEntities)

        // insertOrUpdateBatch uses an `insert into ... on duplicate key update` statement.
        // MySQL behavior is to return 1 if a row was inserted and 2 if it was updated.
        // So, to return the actual number of entities written, we need to count the number of unique pointers.
        writeCount = updatedEntities.map(_.toPointer).toSet.size

        // Re-retrieve the entities we just wrote. This gets the actual record_version values.
        finalRecordVersions <- repository.queries.getEntityVersions(workspaceId, updatedEntities.map(_.toPointer).toSet)

        // Compare the actual record versions, after writing the entities, to the expected record versions
        _ = finalRecordVersions.foreach { rec =>
          val expected = expectedRecordVersions.getOrElse(rec.toPointer, 0)
          val actual = rec.recordVersion
          if (actual != expected) {
            throw new RawlsConcurrentModificationException(
              s"Detected concurrent modifications to entity ${rec.toAttributeEntityReference.entityType}/${rec.toAttributeEntityReference.entityName}. " +
                s"Expected $expected; got $actual." +
                "Please retry this operation."
            )
          }
        }

        // invalidate the attribute keys cache for all entity types in this batch
        _ <- repository.queries.invalidateCache(workspaceId, updatedEntities.map(_.entityType).toSet)

      } yield writeCount
    }

  /** Helper method to apply batch operations to pre-existing entities.
    * This is a recursive call to handle the case where multiple subsequent operations modify
    * the same pre-existing entity. */
  @VisibleForTesting
  def applyAll(updates: Seq[EntityUpdateDefinition],
               existingEntitiesByIdentifier: Map[EntityPointer, Entity]
  ): Seq[Entity] = {

    @tailrec
    def applyOne(updates: Seq[EntityUpdateDefinition],
                 existingEntitiesByIdentifier: Map[EntityPointer, Entity],
                 accum: Seq[Entity]
    ): Seq[Entity] =
      if (updates.isEmpty) {
        // end of updates.
        // now that everything has been applied, de-duplicate the entities.
        // If the same entity appears multiple times in our update, take the last one.
        accum
          .groupBy(_.toPointer)
          .values
          .map(_.last)
          .toSeq
      } else {
        val thisUpdate = updates.head

        val thisUpdatePointer = EntityPointer(thisUpdate.entityType, thisUpdate.name)

        val isPreExisting = existingEntitiesByIdentifier.contains(thisUpdatePointer)

        val thisBaseEntity =
          existingEntitiesByIdentifier.getOrElse(
            thisUpdatePointer,
            Entity(thisUpdate.name, thisUpdate.entityType, Map())
          )

        val updatedEntity = applyOperationsToEntity(thisBaseEntity, thisUpdate.operations)

        // If the entity has not changed and already exists, we can skip it.
        val newAccum = if (isPreExisting && thisBaseEntity == updatedEntity) {
          accum
        } else {
          accum :+ updatedEntity
        }

        applyOne(updates.tail, existingEntitiesByIdentifier + (updatedEntity.toPointer -> updatedEntity), newAccum)
      }

    applyOne(updates, existingEntitiesByIdentifier, Seq())

  }

  /** write this batch of Entity to the database */
  private def insertOrUpdateBatch(batch: Seq[Entity]): ReadWriteAction[Int] =

    for {
      // Batch insert to ENTITY table. Save the whole batch first to handle cases where an entity in this batch
      // has a reference to another entity in the same batch.
      entitiesWritten <- repository.queries.batchWriteEntities(workspaceId, batch, insertOnly = false)

      // Find all requested references within this batch
      allReferences: Set[RefMapping] = findAllReferences(batch)

      // verify all requested references exist
      allExist <-
        if (allReferences.nonEmpty)
          repository.queries.existsAll(workspaceId, allReferences.flatMap(_.to))
        else
          DBIO.successful(true)

      // did we find all the reference sources and targets?
      _ = if (!allExist)
        throw new EntityReferenceNotFoundException("Some entity references do not exist")

    } yield entitiesWritten

}
