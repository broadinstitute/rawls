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
import org.broadinstitute.dsde.rawls.entities.compact.{
  CompactEntityProvider,
  CompactEntityProviderConfig,
  CompactEntityRepository
}
import org.broadinstitute.dsde.rawls.entities.exceptions.{EntityNotFoundException, EntityReferenceNotFoundException}
import org.broadinstitute.dsde.rawls.model.AttributeUpdateOperations.EntityUpdateDefinition
import org.broadinstitute.dsde.rawls.model.{AttributeEntityReference, Entity, RawlsRequestContext}
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
    */
  def handleUpdates(entityUpdates: Source[EntityUpdateDefinition, _],
                    allowUpsert: Boolean,
                    config: CompactEntityProviderConfig,
                    parentContext: RawlsRequestContext
  ): Future[Int] = {

    // Group the updates into batches for efficiency
    val batchedUpdates: Source[Seq[EntityUpdateDefinition], _] = entityUpdates.grouped(config.batchUpsertBatchSize)

    // Apply the incoming operations to a pre-existing entity (for updates) or a blank entity (for inserts)
    // and create the DBIO actions to persist the results.
    val batchActionsSource: Source[ReadWriteAction[Int], _] =
      batchedUpdates.via(flowOperationsToEntities(allowUpsert))

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
    * apply the updates to those entities, then persist those entities. Emits the count of rows written as a DBIO. */
  private def flowOperationsToEntities(
    allowUpsert: Boolean
  ): Flow[Seq[EntityUpdateDefinition], ReadWriteAction[Int], _] =
    Flow[Seq[EntityUpdateDefinition]].map { updates =>
      // Extract the entity type and name from each update
      val updateIdentifiers = updates.map(update => AttributeEntityReference(update.entityType, update.name))

      for {
        // Query the database for any pre-existing entities being updated
        existingEntities <- repository.queries.getEntities(workspaceId, updateIdentifiers.toSet)
        // If this invocation does NOT allow upserts, validate that we found all entities being updated
        _ = if (!allowUpsert && existingEntities.size != updateIdentifiers.size) {
          throw new EntityNotFoundException()
        }

        // Massage the existing entities so they're easier to look up later
        existingEntitiesByIdentifier = existingEntities
          .map(rec => rec.toAttributeEntityReference -> rec.toEntity)
          .toMap

        // How many updates do we have for each entity being updated?
        updateCounts = updateIdentifiers
          .groupBy(identity)
          .map { case (updateIdentifier, updateDefinitions) =>
            updateIdentifier -> updateDefinitions.size
          }
        // what are the existing record_versions?
        existingVersionsByIdentifier = existingEntities
          .map(rec => rec.toAttributeEntityReference -> rec.recordVersion)
          .toMap
        // Increment the existing record_version values with the number of updates for each entity.
        // This gives us the final record_version we should expect for each entity.
        expectedRecordVersions: Map[AttributeEntityReference, Long] = updateCounts.map {
          case (identifier, updateCount) =>
            val existingVersion = existingVersionsByIdentifier.getOrElse(identifier, -1L)
            identifier -> (existingVersion + updateCount)
        }

        // Apply the incoming operations to the existing entities (or to an empty entity if none pre-existed)
        updatedEntities = applyAll(updates, existingEntitiesByIdentifier)

        // Persist the updated entities to the database
        writeCount <- insertBatch(updatedEntities, allowUpsert)

        // Re-retrieve the entities we just wrote. This gets the actual record_version values.
        finalRecordVersions <- repository.queries.getEntityVersions(workspaceId, updateIdentifiers.toSet)

        // Compare the actual record versions, after writing the entities, to the expected record versions
        _ = finalRecordVersions.foreach { rec =>
          val expected = expectedRecordVersions.getOrElse(rec.toAttributeEntityReference, 0)
          val actual = rec.recordVersion
          if (actual != expected) {
            throw new RawlsConcurrentModificationException(
              s"Detected concurrent modifications to entity ${rec.toAttributeEntityReference.entityType}/${rec.toAttributeEntityReference.entityName}. " +
                s"Expected $expected; got $actual." +
                "Please retry this operation."
            )
          }
        }

      } yield writeCount
    }

  /** Helper method to apply batch operations to pre-existing entities.
    * This is a recursive call to handle the case where multiple subsequent operations modify
    * the same pre-existing entity. */
  @VisibleForTesting
  def applyAll(updates: Seq[EntityUpdateDefinition],
               existingEntitiesByIdentifier: Map[AttributeEntityReference, Entity]
  ): Seq[Entity] = {

    @tailrec
    def applyOne(updates: Seq[EntityUpdateDefinition],
                 existingEntitiesByIdentifier: Map[AttributeEntityReference, Entity],
                 accum: Seq[Entity]
    ): Seq[Entity] =
      if (updates.isEmpty) {
        //  end of updates; return the accumulator
        accum
      } else {
        val thisUpdate = updates.head
        val thisBaseEntity = existingEntitiesByIdentifier.getOrElse(
          AttributeEntityReference(thisUpdate.entityType, thisUpdate.name),
          Entity(thisUpdate.name, thisUpdate.entityType, Map())
        )

        val updatedEntity = applyOperationsToEntity(thisBaseEntity, thisUpdate.operations)

        applyOne(updates.tail,
                 existingEntitiesByIdentifier + (updatedEntity.toReference -> updatedEntity),
                 accum :+ updatedEntity
        )

      }

    applyOne(updates, existingEntitiesByIdentifier, Seq())

  }

  /** write this batch of Entity to the database */
  private def insertBatch(batch: Seq[Entity], allowUpsert: Boolean): ReadWriteAction[Int] =

    for {
      // Batch insert to ENTITY table. Save the whole batch first to handle cases where an entity in this batch
      // has a reference to another entity in the same batch.
      entitiesCreated <- repository.queries.batchCreateEntities(workspaceId, batch, allowUpsert = allowUpsert)

      // Find all requested references within this batch
      allReferences: Set[RefMapping] = findAllReferences(batch)

      // verify all requested references exist
      allExist <- repository.queries.existsAll(workspaceId, allReferences.flatMap(_.to))

      // did we find all the reference sources and targets?
      _ = if (!allExist)
        throw new EntityReferenceNotFoundException("Some entity references do not exist")

      _ <- repository.queries.deleteAllReferencesFrom(workspaceId, batch.map(_.toReference).toSet)
      _ <- repository.queries.insertReferences(workspaceId, allReferences)
    } yield entitiesCreated

}
