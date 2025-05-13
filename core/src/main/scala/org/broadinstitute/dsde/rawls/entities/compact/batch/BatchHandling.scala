package org.broadinstitute.dsde.rawls.entities.compact.batch

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import akka.stream.scaladsl.{Flow, Sink, Source}
import com.google.common.annotations.VisibleForTesting
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.dataaccess.slick.{CompactEntityRecord, ReadWriteAction, RefPointers}
import org.broadinstitute.dsde.rawls.entities.compact.{
  CompactEntityProvider,
  CompactEntityProviderConfig,
  CompactEntityRepository,
  CompactEntitySerialization
}
import org.broadinstitute.dsde.rawls.entities.exceptions.EntityNotFoundException
import org.broadinstitute.dsde.rawls.model.AttributeUpdateOperations.EntityUpdateDefinition
import org.broadinstitute.dsde.rawls.model.{AttributeEntityReference, Entity, ErrorReport, RawlsRequestContext}
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
    val updatedEntities: Source[ReadWriteAction[Seq[Entity]], _] =
      batchedUpdates.via(flowOperationsToEntities(allowUpsert))

    // For each batch, generate the db action to write it to the database
    val batchActionsSource: Source[ReadWriteAction[Int], _] = updatedEntities
      .map { batchAction =>
        for {
          batch <- batchAction
          writeCount <- insertBatch(batch, allowUpsert)
          // TODO CORE-428: re-retrieve the saved entities and compare their actual record_version
          //   against their expected record_version
        } yield {
          logger.info(s"batch upsert: batch of $writeCount entities")
          writeCount
        }
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

    // Finally, run the Source.
    val dbResults: Future[Int] = dbResultsSource.runWith(Sink.head)

    dbResults
  }

  /** Stream component to accept a seq of batch updates, look for any pre-existing entities targeted by those updates,
    * then apply the updates to those entities. Emits the entities, with operations applied, as DBIOs. */
  private def flowOperationsToEntities(
    allowUpsert: Boolean
  ): Flow[Seq[EntityUpdateDefinition], ReadWriteAction[Seq[Entity]], _] =
    Flow[Seq[EntityUpdateDefinition]].map { updates =>
      // extract the entity type and name from each update
      val updateIdentifiers = updates.map(update => AttributeEntityReference(update.entityType, update.name))
      // how many updates do we have for each entity being updated?
      val updateCounts = updateIdentifiers
        .groupBy(identity)
        .map { case (updateIdentifier, updateDefinitions) =>
          updateIdentifier -> updateDefinitions.size
        }

      for {
        // query the database for any pre-existing entities being updated
        existingEntities <- repository.queries.getEntities(workspaceId, updateIdentifiers.toSet)
        // if this invocation does NOT allow upserts, validate that we found all entities being updated
        _ = if (!allowUpsert && existingEntities.size != updateIdentifiers.size) {
          throw new EntityNotFoundException()
        }
        // TODO CORE-428: save a data structure of the expected record_versions for each entity
        // massage the existing entities so they're easier to look up later
        existingEntitiesByIdentifier = existingEntities
          .map(rec => rec.toAttributeEntityReference -> rec.toEntity)
          .toMap
        // apply the incoming operations to the existing entities (or to an empty entity if none pre-existed)
        updatedEntities = applyAll(updates, existingEntitiesByIdentifier)

      } yield updatedEntities
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
  private def insertBatch(batch: Seq[Entity], allowUpsert: Boolean): ReadWriteAction[Int] = {

    // nested helper method for building error responses
    def generateReferenceError(message: String, notFounds: Set[AttributeEntityReference]) =
      new RawlsExceptionWithErrorReport(
        ErrorReport(
          StatusCodes.BadRequest,
          message,
          notFounds.map { notFound =>
            ErrorReport(s"${notFound.entityType} ${notFound.entityName} not found", Seq.empty)
          }.toSeq
        )
      )

    for {
      // Batch insert to ENTITY table. Save the whole batch first to handle cases where an entity in this batch
      // has a reference to another entity in the same batch.
      entitiesCreated <- repository.queries.batchCreateEntities(workspaceId, batch, allowUpsert = allowUpsert)

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
        if (notFoundReferenceTargets.nonEmpty) {
          throw generateReferenceError("Could not resolve some entity references", notFoundReferenceTargets)
        }
        // did we find all the reference sources? This should never happen, but let's be defensive
        val notFoundReferenceSources = allReferences.keys.toSet diff actuallyFound
        if (notFoundReferenceSources.nonEmpty)
          throw generateReferenceError("Could not resolve some entity sources", notFoundReferenceSources)
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

}
