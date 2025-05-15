package org.broadinstitute.dsde.rawls.entities.compact.batch

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import akka.stream.scaladsl.{Sink, Source}
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.dataaccess.slick.{ReadWriteAction, RefPointers}
import org.broadinstitute.dsde.rawls.entities.compact.{
  CompactEntityProvider,
  CompactEntityProviderConfig,
  CompactEntityRepository,
  CompactEntitySerialization
}
import org.broadinstitute.dsde.rawls.model.AttributeUpdateOperations.EntityUpdateDefinition
import org.broadinstitute.dsde.rawls.model.{AttributeEntityReference, Entity, ErrorReport, RawlsRequestContext}
import org.broadinstitute.dsde.rawls.util.AttributeSupport
import slick.dbio.DBIO

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

    // Finally, run the Source.
    val dbResults: Future[Int] = dbResultsSource.runWith(Sink.head)

    dbResults
  }

  /** approximate the byte size of this entity by looking at the character length of its JSONized attributes */
  private def calculateEntitySize(entity: Entity): Int =
    CompactEntitySerialization.toSql(entity.attributes).compactPrint.length

  /** write this batch of Entity to the database */
  private def insertBatch(batch: Seq[Entity]): ReadWriteAction[Int] = {

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
      entitiesCreated <- repository.queries.batchCreateEntities(workspaceId, batch)

      // find all requested references within this batch
      allReferences = findAllReferences(batch)

      // generate a combined list of entity type/name pairs for both reference sources and targets
      lookupCriteria: Set[AttributeEntityReference] = allReferences.keys.toSet ++ allReferences.values.flatten.toSet

      // look up the ids for both reference sources and targets
      // TODO CORE-497: update
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
      // TODO CORE-497: update
      _ <- repository.queries.upsertReferences(referencesToInsert)
    } yield entitiesCreated
  }

}
