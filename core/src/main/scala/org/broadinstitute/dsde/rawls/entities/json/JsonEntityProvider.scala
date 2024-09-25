package org.broadinstitute.dsde.rawls.entities.json

import akka.stream.scaladsl.Source
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.dataaccess.SlickDataSource
import org.broadinstitute.dsde.rawls.entities.EntityRequestArguments
import org.broadinstitute.dsde.rawls.entities.base.ExpressionEvaluationSupport.LookupExpression
import org.broadinstitute.dsde.rawls.entities.base.{EntityProvider, ExpressionEvaluationContext, ExpressionValidator}
import org.broadinstitute.dsde.rawls.jobexec.MethodConfigResolver
import org.broadinstitute.dsde.rawls.model.Attributable.{entityIdAttributeSuffix, workspaceIdAttribute, AttributeMap}
import org.broadinstitute.dsde.rawls.model.{
  AttributeEntityReference,
  AttributeEntityReferenceList,
  AttributeName,
  AttributeUpdateOperations,
  AttributeValue,
  Entity,
  EntityCopyResponse,
  EntityQuery,
  EntityQueryResponse,
  EntityQueryResultMetadata,
  EntityTypeMetadata,
  RawlsRequestContext,
  SubmissionValidationEntityInputs,
  Workspace
}
import spray.json._
import DefaultJsonProtocol._
import akka.http.scaladsl.model.StatusCodes
import bio.terra.common.exception.NotImplementedException
import io.opentelemetry.api.common.AttributeKey
import org.apache.commons.lang3.time.StopWatch
import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.dataaccess.slick.{
  DataAccess,
  JsonEntityRecord,
  JsonEntityRefRecord,
  JsonEntitySlickRecord,
  ReadAction,
  RefPointerRecord
}
import org.broadinstitute.dsde.rawls.entities.exceptions.{DataEntityException, DeleteEntitiesConflictException}
import org.broadinstitute.dsde.rawls.model.AttributeUpdateOperations.EntityUpdateDefinition
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._
import org.broadinstitute.dsde.rawls.util.AttributeSupport
import org.broadinstitute.dsde.rawls.util.TracingUtils.{
  setTraceSpanAttribute,
  traceDBIOWithParent,
  traceFutureWithParent
}
import slick.dbio.DBIO
import slick.jdbc.TransactionIsolation

import java.time.Duration
import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Try}

// TODO AJ-2008:
//    - tracing
//    - mark transactions as read-only where possible (does this actually help?)
//    - error-handling
//    - logging
class JsonEntityProvider(requestArguments: EntityRequestArguments,
                         implicit protected val dataSource: SlickDataSource,
                         cacheEnabled: Boolean,
                         queryTimeout: Duration,
                         val workbenchMetricBaseName: String
)(implicit protected val executionContext: ExecutionContext)
    extends EntityProvider
    with AttributeSupport
    with LazyLogging {

  override def entityStoreId: Option[String] = None

  val workspaceId: UUID = requestArguments.workspace.workspaceIdAsUUID // shorthand for methods below

  /**
    * Insert a single entity to the db
    */
  override def createEntity(entity: Entity): Future[Entity] =
    dataSource.inTransaction { dataAccess =>
      for {
        // find and validate all references in the entity-to-be-saved
        referenceTargets <- DBIO.from(validateReferences(entity))

        // save the entity
        _ <- dataAccess.jsonEntityQuery.createEntity(workspaceId, entity)
        // did it save correctly? re-retrieve it. By re-retrieving it, we can 1) get its id, and 2) get the actual,
        // normalized JSON that was persisted to the db. When we return the entity to the user, we return the
        // normalized version.
        savedEntityRecordOption <- dataAccess.jsonEntityQuery.getEntity(workspaceId, entity.entityType, entity.name)
        savedEntityRecord = savedEntityRecordOption.getOrElse(throw new RuntimeException("Could not save entity"))
        // save all references from this entity to other entities
        _ <- DBIO.from(replaceReferences(savedEntityRecord.id, referenceTargets, isInsert = true))
      } yield savedEntityRecord.toEntity
    }

  /**
    * Read a single entity from the db
    */
  override def getEntity(entityType: String, entityName: String): Future[Entity] = dataSource.inTransaction {
    dataAccess =>
      dataAccess.jsonEntityQuery.getEntity(workspaceId, entityType, entityName)
  } map { result => result.map(_.toEntity).get }

  /**
    * Soft-delete specified entities
    */
  override def deleteEntities(entityRefs: Seq[AttributeEntityReference]): Future[Int] = {
    val deleteTargets = entityRefs.toSet
    dataSource.inTransaction { dataAccess =>
      dataAccess.jsonEntityQuery.getRecursiveReferrers(workspaceId, deleteTargets)
    } flatMap { referrers =>
      val referringSet = referrers.map(x => AttributeEntityReference(x.entityType, x.name)).toSet
      if (referringSet != deleteTargets)
        throw new DeleteEntitiesConflictException(referringSet)
      else
        dataSource.inTransaction { dataAccess =>
          dataAccess.jsonEntityQuery.softDelete(workspaceId, deleteTargets)
        }
    }
  }

  /**
    * Return type/count/attribute metadata
    * TODO AJ-2008: assess performance and add caching if necessary
    */
  override def entityTypeMetadata(useCache: Boolean): Future[Map[String, EntityTypeMetadata]] = {

    def attrsV1(dataAccess: DataAccess) = {
      val stopwatch = StopWatch.createStarted()
      dataAccess.jsonEntityQuery.typesAndAttributes(workspaceId) map { result =>
        stopwatch.stop()
        logger.info(s"***** attrsV1 complete in ${stopwatch.getTime}ms")
        result
      }
    }

    def attrsV2(dataAccess: DataAccess) = {
      val stopwatch = StopWatch.createStarted()
      dataAccess.jsonEntityQuery.typesAndAttributesV2(workspaceId) map { result =>
        stopwatch.stop()
        logger.info(s"***** attrsV2 complete in ${stopwatch.getTime}ms")
        result
      }
    }

    val stopwatch = StopWatch.create()
    dataSource.inTransaction { dataAccess =>
      // get the types and counts
      for {
        typesAndCounts <- dataAccess.jsonEntityQuery.typesAndCounts(workspaceId)
        typesAndAttributes <- attrsV1(dataAccess)
        typesAndAttributesV2 <- attrsV2(dataAccess)
      } yield {
        // group attribute names by entity type
        val groupedAttributeNames: Map[String, Seq[String]] =
          typesAndAttributes
            .groupMap(_._1)(_._2)

        // loop through the types and counts and build the EntityTypeMetadata
        typesAndCounts.map { case (entityType: String, count: Int) =>
          // grab attribute names
          val attrNames = groupedAttributeNames.getOrElse(entityType, Seq())
          val metadata = EntityTypeMetadata(count, s"$entityType$entityIdAttributeSuffix", attrNames)
          (entityType, metadata)
        }.toMap
      }
    }
  }

  /**
    * stream a page of entities
    */
  override def queryEntitiesSource(entityType: String,
                                   entityQuery: EntityQuery,
                                   parentContext: RawlsRequestContext
  ): Future[(EntityQueryResultMetadata, Source[Entity, _])] =
    queryEntities(entityType, entityQuery, parentContext).map { queryResponse =>
      // TODO AJ-2008: actually stream!
      (queryResponse.resultMetadata, Source.apply(queryResponse.results))
    }

  /**
    * return a page of entities
    */
  override def queryEntities(entityType: String,
                             entityQuery: EntityQuery,
                             parentContext: RawlsRequestContext
  ): Future[EntityQueryResponse] = dataSource.inTransaction { dataAccess =>
    for {
      results <- dataAccess.jsonEntityQuery.queryEntities(workspaceId, entityType, entityQuery)
      // TODO AJ-2008: optimize; if no filters are present, don't need separate queries for counts
      unfilteredCount <- dataAccess.jsonEntityQuery.countType(workspaceId, entityType)
      filteredCount <- dataAccess.jsonEntityQuery.countQuery(workspaceId, entityType, entityQuery)
    } yield {
      val pageCount: Int = Math.ceil(filteredCount.toFloat / entityQuery.pageSize).toInt
      if (filteredCount > 0 && entityQuery.page > pageCount) {
        throw new DataEntityException(
          code = StatusCodes.BadRequest,
          message = s"requested page ${entityQuery.page} is greater than the number of pages $pageCount"
        )
      }
      val queryMetadata = EntityQueryResultMetadata(unfilteredCount, filteredCount, pageCount)
      EntityQueryResponse(entityQuery, queryMetadata, results)
    }
  }

  /**
    * update multiple entities; they must pre-exist
    */
  override def batchUpdateEntities(
    entityUpdates: Seq[AttributeUpdateOperations.EntityUpdateDefinition]
  ): Future[Iterable[Entity]] = batchUpdateEntitiesImpl(entityUpdates, upsert = false)

  /**
    * upsert multiple entities; will create if they do not pre-exist
    */
  override def batchUpsertEntities(
    entityUpdates: Seq[AttributeUpdateOperations.EntityUpdateDefinition]
  ): Future[Iterable[Entity]] = batchUpdateEntitiesImpl(entityUpdates, upsert = true)

  /**
    * internal implementation for both batchUpsert and batchUpdate
    */
  def batchUpdateEntitiesImpl(entityUpdates: Seq[EntityUpdateDefinition], upsert: Boolean): Future[Iterable[Entity]] = {
    // start tracing
    traceFutureWithParent("JsonEntityProvider.batchUpdateEntitiesImpl", requestArguments.ctx) { localContext =>
      setTraceSpanAttribute(localContext, AttributeKey.stringKey("workspaceId"), workspaceId.toString)
      setTraceSpanAttribute(localContext, AttributeKey.booleanKey("upsert"), java.lang.Boolean.valueOf(upsert))
      setTraceSpanAttribute(localContext,
                            AttributeKey.longKey("entityUpdatesCount"),
                            java.lang.Long.valueOf(entityUpdates.length)
      )
      setTraceSpanAttribute(localContext,
                            AttributeKey.longKey("entityOperationsCount"),
                            java.lang.Long.valueOf(entityUpdates.map(_.operations.length).sum)
      )

      val stopwatch = StopWatch.createStarted()

      val numUpdates = entityUpdates.size
      val numOperations = entityUpdates.flatMap(_.operations).size

      logger.info(s"***** batchUpdateEntitiesImpl processing $numUpdates updates with $numOperations operations")

      // find all attribute names mentioned
      val namesToCheck = for {
        update <- entityUpdates
        operation <- update.operations
      } yield operation.name

      // validate all attribute names
      withAttributeNamespaceCheck(namesToCheck)(() => ())

      dataSource
        .inTransaction { dataAccess =>
          import dataAccess.driver.api._

          // identify all the entities mentioned in entityUpdates
          val allMentionedEntities: Set[AttributeEntityReference] =
            entityUpdates.map(eu => AttributeEntityReference(eu.entityType, eu.name)).toSet

          logger.info(s"***** the $numUpdates updates target ${allMentionedEntities.size} distinct entities.")

          // retrieve all of ${allMentionedEntities} in one query and validate existence if these are not upserts
          dataAccess.jsonEntityQuery.getEntities(workspaceId, allMentionedEntities) flatMap { existingEntities =>
            if (!upsert && existingEntities.size != allMentionedEntities.size) {
              throw new RuntimeException(
                s"Expected all entities being updated to exist; missing ${allMentionedEntities.size - existingEntities.size}"
              )
            }

            logger.info(
              s"***** of the ${allMentionedEntities.size} distinct entities being updated, ${existingEntities.size} already exist."
            )

            // build map of (entityType, name) -> JsonEntityRecord for efficient lookup
            val existingEntityMap: Map[(String, String), JsonEntityRecord] =
              existingEntities.map(rec => (rec.entityType, rec.name) -> rec).toMap

            // iterate through the desired updates and apply them
            val tableRecords: Seq[Option[JsonEntitySlickRecord]] = entityUpdates.map { entityUpdate =>
              // attempt to retrieve an existing entity
              val existingRecordOption = existingEntityMap.get((entityUpdate.entityType, entityUpdate.name))

              // this shouldn't happen because we validated above, but we're being defensive
              if (!upsert && existingRecordOption.isEmpty) {
                throw new RuntimeException("Expected all entities being updated to exist")
              }

              // TODO AJ-2008/AJ-2009: Re-retrieve the existing entity if we are updating the same entity multiple times
              //   see AJ-2009; the existing code does the wrong thing and this code should do better
              val baseEntity: Entity =
                existingRecordOption
                  .map(_.toEntity)
                  .getOrElse(Entity(entityUpdate.name, entityUpdate.entityType, Map()))

              // TODO AJ-2008: collect all the apply errors instead of handling them one-by-one?
              val updatedEntity: Entity = applyOperationsToEntity(baseEntity, entityUpdate.operations)

              // if the entity hasn't changed, skip it
              if (existingRecordOption.nonEmpty && baseEntity.attributes == updatedEntity.attributes) {
                Option.empty[JsonEntitySlickRecord]
              } else {
                // translate back to a JsonEntitySlickRecord for later insert/update
                // TODO AJ-2008: so far we retrieved a JsonEntityRecord, translated it to an Entity, and are now
                //  translating it to JsonEntitySlickRecord; we could do better
                Some(
                  JsonEntitySlickRecord(
                    id = existingRecordOption.map(_.id).getOrElse(0),
                    name = updatedEntity.name,
                    entityType = updatedEntity.entityType,
                    workspaceId = workspaceId,
                    recordVersion = existingRecordOption.map(_.recordVersion).getOrElse(0),
                    deleted = false,
                    deletedDate = None,
                    attributes = Some(updatedEntity.attributes.toJson.compactPrint)
                  )
                )
              }
            }

            // for logging purposes, count the noops
            val noopCount = tableRecords.count(_.isEmpty)

            // separate the records-to-be-saved into inserts and updates
            // we identify inserts as those having id 0
            val (inserts, updates) = tableRecords.flatten.partition(_.id == 0)

            logger.info(
              s"***** all updates have been prepared: ${inserts.size} inserts, ${updates.size} updates, ${noopCount} noop updates."
            )

            // do NOT use the "returning" syntax above, as it forces individual insert statements for each entity.
            // instead, we insert using non-returning syntax, then perform a second query to get the ids
            val insertResult = dataAccess.jsonEntitySlickQuery ++= inserts

            // TODO AJ-2008: don't eagerly kick these off; can cause parallelism problems
            val updateActions = updates.map { upd =>
              dataAccess.jsonEntityQuery.updateEntity(workspaceId, upd.toEntity, upd.recordVersion) map {
                updatedCount =>
                  if (updatedCount == 0) {
                    throw new RuntimeException("Update failed. Concurrent modifications?")
                  }
              }
            }

            // TODO AJ-2008: don't eagerly kick these off; can cause parallelism problems
            val updateRefFutures: Seq[Future[_]] = updates.map { upd =>
              synchronizeReferences(upd.id, upd.toEntity)
            }

            // TODO AJ-2008: can we bulk/batch the ENTITY_REFS work?
            logger.info(s"***** performing inserts ...")
            insertResult.flatMap { _ =>
              // skip any inserts that have zero references
              val insertsWithReferences =
                inserts.flatMap(ins =>
                  if (findAllReferences(ins.toEntity).isEmpty) { None }
                  else { Some(ins) }
                )
              logger.info(s"***** adding references for ${insertsWithReferences.size} inserts ...")

              // retrieve the ids for the inserts that do have references
              dataAccess.jsonEntityQuery.getEntityRefs(
                workspaceId,
                insertsWithReferences.map(x => AttributeEntityReference(x.entityType, x.name)).toSet
              ) flatMap { inserted =>
                // map the inserted ids back to the full entities that were inserted
                val insertedIds = inserted.map(x => (x.entityType, x.name) -> x.id).toMap
                slick.dbio.DBIO.sequence(
                  insertsWithReferences
                    .map { ins =>
                      val id = insertedIds.getOrElse((ins.entityType, ins.name),
                                                     throw new RuntimeException("couldn't find inserted id")
                      )
                      slick.dbio.DBIO.from(synchronizeReferences(id, ins.toEntity, isInsert = true))
                    }
                ) flatMap { _ =>
                  logger.info(s"***** performing updates ...")
                  slick.dbio.DBIO.sequence(updateActions) flatMap { _ =>
                    logger.info(s"***** adding references for updates ...")
                    slick.dbio.DBIO.sequence(updateRefFutures.map(x => slick.dbio.DBIO.from(x))) flatMap { _ =>
                      stopwatch.stop()
                      logger.info(s"***** all writes complete in ${stopwatch.getTime}ms")
                      slick.dbio.DBIO.successful(())
                    }
                  }
                }
              }
            }
          }
        }
        .map { _ =>
          logger.info(s"***** all inserts and updates completed.")
          // returns nothing. EntityApiService explicitly returns a 204 with no response body; so we don't bother
          // returning anything at all from here.
          // TODO AJ-2008: does this have any compatibility issues elsewhere? LocalEntityProvider does return entities.
          Seq()
        }
    } // end trace
  }

  override def copyEntities(sourceWorkspaceContext: Workspace,
                            destWorkspaceContext: Workspace,
                            entityType: String,
                            entityNames: Seq[String],
                            linkExistingEntities: Boolean,
                            parentContext: RawlsRequestContext
  ): Future[EntityCopyResponse] = ???

  override def deleteEntitiesOfType(entityType: String): Future[Int] = ???

  override def evaluateExpressions(expressionEvaluationContext: ExpressionEvaluationContext,
                                   gatherInputsResult: MethodConfigResolver.GatherInputsResult,
                                   workspaceExpressionResults: Map[LookupExpression, Try[Iterable[AttributeValue]]]
  ): Future[LazyList[SubmissionValidationEntityInputs]] = ???

  override def expressionValidator: ExpressionValidator = ???

  // ====================================================================================================
  //  helper methods
  // ====================================================================================================

  // given potential references from an entity, verify that the reference targets all exist,
  // and return their ids.
  private def validateReferences(entity: Entity): Future[Map[AttributeName, Seq[JsonEntityRefRecord]]] = {
    // find all refs in the entity
    val refs: Map[AttributeName, Seq[AttributeEntityReference]] = findAllReferences(entity)

    // short-circuit
    if (refs.isEmpty) {
      Future.successful(Map())
    } else {
      // validate all refs
      val allRefs: Set[AttributeEntityReference] = refs.values.flatten.toSet

      dataSource.inTransaction { dataAccess =>
        dataAccess.jsonEntityQuery.getEntityRefs(workspaceId, allRefs) map { foundRefs =>
          if (foundRefs.size != allRefs.size) {
            throw new RuntimeException("Did not find all references")
          }
          // convert the foundRefs to a map for easier lookup
          val foundMap: Map[(String, String), JsonEntityRefRecord] = foundRefs.map { foundRef =>
            ((foundRef.entityType, foundRef.name), foundRef)
          }.toMap

          // return all the references found in this entity, mapped to the ids they are referencing
          refs.map { case (name: AttributeName, refs: Seq[AttributeEntityReference]) =>
            val refRecords: Seq[JsonEntityRefRecord] = refs.map(ref =>
              foundMap.getOrElse((ref.entityType, ref.entityName),
                                 throw new RuntimeException("unexpected; couldn't find ref")
              )
            )
            (name, refRecords)
          }
        }
      }
    }
  }

  // given an entity, finds all references in that entity, grouped by their attribute names
  private def findAllReferences(entity: Entity): Map[AttributeName, Seq[AttributeEntityReference]] =
    entity.attributes
      .collect {
        case (name: AttributeName, aer: AttributeEntityReference)      => Seq((name, aer))
        case (name: AttributeName, aerl: AttributeEntityReferenceList) => aerl.list.map(ref => (name, ref))
      }
      .flatten
      .toSeq
      .groupMap(_._1)(_._2)

  // given already-validated references, including target ids, update the ENTITY_REFS table for a given source
  // entity
  private def replaceReferences(fromId: Long,
                                foundRefs: Map[AttributeName, Seq[JsonEntityRefRecord]],
                                isInsert: Boolean = false
  ): Future[Map[AttributeName, Seq[JsonEntityRefRecord]]] = {
    // short-circuit
    if (isInsert && foundRefs.isEmpty) {
      return Future.successful(Map())
    }
    dataSource.inTransaction { dataAccess =>
      import dataAccess.driver.api._
      // we don't actually care about the referencing attribute name or referenced type&name; reduce to just the referenced ids.
      val currentEntityRefTargets: Set[Long] = foundRefs.values.flatten.map(_.id).toSet
      logger.trace(s"~~~~~ found ${currentEntityRefTargets.size} ref targets in entity $fromId")
      for {
        // TODO AJ-2008: instead of (retrieve all, then calculate diffs, then execute diffs), try doing it all in the db:
        //  - delete from ENTITY_REFS where from_id = $fromId and to_id not in ($currentEntityRefTargets)
        //  - insert into ENTITY_REFS (from_id, to_id) values ($fromId, $currentEntityRefTargets:_*) on duplicate key update from_id=from_id (noop)
        // retrieve all existing refs in ENTITY_REFS for this entity; create a set of the target ids
        existingRowsSeq <-
          if (isInsert) {
            slick.dbio.DBIO.successful(Seq.empty[Long])
          } else {
            dataAccess.jsonEntityRefSlickQuery.filter(_.fromId === fromId).map(_.toId).result
          }
        existingRefTargets = existingRowsSeq.toSet

        _ = logger.trace(s"~~~~~ found ${existingRefTargets.size} ref targets in db for entity $fromId")
        // find all target ids in the db that are not in the current entity
        deletes = existingRefTargets diff currentEntityRefTargets
        // find all target ids in the current entity that are not in the db
        inserts = currentEntityRefTargets diff existingRefTargets
        insertPairs = inserts.map(toId => (fromId, toId))
        _ = logger.trace(
          s"~~~~~ prepared ${inserts.size} inserts and ${deletes.size} deletes to perform for entity $fromId"
        )
        _ = logger.trace(s"~~~~~ inserts: $insertPairs for entity $fromId")
        // insert what needs to be inserted
        insertResult <-
          if (inserts.nonEmpty) { dataAccess.jsonEntityRefSlickQuery.map(r => (r.fromId, r.toId)) ++= insertPairs }
          else { slick.dbio.DBIO.successful(0) }
//        insertResult <- dataAccess.jsonEntityQuery.bulkInsertReferences(fromId, inserts)
        _ = logger.trace(s"~~~~~ actually inserted ${insertResult} rows for entity $fromId")
        // delete what needs to be deleted
        deleteResult <-
          if (deletes.nonEmpty) {
            dataAccess.jsonEntityRefSlickQuery
              .filter(x => x.fromId === fromId && x.toId.inSetBind(deletes))
              .delete
          } else { slick.dbio.DBIO.successful(0) }
        _ = logger.trace(s"~~~~~ actually deleted ${deleteResult} rows for entity $fromId")
      } yield foundRefs
    }
  }

  // helper to call validateReferences followed by replaceReferences
  private def synchronizeReferences(fromId: Long,
                                    entity: Entity,
                                    isInsert: Boolean = false
  ): Future[Map[AttributeName, Seq[JsonEntityRefRecord]]] = dataSource.inTransaction { _ =>
    for {
      // find and validate all references in this entity. This returns the target internal ids for each reference.
      foundRefs <- DBIO.from(validateReferences(entity))
      //
      _ <- DBIO.from(replaceReferences(fromId, foundRefs, isInsert))
    } yield foundRefs
  }
}
