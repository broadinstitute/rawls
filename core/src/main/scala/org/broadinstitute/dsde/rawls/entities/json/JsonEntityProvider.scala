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
import io.opentelemetry.api.common.AttributeKey
import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.dataaccess.slick.{
  JsonEntityRecord,
  JsonEntityRefRecord,
  JsonEntitySlickRecord,
  ReadAction
}
import org.broadinstitute.dsde.rawls.entities.exceptions.DataEntityException
import org.broadinstitute.dsde.rawls.model.AttributeUpdateOperations.EntityUpdateDefinition
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._
import org.broadinstitute.dsde.rawls.util.AttributeSupport
import org.broadinstitute.dsde.rawls.util.TracingUtils.{setTraceSpanAttribute, traceFutureWithParent}
import slick.dbio.DBIO

import java.time.Duration
import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Try}

// TODO AJ-2008: tracing
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
  override def createEntity(entity: Entity): Future[Entity] = {
    logger.info(s"creating entity $entity")
    dataSource.inTransaction { dataAccess =>
      for {
        // find and validate all references in the entity-to-be-saved
        referenceTargets <- DBIO.from(validateReferences(entity))

        // save the entity
        _ <- dataAccess.jsonEntityQuery.createEntity(workspaceId, entity)
        // did it save correctly?
        savedEntityRecordOption <- dataAccess.jsonEntityQuery.getEntity(workspaceId, entity.entityType, entity.name)
        savedEntityRecord = savedEntityRecordOption.getOrElse(throw new RuntimeException("Could not save entity"))
        // save all references from this entity to other entities
        _ <- dataAccess.jsonEntityQuery.replaceReferences(savedEntityRecord.id, referenceTargets)
      } yield savedEntityRecord.toEntity
    }
  }

  /**
    * Read a single entity from the db
    */
  // TODO AJ-2008: mark transaction as read-only
  override def getEntity(entityType: String, entityName: String): Future[Entity] = dataSource.inTransaction {
    dataAccess =>
      dataAccess.jsonEntityQuery.getEntity(workspaceId, entityType, entityName)
  } map { result => result.map(_.toEntity).get }

  override def deleteEntities(entityRefs: Seq[AttributeEntityReference]): Future[Int] = ???

  // TODO AJ-2008: mark transaction as read-only
  // TODO AJ-2008: probably needs caching for the attribute calculations
  override def entityTypeMetadata(useCache: Boolean): Future[Map[String, EntityTypeMetadata]] =
    dataSource.inTransaction { dataAccess =>
      // get the types and counts
      for {
        typesAndCounts <- dataAccess.jsonEntityQuery.typesAndCounts(workspaceId)
        typesAndAttributes <- dataAccess.jsonEntityQuery.typesAndAttributes(workspaceId)
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

  override def queryEntitiesSource(entityType: String,
                                   entityQuery: EntityQuery,
                                   parentContext: RawlsRequestContext
  ): Future[(EntityQueryResultMetadata, Source[Entity, _])] =
    queryEntities(entityType, entityQuery, parentContext).map { queryResponse =>
      // TODO AJ-2008: actually stream!
      (queryResponse.resultMetadata, Source.apply(queryResponse.results))
    }

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

  override def batchUpdateEntities(
    entityUpdates: Seq[AttributeUpdateOperations.EntityUpdateDefinition]
  ): Future[Iterable[Entity]] = batchUpdateEntitiesImpl(entityUpdates, upsert = false)

  override def batchUpsertEntities(
    entityUpdates: Seq[AttributeUpdateOperations.EntityUpdateDefinition]
  ): Future[Iterable[Entity]] = batchUpdateEntitiesImpl(entityUpdates, upsert = true)

  def batchUpdateEntitiesImpl(entityUpdates: Seq[EntityUpdateDefinition], upsert: Boolean): Future[Iterable[Entity]] = {

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
            val tableRecords: Seq[JsonEntitySlickRecord] = entityUpdates.map { entityUpdate =>
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

              // TODO AJ-2008: handle references

              // translate back to a JsonEntitySlickRecord for later insert/update
              // TODO AJ-2008: so far we retrieved a JsonEntityRecord, translated it to an Entity, and are now
              //  translating it to JsonEntitySlickRecord; we could do better
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
            }

            // separate the records-to-be-saved into inserts and updates
            // we identify inserts as those having id 0
            val (inserts, updates) = tableRecords.partition(_.id == 0)

            logger.info(s"***** all updates have been prepared: ${inserts.size} inserts, ${updates.size} updates.")

            // perform the inserts, then perform the updates
            val insertResult = dataAccess.jsonEntitySlickQuery ++= inserts

            val updateActions = updates.map { upd =>
              dataAccess.jsonEntityQuery.updateEntity(workspaceId, upd.toEntity, upd.recordVersion) map {
                updatedCount =>
                  if (updatedCount == 0) {
                    throw new RuntimeException("Update failed. Concurrent modifications?")
                  }
              }
            }

            logger.info(s"***** performing inserts ...")
            insertResult.flatMap { _ =>
              logger.info(s"***** performing updates ...")
              slick.dbio.DBIO.sequence(updateActions)
            }
          }
        }
        .map { _ =>
          logger.info(s"***** all inserts and updates completed.")
          Seq()
        } // TODO AJ-2008: return entities, not nothing. What does the current impl return?
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
        dataAccess.jsonEntityQuery.getEntities(workspaceId, allRefs) map { foundRefs =>
          if (foundRefs.size != allRefs.size) {
            throw new RuntimeException("Did not find all references")
          }
          // convert the foundRefs to a map for easier lookup
          val foundMap: Map[(String, String), JsonEntityRefRecord] = foundRefs.map { foundRef =>
            ((foundRef.entityType, foundRef.name), JsonEntityRefRecord(foundRef.id, foundRef.name, foundRef.entityType))
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
}
