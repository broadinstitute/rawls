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
import org.broadinstitute.dsde.rawls.dataaccess.slick.{JsonEntityRecord, JsonEntityRefRecord, ReadAction}
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
  override def createEntity(entity: Entity): Future[Entity] =
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

  // TODO AJ-2008: this needs some serious optimization, it issues way too many single individual updates
  def batchUpdateEntitiesImpl(entityUpdates: Seq[EntityUpdateDefinition], upsert: Boolean): Future[Iterable[Entity]] = {
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

      // identify all the entities mentioned in entityUpdates
      val allMentionedEntities: Seq[AttributeEntityReference] =
        entityUpdates.map(eu => AttributeEntityReference(eu.entityType, eu.name))

      dataSource
        .inTransaction { dataAccess =>
          // TODO AJ-2008: retrieve all of ${allMentionedEntities} in one query and validate existence if these are not upserts

          // iterate through the desired updates and apply them
          val queries = entityUpdates.map { entityUpdate =>
            // attempt to retrieve the existing entity
            // TODO AJ-2008: pull from the list we retrieved when possible. Only re-retrieve from the db
            //   if we are updating the same entity multiple times
            //   see AJ-2009; the existing code does the wrong thing and this code should do better

            // TODO AJ-2008: find all the inserts (vs updates), and batch them together first, preserving order
            //   from the entityUpdates list
            dataAccess.jsonEntityQuery.getEntity(workspaceId, entityUpdate.entityType, entityUpdate.name) flatMap {
              foundEntityOption =>
                if (!upsert && foundEntityOption.isEmpty) {
                  throw new RuntimeException("Entity does not exist")
                }
                val baseEntity: Entity =
                  foundEntityOption
                    .map(_.toEntity)
                    .getOrElse(Entity(entityUpdate.name, entityUpdate.entityType, Map.empty))
                // TODO AJ-2008: collect all the apply errors instead of handling them one-by-one?
                val updatedEntity: Entity = applyOperationsToEntity(baseEntity, entityUpdate.operations)

                // insert or update
                foundEntityOption match {
                  // do insert
                  case None => dataAccess.jsonEntityQuery.createEntity(workspaceId, updatedEntity)
                  // do update
                  case Some(foundEntity) =>
                    dataAccess.jsonEntityQuery.updateEntity(workspaceId, updatedEntity, foundEntity.recordVersion) map {
                      updatedCount =>
                        if (updatedCount == 0) {
                          throw new RuntimeException("Update failed. Concurrent modifications?")
                        }
                    }
                }
            }
          }
          DBIO.sequence(queries)
        }
        .map(_ => Seq()) // TODO AJ-2008: return entities, not nothing
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
    }

    // validate all refs
    val allRefs: Set[AttributeEntityReference] = refs.values.flatten.toSet

    dataSource.inTransaction { dataAccess =>
      dataAccess.jsonEntityQuery.validateRefs(workspaceId, allRefs) map { foundRefs =>
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
