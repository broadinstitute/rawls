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
import io.opentelemetry.api.common.AttributeKey
import org.broadinstitute.dsde.rawls.dataaccess.slick.JsonEntityRecord
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
      dataAccess.jsonEntityQuery.createEntity(workspaceId, entity)
    } flatMap { _ => getEntity(entity.entityType, entity.name) }

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
  ): Future[(EntityQueryResultMetadata, Source[Entity, _])] = dataSource.inTransaction { dataAccess =>
    dataAccess.jsonEntityQuery.queryEntities(workspaceId, entityType, entityQuery) map { results =>
      // TODO AJ-2008: total/filtered counts
      // TODO AJ-2008: actually stream!!!!
      val metadata = EntityQueryResultMetadata(1, 2, 3)
      val entitySource = Source.apply(results)
      (metadata, entitySource)
    }
  }

  override def queryEntities(entityType: String,
                             entityQuery: EntityQuery,
                             parentContext: RawlsRequestContext
  ): Future[EntityQueryResponse] = dataSource.inTransaction { dataAccess =>
    dataAccess.jsonEntityQuery.queryEntities(workspaceId, entityType, entityQuery) map { results =>
      // TODO AJ-2008: total/filtered counts
      EntityQueryResponse(entityQuery, EntityQueryResultMetadata(1, 2, 3), results)
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
          // iterate through the desired updates and apply them
          val queries = entityUpdates.map { entityUpdate =>
            // attempt to retrieve the existing entity
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

}
