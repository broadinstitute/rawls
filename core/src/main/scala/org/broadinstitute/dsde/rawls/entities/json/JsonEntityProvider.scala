package org.broadinstitute.dsde.rawls.entities.json

import akka.stream.scaladsl.Source
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.dataaccess.SlickDataSource
import org.broadinstitute.dsde.rawls.entities.EntityRequestArguments
import org.broadinstitute.dsde.rawls.entities.base.ExpressionEvaluationSupport.LookupExpression
import org.broadinstitute.dsde.rawls.entities.base.{EntityProvider, ExpressionEvaluationContext, ExpressionValidator}
import org.broadinstitute.dsde.rawls.jobexec.MethodConfigResolver
import org.broadinstitute.dsde.rawls.model.Attributable.{entityIdAttributeSuffix, AttributeMap}
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
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._

import java.time.Duration
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

// TODO AJ-2008: tracing
class JsonEntityProvider(requestArguments: EntityRequestArguments,
                         implicit protected val dataSource: SlickDataSource,
                         cacheEnabled: Boolean,
                         queryTimeout: Duration,
                         val workbenchMetricBaseName: String
)(implicit protected val executionContext: ExecutionContext)
    extends EntityProvider
    with LazyLogging {

  override def entityStoreId: Option[String] = None

  /**
    * Insert a single entity to the db
    */
  override def createEntity(entity: Entity): Future[Entity] =
    dataSource.inTransaction { dataAccess =>
      dataAccess.jsonEntityQuery.createEntity(requestArguments.workspace.workspaceIdAsUUID, entity)
    } map { jsonEntityRecord =>
      Entity(jsonEntityRecord.name, jsonEntityRecord.entityType, jsonEntityRecord.attributes.convertTo[AttributeMap])
    }

  /**
    * Read a single entity from the db
    */
  // TODO AJ-2008: mark transaction as read-only
  override def getEntity(entityType: String, entityName: String): Future[Entity] = dataSource.inTransaction {
    dataAccess =>
      dataAccess.jsonEntityQuery.getEntity(requestArguments.workspace.workspaceIdAsUUID, entityType, entityName)
  } map { jsonEntityRecord =>
    Entity(jsonEntityRecord.name, jsonEntityRecord.entityType, jsonEntityRecord.attributes.convertTo[AttributeMap])
  }

  override def deleteEntities(entityRefs: Seq[AttributeEntityReference]): Future[Int] = ???

  // TODO AJ-2008: mark transaction as read-only
  // TODO AJ-2008: probably needs caching for the attribute calculations
  override def entityTypeMetadata(useCache: Boolean): Future[Map[String, EntityTypeMetadata]] =
    dataSource.inTransaction { dataAccess =>
      // get the types and counts
      for {
        typesAndCounts <- dataAccess.jsonEntityQuery.typesAndCounts(requestArguments.workspace.workspaceIdAsUUID)
        typesAndAttributes <- dataAccess.jsonEntityQuery.typesAndAttributes(
          requestArguments.workspace.workspaceIdAsUUID
        )
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
                                   query: EntityQuery,
                                   parentContext: RawlsRequestContext
  ): Future[(EntityQueryResultMetadata, Source[Entity, _])] = ???

  override def queryEntities(entityType: String,
                             query: EntityQuery,
                             parentContext: RawlsRequestContext
  ): Future[EntityQueryResponse] = ???

  override def batchUpdateEntities(
    entityUpdates: Seq[AttributeUpdateOperations.EntityUpdateDefinition]
  ): Future[Traversable[Entity]] = ???

  override def batchUpsertEntities(
    entityUpdates: Seq[AttributeUpdateOperations.EntityUpdateDefinition]
  ): Future[Traversable[Entity]] = ???

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
