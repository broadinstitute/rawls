package org.broadinstitute.dsde.rawls.entities.compact

import akka.NotUsed
import akka.stream.scaladsl.Source
import org.broadinstitute.dsde.rawls.dataaccess.SlickDataSource
import org.broadinstitute.dsde.rawls.entities.EntityRequestArguments
import org.broadinstitute.dsde.rawls.entities.base.ExpressionEvaluationSupport.LookupExpression
import org.broadinstitute.dsde.rawls.entities.base.{EntityProvider, ExpressionEvaluationContext, ExpressionValidator}
import org.broadinstitute.dsde.rawls.jobexec.MethodConfigResolver
import org.broadinstitute.dsde.rawls.model.{
  AttributeEntityReference,
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
  RawlsRequestContext,
  SubmissionValidationEntityInputs,
  Workspace
}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

/**
  * Implementation logic for compact data tables. Compact data tables store all an entity's attributes in a
  * single JSON packet within the database. Compare this to the implementation in LocalEntityProvider, which
  * requires at least one row per attribute.
  *
  * @param executionContext scala concurrency context
  */
class CompactEntityProvider(requestArguments: EntityRequestArguments, dataSource: SlickDataSource)(implicit
  protected val executionContext: ExecutionContext
) extends EntityProvider {
  override def entityStoreId: Option[String] = ???

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

  override def createEntity(entity: Entity): Future[Entity] = ???

  override def deleteEntities(entityRefs: Seq[AttributeEntityReference]): Future[Int] = ???

  override def deleteEntitiesOfType(entityType: String): Future[Int] = ???

  override def deleteEntityAttributes(entityType: String, attributeNames: Set[AttributeName]): Future[Unit] = ???

  override def entityTypeMetadata(useCache: Boolean): Future[Map[String, EntityTypeMetadata]] = ???

  override def evaluateExpression(entityType: String,
                                  entityName: String,
                                  expression: String
  ): Future[Seq[AttributeValue]] = ???

  override def evaluateExpressions(expressionEvaluationContext: ExpressionEvaluationContext,
                                   gatherInputsResult: MethodConfigResolver.GatherInputsResult,
                                   workspaceExpressionResults: Map[LookupExpression, Try[Iterable[AttributeValue]]]
  ): Future[LazyList[SubmissionValidationEntityInputs]] = ???

  override def expressionValidator: ExpressionValidator = ???

  override def getEntity(entityType: String, entityName: String): Future[Entity] = ???

  override def listEntities(entityType: String): Source[Entity, NotUsed] = ???

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
                               attributeRenameRequest: AttributeRename
  ): Future[Int] = ???

  override def renameEntity(entityType: String, entityName: String, newName: String): Future[Int] = ???

  override def renameEntityType(oldName: String, renameInfo: EntityTypeRename): Future[Int] = ???

  override def updateEntity(entityType: String,
                            entityName: String,
                            operations: Seq[AttributeUpdateOperations.AttributeUpdateOperation]
  ): Future[Entity] = ???
}
