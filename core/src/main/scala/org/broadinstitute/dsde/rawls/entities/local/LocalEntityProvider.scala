package org.broadinstitute.dsde.rawls.entities.local

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.Source
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.dataaccess.SlickDataSource
import org.broadinstitute.dsde.rawls.dataaccess.slick._
import org.broadinstitute.dsde.rawls.entities.EntityRequestArguments
import org.broadinstitute.dsde.rawls.entities.base.ExpressionEvaluationSupport.{EntityName, LookupExpression}
import org.broadinstitute.dsde.rawls.entities.base.{
  EntityProvider,
  ExpressionEvaluationContext,
  ExpressionEvaluationSupport,
  ExpressionValidator
}
import org.broadinstitute.dsde.rawls.jobexec.MethodConfigResolver.GatherInputsResult
import org.broadinstitute.dsde.rawls.model.AttributeUpdateOperations.EntityUpdateDefinition
import org.broadinstitute.dsde.rawls.model.{
  AttributeName,
  AttributeRename,
  AttributeUpdateOperations,
  AttributeValue,
  Entity,
  EntityCopyResponse,
  EntityPointer,
  EntityQuery,
  EntityQueryResponse,
  EntityQueryResultMetadata,
  EntityTypeMetadata,
  EntityTypeRename,
  RawlsRequestContext,
  SubmissionValidationEntityInputs,
  Workspace
}
import org.broadinstitute.dsde.rawls.util.{AttributeSupport, EntitySupport}

import java.time.Duration
import scala.concurrent.{ExecutionContext, Future}
import scala.language.postfixOps
import scala.util.Try

/**
 * Terra default entity provider, powered by Rawls and Cloud SQL
 */
class LocalEntityProvider(requestArguments: EntityRequestArguments,
                          implicit protected val dataSource: SlickDataSource,
                          _cacheEnabled: Boolean,
                          _queryTimeout: Duration,
                          val workbenchMetricBaseName: String
)(implicit protected val executionContext: ExecutionContext, _actorSystem: ActorSystem)
    extends EntityProvider
    with LazyLogging
    with EntitySupport
    with AttributeSupport
    with ExpressionEvaluationSupport {

  override val entityStoreId: Option[String] = None

  override def entityTypeMetadata(useCache: Boolean,
                                  parentContext: RawlsRequestContext
  ): Future[Map[String, EntityTypeMetadata]] = ???

  override def createEntity(entity: Entity, parentContext: RawlsRequestContext): Future[Entity] = ???

  // EntityApiServiceSpec has good test coverage for this api
  override def deleteEntities(pointers: Seq[EntityPointer], parentContext: RawlsRequestContext): Future[Int] = ???

  override def deleteEntitiesOfType(entityType: String, parentContext: RawlsRequestContext): Future[Int] = ???

  override def deleteEntityAttributes(entityType: EntityName,
                                      attributeNames: Set[AttributeName],
                                      parentContext: RawlsRequestContext
  ): Future[Unit] = ???

  override def evaluateExpression(entityType: EntityName,
                                  entityName: EntityName,
                                  expression: EntityName,
                                  parentContext: RawlsRequestContext
  ): Future[Seq[AttributeValue]] = ???

  override def evaluateExpressions(expressionEvaluationContext: ExpressionEvaluationContext,
                                   gatherInputsResult: GatherInputsResult,
                                   workspaceExpressionResults: Map[LookupExpression, Try[Iterable[AttributeValue]]]
  ): Future[LazyList[SubmissionValidationEntityInputs]] = ???

  override def expressionValidator: ExpressionValidator = ???

  override def getEntity(entityType: String, entityName: String, parentContext: RawlsRequestContext): Future[Entity] =
    ???

  /*
   * Queries the db for a stream of entity attributes.
   */
  override def listEntities(entityType: String): Source[Entity, NotUsed] = ???

  /**
    * Returns the components needed to stream a EntityQueryResponse to an end user in response to the entityQuery API.
    * This method returns fully materialized metadata (row counts, page size, etc) as EntityQueryResultMetadata, and
    * also returns a streaming Source of Entity objects. We avoid materializing the full set of Entity objects for
    * performance and memory reasons.
    *
    * @param entityType the type of entities to return in the result set
    * @param query criteria for filtering and paginating the result set
    * @param parentContext tracing context into which this method will add traces
    * @return a tuple of 1) the fully materialized metadata, and 2) a streaming Source of Entity objects
    */
  override def queryEntitiesSource(entityType: String,
                                   query: EntityQuery,
                                   parentContext: RawlsRequestContext = requestArguments.ctx
  ): Future[(EntityQueryResultMetadata, Source[Entity, _])] = ???

  /* as of this writing, only used in tests. This queryEntities method materializes the entire result set of
   *  entities and is therefore memory-hungry. Runtime code should not use this and should call queryEntitiesSource
   *  instead. This method is still useful for testing and is called by multiple tests.
   * */
  @deprecated("use queryEntitiesSource instead.", "2024-01-09")
  override def queryEntities(entityType: String,
                             query: EntityQuery,
                             parentContext: RawlsRequestContext = requestArguments.ctx
  ): Future[EntityQueryResponse] = ???

  override def batchUpdateEntities(entityUpdates: Source[EntityUpdateDefinition, _],
                                   parentContext: RawlsRequestContext
  ): Future[Int] = ???

  override def batchUpsertEntities(entityUpdates: Source[EntityUpdateDefinition, _],
                                   parentContext: RawlsRequestContext
  ): Future[Int] = ???

  override def saveWorkflowOutputEntities(
    dataAccess: DataAccess,
    workspace: Workspace,
    updatedEntities: Seq[Entity]
  ): ReadWriteAction[Int] = ???

  override def listWorkflowEntities(dataAccess: DataAccess,
                                    workspace: Workspace,
                                    entityIds: Seq[Long]
  ): ReadAction[Map[Long, Entity]] = ???

  override def copyEntities(sourceWorkspaceContext: Workspace,
                            destWorkspaceContext: Workspace,
                            entityType: String,
                            entityNames: Seq[String],
                            linkExistingEntities: Boolean,
                            parentContext: RawlsRequestContext
  ): Future[EntityCopyResponse] = ???

  override def clone(sourceWorkspaceContext: Workspace,
                     destWorkspaceContext: Workspace,
                     parentContext: RawlsRequestContext
  ): WriteAction[(Int, Int)] = ???

  override def renameAttribute(entityType: EntityName,
                               oldAttributeName: AttributeName,
                               attributeRenameRequest: AttributeRename,
                               parentContext: RawlsRequestContext
  ): Future[Int] = ???

  override def renameEntity(entityType: EntityName,
                            entityName: EntityName,
                            newName: EntityName,
                            parentContext: RawlsRequestContext
  ): Future[Int] = ???

  override def renameEntityType(oldName: EntityName,
                                renameInfo: EntityTypeRename,
                                parentContext: RawlsRequestContext
  ): Future[Int] = ???

  override def updateEntity(entityType: EntityName,
                            entityName: EntityName,
                            operations: Seq[AttributeUpdateOperations.AttributeUpdateOperation],
                            parentContext: RawlsRequestContext
  ): Future[Entity] = ???

}
