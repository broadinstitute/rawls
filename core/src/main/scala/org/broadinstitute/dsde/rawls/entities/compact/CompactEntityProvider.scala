package org.broadinstitute.dsde.rawls.entities.compact

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.dataaccess.SlickDataSource
import org.broadinstitute.dsde.rawls.entities.EntityRequestArguments
import org.broadinstitute.dsde.rawls.entities.base.ExpressionEvaluationSupport.LookupExpression
import org.broadinstitute.dsde.rawls.entities.base.{EntityProvider, ExpressionEvaluationContext, ExpressionValidator}
import org.broadinstitute.dsde.rawls.entities.exceptions.{
  DataEntityException,
  EntityNotFoundException,
  EntityReferenceNotFoundException
}
import org.broadinstitute.dsde.rawls.jobexec.MethodConfigResolver
import org.broadinstitute.dsde.rawls.model.{
  AttributeEntityReference,
  AttributeEntityReferenceList,
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
import slick.dbio.DBIO

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

/**
  * Implementation logic for compact data tables. Compact data tables store all an entity's attributes in a
  * single JSON packet within the database. Compare this to the implementation in LocalEntityProvider, which
  * requires at least one row per attribute.
  *
  * @param executionContext scala concurrency context
  */
class CompactEntityProvider(requestArguments: EntityRequestArguments,
                            repository: CompactEntityRepository,
                            dataSource: SlickDataSource
)(implicit
  protected val executionContext: ExecutionContext
) extends EntityProvider
    with LazyLogging {
  override def entityStoreId: Option[String] = None // unused

  val workspaceId: UUID = requestArguments.workspace.workspaceIdAsUUID // shorthand for methods below

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

  // TODO CORE-362: unit tests
  override def createEntity(entity: Entity): Future[Entity] = {
    // find all references in this entity
    val refs: Map[AttributeName, Seq[AttributeEntityReference]] = findAllReferences(entity)
    // find all unique references in this entity
    val uniqueRefs: Set[AttributeEntityReference] = refs.values.flatten.toSet

    dataSource.inTransaction { dataAccess =>
      val query = dataAccess.getCompactEntityQuery
      for {
        // verify that all references in the entity-to-be-saved actually exist
        referencedIds <- query.getReferencedIds(workspaceId, uniqueRefs)
        _ = if (uniqueRefs.size != referencedIds.size)
          throw new EntityReferenceNotFoundException("Some entity references do not exist")
        // save the entity
        _ <- query.createEntity(workspaceId, entity)
        // did it save correctly? re-retrieve it. By re-retrieving it, we can 1) get its id, and 2) get the actual,
        // normalized JSON that was persisted to the db. When we return the entity to the user, we return the
        // normalized version.
        savedEntityRecordOption <- query.getEntity(workspaceId, entity.entityType, entity.name)
        savedEntityRecord = savedEntityRecordOption.getOrElse(throw new DataEntityException("Could not save entity"))
        // save all references from this entity to other entities
        _ <- DBIO.from(replaceReferences(savedEntityRecord.id, referencedIds.toSet, isInsert = true))
      } yield savedEntityRecord.toEntity
    }
  }

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

  override def getEntity(entityType: String, entityName: String): Future[Entity] =
    repository.getEntity(workspaceId, entityType, entityName) map {
      case Some(entityRec) => entityRec.toEntity
      case None            => throw new EntityNotFoundException()
    }

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

  // ====================================================================================================
  //  helper methods
  // ====================================================================================================

  def getRepository(dataSource: SlickDataSource) = new CompactEntityRepository(dataSource)

  // given an entity, finds all references in that entity, grouped by their attribute names
  protected[compact] def findAllReferences(entity: Entity): Map[AttributeName, Seq[AttributeEntityReference]] =
    entity.attributes
      .collect {
        case (name: AttributeName, ref: AttributeEntityReference)         => Seq((name, ref))
        case (name: AttributeName, refList: AttributeEntityReferenceList) => refList.list.map(ref => (name, ref))
      }
      .flatten
      .toSeq
      .groupMap(_._1)(_._2)

  // given already-validated references, represented as target ids, update the ENTITY_REFS table for a given source
  // entity
  protected[compact] def replaceReferences(fromId: Long, toIds: Set[Long], isInsert: Boolean): Future[(Int, Int)] = {
    // short-circuit
    if (isInsert && toIds.isEmpty) {
      Future.successful((0, 0))
    }
    dataSource.inTransaction { _ =>
      for {
        // delete any reference pointers that should no longer exist
        deletes <-
          if (isInsert) {
            DBIO.successful(0)
          } else {
            DBIO.from(repository.deleteReferences(fromId, toIds))
          }
        // upsert all reference pointers that do exist
        upserts <-
          if (toIds.isEmpty) {
            DBIO.successful(0)
          } else {
            DBIO.from(repository.upsertReferences(fromId, toIds))
          }
      } yield (deletes, upserts)
    }
  }

}
