package org.broadinstitute.dsde.rawls.entities.compact

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.dataaccess.SlickDataSource
import org.broadinstitute.dsde.rawls.dataaccess.slick.CompactEntityRefRecord
import org.broadinstitute.dsde.rawls.entities.EntityRequestArguments
import org.broadinstitute.dsde.rawls.entities.base.ExpressionEvaluationSupport.LookupExpression
import org.broadinstitute.dsde.rawls.entities.base.{EntityProvider, ExpressionEvaluationContext, ExpressionValidator}
import org.broadinstitute.dsde.rawls.entities.exceptions.{DataEntityException, EntityNotFoundException}
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
class CompactEntityProvider(requestArguments: EntityRequestArguments, dataSource: SlickDataSource)(implicit
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

  override def createEntity(entity: Entity): Future[Entity] =
    dataSource.inTransaction { dataAccess =>
      for {
        // find and validate all references in the entity-to-be-saved
        referenceTargets <- DBIO.from(validateReferences(entity))
        // save the entity
        _ <- dataAccess.compactEntityQuery.createEntity(workspaceId, entity)
        // did it save correctly? re-retrieve it. By re-retrieving it, we can 1) get its id, and 2) get the actual,
        // normalized JSON that was persisted to the db. When we return the entity to the user, we return the
        // normalized version.
        savedEntityRecordOption <- dataAccess.compactEntityQuery.getEntity(workspaceId, entity.entityType, entity.name)
        savedEntityRecord = savedEntityRecordOption.getOrElse(throw new DataEntityException("Could not save entity"))
        // save all references from this entity to other entities
        _ <- DBIO.from(replaceReferences(savedEntityRecord.id, referenceTargets, isInsert = true))
      } yield savedEntityRecord.toEntity
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
    dataSource.inTransaction { dataAccess =>
      dataAccess.compactEntityQuery.getEntity(workspaceId, entityType, entityName)
    } map { result => result.map(_.toEntity).getOrElse(throw new EntityNotFoundException()) }

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

  // given potential references from an entity, verify that the reference targets all exist,
  // and return their ids.
  private def validateReferences(entity: Entity): Future[Map[AttributeName, Seq[CompactEntityRefRecord]]] = {
    // find all refs in the entity
    val refs: Map[AttributeName, Seq[AttributeEntityReference]] = findAllReferences(entity)

    // short-circuit
    if (refs.isEmpty) {
      Future.successful(Map())
    } else {
      // validate all refs
      val allRefs: Set[AttributeEntityReference] = refs.values.flatten.toSet

      dataSource.inTransaction { dataAccess =>
        dataAccess.compactEntityQuery.getEntityRefs(workspaceId, allRefs) map { foundRefs =>
          if (foundRefs.size != allRefs.size) {
            throw new RuntimeException("Did not find all references")
          }
          // convert the foundRefs to a map for easier lookup
          val foundMap: Map[(String, String), CompactEntityRefRecord] = foundRefs.map { foundRef =>
            ((foundRef.entityType, foundRef.name), foundRef)
          }.toMap

          // return all the references found in this entity, mapped to the ids they are referencing
          refs.map { case (name: AttributeName, refs: Seq[AttributeEntityReference]) =>
            val refRecords: Seq[CompactEntityRefRecord] = refs.map(ref =>
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
  // TODO CORE-362: make visible for unit tests
  private def findAllReferences(entity: Entity): Map[AttributeName, Seq[AttributeEntityReference]] =
    entity.attributes
      .collect {
        case (name: AttributeName, ref: AttributeEntityReference)         => Seq((name, ref))
        case (name: AttributeName, refList: AttributeEntityReferenceList) => refList.list.map(ref => (name, ref))
      }
      .flatten
      .toSeq
      .groupMap(_._1)(_._2)

  // given already-validated references, including target ids, update the ENTITY_REFS table for a given source
  // entity
  private def replaceReferences(fromId: Long,
                                foundRefs: Map[AttributeName, Seq[CompactEntityRefRecord]],
                                isInsert: Boolean = false
  ): Future[Map[AttributeName, Seq[CompactEntityRefRecord]]] = {
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
        // TODO CORE-362: instead of (retrieve all, then calculate diffs, then execute diffs), try doing it all in the db:
        //  - delete from ENTITY_REFS where from_id = $fromId and to_id not in ($currentEntityRefTargets)
        //  - insert into ENTITY_REFS (from_id, to_id) values ($fromId, $currentEntityRefTargets:_*) on duplicate key update from_id=from_id (noop)
        // TODO CORE-362: remove verbose logging
        // retrieve all existing refs in ENTITY_REFS for this entity; create a set of the target ids
        existingRowsSeq <-
          if (isInsert) {
            slick.dbio.DBIO.successful(Seq.empty[Long])
          } else {
            dataAccess.compactEntityRefSlickQuery.filter(_.fromId === fromId).map(_.toId).result
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
          if (inserts.nonEmpty) { dataAccess.compactEntityRefSlickQuery.map(r => (r.fromId, r.toId)) ++= insertPairs }
          else { slick.dbio.DBIO.successful(0) }
        //        insertResult <- dataAccess.jsonEntityQuery.bulkInsertReferences(fromId, inserts)
        _ = logger.trace(s"~~~~~ actually inserted ${insertResult} rows for entity $fromId")
        // delete what needs to be deleted
        deleteResult <-
          if (deletes.nonEmpty) {
            dataAccess.compactEntityRefSlickQuery
              .filter(x => x.fromId === fromId && x.toId.inSetBind(deletes))
              .delete
          } else { slick.dbio.DBIO.successful(0) }
        _ = logger.trace(s"~~~~~ actually deleted ${deleteResult} rows for entity $fromId")
      } yield foundRefs
    }
  }

}
