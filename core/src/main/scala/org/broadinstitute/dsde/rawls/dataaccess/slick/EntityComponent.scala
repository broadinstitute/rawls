package org.broadinstitute.dsde.rawls.dataaccess.slick

import akka.http.scaladsl.model.StatusCodes
import com.google.common.annotations.VisibleForTesting
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.entities.EntityUtils
import org.broadinstitute.dsde.rawls.model.Attributable.AttributeMap
import org.broadinstitute.dsde.rawls.model.{Workspace, _}
import org.broadinstitute.dsde.rawls.util.TracingUtils.traceDBIOWithParent
import slick.dbio.Effect.Read
import slick.jdbc.{GetResult, JdbcProfile}
import slick.sql.SqlStreamingAction

import java.sql.Timestamp
import java.util.{Date, UUID}
import scala.language.postfixOps

//noinspection TypeAnnotation
sealed trait EntityRecordBase {
  val id: Long
  val name: String
  val entityType: String
  val workspaceId: UUID
  val recordVersion: Long
  val deleted: Boolean
  val deletedDate: Option[Timestamp]

  def toReference = AttributeEntityReference(entityType, name)
}

case class EntityRecord(id: Long,
                        name: String,
                        entityType: String,
                        workspaceId: UUID,
                        recordVersion: Long,
                        deleted: Boolean,
                        deletedDate: Option[Timestamp]
) extends EntityRecordBase

// result structure from entity and attribute list raw sql
case class EntityAndAttributesResult(entityRecord: EntityRecord,
                                     attributeRecord: Option[EntityAttributeRecord],
                                     refEntityRecord: Option[EntityRecord]
)

import slick.jdbc.MySQLProfile.api._

sealed abstract class EntityTableBase[RECORD_TYPE <: EntityRecordBase](tag: Tag)
    extends Table[RECORD_TYPE](tag, "ENTITY") {
  def id = column[Long]("id", O.PrimaryKey, O.AutoInc)
  def name = column[String]("name", O.Length(254))
  def entityType = column[String]("entity_type", O.Length(254))
  def workspaceId = column[UUID]("workspace_id")
  def version = column[Long]("record_version")
  def deleted = column[Boolean]("deleted")
  def deletedDate = column[Option[Timestamp]]("deleted_date")
}

class EntityTable(tag: Tag) extends EntityTableBase[EntityRecord](tag) {
  def * =
    (id, name, entityType, workspaceId, version, deleted, deletedDate) <> (EntityRecord.tupled, EntityRecord.unapply)
}

//noinspection TypeAnnotation
trait EntityComponent {
  this: DriverComponent with WorkspaceComponent with AttributeComponent =>

  object entityQuery extends TableQuery(new EntityTable(_)) with LazyLogging {

    type EntityQuery = Query[EntityTable, EntityRecord, Seq]

    @VisibleForTesting
    private def batchInsertEntities(workspaceContext: Workspace,
                                    entities: IterableOnce[Entity]
    ): ReadWriteAction[Seq[EntityRecord]] = {
      def marshalNewEntity(entity: Entity, workspaceId: UUID): EntityRecord =
        EntityRecord(0, entity.name, entity.entityType, workspaceId, 0, deleted = false, deletedDate = None)

      if (entities.iterator.nonEmpty) {
        val entityRecs = entities.iterator.toSeq.map(e => marshalNewEntity(e, workspaceContext.workspaceIdAsUUID))

        workspaceQuery.updateLastModified(workspaceContext.workspaceIdAsUUID) andThen
          DBIO
            .sequence(entityRecs.grouped(batchSize).map(entityQuery ++= _))
            .map(_.flatten.sum)
            .andThen(
              entityQuery.getEntityRecords(workspaceContext.workspaceIdAsUUID, entityRecs.map(_.toReference).toSet)
            )
      } else {
        DBIO.successful(Seq.empty[EntityRecord])
      }
    }

    @VisibleForTesting
    private def insertNewEntities(workspaceContext: Workspace,
                                  entities: Iterable[Entity],
                                  existingEntityRefs: Seq[AttributeEntityReference]
    ): ReadWriteAction[Seq[EntityRecord]] = {
      val newEntities = entities.filterNot(e => existingEntityRefs.contains(e.toReference))
      // only insert the first instance of each entity, if the input contains duplicates.
      // without this distinct, we'll get DB errors because we're inserting the same
      // entityType + entityName combination twice.
      val insertableEntities = newEntities.iterator.distinctBy(_.toReference)

      batchInsertEntities(workspaceContext, insertableEntities)
    }

    @VisibleForTesting
    def optimisticLockUpdate(entityRecs: Seq[EntityRecord]): ReadWriteAction[Seq[Int]] = {
      def findEntityByIdAndVersion(id: Long, version: Long): EntityQuery =
        filter(rec => rec.id === id && rec.version === version)

      def optimisticLockUpdateOne(originalRec: EntityRecord): ReadWriteAction[Int] =
        findEntityByIdAndVersion(originalRec.id, originalRec.recordVersion) update originalRec.copy(recordVersion =
          originalRec.recordVersion + 1
        ) map {
          case 0 =>
            throw new RawlsConcurrentModificationException(
              s"could not update $originalRec because its record version has changed"
            )
          case success => success
        }

      DBIO.sequence(entityRecs map optimisticLockUpdateOne)
    }

    // Raw queries - used when querying for multiple AttributeEntityReferences

    // noinspection SqlDialectInspection,DuplicatedCode
    private object EntityRecordRawSqlQuery extends RawSqlQuery {
      val driver: JdbcProfile = EntityComponent.this.driver
      implicit val getEntityRecord: GetResult[EntityRecord] =
        GetResult(r => EntityRecord(r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<))

      def action(workspaceId: UUID, entities: Set[AttributeEntityReference]): ReadAction[Seq[EntityRecord]] =
        if (entities.isEmpty) {
          DBIO.successful(Seq.empty[EntityRecord])
        } else {
          val baseSelect =
            sql"select id, name, entity_type, workspace_id, record_version, deleted, deleted_date from ENTITY where workspace_id = $workspaceId and ("
          val entityTypeNameTuples = reduceSqlActionsWithDelim(
            entities.map(entity => sql"(entity_type = ${entity.entityType} and name = ${entity.entityName})").toSeq,
            sql" OR "
          )
          concatSqlActions(baseSelect, entityTypeNameTuples, sql")").as[EntityRecord]
        }

      @VisibleForTesting
      def batchHide(workspaceId: UUID, entities: Seq[AttributeEntityReference]): ReadWriteAction[Seq[Int]] = {
        // get unique suffix for renaming
        val renameSuffix = "_" + getSufficientlyRandomSuffix(1000000000) // 1 billion
        val deletedDate = new Timestamp(new Date().getTime)

        // start of the SQL statement:
        val baseUpdateSql =
          sql"""update ENTITY set deleted=1, deleted_date=$deletedDate, name=CONCAT(name, $renameSuffix)
             where deleted=0 AND workspace_id=$workspaceId """

        // optimize for the common case where all entities being deleted have the same type
        val distinctTypes = entities.map(_.entityType).distinct

        val criteriaSql = if (distinctTypes.size == 1) {
          // and entity_type='mytype' and name in ('foo', 'bar', 'baz')
          val matchers = sql"""and entity_type=${distinctTypes.head} and name in ("""
          val entityTypeNameTuples = reduceSqlActionsWithDelim(entities.map(ref => sql"${ref.entityName}"))
          concatSqlActions(matchers, entityTypeNameTuples, sql")")
        } else {
          // and ( (entity_type='mytype1' and name='foo') or (entity_type='mytype2' and name='bar') or (entity_type='mytype3' and name='baz') )
          val matchers = sql"""and ("""
          val entityTypeNameTuples = reduceSqlActionsWithDelim(
            entities.map { ref =>
              sql"(entity_type = ${ref.entityType} and name = ${ref.entityName})"
            },
            sql" OR "
          )
          concatSqlActions(matchers, entityTypeNameTuples, sql")")
        }

        concatSqlActions(baseUpdateSql, criteriaSql).as[Int]
      }

    }

    // noinspection ScalaDocMissingParameterDescription,SqlDialectInspection,RedundantBlock,DuplicatedCode
    private object EntityAndAttributesRawSqlQuery extends RawSqlQuery {
      val driver: JdbcProfile = EntityComponent.this.driver

      // tells slick how to convert a result row from a raw sql query to an instance of EntityAndAttributesResult
      implicit val getEntityAndAttributesResult: GetResult[EntityAndAttributesResult] = GetResult { r =>
        // note that the number and order of all the r.<< match precisely with the select clause of baseEntityAndAttributeSql
        val entityRec = EntityRecord(r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<)

        val attributeIdOption: Option[Long] = r.<<
        val attributeRecOption = attributeIdOption.map(id =>
          EntityAttributeRecord(id, entityRec.id, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<)
        )

        val refEntityRecOption = for {
          attributeRec <- attributeRecOption
          _ <- attributeRec.valueEntityRef
        } yield EntityRecord(r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<)

        EntityAndAttributesResult(entityRec, attributeRecOption, refEntityRecOption)
      }

      // the where clause for this query is filled in specific to the use case
      @VisibleForTesting
      def baseEntityAndAttributeSql(workspace: Workspace): String =
        baseEntityAndAttributeSql(
          workspace.workspaceIdAsUUID
        )

      @VisibleForTesting
      private def baseEntityAndAttributeSql(workspaceId: UUID): String =
        baseEntityAndAttributeSql(determineShard(workspaceId))

      @VisibleForTesting
      private def baseEntityAndAttributeSql(shardId: ShardId): String =
        s"""select e.id, e.name, e.entity_type, e.workspace_id, e.record_version, e.deleted, e.deleted_date,
          a.id, a.namespace, a.name, a.value_string, a.value_number, a.value_boolean, a.value_json, a.value_entity_ref, a.list_index, a.list_length, a.deleted, a.deleted_date,
          e_ref.id, e_ref.name, e_ref.entity_type, e_ref.workspace_id, e_ref.record_version, e_ref.deleted, e_ref.deleted_date
          from ENTITY e
          left outer join ENTITY_ATTRIBUTE_$shardId a on a.owner_id = e.id and a.deleted = e.deleted
          left outer join ENTITY e_ref on a.value_entity_ref = e_ref.id"""

      // Active actions: only return entities and attributes with their deleted flag set to false

      // actions which may include "deleted" hidden entities

      @VisibleForTesting
      def streamForTypeName(workspaceContext: Workspace,
                            entityType: String,
                            entityName: String,
                            desiredFields: Set[AttributeName]
      ): SqlStreamingAction[Seq[EntityAndAttributesResult], EntityAndAttributesResult, Read] = {
        // user requested specific attributes. include them in the where clause.
        val attrNamespaceNameTuples = reduceSqlActionsWithDelim(desiredFields.toSeq.map { attrName =>
          sql"(${attrName.namespace}, ${attrName.name})"
        })
        val attributesFilter =
          if (desiredFields.isEmpty) sql""
          else concatSqlActions(sql" and (a.namespace, a.name) in (", attrNamespaceNameTuples, sql")")

        concatSqlActions(
          sql"""#${baseEntityAndAttributeSql(
              workspaceContext
            )} where e.name = ${entityName} and e.entity_type = ${entityType} and e.workspace_id = ${workspaceContext.workspaceIdAsUUID}""",
          attributesFilter
        ).as[EntityAndAttributesResult]
      }

      @VisibleForTesting
      def batchHide(workspaceContext: Workspace, entities: Seq[AttributeEntityReference]): ReadWriteAction[Seq[Int]] = {
        val shardId = determineShard(workspaceContext.workspaceIdAsUUID)
        // get unique suffix for renaming
        val renameSuffix = "_" + getSufficientlyRandomSuffix(1000000000) // 1 billion
        val deletedDate = new Timestamp(new Date().getTime)
        // issue bulk rename/hide for all entity attributes, given a set of entities
        val baseUpdate =
          sql"""update ENTITY_ATTRIBUTE_#$shardId ea join ENTITY e on ea.owner_id = e.id
                set ea.deleted=1, ea.deleted_date=$deletedDate, ea.name=CONCAT(ea.name, $renameSuffix)
                where e.workspace_id=${workspaceContext.workspaceIdAsUUID} and ea.deleted=0 and ("""
        val entityTypeNameTuples = reduceSqlActionsWithDelim(
          entities.map { ref =>
            sql"(e.entity_type = ${ref.entityType} and e.name = ${ref.entityName})"
          },
          sql" OR "
        )
        concatSqlActions(baseUpdate, entityTypeNameTuples, sql")").as[Int]
      }

    }

    // Raw query for performing actual deletion (not hiding) of everything that depends on an entity

    /*
      These methods are only used by unit tests.
      They return full, materialized result sets without streaming, and these result sets can be
      quite large in real-world usage. Do not use those methods in user-facing runtime code.
     */
    object UnitTestHelpers extends RawSqlQuery {

      val driver: JdbcProfile = EntityComponent.this.driver

      @VisibleForTesting
      def listActiveEntitiesOfType(workspaceContext: Workspace, entityType: String): ReadAction[IterableOnce[Entity]] =
        sql"""#${EntityAndAttributesRawSqlQuery.baseEntityAndAttributeSql(workspaceContext)}
        where e.deleted = false
        and e.entity_type = $entityType
        and e.workspace_id = ${workspaceContext.workspaceIdAsUUID}"""
          .as[EntityAndAttributesResult](EntityAndAttributesRawSqlQuery.getEntityAndAttributesResult)
          .map(query => unmarshalEntities(query))

      // includes "deleted" hidden entities
      @VisibleForTesting
      def listEntities(workspaceContext: Workspace): ReadAction[IterableOnce[Entity]] =
        sql"""#${EntityAndAttributesRawSqlQuery.baseEntityAndAttributeSql(
            workspaceContext
          )} where e.workspace_id = ${workspaceContext.workspaceIdAsUUID}"""
          .as[EntityAndAttributesResult](EntityAndAttributesRawSqlQuery.getEntityAndAttributesResult)
          .map(query => unmarshalEntities(query))

    }

    // Slick queries

    // Active queries: only return entities and attributes with their deleted flag set to false

    @VisibleForTesting
    def findActiveEntityByType(workspaceId: UUID, entityType: String): EntityQuery =
      filter(entRec => entRec.entityType === entityType && entRec.workspaceId === workspaceId && !entRec.deleted)

    @VisibleForTesting
    def findActiveEntityByWorkspace(workspaceId: UUID): EntityQuery =
      filter(entRec => entRec.workspaceId === workspaceId && !entRec.deleted)

    // queries which may include "deleted" hidden entities

    def findEntityByName(workspaceId: UUID, entityType: String, entityName: String): EntityQuery =
      filter(entRec =>
        entRec.name === entityName && entRec.entityType === entityType && entRec.workspaceId === workspaceId
      )

    def findEntityById(id: Long): EntityQuery =
      filter(_.id === id)

    // Actions

    // get a specific entity or set of entities: may include "hidden" deleted entities if not named "active"
    @VisibleForTesting
    def get(workspaceContext: Workspace,
            entityType: String,
            entityName: String,
            desiredFields: Set[AttributeName] = Set.empty
    ): ReadAction[Option[Entity]] =
      EntityAndAttributesRawSqlQuery.streamForTypeName(workspaceContext, entityType, entityName, desiredFields) map (
        query => unmarshalEntities(query)
      ) map (_.headOption)

    def getEntityRecords(workspaceId: UUID, entities: Set[AttributeEntityReference]): ReadAction[Seq[EntityRecord]] = {
      val entitiesGrouped = entities.grouped(batchSize).toSeq

      DBIO
        .sequence(entitiesGrouped map { batch =>
          EntityRecordRawSqlQuery.action(workspaceId, batch)
        })
        .map(_.flatten)
    }

    // create or replace entities

    // TODO: can this be optimized? It nicely reuses the save(..., entities) method, but that method
    // does a lot of work. This single-entity save could, for instance, look for simple cases e.g. no references,
    // and take an easier code path.
    @VisibleForTesting
    def save(workspaceContext: Workspace, entity: Entity): ReadWriteAction[Entity] =
      save(workspaceContext, Seq(entity)).map(_.head)

    @VisibleForTesting
    def save(workspaceContext: Workspace,
             entities: Iterable[Entity],
             parentContext: RawlsTracingContext = RawlsTracingContext()
    ): ReadWriteAction[Iterable[Entity]] = {
      entities.foreach(EntityUtils.validateEntity)

      for {
        _ <- traceDBIOWithParent("updateLastModified", parentContext)(_ =>
          workspaceQuery.updateLastModified(workspaceContext.workspaceIdAsUUID)
        )
        preExistingEntityRecs <- traceDBIOWithParent("getEntityRecords", parentContext)(_ =>
          getEntityRecords(workspaceContext.workspaceIdAsUUID, entities.map(_.toReference).toSet)
        )
        savingEntityRecs <- traceDBIOWithParent("insertNewEntities", parentContext)(_ =>
          entityQuery
            .insertNewEntities(workspaceContext, entities, preExistingEntityRecs.map(_.toReference))
            .map(_ ++ preExistingEntityRecs)
        )
        referencedAndSavingEntityRecs <- traceDBIOWithParent("lookupNotYetLoadedReferences", parentContext)(_ =>
          lookupNotYetLoadedReferences(workspaceContext, entities, savingEntityRecs.map(_.toReference))
            .map(_ ++ savingEntityRecs)
        )

        actuallyUpdatedEntityIds <- rewriteAttributes(
          workspaceContext.workspaceIdAsUUID,
          entities,
          savingEntityRecs.map(_.id),
          referencedAndSavingEntityRecs.map(e => e.toReference -> e.id).toMap,
          parentContext
        )
        // find the pre-existing records that we updated
        actuallyUpdatedPreExistingEntityRecs = preExistingEntityRecs.filter(e =>
          actuallyUpdatedEntityIds.contains(e.id)
        )

        // find any entities that were repeated in the input payload. These repeated entities
        // may translate to one insert and one update.
        repeats = entities.groupBy(_.toReference).filter(_._2.size > 1).keySet
        // narrow the repeats to only those that triggered an insert (as opposed to repeats
        // being multiple updates)
        insertedRepeats = savingEntityRecs.filter(e => repeats.contains(e.toReference))

        // the records that need a call to optimisticLockUpdate are those that:
        //  1) pre-existed and were updated
        //  2) were repeated in the input payload, causing one insert and subsequent update(s)
        recsToUpdate = (actuallyUpdatedPreExistingEntityRecs ++ insertedRepeats).distinct

        _ <- traceDBIOWithParent("optimisticLockUpdate", parentContext)(_ =>
          entityQuery.optimisticLockUpdate(recsToUpdate)
        )
      } yield entities
    }

    @VisibleForTesting
    private def lookupNotYetLoadedReferences(workspaceContext: Workspace,
                                             entities: Iterable[Entity],
                                             alreadyLoadedEntityRefs: Seq[AttributeEntityReference]
    ): ReadAction[Seq[EntityRecord]] = {
      val allRefAttributes = (for {
        entity <- entities
        (_, attribute) <- entity.attributes
        ref <- attribute match {
          case AttributeEntityReferenceList(l) => l
          case r: AttributeEntityReference     => Seq(r)
          case _                               => Seq.empty
        }
      } yield ref).toSet

      lookupNotYetLoadedReferences(workspaceContext, allRefAttributes, alreadyLoadedEntityRefs)
    }

    @VisibleForTesting
    private def lookupNotYetLoadedReferences(workspaceContext: Workspace,
                                             attrReferences: Set[AttributeEntityReference],
                                             alreadyLoadedEntityRefs: Seq[AttributeEntityReference]
    ): ReadAction[Seq[EntityRecord]] = {
      val notYetLoadedEntityRecs = attrReferences -- alreadyLoadedEntityRefs

      getEntityRecords(workspaceContext.workspaceIdAsUUID, notYetLoadedEntityRecs) map { foundEntities =>
        if (foundEntities.size != notYetLoadedEntityRecs.size) {
          val notFoundRefs = notYetLoadedEntityRecs -- foundEntities.map(_.toReference)
          throw new RawlsExceptionWithErrorReport(
            ErrorReport(
              StatusCodes.BadRequest,
              "Could not resolve some entity references",
              notFoundRefs.map { missingRef =>
                ErrorReport(s"${missingRef.entityType} ${missingRef.entityName} not found", Seq.empty)
              }.toSeq
            )
          )
        } else {
          foundEntities
        }
      }
    }

    @VisibleForTesting
    private def rewriteAttributes(workspaceId: UUID,
                                  entitiesToSave: Iterable[Entity],
                                  entityIds: Seq[Long],
                                  entityIdsByRef: Map[AttributeEntityReference, Long],
                                  parentContext: RawlsTracingContext = RawlsTracingContext()
    ) = {
      val attributesToSave = for {
        entity <- entitiesToSave
        (attributeName, attribute) <- entity.attributes
        attributeRec <- entityAttributeShardQuery(workspaceId).marshalAttribute(entityIdsByRef(entity.toReference),
                                                                                attributeName,
                                                                                attribute,
                                                                                entityIdsByRef
        )
      } yield attributeRec

      entityAttributeShardQuery(workspaceId).findByOwnerQuery(entityIds).result flatMap { existingAttributes =>
        entityAttributeShardQuery(workspaceId).rewriteAttrsAction(attributesToSave,
                                                                  existingAttributes,
                                                                  entityAttributeTempQuery.insertScratchAttributes,
                                                                  parentContext
        )
      }
    }

    // "delete" entities by hiding and renaming. we must rename the entity to avoid future name collisions if the user
    // attempts to create a new entity of the same name.
    @VisibleForTesting
    def hide(workspaceContext: Workspace, entRefs: Seq[AttributeEntityReference]): ReadWriteAction[Int] =
      // N.B. we must hide both the entity attributes and the entity itself. Other queries, such
      // as baseEntityAndAttributeSql, use "where ENTITY.deleted = ENTITY_ATTRIBUTE.deleted" during joins.
      // Thus, we need to keep the "deleted" value for attributes in sync with their parent entity,
      // when hiding that entity.
      workspaceQuery.updateLastModified(workspaceContext.workspaceIdAsUUID) andThen
        EntityAndAttributesRawSqlQuery.batchHide(workspaceContext, entRefs) andThen
        EntityRecordRawSqlQuery.batchHide(workspaceContext.workspaceIdAsUUID, entRefs).map(res => res.sum)

    // perform actual deletion (not hiding) of all entities in a workspace
    def deleteFromDb(workspaceContext: Workspace): WriteAction[Int] =
      filter(_.workspaceId === workspaceContext.workspaceIdAsUUID).delete

    // Unmarshal methods

    private def unmarshalEntity(entityRecord: EntityRecord, attributes: AttributeMap): Entity =
      Entity(entityRecord.name, entityRecord.entityType, attributes)

    def unmarshalEntities(
      entityAttributeRecords: Seq[EntityAndAttributesResult]
    ): Seq[Entity] =
      unmarshalEntitiesWithIds(entityAttributeRecords).map { case (_, entity) => entity }

    private def unmarshalEntitiesWithIds(
      entityAttributeRecords: Seq[EntityAndAttributesResult]
    ): Seq[(Long, Entity)] = {
      val allEntityRecords = entityAttributeRecords.map(_.entityRecord).distinct

      // note that not all entities have attributes, thus the collect below
      val entitiesWithAttributes = entityAttributeRecords.collect {
        case EntityAndAttributesResult(entityRec, Some(attributeRec), refEntityRecOption) =>
          ((entityRec.id, attributeRec), refEntityRecOption)
      }

      val attributesByEntityId =
        entityAttributeShardQuery(UUID.randomUUID()).unmarshalAttributes(entitiesWithAttributes)

      allEntityRecords.map { entityRec =>
        entityRec.id -> unmarshalEntity(entityRec, attributesByEntityId.getOrElse(entityRec.id, Map.empty))
      }
    }
  }

}
