package org.broadinstitute.dsde.rawls.dataaccess.slick

import java.sql.Timestamp
import java.util.{Date, UUID}
import akka.http.scaladsl.model.StatusCodes
import org.broadinstitute.dsde.rawls.model.Attributable.AttributeMap
import org.broadinstitute.dsde.rawls.model.WorkspaceShardStates.WorkspaceShardState
import org.broadinstitute.dsde.rawls.model.{Workspace, _}
import org.broadinstitute.dsde.rawls.util.CollectionUtils
import org.broadinstitute.dsde.rawls.{RawlsException, RawlsExceptionWithErrorReport, RawlsFatalExceptionWithErrorReport, model}
import slick.jdbc.{GetResult, JdbcProfile}

import scala.annotation.tailrec
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
  def withoutAllAttributeValues = EntityRecord(id, name, entityType, workspaceId, recordVersion, deleted, deletedDate)
  def withAllAttributeValues(allAttributeValues: Option[String]) = EntityRecordWithInlineAttributes(id, name, entityType, workspaceId, recordVersion, allAttributeValues, deleted, deletedDate)
}

case class EntityRecord(id: Long,
                        name: String,
                        entityType: String,
                        workspaceId: UUID,
                        recordVersion: Long,
                        deleted: Boolean,
                        deletedDate: Option[Timestamp]) extends EntityRecordBase

case class EntityRecordWithInlineAttributes(id: Long,
                                            name: String,
                                            entityType: String,
                                            workspaceId: UUID,
                                            recordVersion: Long,
                                            allAttributeValues: Option[String],
                                            deleted: Boolean,
                                            deletedDate: Option[Timestamp]) extends EntityRecordBase

object EntityComponent {
  //the length of the all_attribute_values column, which is TEXT, -1 becaue i'm nervous
  val allAttributeValuesColumnSize = 65534
}

import slick.jdbc.MySQLProfile.api._

sealed abstract class EntityTableBase[RECORD_TYPE <: EntityRecordBase](tag: Tag) extends Table[RECORD_TYPE](tag, "ENTITY") {
  def id = column[Long]("id", O.PrimaryKey, O.AutoInc)
  def name = column[String]("name", O.Length(254))
  def entityType = column[String]("entity_type", O.Length(254))
  def workspaceId = column[UUID]("workspace_id")
  def version = column[Long]("record_version")
  def deleted = column[Boolean]("deleted")
  def deletedDate = column[Option[Timestamp]]("deleted_date")

//  def workspace = foreignKey("FK_ENTITY_WORKSPACE", workspaceId, workspaceQuery)(_.id)
  def uniqueTypeName = index("idx_entity_type_name", (workspaceId, entityType, name), unique = true)
}

class EntityTable(tag: Tag) extends EntityTableBase[EntityRecord](tag) {
  def * = (id, name, entityType, workspaceId, version, deleted, deletedDate) <> (EntityRecord.tupled, EntityRecord.unapply)
}

class EntityTableWithInlineAttributes(tag: Tag) extends EntityTableBase[EntityRecordWithInlineAttributes](tag) {
  def allAttributeValues = column[Option[String]]("all_attribute_values")

  def * = (id, name, entityType, workspaceId, version, allAttributeValues, deleted, deletedDate) <> (EntityRecordWithInlineAttributes.tupled, EntityRecordWithInlineAttributes.unapply)
}

//noinspection TypeAnnotation
trait EntityComponent {
  this: DriverComponent
    with WorkspaceComponent
    with AttributeComponent
    with EntityTypeStatisticsComponent
    with EntityAttributeStatisticsComponent =>

  object entityQueryWithInlineAttributes extends TableQuery(new EntityTableWithInlineAttributes(_)) {
    type EntityQueryWithInlineAttributes = Query[EntityTableWithInlineAttributes, EntityRecordWithInlineAttributes, Seq]

    // only used by tests
    def findEntityByName(workspaceId: UUID, entityType: String, entityName: String): EntityQueryWithInlineAttributes = {
      filter(entRec => entRec.name === entityName && entRec.entityType === entityType && entRec.workspaceId === workspaceId)
    }

    @tailrec
    def collectAttributeStrings(toProcess: Iterable[Attribute], processed: List[String], charsRemaining: Int): String = {
      if (toProcess.isEmpty || charsRemaining <= 0)
        processed.mkString(" ")
      else {
        val nextAttr = AttributeStringifier(toProcess.head)
        collectAttributeStrings(toProcess.tail, processed :+ nextAttr, charsRemaining - nextAttr.length - 1)
      }
    }

    private def createAllAttributesString(entity: Entity): Option[String] = {
      val maxLength = EntityComponent.allAttributeValuesColumnSize
      Option(s"${entity.name} ${collectAttributeStrings(entity.attributes.values.filterNot(_.isInstanceOf[AttributeList[_]]), List(), maxLength)}".toLowerCase.take(maxLength))
    }

    def batchInsertEntities(workspaceContext: Workspace, entities: TraversableOnce[Entity]): ReadWriteAction[Seq[EntityRecord]] = {
      def marshalNewEntity(entity: Entity, workspaceId: UUID): EntityRecordWithInlineAttributes = {
        EntityRecordWithInlineAttributes(0, entity.name, entity.entityType, workspaceId, 0, createAllAttributesString(entity), deleted = false, deletedDate = None)
      }

      if (entities.nonEmpty) {
        val entityRecs = entities.toSeq.map(e => marshalNewEntity(e, workspaceContext.workspaceIdAsUUID))

        workspaceQuery.updateLastModified(workspaceContext.workspaceIdAsUUID) andThen
          DBIO.sequence(entityRecs.grouped(batchSize).map(entityQueryWithInlineAttributes ++= _)).map(_.flatten.sum).andThen(
            entityQuery.getEntityRecords(workspaceContext.workspaceIdAsUUID, entityRecs.map(_.toReference).toSet))
      }
      else {
        DBIO.successful(Seq.empty[EntityRecord])
      }
    }

    def insertNewEntities(workspaceContext: Workspace, entities: Traversable[Entity], existingEntityRefs: Seq[AttributeEntityReference]): ReadWriteAction[Seq[EntityRecord]] = {
      val newEntities = entities.filterNot { e => existingEntityRefs.contains(e.toReference) }
      batchInsertEntities(workspaceContext, newEntities)
    }

    def optimisticLockUpdate(entityRecs: Seq[EntityRecord], entities: Traversable[Entity]): ReadWriteAction[Seq[Int]] = {
      def populateAllAttributeValues(entityRecsFromDb: Seq[EntityRecord], entitiesToSave: Traversable[Entity]): Seq[EntityRecordWithInlineAttributes] = {
        val entitiesByRef = entitiesToSave.map(e => e.toReference -> e).toMap
        entityRecsFromDb map { rec => rec.withAllAttributeValues(createAllAttributesString(entitiesByRef(rec.toReference))) }
      }

      def findEntityByIdAndVersion(id: Long, version: Long): EntityQueryWithInlineAttributes = {
        filter(rec => rec.id === id && rec.version === version)
      }

      def optimisticLockUpdateOne(originalRec: EntityRecordWithInlineAttributes): ReadWriteAction[Int] = {
        findEntityByIdAndVersion(originalRec.id, originalRec.recordVersion) update originalRec.copy(recordVersion = originalRec.recordVersion + 1) map {
          case 0 => throw new RawlsConcurrentModificationException(s"could not update $originalRec because its record version has changed")
          case success => success
        }
      }

      DBIO.sequence(populateAllAttributeValues(entityRecs, entities) map optimisticLockUpdateOne)
    }
  }

  object entityQuery extends TableQuery(new EntityTable(_)) {

    type EntityQuery = Query[EntityTable, EntityRecord, Seq]
    type EntityAttributeQuery = Query[EntityAttributeTable, EntityAttributeRecord, Seq]

    // Raw queries - used when querying for multiple AttributeEntityReferences

    //noinspection SqlDialectInspection,DuplicatedCode
    private object EntityRecordRawSqlQuery extends RawSqlQuery {
      val driver: JdbcProfile = EntityComponent.this.driver
      implicit val getEntityRecord = GetResult { r => EntityRecord(r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<) }

      def action(workspaceId: UUID, entities: Set[AttributeEntityReference]): ReadAction[Seq[EntityRecord]] = {
        if( entities.isEmpty ) {
          DBIO.successful(Seq.empty[EntityRecord])
        } else {
          val baseSelect = sql"select id, name, entity_type, workspace_id, record_version, deleted, deleted_date from ENTITY where workspace_id = $workspaceId and (entity_type, name) in ("
          val entityTypeNameTuples = reduceSqlActionsWithDelim(entities.map { entity => sql"(${entity.entityType}, ${entity.entityName})" }.toSeq)
          concatSqlActions(baseSelect, entityTypeNameTuples, sql")").as[EntityRecord]
        }
      }

      def batchHide(workspaceId: UUID, entities: Seq[AttributeEntityReference]): ReadWriteAction[Seq[Int]] = {
        // get unique suffix for renaming
        val renameSuffix = "_" + getSufficientlyRandomSuffix(1000000000) // 1 billion
        val deletedDate = new Timestamp(new Date().getTime)
        // issue bulk rename/hide for all entities
        val baseUpdate = sql"""update ENTITY set deleted=1, deleted_date=$deletedDate, name=CONCAT(name, $renameSuffix) where deleted=0 AND workspace_id=$workspaceId and (entity_type, name) in ("""
        val entityTypeNameTuples = reduceSqlActionsWithDelim(entities.map { ref => sql"(${ref.entityType}, ${ref.entityName})" })
        concatSqlActions(baseUpdate, entityTypeNameTuples, sql")").as[Int]
      }

      def activeActionForRefs(workspaceId: UUID, entities: Set[AttributeEntityReference]): ReadAction[Seq[AttributeEntityReference]] = {
        if( entities.isEmpty ) {
          DBIO.successful(Seq.empty[AttributeEntityReference])
        } else {
          val baseSelect = sql"select entity_type, name from ENTITY where workspace_id = $workspaceId and deleted = 0 and (entity_type, name) in ("
          val entityTypeNameTuples = reduceSqlActionsWithDelim(entities.map { entity => sql"(${entity.entityType}, ${entity.entityName})" }.toSeq)
          concatSqlActions(baseSelect, entityTypeNameTuples, sql")").as[(String, String)].map { vect => vect.map { pair => AttributeEntityReference(pair._1, pair._2) } }
        }
      }

    }

    //noinspection ScalaDocMissingParameterDescription,SqlDialectInspection,RedundantBlock,DuplicatedCode
    private object EntityAndAttributesRawSqlQuery extends RawSqlQuery {
      val driver: JdbcProfile = EntityComponent.this.driver

      // result structure from entity and attribute list raw sql
      case class EntityAndAttributesResult(entityRecord: EntityRecord, attributeRecord: Option[EntityAttributeRecord], refEntityRecord: Option[EntityRecord])

      // tells slick how to convert a result row from a raw sql query to an instance of EntityAndAttributesResult
      implicit val getEntityAndAttributesResult = GetResult { r =>
        // note that the number and order of all the r.<< match precisely with the select clause of baseEntityAndAttributeSql
        val entityRec = EntityRecord(r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<)

        val attributeIdOption: Option[Long] = r.<<
        val attributeRecOption = attributeIdOption.map(id => EntityAttributeRecord(id, entityRec.id, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<))

        val refEntityRecOption = for {
          attributeRec <- attributeRecOption
          _ <- attributeRec.valueEntityRef
        } yield {
          EntityRecord(r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<)
        }

        EntityAndAttributesResult(entityRec, attributeRecOption, refEntityRecOption)
      }

      // the where clause for this query is filled in specific to the use case
      def baseEntityAndAttributeSql(workspace: Workspace): String = baseEntityAndAttributeSql(workspace.workspaceIdAsUUID, workspace.shardState)

      def baseEntityAndAttributeSql(workspaceId: UUID, shardState: WorkspaceShardState): String = baseEntityAndAttributeSql(determineShard(workspaceId, shardState))

      private def baseEntityAndAttributeSql(shardId: ShardId): String = {
        s"""select e.id, e.name, e.entity_type, e.workspace_id, e.record_version, e.deleted, e.deleted_date,
          a.id, a.namespace, a.name, a.value_string, a.value_number, a.value_boolean, a.value_json, a.value_entity_ref, a.list_index, a.list_length, a.deleted, a.deleted_date,
          e_ref.id, e_ref.name, e_ref.entity_type, e_ref.workspace_id, e_ref.record_version, e_ref.deleted, e_ref.deleted_date
          from ENTITY e
          left outer join ENTITY_ATTRIBUTE_$shardId a on a.owner_id = e.id and a.deleted = e.deleted
          left outer join ENTITY e_ref on a.value_entity_ref = e_ref.id"""
      }

      // Active actions: only return entities and attributes with their deleted flag set to false

      def activeActionForType(workspaceContext: Workspace, entityType: String): ReadAction[Seq[EntityAndAttributesResult]] = {
        sql"""#${baseEntityAndAttributeSql(workspaceContext)} where e.deleted = false and e.entity_type = ${entityType} and e.workspace_id = ${workspaceContext.workspaceIdAsUUID}""".as[EntityAndAttributesResult]
      }

      def activeActionForRefs(workspaceContext: Workspace, entityRefs: Set[AttributeEntityReference]): ReadAction[Seq[EntityAndAttributesResult]] = {
        if( entityRefs.isEmpty ) {
          DBIO.successful(Seq.empty[EntityAndAttributesResult])
        } else {
          val baseSelect = sql"""#${baseEntityAndAttributeSql(workspaceContext)} where e.deleted = false and e.workspace_id = ${workspaceContext.workspaceIdAsUUID} and (e.entity_type, e.name) in ("""
          val entityTypeNameTuples = reduceSqlActionsWithDelim(entityRefs.map { ref => sql"(${ref.entityType}, ${ref.entityName})" }.toSeq)
          concatSqlActions(baseSelect, entityTypeNameTuples, sql")").as[EntityAndAttributesResult]
        }

      }

      def activeActionForWorkspace(workspaceContext: Workspace): ReadAction[Seq[EntityAndAttributesResult]] = {
        sql"""#${baseEntityAndAttributeSql(workspaceContext)} where e.deleted = false and e.workspace_id = ${workspaceContext.workspaceIdAsUUID}""".as[EntityAndAttributesResult]
      }

      /**
        * Generates a sub query that can be filtered, sorted, sliced
        * @param workspaceId
        * @param entityType
        * @param sortFieldName
        * @return
        */
      private def paginationSubquery(workspaceId: UUID, shardState: WorkspaceShardState, entityType: String, sortFieldName: String) = {
        val shardId = determineShard(workspaceId, shardState)

        val (sortColumns, sortJoin) = sortFieldName match {
          case "name" => (
            // no additional sort columns
            "",
            // no additional join required
            sql"")
          case _ => (
            // select each attribute column and the referenced entity name
            """, sort_a.list_length as sort_list_length, sort_a.value_string as sort_field_string, sort_a.value_number as sort_field_number, sort_a.value_boolean as sort_field_boolean, sort_a.value_json as sort_field_json, sort_e_ref.name as sort_field_ref""",
            // join to attribute and entity (for references) table, grab only the named sort attribute and only the first element of a list
            sql"""left outer join ENTITY_ATTRIBUTE_#$shardId sort_a on sort_a.owner_id = e.id and sort_a.name = $sortFieldName and ifnull(sort_a.list_index, 0) = 0 left outer join ENTITY sort_e_ref on sort_a.value_entity_ref = sort_e_ref.id """)
        }

        concatSqlActions(sql"""select e.id, e.name, e.all_attribute_values #$sortColumns from ENTITY e """, sortJoin, sql""" where e.deleted = 'false' and e.entity_type = $entityType and e.workspace_id = $workspaceId """)
      }

      def activeActionForPagination(workspaceContext: Workspace, entityType: String, entityQuery: model.EntityQuery): ReadAction[(Int, Int, Seq[EntityAndAttributesResult])] = {
        /*
        The query here starts with baseEntityAndAttributeSql which is the typical select
        to pull entities will all attributes and references. A join is added on a sub select from ENTITY
        (and ENTITY_ATTRIBUTE if there is a sort field other than name) which constrains to only entities of the
        right type, workspace, and matches the filters then sorts and slices out the appropriate page of entities.
         */

        def filterSql(prefix: String, alias: String) = {
          val filtersOption = entityQuery.filterTerms.map { _.split(" ").toSeq.map { term =>
            sql"concat(#$alias.name, ' ', #$alias.all_attribute_values) like ${'%' + term.toLowerCase + '%'}"
          }}

          filtersOption match {
            case None => sql""
            case Some(filters) =>
              concatSqlActions(sql"#$prefix ", reduceSqlActionsWithDelim(filters, sql" and "))
          }
        }

        def order(alias: String) = entityQuery.sortField match {
          case "name" => sql" order by #$alias.name #${SortDirections.toSql(entityQuery.sortDirection)} "
          case _ => sql" order by #$alias.sort_list_length #${SortDirections.toSql(entityQuery.sortDirection)}, #$alias.sort_field_string #${SortDirections.toSql(entityQuery.sortDirection)}, #$alias.sort_field_number #${SortDirections.toSql(entityQuery.sortDirection)}, #$alias.sort_field_boolean #${SortDirections.toSql(entityQuery.sortDirection)}, #$alias.sort_field_ref #${SortDirections.toSql(entityQuery.sortDirection)}, #$alias.name #${SortDirections.toSql(entityQuery.sortDirection)} "
        }

        val paginationJoin = concatSqlActions(
          sql""" join (select * from (""",
          paginationSubquery(workspaceContext.workspaceIdAsUUID, workspaceContext.shardState, entityType, entityQuery.sortField),
          sql") pagination ",
          filterSql("where", "pagination"),
          order("pagination"),
          sql" limit #${entityQuery.pageSize} offset #${(entityQuery.page-1) * entityQuery.pageSize} ) p on p.id = e.id "
        )

        def filteredCountQuery: ReadAction[Vector[Int]] = {
          val filteredQuery =
            sql"""select count(1) from ENTITY e
                                   where e.deleted = 0
                                   and e.entity_type = $entityType
                                   and e.workspace_id = ${workspaceContext.workspaceIdAsUUID} """
          concatSqlActions(filteredQuery, filterSql("and", "e")).as[Int]
        }

        for {
          unfilteredCount <- findActiveEntityByType(workspaceContext.workspaceIdAsUUID, entityType).length.result
          filteredCount <- if (entityQuery.filterTerms.isEmpty) {
                              // if the query has no filter, then "filteredCount" and "unfilteredCount" will always be the same; no need to make another query
                              DBIO.successful(Vector(unfilteredCount))
                            } else {
                              filteredCountQuery
                            }
          page <- concatSqlActions(sql"#${baseEntityAndAttributeSql(workspaceContext)}", paginationJoin, order("p")).as[EntityAndAttributesResult]
        } yield (unfilteredCount, filteredCount.head, page)
      }

      // actions which may include "deleted" hidden entities

      def actionForTypeName(workspaceContext: Workspace, entityType: String, entityName: String): ReadAction[Seq[EntityAndAttributesResult]] = {
        sql"""#${baseEntityAndAttributeSql(workspaceContext)} where e.name = ${entityName} and e.entity_type = ${entityType} and e.workspace_id = ${workspaceContext.workspaceIdAsUUID}""".as[EntityAndAttributesResult]
      }

      def actionForIds(workspaceId: UUID, shardState: WorkspaceShardState, entityIds: Set[Long]): ReadAction[Seq[EntityAndAttributesResult]] = {
        if( entityIds.isEmpty ) {
          DBIO.successful(Seq.empty[EntityAndAttributesResult])
        } else {
          val baseSelect = sql"""#${baseEntityAndAttributeSql(workspaceId, shardState)} where e.id in ("""
          val entityIdSql = reduceSqlActionsWithDelim(entityIds.map { id => sql"$id" }.toSeq)
          concatSqlActions(baseSelect, entityIdSql, sql")").as[EntityAndAttributesResult]
        }
      }

      def actionForWorkspace(workspaceContext: Workspace): ReadAction[Seq[EntityAndAttributesResult]] = {
        sql"""#${baseEntityAndAttributeSql(workspaceContext)} where e.workspace_id = ${workspaceContext.workspaceIdAsUUID}""".as[EntityAndAttributesResult]
      }

      def batchHide(workspaceContext: Workspace, entities: Seq[AttributeEntityReference]): ReadWriteAction[Seq[Int]] = {
        val shardId = determineShard(workspaceContext.workspaceIdAsUUID, workspaceContext.shardState)
        // get unique suffix for renaming
        val renameSuffix = "_" + getSufficientlyRandomSuffix(1000000000) // 1 billion
        val deletedDate = new Timestamp(new Date().getTime)
        // issue bulk rename/hide for all entity attributes, given a set of entities
        val baseUpdate =
          sql"""update ENTITY_ATTRIBUTE_#$shardId ea join ENTITY e on ea.owner_id = e.id
                set ea.deleted=1, ea.deleted_date=$deletedDate, ea.name=CONCAT(ea.name, $renameSuffix)
                where e.workspace_id=${workspaceContext.workspaceIdAsUUID} and ea.deleted=0 and (e.entity_type, e.name) in ("""
        val entityTypeNameTuples = reduceSqlActionsWithDelim(entities.map { ref => sql"(${ref.entityType}, ${ref.entityName})" })
        concatSqlActions(baseUpdate, entityTypeNameTuples, sql")").as[Int]
      }
    }

    // Raw query for performing actual deletion (not hiding) of everything that depends on an entity

    //noinspection SqlDialectInspection
    private object EntityDependenciesDeletionQuery extends RawSqlQuery {
      val driver: JdbcProfile = EntityComponent.this.driver

      def deleteAction(workspaceContext: Workspace): WriteAction[Int] = {
        val shardId = determineShard(workspaceContext.workspaceIdAsUUID, workspaceContext.shardState)

        sqlu"""delete ea from ENTITY_ATTRIBUTE_#$shardId ea
               inner join ENTITY e
               on ea.owner_id = e.id
               where e.workspace_id=${workspaceContext.workspaceIdAsUUID}
          """
      }
    }

    // Slick queries

    // Active queries: only return entities and attributes with their deleted flag set to false

    def findActiveEntityByType(workspaceId: UUID, entityType: String): EntityQuery = {
      filter(entRec => entRec.entityType === entityType && entRec.workspaceId === workspaceId && ! entRec.deleted)
    }

    def findActiveEntityByWorkspace(workspaceId: UUID): EntityQuery = {
      filter(entRec => entRec.workspaceId === workspaceId && ! entRec.deleted)
    }

    /**
      * given a set of AttributeEntityReference, query the db and return those refs that
      * are 1) active and 2) exist in the specified workspace. Use this method to validate user input
      * with the lightest SQL query; it does not fetch attributes or extraneous columns
      *
      * @param workspaceId the workspace to query for entity refs
      * @param entities the refs for which to query
      * @return the subset of refs that are active and found in the workspace
      */
    def getActiveRefs(workspaceId: UUID, entities: Set[AttributeEntityReference]): ReadAction[Seq[AttributeEntityReference]] = {
      EntityRecordRawSqlQuery.activeActionForRefs(workspaceId, entities)
    }

    private def findActiveAttributesByEntityId(workspaceId: UUID, shardState: WorkspaceShardState, entityId: Rep[Long]): EntityAttributeQuery = for {
      entityAttrRec <- entityAttributeShardQuery(workspaceId, shardState) if entityAttrRec.ownerId === entityId && ! entityAttrRec.deleted
    } yield entityAttrRec

    // queries which may include "deleted" hidden entities

    def findEntityByName(workspaceId: UUID, entityType: String, entityName: String): EntityQuery = {
      filter(entRec => entRec.name === entityName && entRec.entityType === entityType && entRec.workspaceId === workspaceId)
    }

    def findEntityById(id: Long): EntityQuery = {
      filter(_.id === id)
    }

    // Actions

    // get a specific entity or set of entities: may include "hidden" deleted entities if not named "active"

    def get(workspaceContext: Workspace, entityType: String, entityName: String): ReadAction[Option[Entity]] = {
      EntityAndAttributesRawSqlQuery.actionForTypeName(workspaceContext, entityType, entityName) map(query => unmarshalEntities(query, workspaceContext.shardState)) map(_.headOption)
    }

    def getEntities(workspaceId: UUID, shardState: WorkspaceShardState, entityIds: Traversable[Long]): ReadAction[Seq[(Long, Entity)]] = {
      EntityAndAttributesRawSqlQuery.actionForIds(workspaceId, shardState, entityIds.toSet) map(query => unmarshalEntitiesWithIds(query, shardState))
    }

    def getEntityRecords(workspaceId: UUID, entities: Set[AttributeEntityReference]): ReadAction[Seq[EntityRecord]] = {
      val entitiesGrouped = entities.grouped(batchSize).toSeq

      DBIO.sequence(entitiesGrouped map { batch =>
        EntityRecordRawSqlQuery.action(workspaceId, batch)
      }).map(_.flatten)
    }

    def getActiveEntities(workspaceContext: Workspace, entityRefs: Traversable[AttributeEntityReference]): ReadAction[TraversableOnce[Entity]] = {
      EntityAndAttributesRawSqlQuery.activeActionForRefs(workspaceContext, entityRefs.toSet) map(query => unmarshalEntities(query, workspaceContext.shardState))
    }

    // list all entities or those in a category

    def listActiveEntities(workspaceContext: Workspace): ReadAction[TraversableOnce[Entity]] = {
      EntityAndAttributesRawSqlQuery.activeActionForWorkspace(workspaceContext) map(query => unmarshalEntities(query, workspaceContext.shardState))
    }

    // includes "deleted" hidden entities
    def listEntities(workspaceContext: Workspace): ReadAction[TraversableOnce[Entity]] = {
      EntityAndAttributesRawSqlQuery.actionForWorkspace(workspaceContext) map(query => unmarshalEntities(query, workspaceContext.shardState))
    }

    def listActiveEntitiesOfType(workspaceContext: Workspace, entityType: String): ReadAction[TraversableOnce[Entity]] = {
      EntityAndAttributesRawSqlQuery.activeActionForType(workspaceContext, entityType) map(query => unmarshalEntities(query, workspaceContext.shardState))
    }

    // get entity types, counts, and attribute names to populate UI tables.  Active entities and attributes only.

    def getEntityTypeMetadata(workspaceContext: Workspace): ReadAction[Map[String, EntityTypeMetadata]] = {
      val typesAndCountsQ = getEntityTypesWithCounts(workspaceContext.workspaceIdAsUUID)
      val typesAndAttrsQ = getAttrNamesAndEntityTypes(workspaceContext.workspaceIdAsUUID, workspaceContext.shardState)

      generateEntityMetadataMap(typesAndCountsQ, typesAndAttrsQ)
    }

    def getEntityTypesWithCounts(workspaceId: UUID): ReadAction[Map[String, Int]] = {
      findActiveEntityByWorkspace(workspaceId).groupBy(e => e.entityType).map { case (entityType, entities) =>
        (entityType, entities.length)
      }.result map { result =>
        result.toMap
      }
    }

    def getAttrNamesAndEntityTypes(workspaceId: UUID, shardState: WorkspaceShardState): ReadAction[Map[String, Seq[AttributeName]]] = {
      val typesAndAttrNames = for {
        entityRec <- findActiveEntityByWorkspace(workspaceId)
        attrib <- findActiveAttributesByEntityId(workspaceId, shardState, entityRec.id)
      } yield {
        (entityRec.entityType, (attrib.namespace, attrib.name))
      }

      typesAndAttrNames.distinct.result map { result =>
        CollectionUtils.groupByTuples (result.map {
          case (entityType:String, (ns:String, n:String)) => (entityType, AttributeName(ns, n))
        })
      }
    }

    // get paginated entities for UI display, as a result of executing a query

    def loadEntityPage(workspaceContext: Workspace, entityType: String, entityQuery: model.EntityQuery): ReadAction[(Int, Int, Iterable[Entity])] = {
      EntityAndAttributesRawSqlQuery.activeActionForPagination(workspaceContext, entityType, entityQuery) map { case (unfilteredCount, filteredCount, pagination) =>
        (unfilteredCount, filteredCount, unmarshalEntities(pagination, workspaceContext.shardState))
      }
    }

    // create or replace entities

    // TODO: can this be optimized? It nicely reuses the save(..., entities) method, but that method
    // does a lot of work. This single-entity save could, for instance, look for simple cases e.g. no references,
    // and take an easier code path.
    def save(workspaceContext: Workspace, entity: Entity): ReadWriteAction[Entity] = {
      save(workspaceContext, Seq(entity)).map(_.head)
    }

    def save(workspaceContext: Workspace, entities: Traversable[Entity]): ReadWriteAction[Traversable[Entity]] = {
      entities.foreach(validateEntity)

      for {
        _ <- workspaceQuery.updateLastModified(workspaceContext.workspaceIdAsUUID)
        preExistingEntityRecs <- getEntityRecords(workspaceContext.workspaceIdAsUUID, entities.map(_.toReference).toSet)
        savingEntityRecs <- entityQueryWithInlineAttributes.insertNewEntities(workspaceContext, entities, preExistingEntityRecs.map(_.toReference)).map(_ ++ preExistingEntityRecs)
        referencedAndSavingEntityRecs <- lookupNotYetLoadedReferences(workspaceContext, entities, savingEntityRecs.map(_.toReference)).map(_ ++ savingEntityRecs)
        actuallyUpdatedEntityIds <- rewriteAttributes(workspaceContext.workspaceIdAsUUID, workspaceContext.shardState, entities, savingEntityRecs.map(_.id), referencedAndSavingEntityRecs.map(e => e.toReference -> e.id).toMap)
        actuallyUpdatedPreExistingEntityRecs = preExistingEntityRecs.filter(e => actuallyUpdatedEntityIds.contains(e.id))
        _ <- entityQueryWithInlineAttributes.optimisticLockUpdate(actuallyUpdatedPreExistingEntityRecs, entities)
      } yield entities
    }

    private def applyEntityPatch(workspaceContext: Workspace, entityRecord: EntityRecord, upserts: AttributeMap, deletes: Traversable[AttributeName]) = {
      //yank the attribute list for this entity to determine what to do with upserts
      entityAttributeShardQuery(workspaceContext).findByOwnerQuery(Seq(entityRecord.id)).map(attr => (attr.namespace, attr.name, attr.id)).result flatMap { attrCols =>

        val existingAttrsToRecordIds: Map[AttributeName, Set[Long]] =
          attrCols.groupBy { case (namespace, name, _) =>
            (namespace, name)
          }.map { case ((namespace, name), ids) =>
            AttributeName(namespace, name) -> ids.map(_._3).toSet //maintain the full list of attribute ids since list members are stored as individual attributes
          }

        val entityRefsToLookup = upserts.valuesIterator.collect { case e: AttributeEntityReference => e }.toSet

        /*
          Additional check for resizing entities whose Attribute value type is AttributeList[_] in response to bugs
          mentioned in WA-32 and WA-153. Currently the update query is such that it matches the listIndex of each
          attribute value and updates the list index and list size. Previously if the size of the list changes, those
          changes were not reflected in the table resulting in the behavior mentioned in the WA-32.

          Hence, for any attribute in the update list with value type as AttributeList[_], check if the size of the
          list has changed. If yes, based on increase or decrease of the list size, add those extra records into insertRecs
          or delete extra records from the entity table respectively.

          NOTE: Attributes that are not lists are treated as a list of size one.
        */

        // Tuple of
        // insertRecs: Seq[EntityAttributeRecord]
        // updateRecs: Seq[EntityAttributeRecord]
        // extraDeleteIds: Seq[Long]
        type AttributeModifications = (Seq[EntityAttributeRecord], Seq[EntityAttributeRecord], Seq[Long])

        def checkAndUpdateRecSize(name: AttributeName,
                               existingAttrSize: Int,
                               updateAttrSize: Int,
                               attrRecords: Seq[EntityAttributeRecord]): AttributeModifications = {
          if (updateAttrSize > existingAttrSize) {
            // since the size of the "list" has increased, move these new records to insertRecs
            val (newInsertRecs, newUpdateRecs) = attrRecords.partition {
              _.listIndex.getOrElse(0) > (existingAttrSize - 1)
            }

            (newInsertRecs, newUpdateRecs, Seq.empty[Long])
          } else if (updateAttrSize < existingAttrSize) {
            // since the size of the list has decreased, delete the extra rows from table
            val deleteIds = existingAttrsToRecordIds(name).toSeq.takeRight(existingAttrSize - updateAttrSize)
            (Seq.empty[EntityAttributeRecord], attrRecords, deleteIds)
          }
          else (Seq.empty[EntityAttributeRecord], attrRecords, Seq.empty[Long]) // the list size hasn't changed
        }

        def recordsForUpdateAttribute(name: AttributeName,
                                      attribute: Attribute,
                                      attrRecords: Seq[EntityAttributeRecord]): AttributeModifications = {
          val existingAttrSize = existingAttrsToRecordIds.get(name).map(_.size).getOrElse(0)
          attribute match {
            case list: AttributeList[_] => checkAndUpdateRecSize(name, existingAttrSize, list.list.size, attrRecords)
            case _ => checkAndUpdateRecSize(name, existingAttrSize, 1, attrRecords)
          }
        }

        lookupNotYetLoadedReferences(workspaceContext, entityRefsToLookup, Seq(entityRecord.toReference)) flatMap { entityRefRecs =>
          val allTheEntityRefs = entityRefRecs ++ Seq(entityRecord) //re-add the current entity
          val refsToIds = allTheEntityRefs.map(e => e.toReference -> e.id).toMap

          //get ids that need to be deleted from db
          val deleteIds = (for {
            attributeName <- deletes
          } yield existingAttrsToRecordIds.get(attributeName)).flatten.flatten

          val (insertRecs: Seq[EntityAttributeRecord], updateRecs: Seq[EntityAttributeRecord], extraDeleteIds: Seq[Long]) = upserts.map {
            case (name, attribute) =>
              val attrRecords = entityAttributeShardQuery(workspaceContext).marshalAttribute(refsToIds(entityRecord.toReference), name, attribute, refsToIds)

              recordsForUpdateAttribute(name, attribute, attrRecords)
          }.foldLeft((Seq.empty[EntityAttributeRecord], Seq.empty[EntityAttributeRecord], Seq.empty[Long])) {
              case ((insert1, update1, delete1), (insert2, update2, delete2)) => (insert1 ++ insert2, update1 ++ update2, delete1 ++ delete2)
          }

          val totalDeleteIds = deleteIds ++ extraDeleteIds

          entityAttributeShardQuery(workspaceContext).patchAttributesAction(insertRecs, updateRecs, totalDeleteIds, entityAttributeTempQuery.insertScratchAttributes)
        }
      }
    }

    //"patch" this entity by applying the upserts and the deletes to its attributes, then save. a little more database efficient than a "full" save, but requires the entity already exist.
    def saveEntityPatch(workspaceContext: Workspace, entityRef: AttributeEntityReference, upserts: AttributeMap, deletes: Traversable[AttributeName]) = {
      val deleteIntersectUpsert = deletes.toSet intersect upserts.keySet
      if (upserts.isEmpty && deletes.isEmpty) {
        DBIO.successful(()) //no-op
      } else if (deleteIntersectUpsert.nonEmpty) {
        DBIO.failed(new RawlsException(s"Can't saveEntityPatch on $entityRef because upserts and deletes share attributes $deleteIntersectUpsert"))
      } else {
        getEntityRecords(workspaceContext.workspaceIdAsUUID, Set(entityRef)) flatMap { entityRecs =>
          if (entityRecs.length != 1) {
            throw new RawlsException(s"saveEntityPatch looked up $entityRef expecting 1 record, got ${entityRecs.length} instead")
          }

          val entityRecord = entityRecs.head
          upserts.keys.foreach { attrName =>
            validateUserDefinedString(attrName.name)
            validateAttributeName(attrName, entityRecord.entityType)
          }

          for {
            _ <- applyEntityPatch(workspaceContext, entityRecord, upserts, deletes)
            updatedEntities <- entityQuery.getEntities(workspaceContext.workspaceIdAsUUID, workspaceContext.shardState, Seq(entityRecord.id))
            _ <- entityQueryWithInlineAttributes.optimisticLockUpdate(entityRecs, updatedEntities.map(elem => elem._2))
          } yield {}
        }
      }
    }

    private def lookupNotYetLoadedReferences(workspaceContext: Workspace, entities: Traversable[Entity], alreadyLoadedEntityRefs: Seq[AttributeEntityReference]): ReadAction[Seq[EntityRecord]] = {
      val allRefAttributes = (for {
        entity <- entities
        (_, attribute) <- entity.attributes
        ref <- attribute match {
          case AttributeEntityReferenceList(l) => l
          case r: AttributeEntityReference => Seq(r)
          case _ => Seq.empty
        }
      } yield ref).toSet

      lookupNotYetLoadedReferences(workspaceContext, allRefAttributes, alreadyLoadedEntityRefs)
    }

    private def lookupNotYetLoadedReferences(workspaceContext: Workspace, attrReferences: Set[AttributeEntityReference], alreadyLoadedEntityRefs: Seq[AttributeEntityReference]): ReadAction[Seq[EntityRecord]] = {
      val notYetLoadedEntityRecs = attrReferences -- alreadyLoadedEntityRefs

      getEntityRecords(workspaceContext.workspaceIdAsUUID, notYetLoadedEntityRecs) map { foundEntities =>
        if (foundEntities.size != notYetLoadedEntityRecs.size) {
          val notFoundRefs = notYetLoadedEntityRecs -- foundEntities.map(_.toReference)
          throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.BadRequest, "Could not resolve some entity references", notFoundRefs.map { missingRef =>
            ErrorReport(s"${missingRef.entityType} ${missingRef.entityName} not found", Seq.empty)
          }.toSeq))
        } else {
          foundEntities
        }
      }
    }

    private def rewriteAttributes(workspaceId: UUID, shardState: WorkspaceShardState, entitiesToSave: Traversable[Entity], entityIds: Seq[Long], entityIdsByRef: Map[AttributeEntityReference, Long]) = {
      val attributesToSave = for {
        entity <- entitiesToSave
        (attributeName, attribute) <- entity.attributes
        attributeRec <- entityAttributeShardQuery(workspaceId, shardState).marshalAttribute(entityIdsByRef(entity.toReference), attributeName, attribute, entityIdsByRef)
      } yield attributeRec

      entityAttributeShardQuery(workspaceId, shardState).findByOwnerQuery(entityIds).result flatMap { existingAttributes =>
        entityAttributeShardQuery(workspaceId, shardState).rewriteAttrsAction(attributesToSave, existingAttributes, entityAttributeTempQuery.insertScratchAttributes)
      }
    }

    // "delete" entities by hiding and renaming. we must rename the entity to avoid future name collisions if the user
    // attempts to create a new entity of the same name.
    def hide(workspaceContext: Workspace, entRefs: Seq[AttributeEntityReference]): ReadWriteAction[Int] = {
      // N.B. we must hide both the entity attributes and the entity itself. Other queries, such
      // as baseEntityAndAttributeSql, use "where ENTITY.deleted = ENTITY_ATTRIBUTE.deleted" during joins.
      // Thus, we need to keep the "deleted" value for attributes in sync with their parent entity,
      // when hiding that entity.
      workspaceQuery.updateLastModified(workspaceContext.workspaceIdAsUUID) andThen
        EntityAndAttributesRawSqlQuery.batchHide(workspaceContext, entRefs) andThen
        EntityRecordRawSqlQuery.batchHide(workspaceContext.workspaceIdAsUUID, entRefs).map( res => res.sum)
    }

    // perform actual deletion (not hiding) of all entities in a workspace

    def deleteFromDb(workspaceContext: Workspace): WriteAction[Int] = {
      EntityDependenciesDeletionQuery.deleteAction(workspaceContext) andThen {
        filter(_.workspaceId === workspaceContext.workspaceIdAsUUID).delete
      }
    }

    def rename(workspaceContext: Workspace, entityType: String, oldName: String, newName: String): ReadWriteAction[Int] = {
      validateEntityName(newName)
      workspaceQuery.updateLastModified(workspaceContext.workspaceIdAsUUID) andThen
        findEntityByName(workspaceContext.workspaceIdAsUUID, entityType, oldName).map(_.name).update(newName)
    }

    // copy all entities from one workspace to another (empty) workspace

    def copyAllEntities(sourceWorkspaceContext: Workspace, destWorkspaceContext: Workspace): ReadWriteAction[Int] = {
      listActiveEntities(sourceWorkspaceContext).flatMap(copyEntities(destWorkspaceContext, _, Seq.empty))
    }

    // copy entities from one workspace to another, checking for conflicts first

    def checkAndCopyEntities(sourceWorkspaceContext: Workspace, destWorkspaceContext: Workspace, entityType: String, entityNames: Seq[String], linkExistingEntities: Boolean): ReadWriteAction[EntityCopyResponse] = {
      def getHardConflicts(workspaceId: UUID, entityRefs: Seq[AttributeEntityReference]) = {
        val batchActions = createBatches(entityRefs.toSet).map(batch => getEntityRecords(workspaceId, batch))
        DBIO.sequence(batchActions).map(_.flatten.toSeq).map { recs =>
          recs.map(_.toReference)
        }
      }

      def getSoftConflicts(paths: Seq[EntityPath]) = {
        getCopyConflicts(destWorkspaceContext, paths.map(_.path.last)).map { conflicts =>
          val conflictsAsRefs = conflicts.toSeq.map(_.toReference)
          paths.filter(p => conflictsAsRefs.contains(p.path.last))
        }
      }

      def buildSoftConflictTree(pathsRemaining: EntityPath): Seq[EntitySoftConflict] = {
        if(pathsRemaining.path.isEmpty) Seq.empty
        else Seq(EntitySoftConflict(pathsRemaining.path.head.entityType, pathsRemaining.path.head.entityName, buildSoftConflictTree(EntityPath(pathsRemaining.path.tail))))
      }

      val entitiesToCopyRefs = entityNames.map(name => AttributeEntityReference(entityType, name))

      getHardConflicts(destWorkspaceContext.workspaceIdAsUUID, entitiesToCopyRefs).flatMap {
        case Seq() =>
          val pathsAndConflicts = for {
            entityPaths <- getEntitySubtrees(sourceWorkspaceContext, entityType, entityNames.toSet)
            softConflicts <- getSoftConflicts(entityPaths)
          } yield (entityPaths, softConflicts)

          pathsAndConflicts.flatMap { case (entityPaths, softConflicts) =>
            if(softConflicts.isEmpty || linkExistingEntities) {
              val allEntityRefs = entityPaths.flatMap(_.path)
              val allConflictRefs = softConflicts.flatMap(_.path)
              val entitiesToCopy = allEntityRefs diff allConflictRefs

              for {
                entities <- getActiveEntities(sourceWorkspaceContext, entitiesToCopy)
                _ <- copyEntities(destWorkspaceContext, entities.toSet, allConflictRefs)
              } yield EntityCopyResponse(entities.map(_.toReference).toSeq, Seq.empty, Seq.empty)
            } else {
              val unmergedSoftConflicts = softConflicts.flatMap(buildSoftConflictTree).groupBy(c => (c.entityType, c.entityName)).map {
                case ((conflictType, conflictName), conflicts) => EntitySoftConflict(conflictType, conflictName, conflicts.flatMap(_.conflicts))
              }.toSeq
              DBIO.successful(EntityCopyResponse(Seq.empty, Seq.empty, unmergedSoftConflicts))
            }
          }
        case hardConflicts => DBIO.successful(EntityCopyResponse(Seq.empty, hardConflicts.map(c => EntityHardConflict(c.entityType, c.entityName)), Seq.empty))
      }
    }

    private def copyEntities(destWorkspaceContext: Workspace, entitiesToCopy: TraversableOnce[Entity], entitiesToReference: Seq[AttributeEntityReference]): ReadWriteAction[Int] = {
      // insert entities to destination workspace
      entityQueryWithInlineAttributes.batchInsertEntities(destWorkspaceContext, entitiesToCopy) flatMap { insertedRecs =>
        // if any of the inserted entities
        getEntityRecords(destWorkspaceContext.workspaceIdAsUUID, entitiesToReference.toSet) flatMap { toReferenceRecs =>
          val idsByRef = (insertedRecs ++ toReferenceRecs).map { rec => rec.toReference -> rec.id }.toMap
          val entityIdAndEntity = entitiesToCopy map { ent => idsByRef(ent.toReference) -> ent }

          val attributes = for {
            (entityId, entity) <- entityIdAndEntity
            (attributeName, attr) <- entity.attributes
            rec <- entityAttributeShardQuery(destWorkspaceContext).marshalAttribute(entityId, attributeName, attr, idsByRef)
          } yield rec

          entityAttributeShardQuery(destWorkspaceContext).batchInsertAttributes(attributes.toSeq)
        }
      }
    }

    // retrieve all paths from these entities, for checkAndCopyEntities

    private[slick] def getEntitySubtrees(workspaceContext: Workspace, entityType: String, entityNames: Set[String]): ReadAction[Seq[EntityPath]] = {
      val startingEntityRecsAction = filter(rec => rec.workspaceId === workspaceContext.workspaceIdAsUUID && rec.entityType === entityType && rec.name.inSetBind(entityNames))

      startingEntityRecsAction.result.flatMap { startingEntityRecs =>
        val refsToId = startingEntityRecs.map(rec => EntityPath(Seq(rec.toReference)) -> rec.id).toMap
        recursiveGetEntityReferences(workspaceContext, Down, startingEntityRecs.map(_.id).toSet, refsToId)
      }
    }

    // return the entities already present in the destination workspace

    private[slick] def getCopyConflicts(destWorkspaceContext: Workspace, entitiesToCopy: TraversableOnce[AttributeEntityReference]): ReadAction[TraversableOnce[EntityRecord]] = {
      getEntityRecords(destWorkspaceContext.workspaceIdAsUUID, entitiesToCopy.toSet)
    }

    // the opposite of getEntitySubtrees: traverse the graph to retrieve all entities which ultimately refer to these

    def getAllReferringEntities(context: Workspace, entities: Set[AttributeEntityReference]): ReadAction[Set[AttributeEntityReference]] = {
      getEntityRecords(context.workspaceIdAsUUID, entities) flatMap { entityRecs =>
        val refsToId = entityRecs.map(rec => EntityPath(Seq(rec.toReference)) -> rec.id).toMap
        recursiveGetEntityReferences(context, Up, entityRecs.map(_.id).toSet, refsToId)
      } flatMap { refs =>
        val entityAction = EntityAndAttributesRawSqlQuery.activeActionForRefs(context, refs.flatMap(_.path).toSet) map(query => unmarshalEntities(query, context.shardState))
        entityAction map { _.toSet map { e: Entity => e.toReference } }
      }
    }
    
    sealed trait RecursionDirection
    case object Up extends RecursionDirection
    case object Down extends RecursionDirection

    /**
      * Starting with entities specified by entityIds, recursively walk references accumulating all the ids
      * @param direction whether to walk Down (my object references others) or Up (my object is referenced by others) the graph
      * @param entityIds the ids to start with
      * @param accumulatedPathsWithLastId the ids accumulated from the prior call. If you wish entityIds to be in the overall
      *                       results, start with entityIds == accumulatedIds, otherwise start with Seq.empty but note
      *                       that if there is a cycle some of entityIds may be in the result anyway
      * @return the ids of all the entities referred to by entityIds
      */
    private def recursiveGetEntityReferences(workspace: Workspace, direction: RecursionDirection, entityIds: Set[Long], accumulatedPathsWithLastId: Map[EntityPath, Long]): ReadAction[Seq[EntityPath]] = {
      def oneLevelDown(idBatch: Set[Long]): ReadAction[Set[(Long, EntityRecord)]] = {
        val query = entityAttributeShardQuery(workspace) filter (_.ownerId inSetBind idBatch) join
          this on { (attr, ent) => attr.valueEntityRef === ent.id && ! attr.deleted } map { case (attr, entity) => (attr.ownerId, entity)}
        query.result.map(_.toSet)
      }

      def oneLevelUp(idBatch: Set[Long]): ReadAction[Set[(Long, EntityRecord)]] = {
        val query = entityAttributeShardQuery(workspace) filter (_.valueEntityRef inSetBind idBatch) join
          this on { (attr, ent) => attr.ownerId === ent.id && ! ent.deleted } map { case (attr, entity) => (attr.valueEntityRef.get, entity)}
        query.result.map(_.toSet)
      }

      // need to batch because some RDBMSes have a limit on the length of an in clause
      val batchedEntityIds: Iterable[Set[Long]] = createBatches(entityIds)

      val batchActions: Iterable[ReadAction[Set[(Long, EntityRecord)]]] = direction match {
        case Down => batchedEntityIds map oneLevelDown
        case Up => batchedEntityIds map oneLevelUp
      }

      DBIO.sequence(batchActions).map(_.flatten.toSet).flatMap { priorIdWithCurrentRec =>
        val currentPaths = priorIdWithCurrentRec.flatMap { case (priorId, currentRec) =>
          val pathsThatEndWithPrior = accumulatedPathsWithLastId.filter { case (_, id) => id == priorId }
          pathsThatEndWithPrior.keys.map(_.path).map { path => (EntityPath(path :+ currentRec.toReference), currentRec.id)}
        }.toMap

        val untraversedIds = priorIdWithCurrentRec.map(_._2.id) -- accumulatedPathsWithLastId.values
        if (untraversedIds.isEmpty) {
          DBIO.successful(accumulatedPathsWithLastId.keys.toSeq)
        } else {
          recursiveGetEntityReferences(workspace, direction, untraversedIds, accumulatedPathsWithLastId ++ currentPaths)
        }
      }
    }

    // Utility methods

    private def validateEntity(entity: Entity): Unit = {
      if (entity.entityType.equalsIgnoreCase(Attributable.workspaceEntityType)) {
        throw new RawlsFatalExceptionWithErrorReport(errorReport = ErrorReport(
          message = s"Entity type ${Attributable.workspaceEntityType} is reserved and cannot be overwritten",
          statusCode = StatusCodes.BadRequest
        ))
      }
      validateUserDefinedString(entity.entityType)
      validateEntityName(entity.name)
      entity.attributes.keys.foreach { attrName =>
        validateUserDefinedString(attrName.name)
        validateAttributeName(attrName, entity.entityType)
      }
    }

    def generateEntityMetadataMap(typesAndCountsQ: ReadAction[Map[String, Int]], typesAndAttrsQ: ReadAction[Map[String, Seq[AttributeName]]]) = {
      typesAndCountsQ flatMap { typesAndCounts =>
        typesAndAttrsQ map { typesAndAttrs =>
          (typesAndCounts.keySet ++ typesAndAttrs.keySet) map { entityType =>
            (entityType, EntityTypeMetadata(
              typesAndCounts.getOrElse(entityType, 0),
              entityType + Attributable.entityIdAttributeSuffix,
              typesAndAttrs.getOrElse(entityType, Seq()).map(AttributeName.toDelimitedName).sortBy(_.toLowerCase)))
          } toMap
        }
      }
    }

    // Unmarshal methods

    // NOTE: marshalNewEntity is in entityQueryWithInlineAttributes because it helps save the inline attributes to DB

    private def unmarshalEntity(entityRecord: EntityRecord, attributes: AttributeMap): Entity = {
      Entity(entityRecord.name, entityRecord.entityType, attributes)
    }

    private def unmarshalEntities(entityAttributeRecords: Seq[entityQuery.EntityAndAttributesRawSqlQuery.EntityAndAttributesResult], shardState: WorkspaceShardState): Seq[Entity] = {
      unmarshalEntitiesWithIds(entityAttributeRecords, shardState).map { case (_, entity) => entity }
    }

    private def unmarshalEntitiesWithIds(entityAttributeRecords: Seq[entityQuery.EntityAndAttributesRawSqlQuery.EntityAndAttributesResult], shardState: WorkspaceShardState): Seq[(Long, Entity)] = {
      val allEntityRecords = entityAttributeRecords.map(_.entityRecord).distinct

      // note that not all entities have attributes, thus the collect below
      val entitiesWithAttributes = entityAttributeRecords.collect {
        case EntityAndAttributesRawSqlQuery.EntityAndAttributesResult(entityRec, Some(attributeRec), refEntityRecOption) => ((entityRec.id, attributeRec), refEntityRecOption)
      }

      val attributesByEntityId = entityAttributeShardQuery(UUID.randomUUID(), shardState).unmarshalAttributes(entitiesWithAttributes)

      allEntityRecords.map { entityRec =>
        entityRec.id -> unmarshalEntity(entityRec, attributesByEntityId.getOrElse(entityRec.id, Map.empty))
      }
    }
  }
}
