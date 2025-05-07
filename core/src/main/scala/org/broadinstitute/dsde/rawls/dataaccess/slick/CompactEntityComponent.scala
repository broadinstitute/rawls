package org.broadinstitute.dsde.rawls.dataaccess.slick

import com.google.common.annotations.VisibleForTesting
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.entities.compact.CompactEntitySerialization
import org.broadinstitute.dsde.rawls.model.{
  Attributable,
  AttributeEntityReference,
  AttributeName,
  Entity,
  EntityColumnFilter,
  EntityQuery,
  FilterOperators,
  SortDirections
}
import slick.dbio.Effect.Read
import slick.jdbc.MySQLProfile.api._
import slick.jdbc._
import slick.sql.SqlStreamingAction
import spray.json._

import java.util.UUID
import scala.concurrent.ExecutionContext

trait CompactEntityComponent extends LazyLogging {
  this: DriverComponent =>

  /** low-level raw SQL queries for ENTITY. */
  object compactEntityQuery extends CompactEntityQuery(this)
}

class CompactEntityQuery(driverComponent: DriverComponent) extends RawSqlQuery with CompactEntitySerialization {
  override val driver = driverComponent.driver
  import driverComponent.uniqueResult

  implicit val executionContext: ExecutionContext = driverComponent.executionContext

  // read a json column from the db and translate into a JsValue
  implicit val GetJsValueResult: GetResult[JsValue] = GetResult(r => r.nextString().parseJson)

  // write a JsValue to the database by converting it to a string (the db column is still JSON)
  implicit object SetJsValueParameter extends SetParameter[JsValue] {
    def apply(v: JsValue, pp: PositionedParameters): Unit =
      pp.setString(v.compactPrint)
  }

  private val basicCompactEntitySelect =
    "select id, name, entity_type, workspace_id, record_version, deleted, attributes"

  // matches basicCompactEntitySelect
  implicit val getJsonEntityRecord: GetResult[CompactEntityRecord] =
    GetResult(r => CompactEntityRecord(r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<))

  implicit val getJsonEntityRefRecord: GetResult[CompactEntityRefRecord] =
    GetResult(r => CompactEntityRefRecord(r.<<, r.<<, r.<<))

  implicit val getKeysRecord: GetResult[KeysRecord] =
    GetResult(r => KeysRecord(r.<<, r.<<, r.<<, r.<<, r.<<))

  implicit val getEntityTypeAndAttributeKey: GetResult[EntityTypeAndAttributeKey] =
    GetResult(r => EntityTypeAndAttributeKey(r.<<, AttributeName.fromDelimitedName(r.<<)))

  implicit val getEntityTypeAndCount: GetResult[EntityTypeAndCount] =
    GetResult(r => EntityTypeAndCount(r.<<, r.<<))

  private val fromEntityWhereNotDeleted = "from ENTITY e where e.deleted = 0"

  /**
    * Insert a single entity to the db.
    *
    * Note this does NOT handle persisting refs. See CompactEntityProvider.createEntity if you need to persist refs.
    *
    * `execution plan: single-row insert`
    */
  def createEntity(workspaceId: UUID, entity: Entity): ReadWriteAction[Int] = {
    val attributesJson: JsValue = toSql(entity.attributes)

    sqlu"""insert into ENTITY(name, entity_type, workspace_id, record_version, deleted, attributes)
          values (${entity.name}, ${entity.entityType}, $workspaceId, 0, 0, $attributesJson)"""
  }

  /**
    * Read a single entity from the db
    *
    * `execution plan: single row constant; fully indexed by idx_entity_type_name`
    */
  def getEntity(workspaceId: UUID, entityType: String, entityName: String): ReadAction[Option[CompactEntityRecord]] =
    uniqueResult(singleEntityQuery(workspaceId, entityType, entityName).as[CompactEntityRecord])

  private def singleEntityQuery(workspaceId: UUID, entityType: String, entityName: String) =
    sql"""#$basicCompactEntitySelect
          #$fromEntityWhereNotDeleted
          and workspace_id = $workspaceId
          and entity_type = $entityType
          and name = $entityName"""

  /** Given a set of entity references, return the ids being referenced.
    * Ignores deleted entities.
    *
    * `execution plan: Using index condition; Using where. Covered by idx_entity_type_name`
    */
  // should this return CompactEntityRefRecord instead of Long? Do we ever need to know which ids
  // belong to which reference?
  def getReferencedIds(workspaceId: UUID, refs: Set[AttributeEntityReference]): ReadAction[Seq[Long]] =
    // short-circuit
    if (refs.isEmpty) {
      DBIO.successful(Seq())
    } else {
      // group the entity type/name pairs by type
      val groupedReferences: Map[String, Set[String]] = refs.groupMap(_.entityType)(_.entityName)

      // build clauses for the type/name pairs
      val clauses: Iterable[SQLActionBuilder] = groupedReferences.map {
        case (entityType: String, entityNames: Set[String]) =>
          // build the "IN" clause values
          val entityNamesSql = reduceSqlActionsWithDelim(entityNames.map(name => sql"$name").toSeq, sql",")
          concatSqlActions(
            sql""" (entity_type = $entityType and name in (""",
            entityNamesSql,
            sql")) "
          )
      }

      // build the overall query
      val query = concatSqlActions(
        sql"""select id
               #$fromEntityWhereNotDeleted
               and workspace_id = $workspaceId
               and ( """,
        reduceSqlActionsWithDelim(clauses.toSeq, sql" or "),
        sql""" );"""
      )

      // execute
      query.as[Long]
    }

  /**
    * Delete all rows in ENTITY_REFS for the specified "from" id, _except_ for those
    * rows in "idsToKeep"
    *
    * Delete from ENTITY_REFS where to_id not in (toIds) and from_id = ?
    *
    * Returns the number of rows deleted.
    *
    * `execution plan: Index range scan; using where. Possible indexes: unq_from_to,idx_to; actual index: unq_from_to.`
    */
  // The index range scan is caused by the "not in" clause. I believe this is still optimal as compared to
  // performing a select, performing a diff in the Scala layer, then sending an optimized delete query back to MySQL
  def deleteReferences(fromId: Long, idsToKeep: Set[Long]): ReadWriteAction[Int] = {
    val query = if (idsToKeep.isEmpty) {
      sql"""delete from ENTITY_REFS where from_id = $fromId;"""
    } else {
      val allValues =
        reduceSqlActionsWithDelim(idsToKeep.map(x => sql"$x").toSeq, sql",")

      concatSqlActions(
        sql"""delete from ENTITY_REFS
           where from_id = $fromId
           and to_id not in (""",
        allValues,
        sql""");"""
      )
    }

    query.asUpdate
  }

  /**
    * Insert into ENTITY_REFS(from_id, to_id) values(...) on duplicate key update from_id=from_id
    *
    * Returns the number of rows upserted.
    *
    * `execution plan: batched insert (one statement, multiple rows)`
    */
  def upsertReferences(fromId: Long, toIds: Set[Long]): ReadWriteAction[Int] = {
    val insertValues: Iterable[SQLActionBuilder] = toIds.map { toId =>
      sql"($fromId,$toId)"
    }
    val allInsertValues = reduceSqlActionsWithDelim(insertValues.toSeq, sql",")

    val query = concatSqlActions(
      sql"""insert into ENTITY_REFS(from_id, to_id)
              values
              """,
      allInsertValues,
      sql"""
              on duplicate key update from_id=from_id;"""
    )
    // The `on duplicate key update ...` makes this `insert` an upsert, not throwing errors on any
    // pre-existing rows. The `update from_id=from_id` is a noop update, saying "leave this row alone"

    query.asUpdate
  }

  /**
   * Get all entity attribute keys for a workspace.
   *
   * `execution plan: Index range scan; using where. Index: idx_entity_keys_workspace_and_entity_type.`
   */
  def listEntityKeys(workspaceId: UUID): ReadAction[Seq[EntityTypeAndAttributeKey]] =
    sql"""SELECT distinct entity_type, attribute_key
      FROM ENTITY_KEYS , JSON_TABLE(attribute_keys, '$$[*]' COLUMNS(attribute_key VARCHAR(256) PATH '$$')) t
      where workspace_id=$workspaceId;""".as[EntityTypeAndAttributeKey]

  /**
   * Gets the count of entities in a workspace, grouped by entity type.
   *
   * `execution plan: Index range scan; using where. Index: idx_entity_keys_workspace_and_entity_type.`
   */
  def countEntitiesGroupedByType(workspaceId: UUID): ReadAction[Seq[EntityTypeAndCount]] =
    // ENTITY_KEYS should be smaller than ENTITY and already excludes deleted entities
    sql"""SELECT entity_type, COUNT(*)
      FROM ENTITY_KEYS
      WHERE workspace_id = $workspaceId
      GROUP BY entity_type;""".as[EntityTypeAndCount]

  def countEntities(workspaceId: UUID, entityType: String): ReadWriteAction[Int] =
    concatSqlActions(
      sql"select count(*) ",
      fromActiveEntitiesOfTypeInWorkspace(workspaceId, entityType)
    ).as[Int].map(_.head)

  def countEntitiesWithColumnFilter(workspaceId: UUID,
                                    entityType: String,
                                    columnFilter: EntityColumnFilter
  ): ReadWriteAction[Int] =
    concatSqlActions(
      sql"select count(*) ",
      fromActiveEntitiesOfTypeInWorkspace(workspaceId, entityType),
      columnFilterCondition(columnFilter)
    ).as[Int].map(_.head)

  def countEntitiesWithFilterTerms(workspaceId: UUID,
                                   entityType: String,
                                   entityQuery: EntityQuery
  ): ReadWriteAction[Int] =
    concatSqlActions(
      sql"select count(*) ",
      fromActiveEntitiesOfTypeInWorkspace(workspaceId, entityType),
      filterTermsCondition(entityQuery)
    ).as[Int].map(_.head)

  def queryEntitiesWithFilterTerms(workspaceId: UUID,
                                   entityType: String,
                                   entityQuery: EntityQuery
  ): SqlStreamingAction[Seq[CompactEntityRecord], CompactEntityRecord, Read] =
    concatSqlActions(
      selectCompactEntityColumns,
      fromActiveEntitiesOfTypeInWorkspace(workspaceId, entityType),
      filterTermsCondition(entityQuery),
      orderBy(entityQuery),
      paginationClause(entityQuery)
    ).as[CompactEntityRecord](entityResultGetterWithFieldsFilter(entityQuery))

  def queryEntitiesWithColumnFilter(workspaceId: UUID,
                                    entityType: String,
                                    entityQuery: EntityQuery,
                                    columnFilter: EntityColumnFilter
  ): SqlStreamingAction[Seq[CompactEntityRecord], CompactEntityRecord, Read] =
    concatSqlActions(
      selectCompactEntityColumns,
      fromActiveEntitiesOfTypeInWorkspace(workspaceId, entityType),
      columnFilterCondition(columnFilter),
      orderBy(entityQuery),
      paginationClause(entityQuery)
    ).as[CompactEntityRecord](entityResultGetterWithFieldsFilter(entityQuery))

  def queryEntitiesWithNoFilter(workspaceId: UUID,
                                entityType: String,
                                entityQuery: EntityQuery
  ): SqlStreamingAction[Seq[CompactEntityRecord], CompactEntityRecord, Read] =
    concatSqlActions(
      selectCompactEntityColumns,
      fromActiveEntitiesOfTypeInWorkspace(workspaceId, entityType),
      orderBy(entityQuery),
      paginationClause(entityQuery)
    ).as[CompactEntityRecord](entityResultGetterWithFieldsFilter(entityQuery))

  // ====================================================================================================
  //  entity query helpers
  //      methods in this section are used for building entity query functions
  // ====================================================================================================

  private val selectCompactEntityColumns =
    sql"select id, name, entity_type, workspace_id, record_version, deleted, attributes"

  private def fromActiveEntitiesOfTypeInWorkspace(workspaceId: UUID, entityType: String) =
    sql" from ENTITY e where e.workspace_id = $workspaceId and e.entity_type = $entityType and e.deleted = 0"

  private def filterTermsCondition(entityQuery: EntityQuery) = {
    // note the lower casing for case insensitive search
    val filterClauses = entityQuery.filterTermsList.map { filterTerm =>
      sql"""JSON_SEARCH(lower(e.attributes -> '#${CompactEntitySerialization.slickAttrsPath}'), 'one', ${'%' + filterTerm.toLowerCase + '%'})"""
    }
    concatSqlActions(
      sql" and (",
      reduceSqlActionsWithDelim(filterClauses, sql" #${FilterOperators.toSql(entityQuery.filterOperator)} "),
      sql")"
    )
  }

  private def columnFilterCondition(columnFilter: EntityColumnFilter) =
    // CAST, JSON_UNQUOTE and JSON_EXTRACT are used to handle strings and numbers and do a case insensitive comparison
    sql" and CAST(e.attributes ->> ${slickAttributePath(columnFilter.attributeName)} AS CHAR) = ${columnFilter.term}"

  private def orderBy(entityQuery: EntityQuery): SQLActionBuilder =
    concatSqlActions(
      sql" order by ",
      entityQuery.sortField match {
        case Attributable.nameReservedAttribute => sql" #${Attributable.nameReservedAttribute}"
        case attr =>
          // the order of the columns here is also the sort precedence, list length first, then scalar value
          // Sorting on a list column should sort by the list size and sorting on a scalar column sorts on the column value.
          // If the column is a mixed type then all scalars will group together sorted by value then all the lists will follow sorted by size.
          sql" JSON_LENGTH(e.attributes -> ${slickAttributePath(attr)}), e.attributes -> ${slickAttributePath(attr)}"
      },
      sql" #${SortDirections.toSql(entityQuery.sortDirection)}"
    )

  private def paginationClause(entityQuery: EntityQuery): SQLActionBuilder =
    sql" limit ${entityQuery.pageSize} offset ${entityQuery.offset}"

  private def entityResultGetterWithFieldsFilter(entityQuery: EntityQuery) =
    entityQuery.fields.fields match {
      case Some(fields) =>
        // this special GetResult instance is the only way I found to filter the fields and keep this a streaming result
        val desiredFields = fields.map(AttributeName.fromDelimitedName)
        val getResultFilteringFields =
          GetResult(r => CompactEntityRecord(r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, keepOnlyFields(r.<<, desiredFields)))
        getResultFilteringFields
      case _ => getJsonEntityRecord
    }

  // ====================================================================================================
  //  migration helpers
  //      methods in this section are only used for migrating data from legacy->compact format
  // ====================================================================================================

  def migrationCreateTempTable: ReadWriteAction[Int] =
    sql"""create temporary table ENTITY_MIGRATION_TEMP(
                name varchar(254) CHARACTER SET utf8 COLLATE utf8_bin NOT NULL,
                entity_type varchar(254) CHARACTER SET utf8mb3 COLLATE utf8mb3_bin NOT NULL,
                attributes json,
                UNIQUE KEY `idx_temp_entity_type_name` (entity_type,name));""".asUpdate

  def migrationDeleteTempTable: ReadWriteAction[Int] =
    sql"""drop temporary table ENTITY_MIGRATION_TEMP  ;""".asUpdate

  def migrationInsertAttributesToTempTable(entities: Seq[Entity]): ReadWriteAction[Int] = {
    val values = entities.map { entity =>
      val attrsJson = toSql(entity.attributes)
      sql"(${entity.name}, ${entity.entityType}, $attrsJson)"
    }

    val insertBase = sql"""insert into ENTITY_MIGRATION_TEMP(name, entity_type, attributes)
          values """

    concatSqlActions(insertBase, reduceSqlActionsWithDelim(values, sql",")).asUpdate
  }

  def migrationUpdateFromTempTable(workspaceId: UUID): ReadWriteAction[Int] =
    sql"""update ENTITY e
          join ENTITY_MIGRATION_TEMP tmp
          on e.name = tmp.name and e.entity_type = tmp.entity_type and e.workspace_id = $workspaceId
          set e.attributes = tmp.attributes;""".asUpdate

  def migrationClearAllAttributesString(workspaceId: UUID): ReadWriteAction[Int] =
    sql"""update ENTITY
          set all_attribute_values = null
          where workspace_id = $workspaceId;""".asUpdate

  def migrationAddReferences(workspaceId: UUID, shardId: String): ReadWriteAction[Int] =
    sql"""insert into ENTITY_REFS(from_id, to_id)
         select e.id, ea.value_entity_ref
         from ENTITY e, ENTITY_ATTRIBUTE_#$shardId ea
         where ea.owner_id = e.id
         and e.workspace_id = $workspaceId
         and e.deleted = 0
         and ea.value_entity_ref is not null;""".asUpdate

  // note this cleans up legacy attributes for soft-deleted entities as well as active entities
  def migrationDeleteLegacyReferences(workspaceId: UUID, shardId: String): ReadWriteAction[Int] =
    sql"""delete ea
         from ENTITY e, ENTITY_ATTRIBUTE_#$shardId ea
         where ea.owner_id = e.id
         and e.workspace_id = $workspaceId""".asUpdate

  // ====================================================================================================
  //  testing helpers
  // ====================================================================================================

  // return all reference targets for a given reference source
  // `execution plan: non-unique key lookup; fully indexed by unq_from_to`
  @VisibleForTesting
  protected[slick] def getReferencedIds(fromId: Long): ReadAction[Seq[Long]] =
    sql"""select to_id from ENTITY_REFS where from_id = $fromId;""".as[Long]

  // return the ENTITY_KEYS row for a given entity
  // `execution plan: single row constant; fully indexed by primary key`
  @VisibleForTesting
  protected[slick] def getKeys(entityId: Long): ReadAction[Option[KeysRecord]] = {
    val query = sql"""select id, workspace_id, entity_type, attribute_keys, last_updated
            from ENTITY_KEYS
            where id = $entityId;""".as[KeysRecord]
    uniqueResult(query)
  }

}
