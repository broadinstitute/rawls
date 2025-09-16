package org.broadinstitute.dsde.rawls.dataaccess.slick

import akka.http.scaladsl.model.StatusCodes
import com.google.common.annotations.VisibleForTesting
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.entities.EntityUtils
import org.broadinstitute.dsde.rawls.entities.compact.CompactEntitySerialization
import org.broadinstitute.dsde.rawls.entities.exceptions.AttributeException

import java.sql.Timestamp
import java.util.{Date, UUID}
import org.broadinstitute.dsde.rawls.model.FilterOperators.FilterOperator
import org.broadinstitute.dsde.rawls.model.{
  Attributable,
  AttributeEntityReference,
  AttributeEntityReferenceList,
  AttributeName,
  AttributeRename,
  Entity,
  EntityColumnFilter,
  EntityPointer,
  EntityQuery,
  FilterOperators,
  SortDirections
}
import slick.dbio.Effect.Read
import slick.jdbc.MySQLProfile.api._
import slick.jdbc._
import slick.sql.SqlStreamingAction
import spray.json._

import scala.concurrent.ExecutionContext
import scala.language.postfixOps

trait CompactEntityComponent extends LazyLogging {
  this: DriverComponent =>

  /** low-level raw SQL queries for ENTITY. */
  object compactEntityQuery extends CompactEntityQuery(this)
}

class CompactEntityQuery(driverComponent: DriverComponent)
    extends CompactEntityMigration
    with CompactEntityKeysCache
    with RawSqlQuery
    with CompactEntitySerialization {
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

  implicit val getJsonEntityVersionRecord: GetResult[CompactEntityVersionRecord] =
    GetResult(r => CompactEntityVersionRecord(r.<<, r.<<, r.<<, r.<<))

  implicit val getKeysRecord: GetResult[KeysRecord] =
    GetResult(r => KeysRecord(r.<<, r.<<, r.<<, r.<<, r.<<))

  implicit val getEntityTypeAndAttributeKey: GetResult[EntityTypeAndAttributeKey] =
    GetResult(r => EntityTypeAndAttributeKey(r.<<, AttributeName.fromDelimitedName(r.<<)))

  implicit val getEntityTypeAndCount: GetResult[EntityTypeAndCount] =
    GetResult(r => EntityTypeAndCount(r.<<, r.<<))

  implicit val getEntityPointer: GetResult[EntityPointer] =
    GetResult(r => EntityPointer(r.<<, r.<<))

  implicit val getEntity: GetResult[Entity] =
    GetResult(r => Entity(r.<<, r.<<, fromSql(r.<<)))

  implicit val getRefPointerRecord: GetResult[RefPointerRecord] =
    GetResult(r =>
      RefPointerRecord(
        r.<<,
        r.<<,
        r.<<,
        r.<<,
        r.<<,
        r.<<,
        r.<<
      )
    )

  private val fromEntityWhereNotDeleted = "from ENTITY e where e.deleted = 0"

  /**
    * Insert multiple entities to the db.
    *
    * Note this does NOT handle persisting refs. See CompactEntityProvider.createEntity if you need to persist refs.
    *
    * `execution plan: multiple-row insert`
    */
  def batchWriteEntities(workspaceId: UUID, entities: Seq[Entity], insertOnly: Boolean): ReadWriteAction[Int] =
    if (entities.isEmpty)
      DBIO.successful(0)
    else {
      val baseSql =
        sql"""insert into ENTITY(name, entity_type, workspace_id, record_version, deleted, attributes) values """

      val values = entities.map { entity =>
        val attributesJson: JsValue = toSql(entity.attributes)

        sql"""(${entity.name}, ${entity.entityType}, $workspaceId, 0, 0, $attributesJson)"""
      }

      // when called with insertOnly=true, the SQL statement is a simple `insert into ...`.
      // when called with insertOnly=false, the SQL statement is `insert into ... on duplicate key update`.
      val upsertSql = if (insertOnly) {
        sql""
      } else {
        sql""" as newvalues on duplicate key update ENTITY.record_version = ENTITY.record_version+1, ENTITY.attributes = newvalues.attributes;"""
      }

      concatSqlActions(baseSql, reduceSqlActionsWithDelim(values, sql","), upsertSql).asUpdate
    }

  /**
    * Insert a single entity to the db.
    *
    * Note this does NOT handle persisting refs. See CompactEntityProvider.createEntity if you need to persist refs.
    *
    * `execution plan: single-row insert`
    */
  def createEntity(workspaceId: UUID, entity: Entity): ReadWriteAction[Int] =
    batchWriteEntities(workspaceId, Seq(entity), insertOnly = true)

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

  /** Given a set of entity type/name pairs, return the CompactEntityRecord for those pairs.
    *
    * `execution plan: index range scan on idx_entity_type_name`
    */
  def getEntities(workspaceId: UUID, refs: Set[EntityPointer]): ReadAction[Seq[CompactEntityRecord]] =
    // short-circuit
    if (refs.isEmpty) {
      DBIO.successful(Seq())
    } else {
      val typeNameClauses = generateTypeNameSql(refs)

      // build the overall query
      val query = concatSqlActions(
        sql"""select id, name, entity_type, workspace_id, record_version, deleted, attributes
               from ENTITY
               where workspace_id = $workspaceId
               and deleted = 0
               and ( """,
        reduceSqlActionsWithDelim(typeNameClauses.toSeq, sql" or "),
        sql""" );"""
      )

      // execute
      query.as[CompactEntityRecord]
    }

  /** Given a set of entity ids, return the CompactEntityRecord for those ids.
   *
   * execution plan: index range scan on primary key. Might also use idx_entity_type_name
   *    depending on the query planner's whims
   */
  def getEntitiesByIds(workspaceId: UUID, ids: Seq[Long]): ReadAction[Seq[CompactEntityRecord]] =
    // short-circuit
    if (ids.isEmpty) {
      DBIO.successful(Seq())
    } else {
      val inClause = reduceSqlActionsWithDelim(ids.map(id => sql"$id"), sql", ")

      // build the overall query
      val query = concatSqlActions(
        sql"""select id, name, entity_type, workspace_id, record_version, deleted, attributes
               from ENTITY
               where workspace_id = $workspaceId
               and deleted = 0
               and id in ( """,
        inClause,
        sql""" );"""
      )

      // execute
      query.as[CompactEntityRecord]
    }

  /** Given a set of entity type/name pairs, return the CompactEntityVersionRecord for those pairs.
    *
    * `execution plan: index range scan on idx_entity_type_name`
    */
  def getEntityVersions(workspaceId: UUID, refs: Set[EntityPointer]): ReadAction[Seq[CompactEntityVersionRecord]] =
    // short-circuit
    if (refs.isEmpty) {
      DBIO.successful(Seq())
    } else {
      val typeNameClauses = generateTypeNameSql(refs)

      // build the overall query
      val query = concatSqlActions(
        sql"""select id, name, entity_type, record_version
               from ENTITY
               where workspace_id = $workspaceId
               and deleted = 0
               and ( """,
        reduceSqlActionsWithDelim(typeNameClauses.toSeq, sql" or "),
        sql""" );"""
      )

      // execute
      query.as[CompactEntityVersionRecord]
    }

  /**
   * Get all entities of a given type in a workspace.
   *
   * `execution plan: index range scan; using where. Index: idx_entity_type_name`
   */
  def listEntities(workspaceId: UUID,
                   entityType: String
  ): SqlStreamingAction[Seq[CompactEntityRecord], CompactEntityRecord, Read] =
    sql"""#$basicCompactEntitySelect
      #$fromEntityWhereNotDeleted
      and workspace_id = $workspaceId
      and entity_type = $entityType""".as[CompactEntityRecord]

  /** Given a set of entity type/name pairs, return the CompactEntityRefRecord for those pairs.
    * The CompactEntityRefRecord includes the internal database id for these entities.
    *
    * `execution plan: index range scan on idx_entity_type_name`
    */
  def getEntityRefs(workspaceId: UUID, refs: Set[EntityPointer]): ReadAction[Seq[CompactEntityRefRecord]] =
    // short-circuit
    if (refs.isEmpty) {
      DBIO.successful(Seq())
    } else {
      val typeNameClauses = generateTypeNameSql(refs)

      // build the overall query
      val query = concatSqlActions(
        sql"""select id, name, entity_type
               from ENTITY
               where workspace_id = $workspaceId
               and deleted = 0
               and ( """,
        reduceSqlActionsWithDelim(typeNameClauses.toSeq, sql" or "),
        sql""" );"""
      )

      // execute
      query.as[CompactEntityRefRecord]
    }

  /**
   * Copies entities and their references from a source workspace to a destination workspace.
   *
   * This method performs the following operations:
   * - Copies the specified entities from the source workspace to the destination workspace.
   * - Copies the references associated with those entities to the destination workspace.
   * - Handles the copying in batches to optimize performance and avoid memory issues.
   *
   * Execution Plan:
   *         - Splits the entities into batches based on the `batchSize`.
   *         - Copies each batch of entities and their references using `copyEntities` and `copyEntityReferences`.
   *         - Aggregates the results from all batches to return the total counts.
   */
  def copyEntitiesToNewWorkspace(sourceWs: UUID,
                                 destWs: UUID,
                                 entityRefs: Set[EntityPointer] = Set(),
                                 batchSize: Int = driverComponent.batchSize
  ): WriteAction[Int] = {

    def copyChunkOfEntitiesOrAllEntities(chunk: Set[EntityPointer] = Set()) =
      for {
        entitiesCopiedCount <- copyEntities(sourceWs, destWs, chunk)
      } yield entitiesCopiedCount

    val chunks: Iterator[Set[EntityPointer]] = entityRefs.grouped(batchSize)

    val allCopies = DBIO.sequence(chunks map copyChunkOfEntitiesOrAllEntities)
    allCopies.map { copyActionResults: Iterator[Int] => copyActionResults.sum }
  }

  /**
   * Recursively retrieves the specified entities and all entities they reference.
   *
   * This method performs a recursive query on the `ENTITY_REFS` table to find all downstream entities
   * referenced by the input entities. It returns a `Set[RefMapping]`, where each `RefMapping` contains:
   * - `from`: The originating entity.
   * - `to`: A set of entities that the originating entity references, including all downstream references.
   *
   * Execution Plan:
   * - Uses recursive SQL queries to traverse the `ENTITY_REFS` table.
   * - Performs a union operation to include all downstream references.
   * - Groups the results by the originating entity and maps them to `RefMapping`.
   *
   * execution plan:
   *  - main query does a full scan on a derived table (the CTE result)
   *  - anchor query (first select statement) uses the index idx_to to look up rows in ENTITY_REFS
   *  - union does a full scan of the intermediate result and joins the recursive result (er1) to ENTITY_REFS using the index idx_to
   *  - Combining the anchor and recursive result uses a temporary table
   */
  def recursiveGetEntityReferences(workspaceId: UUID,
                                   entities: Set[EntityPointer],
                                   batchSize: Int = driverComponent.batchSize
  ): ReadAction[Set[RefMapping]] =
    if (entities.isEmpty) {
      DBIO.successful(Set.empty[RefMapping])
    } else if (entities.size > batchSize) {
      val batches = entities.grouped(batchSize).toSeq
      DBIO
        .sequence(batches.map(batch => recursiveGetEntityReferences(workspaceId, batch)))
        .map(_.flatten.toSet)
    } else {
      val entityTypeNameClauses = reduceSqlActionsWithDelim(generateTypeNameSql(entities).toSeq, sql" or ")

      val query = concatSqlActions(
        sql"""with recursive EntityReferences as (
                select workspace_id, id as from_entity_id, entity_type as from_entity_type, name as from_name,
             	  jt.from_attribute_name, jt.to_entity_type, jt.to_name
               from ENTITY left outer join JSON_TABLE(
                             attributes,
                             '$$.refs[*]' COLUMNS (
                                 from_attribute_name varchar(254) CHARACTER SET utf8mb3 COLLATE utf8mb3_bin PATH '$$.a',
             					to_entity_type varchar(254) CHARACTER SET utf8mb3 COLLATE utf8mb3_bin PATH '$$.t',
             		            to_name varchar(254) PATH '$$.n'
                              )) jt on true
             	where workspace_id = $workspaceId
             	and (""",
        entityTypeNameClauses,
        sql""")
             			union distinct
             	select e.workspace_id, e.id as from_entity_id, e.entity_type as from_entity_type, e.name as from_name,
             	  jt.from_attribute_name, jt.to_entity_type, jt.to_name
             	from EntityReferences er1, ENTITY e, JSON_TABLE(
                             e.attributes,
                             '$$.refs[*]' COLUMNS (
                                 from_attribute_name varchar(254) CHARACTER SET utf8mb3 COLLATE utf8mb3_bin PATH '$$.a',
             					to_entity_type varchar(254) CHARACTER SET utf8mb3 COLLATE utf8mb3_bin PATH '$$.t',
             		            to_name varchar(254) PATH '$$.n'
                              )) jt
             	where er1.to_entity_type = e.entity_type and er1.to_name = e.name
             	and e.workspace_id = $workspaceId
             )
             select workspace_id, from_entity_id, from_entity_type, from_name, from_attribute_name, to_entity_type, to_name
                     from EntityReferences;"""
      )

      query.as[RefPointerRecord].map { rows =>
        rows
          .groupMap(row => EntityPointer(row.fromEntityType, row.fromName))(row =>
            EntityPointer(row.toEntityType, row.toName)
          )
          .view
          // the query can return nulls in the "to" columns; filter those here
          .mapValues(_.filterNot(x => Option(x.entityType).isEmpty || Option(x.entityName).isEmpty).toSet)
          .toMap
          .map { case (key, value) => RefMapping(key, value) }
          .toSet
      }
    }

  /**
   * Copies entities from a source workspace to a destination workspace.
   *
   * This method inserts new rows into the `ENTITY` table for the destination workspace
   * based on the entities in the source workspace. It excludes entities that are marked as deleted.
   *
   * Execution Plan:
   * - If `refs` is empty, returns 0 without performing any database operations.
   * - Generates SQL clauses for the entity type and name pairs in `refs`.
   * - Executes an `INSERT INTO ... SELECT` query to copy entities from the source workspace to the destination workspace.
   * - Excludes entities marked as deleted in the source workspace.
   *
   * `query execution plan (for select): index range scan on idx_entity_type_name.`
   */
  def copyEntities(sourceWorkspaceId: UUID, destWorkspaceId: UUID, refs: Set[EntityPointer]): WriteAction[Int] =
    if (refs.isEmpty) {
      DBIO.successful(0)
    } else {
      val typeNameClauses = generateTypeNameSql(refs)
      val sql = concatSqlActions(
        sql"""insert into ENTITY(name, entity_type, workspace_id, record_version, deleted, attributes)
               select name, entity_type, $destWorkspaceId, record_version, 0, attributes
               from ENTITY e
               where e.workspace_id = $sourceWorkspaceId
               and deleted = 0"""
      )
      val entityTypeNameTuples = reduceSqlActionsWithDelim(typeNameClauses.toSeq, sql" or ")
      concatSqlActions(sql, sql" and (", entityTypeNameTuples, sql")").as[Int].head
    }

  def copyAllEntities(sourceWorkspaceId: UUID, destWorkspaceId: UUID): WriteAction[Int] = {
    val sql = concatSqlActions(
      sql"""insert into ENTITY(name, entity_type, workspace_id, record_version, deleted, attributes)
             select name, entity_type, $destWorkspaceId, record_version, 0, attributes
             from ENTITY e
             where e.workspace_id = $sourceWorkspaceId
             and deleted = 0"""
    )
    sql.as[Int].head
  }

  /** Given a set of entity type/name pairs, return the count of those entities that exist.
    *
    * `execution plan: uses idx_entity_type_name index. Extra: Using index condition; Using where`
    */
  def countExisting(workspaceId: UUID, refs: Set[EntityPointer]): ReadAction[Int] =
    // short-circuit
    if (refs.isEmpty) {
      DBIO.successful(0)
    } else {
      val typeNameClauses = generateTypeNameSql(refs)

      // build the overall query
      val query = concatSqlActions(
        sql"""select count(*)
               from ENTITY
               where workspace_id = $workspaceId
               and deleted = 0
               and ( """,
        reduceSqlActionsWithDelim(typeNameClauses.toSeq, sql" or "),
        sql""" );"""
      )

      // execute
      query.as[Int].head
    }

  /** Given a set of entity type/name pairs, determine if all of those pairs exist.
    *
    * `execution plan: uses idx_entity_type_name index. Extra: Using index condition; Using where` (always the same as countExisting())
    */
  def existsAll(workspaceId: UUID, refs: Set[EntityPointer]): ReadAction[Boolean] =
    countExisting(workspaceId, refs).map(count => count == refs.size)

  /**
   * Get all entity attribute keys for a workspace.
   *
   * execution plan:
   *    ENTITY: Using index condition (Using index condition; Using temporary); Using temporary
   *    t: Table function: json_table; Using temporary
   */
  def listEntityKeysViaEntity(workspaceId: UUID): ReadAction[Seq[EntityTypeAndAttributeKey]] =
    sql"""SELECT distinct entity_type, attribute_key
      FROM ENTITY, JSON_TABLE(JSON_KEYS(attributes, $slickAttrsPath), '$$[*]' COLUMNS(attribute_key VARCHAR(256) CHARACTER SET utf8mb3 COLLATE utf8mb3_bin PATH '$$')) t
      where workspace_id=$workspaceId and deleted = 0;""".as[EntityTypeAndAttributeKey]

  /**
   * Get the attribute keys for the given workspace and entity types.
   *
   * execution plan:
   *    ENTITY: Using index condition (Using index condition; Using temporary); Using temporary
   *    t: Table function: json_table; Using temporary
   */
  def listEntityKeysViaEntity(workspaceId: UUID,
                              entityTypes: Set[String]
  ): ReadAction[Seq[EntityTypeAndAttributeKey]] = {
    val inClause = reduceSqlActionsWithDelim(entityTypes.map(t => sql"$t").toSeq, sql", ")

    concatSqlActions(
      sql"""SELECT distinct entity_type, attribute_key
      FROM ENTITY, JSON_TABLE(JSON_KEYS(attributes, $slickAttrsPath), '$$[*]' COLUMNS(attribute_key VARCHAR(256) CHARACTER SET utf8mb3 COLLATE utf8mb3_bin PATH '$$')) t
      where workspace_id=$workspaceId and deleted = 0 and entity_type in (""",
      inClause,
      sql""");"""
    ).as[EntityTypeAndAttributeKey]
  }

  /**
   * Gets the count of entities in a workspace, grouped by entity type.
   *
   * `execution plan: Index range scan; using where. Index: idx_entity_keys_workspace_and_entity_type.`
   */
  def countEntitiesGroupedByType(workspaceId: UUID): ReadAction[Seq[EntityTypeAndCount]] =
    sql"""SELECT entity_type, COUNT(*)
      FROM ENTITY
      WHERE workspace_id = $workspaceId
      AND deleted = 0
      GROUP BY entity_type;""".as[EntityTypeAndCount]

  /**
   * Soft-deletes the given entities: removes their attributes and sets deleted=1 and deletedDate=now
   *
   * `execution plan: Index range scan; using where, using temporary. Index: idx_entity_type_name.`
   */
  def batchHide(workspaceId: UUID, entities: Seq[EntityPointer]): ReadWriteAction[Int] = {
    // get unique suffix for renaming
    val renameSuffix = "_" + driverComponent.getSufficientlyRandomSuffix(1000000000) // 1 billion
    val deletedDate = new Timestamp(new Date().getTime)

    // start of the SQL statement:
    val baseUpdateSql =
      sql"""update ENTITY set deleted=1, attributes=null, deleted_date=$deletedDate, name=CONCAT(name, $renameSuffix)
           where deleted=0 AND workspace_id=$workspaceId AND """

    // join all the clauses with "or": `(entity_type = ? and name in (?)) or `(entity_type = ? and name in (?))`
    val criteriaSql = reduceSqlActionsWithDelim(generateTypeNameSql(entities.toSet).toSeq, sql" or ")
    val wrappedCriteriaSql = concatSqlActions(sql"(", criteriaSql, sql")")

    concatSqlActions(baseUpdateSql, wrappedCriteriaSql).asUpdate
  }

  /**
   * Soft-deletes all entities of the given type: removes their attributes and sets deleted=1 and deletedDate=now
   *
   * `execution plan: Index range scan; using where, using temporary. Index: idx_entity_type_name.`
   */
  def batchHideType(workspaceId: UUID, entityType: String): ReadWriteAction[Int] = {
    // get unique suffix for renaming
    val renameSuffix = "_" + driverComponent.getSufficientlyRandomSuffix(1000000000) // 1 billion
    val deletedDate = new Timestamp(new Date().getTime)

    val query =
      sql"""update ENTITY set deleted=1, attributes=null, deleted_date=$deletedDate, name=CONCAT(name, $renameSuffix)
           where entity_type=$entityType AND deleted=0 AND workspace_id=$workspaceId """

    query.asUpdate
  }

  /**
    * Hard-delete all the specified entities that do not have any foreign keys pointed at them.
    *
    * execution plan: index range scan; using where. Index: idx_entity_type_name
    */
  def deleteEntities(workspaceId: UUID, entities: Seq[EntityPointer]): ReadWriteAction[Int] = {
    // join all the clauses with "or": `(entity_type = ? and name in (?)) or `(entity_type = ? and name in (?))`
    val criteriaSql: SQLActionBuilder = reduceSqlActionsWithDelim(generateTypeNameSql(entities.toSet).toSeq, sql" or ")
    val whereClause = concatSqlActions(sql"(", criteriaSql, sql")")
    val finalSql = deleteEntitiesImpl(workspaceId, whereClause)
    finalSql.asUpdate
  }

  /**
    * Hard-delete all entities of a given type that do not have any foreign keys pointed at them.
    *
    * Note this does not have a "where deleted=0" clause. Thus, it will also hard-delete any entities
    * that were previously soft-deleted but which no longer have anything pointing at them (this is unlikely)
    *
    * execution plan: index range scan; using where. Index: idx_entity_type_name
    */
  def deleteEntitiesOfType(workspaceId: UUID, entityType: String): ReadWriteAction[Int] = {
    val whereClause = sql"entity_type = $entityType"
    val finalSql = deleteEntitiesImpl(workspaceId, whereClause)
    finalSql.asUpdate
  }

  // Private helper for deleteEntities and deleteEntitiesOfType.
  // The `ignore` keyword in the delete statement means this will delete all rows which do NOT
  // have foreign keys pointing to them. It will skip over, and leave in place, any rows which
  // do have foreign keys pointing to them.
  private def deleteEntitiesImpl(workspaceId: UUID, whereClause: SQLActionBuilder): SQLActionBuilder =
    concatSqlActions(
      sql"""delete ignore from ENTITY
              where workspace_id = $workspaceId
              and deleted = 0
              and """,
      whereClause
    )

  // Gets any entities that have references to the entities in the given list
  // Excludes entities that are in the list
  //
  // `execution plan:
  //    Using index condition (idx_entity_type_name); Using where
  //    Table function: json_table; Using temporary; Using where`
  def getReferencesTo(workspaceId: UUID, refs: Seq[EntityPointer]): ReadAction[Seq[EntityPointer]] = {
    val toNameClause = reduceSqlActionsWithDelim(
      generateTypeNameSql(refs.toSet, typeColumn = "to_entity_type", nameColumn = "to_name").toSeq,
      sql" or "
    )
    val fromNameClause = reduceSqlActionsWithDelim(
      generateTypeNameSql(refs.toSet).toSeq,
      sql" or "
    )

    val baseSql =
      sql"""select e.entity_type, e.name
            from ENTITY e, JSON_TABLE(
                e.attributes,
                '$$.refs[*]' COLUMNS (
					to_entity_type varchar(254) CHARACTER SET utf8mb3 COLLATE utf8mb3_bin PATH '$$.t',
		            to_name varchar(254) PATH '$$.n'
                 )) jt
        where e.workspace_id = $workspaceId
        and e.deleted = 0
        and ("""

    concatSqlActions(baseSql, toNameClause, sql") and NOT (", fromNameClause, sql")")
      .as[EntityPointer]
  }

  // Gets entities that have references to any entities of the given type
  // Excludes entities with the same type
  //
  // `execution plan:
  //    Using index condition (idx_entity_type_name); Using where`
  def getReferencesToType(workspaceId: UUID, entityType: String): ReadAction[Seq[EntityPointer]] =
    sql"""select entity_type, name
          from ENTITY
          where workspace_id = $workspaceId
          and entity_type != $entityType
          and deleted = 0
          and JSON_CONTAINS(attributes, JSON_OBJECT('t', $entityType), $slickRefsPath);""".as[EntityPointer]

  /*
   * Helper: generate `(entity_type = ? and name in (?, ?, ?))` sql clauses for a set of
   * EntityKeys.
   */
  private def generateTypeNameSql(refs: Set[EntityPointer],
                                  typeColumn: String = "entity_type",
                                  nameColumn: String = "name"
  ): Iterable[SQLActionBuilder] = {
    // group the entity type/name pairs by type
    val groupedReferences: Map[String, Set[String]] = refs.groupMap(_.entityType)(_.entityName)

    // build clauses for the type/name pairs
    groupedReferences.map { case (entityType: String, entityNames: Set[String]) =>
      // build the "IN" clause values
      val entityNamesSql = reduceSqlActionsWithDelim(entityNames.map(name => sql"$name").toSeq, sql",")
      concatSqlActions(
        sql""" (#$typeColumn = $entityType and #$nameColumn in (""",
        entityNamesSql,
        sql")) "
      )
    }
  }

  def countEntities(workspaceId: UUID, entityType: String): ReadWriteAction[Int] =
    countEntitiesWithFilter(workspaceId, entityType, sql"")

  def countEntitiesWithColumnFilter(workspaceId: UUID,
                                    entityType: String,
                                    columnFilter: EntityColumnFilter
  ): ReadWriteAction[Int] = countEntitiesWithFilter(workspaceId, entityType, columnFilterCondition(columnFilter))

  def countEntitiesWithFilterTerms(workspaceId: UUID,
                                   entityType: String,
                                   entityQuery: EntityQuery,
                                   filterTerms: Seq[String]
  ): ReadWriteAction[Int] =
    countEntitiesWithFilter(workspaceId, entityType, filterTermsCondition(filterTerms, entityQuery.filterOperator))

  def queryEntitiesWithFilterTerms(workspaceId: UUID,
                                   entityType: String,
                                   entityQuery: EntityQuery,
                                   filterTerms: Seq[String]
  ): SqlStreamingAction[Seq[Entity], Entity, Read] =
    queryEntitiesWithFilter(workspaceId,
                            entityType,
                            entityQuery,
                            filterTermsCondition(filterTerms, entityQuery.filterOperator)
    )

  def queryEntitiesWithColumnFilter(workspaceId: UUID,
                                    entityType: String,
                                    entityQuery: EntityQuery,
                                    columnFilter: EntityColumnFilter
  ): SqlStreamingAction[Seq[Entity], Entity, Read] =
    queryEntitiesWithFilter(workspaceId, entityType, entityQuery, columnFilterCondition(columnFilter))

  def queryEntitiesWithNoFilter(workspaceId: UUID,
                                entityType: String,
                                entityQuery: EntityQuery
  ): SqlStreamingAction[Seq[Entity], Entity, Read] =
    queryEntitiesWithFilter(workspaceId, entityType, entityQuery, sql"")

  /**
   * Queries related records in a workspace by traversing relationships defined in the attributes of entities.
   * This method supports recursive traversal of relationships, handling both arrays and objects in JSON attributes.
   *
   * @param workspaceId      The UUID of the workspace containing the entities.
   * @param startingEntityType  The type of the entity to start the query from.
   * @param startingEntityName    The name of the entity to start the query from.
   * @param relationChain   A chain of strings representing the relation columns between entities
   * @param rootEntityType The entity type to group the results by
   * @return                 A `ReadAction` that resolves to a map of entity name to a Seq[`CompactEntityRecord`]
   *                         that includes all entities found by traversing the specified relationships.
   */
  def queryRelatedRecordsWithRelationChain(
    workspaceId: UUID,
    startingEntityType: String,
    startingEntityName: String,
    relationChain: Seq[String],
    rootEntityType: String
  ): ReadAction[Map[String, Seq[CompactEntityRecord]]] =
    if (relationChain.isEmpty) { // The method shouldn't have been called in this case
      DBIO.successful(Map.empty[String, Seq[CompactEntityRecord]])
    } else {
      // Start with the root entity
      val initialEntities = Set(EntityPointer(startingEntityType, startingEntityName))

      // Traverse the chain, tracking entity type at each step
      def traverse(
        currentEntities: Set[EntityPointer],
        remainingChain: Seq[String]
      ): ReadAction[Map[String, Seq[EntityPointer]]] =
        if (remainingChain.isEmpty) {
          // End of chain, group by current entity names
          DBIO.successful(currentEntities.groupBy(_.entityName).view.mapValues(_.toSeq).toMap)
        } else {
          val nextRelation = remainingChain.head
          // Traverse one step to get next entities and their type
          getEntityTypeForRelation(workspaceId, currentEntities, nextRelation).flatMap { case (_, nextEntityType) =>
            getAllReferencesForRelation(workspaceId, currentEntities, nextRelation).flatMap { nextEntities =>
              if (nextEntities.isEmpty) {
                DBIO.successful(Map.empty)
              } else if (nextEntityType == rootEntityType) {
                // Group by rootEntityType at this step
                val grouped = nextEntities.groupBy(_.entityName).view.mapValues(_.toSeq).toMap
                if (remainingChain.tail.isEmpty) {
                  DBIO.successful(grouped)
                } else {
                  // For each group, continue traversing the rest of the chain
                  val rest = remainingChain.tail
                  DBIO
                    .sequence(
                      grouped.map { case (groupName, groupEntities) =>
                        traverse(groupEntities.toSet, rest).map { subGroup =>
                          // Flatten the subgroups and keep the grouping by groupName
                          // subGroup: Map[String, Seq[EntityPointer]]
                          // We want to collect all the values (Seq[EntityPointer]) into one Seq for this groupName
                          groupName -> subGroup.values.flatten.toSeq
                        }
                      }.toSeq
                    )
                    .map(_.toMap)
                }
              } else {
                // Not at rootEntityType yet, keep traversing
                traverse(nextEntities, remainingChain.tail)
              }
            }
          }
        }

      // Traverse and get entity pointers grouped by rootEntityType
      traverse(initialEntities, relationChain).flatMap { groupedPointers =>
        // Fetch full records for all final entities
        val allPointers = groupedPointers.values.flatten.toSet
        getEntities(workspaceId, allPointers).map { entities =>
          val entitiesByPointer = entities.groupBy(e => EntityPointer(e.entityType, e.name))
          groupedPointers.map { case (groupName, pointers) =>
            groupName -> pointers.flatMap(entitiesByPointer.get).flatten.toSeq
          }
        }
      }
    }

  // Helper: fetch all referenced entities for a given relation from a set of entities
  private def getAllReferencesForRelation(
    workspaceId: UUID,
    currentEntities: Set[EntityPointer],
    relation: String
  ): ReadAction[Set[EntityPointer]] =
    if (currentEntities.isEmpty) {
      DBIO.successful(Set.empty[EntityPointer])
    } else {
      getEntities(workspaceId, currentEntities).map { entities =>
        entities.flatMap { entityRecord =>
          val attrValue = entityRecord.toEntity.attributes.get(AttributeName.fromDelimitedName(relation))
          attrValue match {
            case Some(rel: AttributeEntityReference)      => Seq(rel.toPointer)
            case Some(rels: AttributeEntityReferenceList) => rels.list.map(_.toPointer)
            case _                                        => Seq.empty[EntityPointer]
          }
        }.toSet
      }
    }

  private def traverseOneStep(
    workspaceId: UUID,
    currentEntities: Set[EntityPointer],
    relation: String
  ): ReadAction[Set[EntityPointer]] =
    if (currentEntities.isEmpty) {
      DBIO.successful(Set.empty[EntityPointer])
    } else {
      // retrieve the current entities to get their attributes
      getEntities(workspaceId, currentEntities).flatMap { entities =>
        // find all references in the specified relation attribute
        // TODO: should this reuse CompactEntityProvider.findAllReferences ?
        val references = entities.flatMap { entityRecord =>
          val attrValue = entityRecord.toEntity.attributes.get(AttributeName.fromDelimitedName(relation))
          attrValue match {
            case Some(rel: AttributeEntityReference)      => Seq(rel.toPointer)
            case Some(rels: AttributeEntityReferenceList) => rels.list.map(_.toPointer)
            case _                                        => Seq.empty[EntityPointer]
          }
        }
        DBIO.successful(references.toSet)
      }
    }

  /**
   * Traverse the entity relation chain to determine the final entity type.
   * This method follows the entity relationships defined by the relation chain
   * but only returns the entity type information, not the full entities.
   *
   * @param workspaceId The UUID of the workspace containing the entities.
   * @param startingEntityType The type of the entity to start the traversal from.
   * @param startingEntityName The name of the specific entity to start from.
   * @param entityRelationChain A list of attribute names to traverse (e.g., ["samples", "participant"]).
   * @return A ReadAction that resolves to the entity type found at the end of the traversal chain,
   *         or None if no entity is found or the relation chain doesn't exist.
   */
  def determineEntityTypeAtEndOfChain(
    workspaceId: UUID,
    startingEntityType: String,
    startingEntityName: String,
    entityRelationChain: List[String]
  ): ReadAction[Option[String]] =
    if (entityRelationChain.isEmpty) {
      // No traversal needed, return the starting type
      DBIO.successful(Some(startingEntityType))
    } else {
      // Start with the root entity
      val initialEntities = Set(EntityPointer(startingEntityType, startingEntityName))

      // Follow the relation chain to get the entity type at each step
      entityRelationChain
        .foldLeft(DBIO.successful((initialEntities, startingEntityType)): ReadAction[(Set[EntityPointer], String)]) {
          case (previousStep, relation) =>
            previousStep.flatMap { case (currentEntities, _) =>
              println("previousstep entities: ")
              currentEntities.foreach(e => print(s"${e.entityType}/${e.entityName} "))
              println(s"\nTraversing relation: $relation")
              if (currentEntities.isEmpty) {
                DBIO.successful((Set.empty[EntityPointer], ""))
              } else {
                // For each entity, find the relation attribute and determine its entity type
                // We only need to determine the type of the first valid reference
                getEntityTypeForRelation(workspaceId, currentEntities, relation)
              }
            }
        }
        .map { case (entities, entityType) =>
          if (entities.isEmpty) None else Some(entityType)
        }
    }

  /**
   * Helper method to determine the entity type for a given relation without fetching all entity data.
   */
  private def getEntityTypeForRelation(
    workspaceId: UUID,
    currentEntities: Set[EntityPointer],
    relation: String
  ): ReadAction[(Set[EntityPointer], String)] = {
    currentEntities.foreach(e => print(s"${e.entityType}/${e.entityName} "))
    // Query just enough to get the type from the first entity that has the relation
    val query = concatSqlActions(
      sql"""select e.entity_type, e.name, jt.ref_entity_type, jt.ref_name
         from ENTITY e
         join JSON_TABLE(
           e.attributes,
           '$$.refs[*]' COLUMNS (
             ref_attr_name varchar(254) CHARACTER SET utf8mb3 COLLATE utf8mb3_bin PATH '$$.a',
             ref_entity_type varchar(254) CHARACTER SET utf8mb3 COLLATE utf8mb3_bin PATH '$$.t',
             ref_name varchar(254) PATH '$$.n'
           )
         ) jt
         where e.workspace_id = $workspaceId
         and e.deleted = 0
         and jt.ref_attr_name = $relation
         and (""",
      reduceSqlActionsWithDelim(
        currentEntities.map(e => sql"(e.entity_type = ${e.entityType} and e.name = ${e.entityName})").toSeq,
        sql" or "
      ),
      sql""") limit 1"""
    )

    case class TypeRelation(entityType: String, entityName: String, refEntityType: String, refName: String)
    implicit val getTypeRelation: GetResult[TypeRelation] =
      GetResult(r => TypeRelation(r.<<, r.<<, r.<<, r.<<))

    query.as[TypeRelation].map { results =>
      if (results.isEmpty) {
        (Set.empty[EntityPointer], "")
      } else {
        // Group results by referenced entity type
        val groupedByType = results.groupBy(_.refEntityType)
        if (groupedByType.size > 1) {
          // If there are multiple entity types referenced, log a warning but continue with the first type
          logger.warn(s"Multiple entity types referenced by relation '$relation': ${groupedByType.keys.mkString(", ")}")
        }

        // Use the first entity type we find
        val firstType = results.head.refEntityType

        // Create EntityPointers for all entities of this type
        val entityPointers = results
          .filter(_.refEntityType == firstType)
          .map(r => EntityPointer(r.refEntityType, r.refName))
          .toSet

        (entityPointers, firstType)
      }
    }
  }

  /**
   * Renames an entity in the ENTITY table.
   *
   * Returns the number of entities that were renamed.
   *
   * execution plans:
   *    updateEntityNameSql: index range scan on idx_entity_type_name
   *    updateReferencesInAttributesSql: index range scan on idx_entity_type_name
   *    updateSortValues: ugly. the two CTEs are materialized and the query does full table scans against those.
   *      The initial "where workspace_id=?" on the first CTE uses the idx_entity_type_name index, and the following
   *      full table scans are against only the materialized temp tables with rows matching the workspace id.
   */
  def renameEntity(workspaceId: UUID, entityType: String, oldName: String, newName: String): ReadWriteAction[Int] = {
    // validation ensures that entityType, oldName, and newName are SQL-safe
    EntityUtils.validateEntityName(oldName)
    EntityUtils.validateEntityName(newName)
    EntityUtils.validateEntityType(entityType)
    // Update the entity name in the ENTITY table
    // explain plan: index range scan on idx_entity_type_name
    val updateEntityNameSql = sql"""update ENTITY set name = $newName, record_version = record_version + 1
          where workspace_id = $workspaceId
          and entity_type = $entityType
          and name = $oldName
          and deleted = 0"""

    // Update all embedded references in the $.refs array.
    // This is done via JSON_REPLACE(CAST(REPLACE(JSON_EXTRACT))). Explaining from the inside out:
    //   - JSON_EXTRACT(attributes, '$$.refs') gets the refs array
    //   - REPLACE(...) treats the refs array as a plain string, and replaces all occurrences of
    //      the old name with the new name
    //   - CAST(... as JSON) converts the modified string back to a JSON array
    //   - JSON_REPLACE(...) replaces the original refs array with the modified one
    // We use a string replace here because it is significantly more performant than calling JSON_REPLACE
    //  individually for each value that needs to be changed. Since we control the serialization format of the
    //  refs array, and only perform the string replace inside that array, we avoid any problems with other
    //  user-supplied values.
    val oldRef = s""""n": "$oldName", "t": "$entityType""""
    val newRef = s""""n": "$newName", "t": "$entityType""""
    val updateReferencesInAttributesSql = sql"""update ENTITY
            set attributes = JSON_REPLACE(attributes, $slickRefsPath,
              CAST(REPLACE(JSON_EXTRACT(attributes, $slickRefsPath), $oldRef, $newRef) as JSON))
            where workspace_id = $workspaceId
            and deleted = 0
            and JSON_CONTAINS(attributes, JSON_OBJECT('n', $oldName, 't', $entityType), $slickRefsPath)""".asUpdate

    // Update sortable values in the $.attrs object.
    //  1. search $.refs to find all scalar references to the target type/name
    //  2. extract the attribute name and isScalar boolean for each of those references
    //  3. re-filter to those references where isScalar is true and entityType matches; this prevents false positives from JSON_SEARCH
    //  4. update the specific entity/attribute name combinations
    val updateSortValues =
      sql"""with paths as(
              select id,
              REPLACE(JSON_UNQUOTE(JSON_SEARCH(attributes, 'all', $oldName, null, '$$.refs')), '.n', '') as path
              from ENTITY
              where workspace_id = $workspaceId
              and JSON_CONTAINS(attributes, JSON_OBJECT('n', $oldName, 't', $entityType, 'z', true), $slickRefsPath)
            ),
            attrnames as (
              select paths.id,
                JSON_EXTRACT(attributes, CONCAT(paths.path, '.a')) as attr,
                JSON_EXTRACT(attributes, CONCAT(paths.path, '.z')) as is_scalar,
                JSON_EXTRACT(attributes, CONCAT(paths.path, '.t')) as entity_type
              from ENTITY e join paths on e.id = paths.id
              having is_scalar = true and entity_type = $entityType
            )
          update ENTITY e
          join attrnames on e.id = attrnames.id
          set e.attributes = JSON_REPLACE(e.attributes, CONCAT('$$.attrs.', attrnames.attr), $newName) where e.id = attrnames.id;
         """.asUpdate

    // Execute all updates
    for {
      _ <- updateSortValues
      _ <- updateReferencesInAttributesSql
      entityRowsUpdated <- updateEntityNameSql.asUpdate
    } yield entityRowsUpdated
  }

  /**
    * Rename an entity type, updating both the ENTITY table and embedded references in entity attributes.
    *
    * Returns the number of entities that were renamed.
    *
    * execution plan:
    *     updateEntityTypeSql: index range scan on idx_entity_type_name
    *     updateReferencesInAttributesSql: index range scan on idx_entity_type_name
    */
  def renameEntityType(workspaceId: UUID, oldType: String, newType: String): ReadWriteAction[Int] = {
    // validation ensures that oldType and newType are SQL-safe
    EntityUtils.validateEntityType(oldType)
    EntityUtils.validateEntityType(newType)
    // Update the entity type in the ENTITY table
    // explain plan: index range scan on idx_entity_type_name
    val updateEntityTypeSql =
      sql"""update ENTITY set entity_type = $newType, record_version = record_version + 1
            where workspace_id = $workspaceId and entity_type = $oldType and deleted = 0"""

    // Update all embedded references in the $.refs array.
    // This is done via JSON_REPLACE(CAST(REPLACE(JSON_EXTRACT))). Explaining from the inside out:
    //   - JSON_EXTRACT(attributes, '$$.refs') gets the refs array
    //   - REPLACE(...) treats the refs array as a plain string, and replaces all occurrences of
    //      the old type with the new type
    //   - CAST(... as JSON) converts the modified string back to a JSON array
    //   - JSON_REPLACE(...) replaces the original refs array with the modified one
    // We use a string replace here because it is significantly more performant than calling JSON_REPLACE
    //  individually for each value that needs to be changed. Since we control the serialization format of the
    //  refs array, and only perform the string replace inside that array, we avoid any problems with other
    //  user-supplied values.
    val updateReferencesInAttributesSql = sql"""update ENTITY
            set attributes = JSON_REPLACE(attributes, $slickRefsPath,
              CAST(REPLACE(JSON_EXTRACT(attributes, $slickRefsPath), '"t": "#$oldType"', '"t": "#$newType"') as JSON))
            where workspace_id = $workspaceId
            and deleted = 0
            and JSON_CONTAINS(attributes, JSON_OBJECT('t', $oldType), $slickRefsPath)""".asUpdate

    // Execute all updates in same transaction
    for {
      _ <- updateReferencesInAttributesSql
      entityRowsUpdated <- updateEntityTypeSql.asUpdate
    } yield entityRowsUpdated
  }

  /**
   * Renames a single attribute across all entities of the given type in a workspace.
   * This handles both attribute name changes in the entity's attributes JSON structure and
   * also updates any references to this attribute in other entities.
   *
   * @return Number of entities that were updated
   *
   * execution plans:
   * - updateAttrsKeySql: index range scan on idx_entity_type_name
   * - updateRefsSql: index range scan on idx_entity_type_name
   */
  def renameAttribute(
    workspaceId: UUID,
    entityType: String,
    oldAttributeName: AttributeName,
    renameRequest: AttributeRename
  ): ReadWriteAction[Int] = {
    val newAttributeName = renameRequest.newAttributeName

    // Validation ensures that entityType, oldName, and newName are SQL-safe
    EntityUtils.validateEntityName(oldAttributeName.name)
    EntityUtils.validateEntityName(newAttributeName.name)
    EntityUtils.validateEntityType(entityType)

    val oldAttrDelimited = AttributeName.toDelimitedName(oldAttributeName)
    val newAttrDelimited = AttributeName.toDelimitedName(newAttributeName)

    val oldAttrPath = slickAttributePath(oldAttributeName)
    val newAttrPath = slickAttributePath(newAttributeName)

    // rename is implemented as JSON_REMOVE(JSON_SET(JSON_EXTRACT))
    // JSON_EXTRACT gets the value of the old attribute
    // JSON_SET creates the new attribute with that value
    // JSON_REMOVE deletes the old attribute
    val updateAttrsKeySql =
      sql"""update ENTITY
        set record_version = record_version + 1,
        attributes = JSON_REMOVE(
                       JSON_SET(
                         attributes,
                         $newAttrPath,
                         JSON_EXTRACT(attributes, $oldAttrPath)),
                       $oldAttrPath
                     )
        where workspace_id = $workspaceId
        and entity_type = $entityType
        and deleted = 0
        and JSON_CONTAINS_PATH(attributes, 'one', $oldAttrPath)
     """.asUpdate

    // Renames references in $.refs using JSON_REPLACE + REPLACE
    val searchPattern = s""""a": "$oldAttrDelimited""""
    val replacePattern = s""""a": "$newAttrDelimited""""

    val updateRefsSql = sql"""update ENTITY
        set attributes = JSON_SET(
          attributes,
          $slickRefsPath,
          CAST(
            REPLACE(
              JSON_EXTRACT(attributes, $slickRefsPath),
              $searchPattern,
              $replacePattern
            ) AS JSON
          )
        )
        where workspace_id = $workspaceId
        and deleted = 0
        and JSON_CONTAINS(attributes, JSON_OBJECT('a', $oldAttrDelimited), $slickRefsPath)""".asUpdate

    // Execute all updates
    for {
      _ <- updateRefsSql
      entityRowsUpdated <- updateAttrsKeySql
    } yield entityRowsUpdated
  }

  /**
    * Determine if an attribute exists in any entity of the given type and workspace.
    *
   * `execution plan: Using index condition; Using where. Index used: idx_entity_type_name`
    */
  def attributeExists(workspaceId: UUID, entityType: String, attributeName: AttributeName): ReadAction[Boolean] =
    anyAttributeExists(workspaceId, entityType, Set(attributeName))

  /**
    * Determine if an attribute exists in any entity of the given type and workspace.
    *
    * `execution plan: Using index condition; Using where. Index used: idx_entity_type_name`
    */
  def anyAttributeExists(workspaceId: UUID,
                         entityType: String,
                         attributeNames: Set[AttributeName]
  ): ReadAction[Boolean] = {
    // values for the attribute paths
    val attrPaths = attributeNames.map(attr => sql"${slickAttributePath(attr)}")

    val containsClause = reduceSqlActionsWithDelim(attrPaths.toSeq, sql", ")

    val baseSql = sql"""select exists (select 1 from ENTITY
         where workspace_id = $workspaceId
          and entity_type = $entityType
          and deleted = 0
          and JSON_CONTAINS_PATH(attributes, 'one', """

    concatSqlActions(baseSql, containsClause, sql"))")
      .as[Boolean]
      .head
  }

  /**
    * Remove the specified attributes from all entities of the given type and workspace.
    *
    * `execution plan: index range scan on idx_entity_type_name (with lots of JSON and string manipulation)`
    */
  def deleteAttributes(workspaceId: UUID,
                       entityType: String,
                       attributeNames: Set[AttributeName]
  ): ReadWriteAction[Int] =

    if (attributeNames.isEmpty) {
      DBIO.failed(
        new AttributeException(
          message = "The supplied set of attributes to remove cannot be empty.",
          code = StatusCodes.BadRequest
        )
      )
    } else {
      // Map all attributes-to-be-removed into a single regex. In pseudocode, this regex looks for:
      //     {"a": "attr1 OR attr2 OR attr3", ... },
      // with the final comma being optional.
      // This should hit on any instance of any element of `attributeNames` inside the $.refs array.
      val allAttrNames = attributeNames.map(x => AttributeName.toDelimitedName(x).replace(":", "\\:")).mkString("|")
      val regex = s"""\\{\\"a\\": \\"(?:$allAttrNames)\\",[^}]+\\},?"""

      // Remove all elements in the $.refs array for the attributes we want to delete.
      // This is done via JSON_REPLACE(CAST(REGEXP_REPLACE(REGEXP_REPLACE(JSON_EXTRACT)))). Explaining from the inside out:
      //   - JSON_EXTRACT(attributes, '$$.refs') gets the refs array
      //   - REGEXP_REPLACE(...) treats the refs array as a plain string, and deletes all elements matching our regex
      //   - REGEXP_REPLACE(...) handles the case where we have removed the last object in the $.refs array
      //                           and therefore need to remove the trailing comma
      //   - CAST(... as JSON) converts the modified string back to a JSON array
      //   - JSON_REPLACE(...) replaces the original refs array with the modified one
      // We use a string replace here because it is significantly more performant than calling JSON_REMOVE
      //  individually for each value that needs to be changed. Since we control the serialization format of the
      //  refs array, and only perform the string replace inside that array, we avoid any problems with other
      //  user-supplied values.
      //
      // All of this will be wrapped by JSON_REMOVE later - see the `removeSql` variable. That wrapping JSON_REMOVE
      //  will handle removing the attributes from the `attrs` object.
      val replaceRefsSql = sql"""JSON_REPLACE(attributes,
                                              $slickRefsPath,
                                              CAST(
                                              REGEXP_REPLACE(
                                                REGEXP_REPLACE(JSON_EXTRACT(attributes, $slickRefsPath), $regex, ''),
                                                ',[:space:]*\\]',
                                                ']'
                                               )
                                               as JSON))"""

      // SQL to pass the supplied attribute names as bind parameters; used by JSON_REMOVE and JSON_CONTAINS_PATH
      val attributeParameters =
        reduceSqlActionsWithDelim(attributeNames.map(attr => sql"${slickAttributePath(attr)}").toSeq, sql", ")

      // JSON_REMOVE to update the attributes json and delete the specified attributes
      val removeSql = concatSqlActions(
        sql"JSON_REMOVE(",
        replaceRefsSql,
        sql", ",
        attributeParameters,
        sql")"
      )

      // Build a where clause that targets only those entities which actually contain the attributes to be removed;
      // this way, we don't issue needless updates to entities that don't have the attributes.
      val hasAttributesClause = concatSqlActions(
        sql"JSON_CONTAINS_PATH(attributes, 'one', ",
        attributeParameters,
        sql")"
      )

      val startSql = sql"update ENTITY set record_version = record_version + 1, attributes = "

      val whereSql = sql" where workspace_id = $workspaceId and entity_type = $entityType and deleted = 0 and "

      concatSqlActions(startSql, removeSql, whereSql, hasAttributesClause).asUpdate
      /* The final query looks like:

          update ENTITY set record_version = record_version + 1,
            attributes = JSON_REMOVE(
                          JSON_REPLACE(attributes,
                                       '$.refs',
                                        CAST(
                                          REPLACE(
                                            REGEXP_REPLACE(
                                              JSON_EXTRACT(attributes,'$.refs'),
                                              '\{"a": "(?:attrToRemove1|attrToRemove2)",[^}]+\},?',
                                              ''),
                                            ', ]',
                                            ']'
                                        as JSON)),
                         '$.attrs.attrToRemove1', '$.attrs.attrToRemove2')
          where workspace_id = ?
            and entity_type = ?
            and deleted = 0
            and JSON_CONTAINS_PATH(attributes, 'one', '$.attrs.attrToRemove1', '$.attrs.attrToRemove2')
       */
    }

  // ====================================================================================================
  //  entity query helpers
  //      methods in this section are used for building entity query functions
  // ====================================================================================================

  private def countEntitiesWithFilter(workspaceId: UUID,
                                      entityType: String,
                                      filter: SQLActionBuilder
  ): ReadWriteAction[Int] =
    concatSqlActions(
      sql"select count(*) ",
      fromActiveEntitiesOfTypeInWorkspace(workspaceId, entityType),
      filter
    ).as[Int].map(_.head)

  private def queryEntitiesWithFilter(workspaceId: UUID,
                                      entityType: String,
                                      entityQuery: EntityQuery,
                                      filter: SQLActionBuilder
  ): SqlStreamingAction[Seq[Entity], Entity, Read] =
    concatSqlActions(
      selectEntityColumns,
      filteredAttributesColumn(entityQuery),
      fromActiveEntitiesOfTypeInWorkspace(workspaceId, entityType),
      filter,
      orderBy(entityQuery),
      paginationClause(entityQuery)
    ).as[Entity]

  private val selectEntityColumns =
    sql"select name, entity_type, "

  /**
   * Constructs the SQL fragment to extract specific attributes from the attributes JSON column
   * in the database based on the fields specified in the EntityQuery object.
   *
   * If specific fields are provided in entityQuery.fields, the method dynamically generates
   * a JSON object containing only those fields. Each field is extracted from the attributes column
   * using the -> operator. If no fields are specified, the entire attributes column is selected.
   *
   * Example sql produced:
   * JSON_OBJECT(
   *   'v', e.attributes -> '$.v',
   *   'refs', e.attributes -> '$.refs',
   *   'attrs', JSON_OBJECT(
   *     ?, e.attributes -> ?,
   *     ?, e.attributes -> ?
   *   ))
   */
  private def filteredAttributesColumn(entityQuery: EntityQuery) =
    entityQuery.fields.fields match {
      case Some(fields) if fields.nonEmpty =>
        // For each field, create a JSON_OBJECT that contains the field if it exists;
        // else create an empty JSON_OBJECT. These objects are all merged together below;
        // this ensures that if a field is missing in the db, it will not be present in the result.
        val fieldObjects = fields.map { field =>
          sql"IF(JSON_CONTAINS_PATH(e.attributes, 'one', ${slickAttributePath(field)}), JSON_OBJECT($field, e.attributes -> ${slickAttributePath(field)}), JSON_OBJECT())"
        }
        concatSqlActions(
          sql"""JSON_OBJECT(
               '#${CompactEntitySerialization.VERSION_KEY}', e.attributes -> '$$.#${CompactEntitySerialization.VERSION_KEY}',
               '#${CompactEntitySerialization.REFS_KEY}', e.attributes -> '$$.#${CompactEntitySerialization.REFS_KEY}',
               '#${CompactEntitySerialization.ATTRS_KEY}', JSON_MERGE_PATCH(
                  JSON_OBJECT(),""",
          reduceSqlActionsWithDelim(fieldObjects.toSeq, sql","),
          sql"))"
        )
      case _ => sql"attributes"
    }

  private def fromActiveEntitiesOfTypeInWorkspace(workspaceId: UUID, entityType: String) =
    sql" from ENTITY e where e.workspace_id = $workspaceId and e.entity_type = $entityType and e.deleted = 0"

  private def filterTermsCondition(filterTerms: Seq[String], operator: FilterOperator) = {
    // note the lower casing for case-insensitive search in JSON_SEARCH;
    // this is not necessary for the name search since the name column uses a case-insensitive collation
    val filterClauses = filterTerms.map { filterTerm =>
      sql"""(e.name like ${'%' + filterTerm + '%'}
              or JSON_SEARCH(lower(e.attributes -> '#${CompactEntitySerialization.slickAttrsPath}'), 'one', ${'%' + filterTerm.toLowerCase + '%'})
            )"""
    }
    concatSqlActions(
      sql" and (",
      reduceSqlActionsWithDelim(filterClauses, sql" #${FilterOperators.toSql(operator)} "),
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
        case attr                               =>
          // the order of the columns here is also the sort precedence, list length first, then scalar value
          // Sorting on a list column should sort by the list size and sorting on a scalar column sorts on the column value.
          // If the column is a mixed type then all scalars will group together sorted by value then all the lists will follow sorted by size.
          sql" JSON_LENGTH(e.attributes -> ${slickAttributePath(attr)}), e.attributes -> ${slickAttributePath(attr)}"
      },
      sql" #${SortDirections.toSql(entityQuery.sortDirection)}, name #${SortDirections.toSql(entityQuery.sortDirection)}"
    )

  private def paginationClause(entityQuery: EntityQuery): SQLActionBuilder =
    sql" limit ${entityQuery.pageSize} offset ${entityQuery.offset}"

  // ====================================================================================================
  //  testing helpers
  // ====================================================================================================

  /** look up the types&names of all entities referenced by the given entity */
  // N.B. MySQL effectively pushes this query's where clause down into the ENTITY_REFS view,
  // so the query is efficient.
  @VisibleForTesting
  def getReferencesFrom(workspaceId: UUID, from: EntityPointer): ReadAction[Seq[EntityPointer]] =
    sql"""select to_entity_type, to_name
         from ENTITY_REFS
         where workspace_id = $workspaceId
         and from_entity_type = ${from.entityType}
         and from_name = ${from.entityName}""".as[EntityPointer]

  @VisibleForTesting
  protected[slick] def getDeletedEntity(workspaceId: UUID,
                                        entityType: String,
                                        entityName: String
  ): ReadAction[Option[CompactEntityRecord]] = {
    val likeEntityName = s"%$entityName%"
    val selectStatement: SQLActionBuilder =
      sql"""select id, name, entity_type, workspace_id, record_version, deleted, attributes
              from ENTITY
              where workspace_id = $workspaceId
              and entity_type = $entityType
              and name like $likeEntityName;"""

    uniqueResult(selectStatement.as[CompactEntityRecord])
  }

  @VisibleForTesting
  def listActiveEntitiesOfType(workspaceId: UUID, entityType: String): ReadAction[Seq[Entity]] =
    sql"""select name, entity_type, attributes
        from ENTITY
        where workspace_id = $workspaceId
        and entity_type = $entityType
        and deleted = false"""
      .as[Entity]

  // All entities in workspace, include deleted if deleted is true
  @VisibleForTesting
  def listAllEntities(workspaceId: UUID, deleted: Boolean): ReadAction[Seq[CompactEntityRecord]] = {
    val deletedClause = if (!deleted) sql" and deleted = 0" else sql""
    concatSqlActions(
      sql"""#$basicCompactEntitySelect
          from ENTITY
          where workspace_id = $workspaceId""",
      deletedClause
    ).as[CompactEntityRecord]
  }

}
