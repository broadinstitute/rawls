package org.broadinstitute.dsde.rawls.dataaccess.slick

import akka.http.scaladsl.model.StatusCodes
import com.google.common.annotations.VisibleForTesting
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.entities.compact.CompactEntitySerialization
import org.broadinstitute.dsde.rawls.entities.exceptions.AttributeException
import org.broadinstitute.dsde.rawls.model.AttributeFormat.{ENTITY_NAME_KEY, ENTITY_TYPE_KEY}

import java.sql.Timestamp
import java.util.{Date, UUID}
import org.broadinstitute.dsde.rawls.model.FilterOperators.FilterOperator
import org.broadinstitute.dsde.rawls.model.{
  Attributable,
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
import slick.util.SQLBuilder
import spray.json._

import scala.annotation.tailrec
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

  private val fromEntityWhereNotDeleted = "from ENTITY e where e.deleted = 0"

  /**
    * Insert a single entity to the db.
    *
    * Note this does NOT handle persisting refs. See CompactEntityProvider.createEntity if you need to persist refs.
    *
    * `execution plan: multiple-row insert`
    */
  def batchCreateEntities(workspaceId: UUID, entities: Seq[Entity], insertOnly: Boolean): ReadWriteAction[Int] = {
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
      sql""" on duplicate key update record_version = record_version+1, attributes = VALUES(attributes);"""
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
    batchCreateEntities(workspaceId, Seq(entity), insertOnly = true)

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
   * Delete all rows in ENTITY_REFS for the specified entities
   *
   * Returns the number of rows deleted
   *
   * `execution plan: Uses unq_from_to index. Extra: Using where`
   */
  def deleteAllReferencesFrom(workspaceId: UUID, fromRefs: Set[EntityPointer]): ReadWriteAction[Int] = {
    val typeNameClauses = generateTypeNameSql(fromRefs, typeColumn = "from_entity_type", nameColumn = "from_name")

    val baseSql =
      sql"""delete from ENTITY_REFS
        where workspace_id = $workspaceId
        and ("""

    concatSqlActions(baseSql, reduceSqlActionsWithDelim(typeNameClauses.toSeq, sql" or "), sql")").asUpdate
  }

  /**
   * Delete all rows in ENTITY_REFS for all entities of the given type
   *
   * Returns the number of rows deleted
   *
   * `execution plan: Uses unq_from_to index. Extra: Using where`
   */
  def deleteAllReferencesFromType(workspaceId: UUID, fromType: String): ReadWriteAction[Int] =
    sql"""delete from ENTITY_REFS where workspace_id = $workspaceId and from_entity_type = $fromType""".asUpdate

  /**
    * Delete all rows in ENTITY_REFS representing references from specific attributes in entities of a given type.
    * @param workspaceId the workspace containing references
    * @param fromType the entity type containing reference
    * @param fromAttributes the attributes in the fromType entities that contain references to other entities
    * @return the number of rows deleted
    */
  def deleteAllReferencesFromAttributes(workspaceId: UUID,
                                        fromType: String,
                                        fromAttributes: Set[AttributeName]
  ): ReadWriteAction[Int] =
    if (fromAttributes.isEmpty) {
      DBIO.successful(0)
    } else {
      val entityTypePath = s"$$.$ENTITY_TYPE_KEY"
      val entityNamePath = s"$$.$ENTITY_TYPE_KEY"

      val readQueries = fromAttributes.map { attributeName =>
        val attributePath = slickAttributePath(attributeName)

        sql"""SELECT
                JSON_UNQUOTE(JSON_EXTRACT(value, $entityTypePath)) as entityType,
                JSON_UNQUOTE(JSON_EXTRACT(value, $entityNamePath)) as entityName
              FROM ENTITY,
              JSON_TABLE(
                CASE
                  WHEN JSON_TYPE(JSON_EXTRACT(attributes, $attributePath)) = 'ARRAY' THEN JSON_EXTRACT(attributes, $attributePath)
                  ELSE JSON_ARRAY(JSON_EXTRACT(attributes, $attributePath))
                END,
                '$$[*]' COLUMNS(value json PATH '$$')
              ) AS jt
              where JSON_TYPE(value) = 'OBJECT'
              and JSON_CONTAINS_PATH(value, 'all', $entityTypePath, $entityNamePath)
           """
      }

      val allReads = reduceSqlActionsWithDelim(readQueries.toSeq, sql" union all ")

      concatSqlActions(
        sql"""delete from ENTITY_REFS
            where workspace_id = $workspaceId
            and from_entity_type = $fromType
            and (to_entity_type, to_name) in (""",
        allReads,
        sql")"
      ).asUpdate
    }

  /**
    * Insert references into ENTITY_REFS.
    *
    * Returns the number of rows upserted.
    *
    * `execution plan: multi-row insert`
    */
  def insertReferences(workspaceId: UUID, references: Set[RefMapping]): ReadWriteAction[Int] =
    // short-circuit
    if (references.isEmpty) {
      DBIO.successful(0)
    } else {
      val baseSql =
        sql"""insert into ENTITY_REFS(workspace_id, from_entity_type, from_name, to_entity_type, to_name) values """

      val valuesSql = references.flatMap { refPointers =>
        refPointers.to.map { toEntity =>
          sql"($workspaceId, ${refPointers.from.entityType}, ${refPointers.from.entityName}, ${toEntity.entityType}, ${toEntity.entityName})"
        }
      }

      concatSqlActions(baseSql, reduceSqlActionsWithDelim(valuesSql.toSeq)).asUpdate
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

  /**
   * Soft-deletes the given entities: removes their attributes and sets deleted=1 and deletedDate=now
   * Does not remove rows from ENTITY_REFS table
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
   * Does not remove rows from ENTITY_REFS table
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
    * Hard-delete all of the specified entities that do not have any foreign keys pointed at them.
    *
    * execution plan: admittedly a mess, but no full table scans. Uses indexes and wheres. Lots of joins and unions
    *   and requires a temporary table for the big union.
    */
  def deleteEntities(workspaceId: UUID, entities: Seq[EntityPointer]): ReadWriteAction[Int] = {
    // join all the clauses with "or": `(entity_type = ? and name in (?)) or `(entity_type = ? and name in (?))`
    val criteriaSql: SQLActionBuilder = reduceSqlActionsWithDelim(generateTypeNameSql(entities.toSet).toSeq, sql" or ")
    val whereClause = concatSqlActions(sql"where (", criteriaSql, sql")")
    val finalSql = deleteEntitiesImpl(workspaceId, whereClause)
    finalSql.asUpdate
  }

  /**
    * Hard-delete all entities of a given type that do not have any foreign keys pointed at them.
    *
    * Note this does not have a "where deleted=0" clause. Thus, it will also hard-delete any entities
    * that were previously soft-deleted but which no longer have anything pointing at them (this is unlikely)
    *
    * execution plan: admittedly a mess, but no full table scans. Uses indexes and wheres. Lots of joins and unions
    *   and requires a temporary table for the big union.
    */
  def deleteEntitiesOfType(workspaceId: UUID, entityType: String): ReadWriteAction[Int] = {
    val whereClause = sql"where entity_type = $entityType"
    val finalSql = deleteEntitiesImpl(workspaceId, whereClause)
    finalSql.asUpdate
  }

  // private helper for deleteEntities and deleteEntitiesOfType
  private def deleteEntitiesImpl(workspaceId: UUID, whereClause: SQLActionBuilder): SQLActionBuilder = {
    val startSql = sql"""with
            CANDIDATE_WORKFLOWS as (select wf.ID, wf.ENTITY_ID
              from WORKFLOW wf, SUBMISSION s
              where wf.SUBMISSION_ID = s.ID and s.WORKSPACE_ID = $workspaceId)
          delete e from ENTITY e """

    // where clause gets inserted here, e.g. `where e.entity_type = ?`

    val endSql = sql""" and e.workspace_id = $workspaceId
          and e.id not in (
            select value_entity_ref from WORKSPACE_ATTRIBUTE where owner_id = $workspaceId
              union
            select ENTITY_ID from SUBMISSION where WORKSPACE_ID = $workspaceId
              union
            select cw.ENTITY_ID from CANDIDATE_WORKFLOWS cw
              union
            select sa.value_entity_ref
            from SUBMISSION_ATTRIBUTE sa, SUBMISSION_VALIDATION sv, CANDIDATE_WORKFLOWS cw
            where sa.owner_id = sv.id and sv.WORKFLOW_ID = cw.ID
          )
       """

    concatSqlActions(startSql, whereClause, endSql)
  }

  // Gets any entities that have references to the entities in the given list
  // Excludes entities that are in the list
  // `execution plan: uses idx_to index. Extra: Using where; Using index` (I think this may also use unq_from_to in some cases)
  def getReferencesTo(workspaceId: UUID, refs: Seq[EntityPointer]): ReadAction[Seq[EntityPointer]] = {
    val toNameClause = reduceSqlActionsWithDelim(
      generateTypeNameSql(refs.toSet, typeColumn = "to_entity_type", nameColumn = "to_name").toSeq,
      sql" or "
    )
    val fromNameClause = reduceSqlActionsWithDelim(
      generateTypeNameSql(refs.toSet, typeColumn = "from_entity_type", nameColumn = "from_name").toSeq,
      sql" or "
    )

    val baseSql =
      sql"""select from_entity_type, from_name
            from ENTITY_REFS
        where workspace_id = $workspaceId
        and ("""

    concatSqlActions(baseSql, toNameClause, sql") and NOT (", fromNameClause, sql")")
      .as[EntityPointer]
  }

  // Gets entities that have references to any entities of the given type
  // Excludes entities with the same type
  // `execution plan: Uses unq_from_to index. Extra: Using where`
  def getReferencesToType(workspaceId: UUID, entityType: String): ReadAction[Seq[EntityPointer]] =
    sql"""select from_entity_type, from_name
         from ENTITY_REFS
         where workspace_id = $workspaceId
         and to_entity_type = $entityType
         and from_entity_type != $entityType
       """.as[EntityPointer]

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
   * Rename an entity type, updating both the ENTITY table and entity references in ENTITY_REFS.
   *
   * Returns the number of entities that were renamed.
   *
   * `execution plan: multiple statements that update both ENTITY and ENTITY_REFS tables`
   */
  def renameEntityType(workspaceId: UUID, oldType: String, newType: String): ReadWriteAction[Int] = {
    // Update the entity type in the ENTITY table
    // explain plan: index range scan on idx_entity_type_name
    val updateEntityTypeSql =
      sql"""update ENTITY set entity_type = $newType, record_version = record_version + 1
            where workspace_id = $workspaceId and entity_type = $oldType and deleted = 0"""

    // Update entity references in the attributes JSON column
    // This requires a custom function that can do complex JSON updates which MySQL doesn't provide natively
    // The best approach would be to add a custom MySQL function for JSON path replacement
    // For now, we'll do this in application code when accessing entities

    // Update from_entity_type in ENTITY_REFS table
    // explain plan: index range scan on unq_from_to
    val updateFromReferencesSql =
      sql"""update ENTITY_REFS
            set from_entity_type = $newType
            where workspace_id = $workspaceId
            and from_entity_type = $oldType"""

    // Update to_entity_type in ENTITY_REFS table
    // explain plan: index range scan on unq_from_to
    val updateToReferencesSql =
      sql"""update ENTITY_REFS
            set to_entity_type = $newType
            where workspace_id = $workspaceId
            and to_entity_type = $oldType"""

    // Get all paths in attributes that reference the old type
    // This is a bit tricky because the attributes column is JSON and we need to search for the old type
    // in all possible paths. We use JSON_SEARCH to find the paths and JSON_TABLE to extract them.
    // We also need to handle the case where the reference is singular or not in an array.
    // Also exclude values in the json that match the old type but are not entity references.
    // Uses ENTITY_REFS table to find the attributes that reference the old type so must be run before
    // the ENTITY_REFS table is updated.
    // explain plan: non-unique index scan on idx_entity_type_name and idx_to
    val attrRefRegex = """'\\$\\.attrs\\.[^.]+\\.entityType'"""
    val getReferencePathsInAttributesSql =
      sql"""
        with entity_attrs as 
          (select e.attributes
          from ENTITY e
          join ENTITY_REFS er on e.workspace_id = er.workspace_id and e.entity_type = er.from_entity_type and e.name = er.from_name
          where er.workspace_id = $workspaceId
          and er.to_entity_type = $oldType)
        select type_path
        from entity_attrs e,
        json_table(json_search(e.attributes, 'all', $oldType), '$$[*]' COLUMNS( type_path VARCHAR(100) PATH '$$' ERROR ON ERROR )) jt
        where type_path REGEXP #$attrRefRegex
        union
        select json_unquote(json_search(e.attributes, 'all', $oldType))
        from entity_attrs e
        where json_type(json_search(e.attributes, 'all', $oldType)) != 'ARRAY'
        and json_unquote(json_search(e.attributes, 'all', $oldType)) REGEXP #$attrRefRegex
        """

    // explain plan: non-unique index scan on idx_entity_type_name and idx_to
    def updateReferencesInAttributesSql(paths: Seq[String]) = {
      val replaceParamsSqls = paths.map(path => sql"$path, $newType")
      concatSqlActions(
        sql"""
          update ENTITY e 
          join ENTITY_REFS er on e.workspace_id = er.workspace_id and e.entity_type = er.from_entity_type and e.name = er.from_name
          set e.attributes = JSON_REPLACE(e.attributes, 
        """,
        reduceSqlActionsWithDelim(replaceParamsSqls.toSeq, sql","),
        sql""") where er.workspace_id = $workspaceId and er.to_entity_type = $oldType"""
      )
    }

    // Execute all updates in same transaction
    for {
      paths <- getReferencePathsInAttributesSql.as[String]
      _ <-
        if (paths.isEmpty) {
          DBIO.successful(0)
        } else {
          DBIO.seq(
            updateReferencesInAttributesSql(paths).asUpdate,
            updateToReferencesSql.asUpdate
          )
        }
      _ <- updateFromReferencesSql.asUpdate
      entityRowsUpdated <- updateEntityTypeSql.asUpdate
    } yield entityRowsUpdated
  }

  /**
    * Renames a single attribute across all entities of the given type and workspace.
    * This method assumes the old and new attribute names have already been validated!
    *
    * `Using where. Index used: idx_entity_type_name`
    */
  def renameAttribute(workspaceId: UUID,
                      entityType: String,
                      oldAttributeName: AttributeName,
                      renameRequest: AttributeRename
  ): ReadWriteAction[Int] =
    // rename is implemented as JSON_REMOVE(JSON_SET(JSON_EXTRACT))
    // JSON_EXTRACT gets the value of the old attribute
    // JSON_SET creates the new attribute with that value
    // JSON_REMOVE deletes the old attribute
    sql"""update ENTITY
          set record_version = record_version + 1,
          attributes = JSON_REMOVE(
                         JSON_SET(
                           attributes,
                           ${slickAttributePath(renameRequest.newAttributeName)},
                           JSON_EXTRACT(attributes, ${slickAttributePath(oldAttributeName)})),
                         ${slickAttributePath(oldAttributeName)}
                       )
          where workspace_id = $workspaceId
          and entity_type = $entityType
          and deleted = 0
          and JSON_CONTAINS_PATH(attributes, 'one', ${slickAttributePath(oldAttributeName)})
       """.asUpdate

  /**
    * Determine if an attribute exists in any entity of the given type and workspace.
    *
    * `Using index condition; Using where. Index used: idx_entity_keys_workspace_and_entity_type`
    */
  def attributeExists(workspaceId: UUID, entityType: String, attributeName: AttributeName): ReadAction[Boolean] =
    anyAttributeExists(workspaceId, entityType, Set(attributeName))

  /**
    * Determine if an attribute exists in any entity of the given type and workspace.
    *
    * TODO CORE-468: execution plan
    */
  def anyAttributeExists(workspaceId: UUID,
                         entityType: String,
                         attributeNames: Set[AttributeName]
  ): ReadAction[Boolean] = {
    val containsClauses = attributeNames.map { attributeName =>
      sql"""JSON_CONTAINS(attribute_keys, JSON_QUOTE(${AttributeName.toDelimitedName(attributeName)}))"""
    }
    val clause = reduceSqlActionsWithDelim(containsClauses.toSeq, sql" or ")

    val baseSql = sql"""select exists (select 1 from ENTITY_KEYS
         where workspace_id = $workspaceId
          and entity_type = $entityType
          and ("""

    concatSqlActions(baseSql, clause, sql"))")
      .as[Boolean]
      .head
  }

  /**
    * Determine if a reference-containing attribute exists in any entity of the given type and workspace.
    *
    * TODO CORE-468: execution plan
    */
  def anyAttributeHasReference(workspaceId: UUID,
                               entityType: String,
                               attributeNames: Set[AttributeName]
  ): ReadAction[Boolean] = {
    val containsClauses = attributeNames.map { attributeName =>
      val refTypePathScalar = s"${slickAttributePath(attributeName)}.$ENTITY_TYPE_KEY"
      val refNamePathScalar = s"${slickAttributePath(attributeName)}.$ENTITY_NAME_KEY"
      val refTypePathArray = s"${slickAttributePath(attributeName)}[*].$ENTITY_TYPE_KEY"
      val refNamePathArray = s"${slickAttributePath(attributeName)}[*].$ENTITY_NAME_KEY"
      sql"""JSON_CONTAINS_PATH(attributes, 'all', $refTypePathScalar, $refNamePathScalar)
              or
            JSON_CONTAINS_PATH(attributes, 'all', $refTypePathArray, $refNamePathArray)
         """
    }
    val clause = reduceSqlActionsWithDelim(containsClauses.toSeq, sql" or ")

    val baseSql =
      sql"""select exists (select 1 from ENTITY
            where workspace_id = $workspaceId
            and entity_type = $entityType
            and deleted = 0
            and ("""

    concatSqlActions(baseSql, clause, sql"))")
      .as[Boolean]
      .head
  }

  /**
    * Removed the specified attributes from all entities of the given type and workspace.
    *
    * Note this does NOT also update ENTITY_REFS; see ??? to do that.
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
      // SQL to pass the supplied attribute names as bind parameters
      val attributeParameters =
        reduceSqlActionsWithDelim(attributeNames.map(attr => sql"${slickAttributePath(attr)}").toSeq, sql", ")

      // JSON_REMOVE to update the attributes json and delete the specified attributes
      val removeSql = concatSqlActions(
        sql"JSON_REMOVE(attributes, ",
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
   *   'attrs', JSON_OBJECT(
   *     ?, e.attributes -> ?,
   *     ?, e.attributes -> ?
   *   ))
   */
  private def filteredAttributesColumn(entityQuery: EntityQuery) =
    entityQuery.fields.fields match {
      case Some(fields) =>
        val fieldSqls = fields.map { field =>
          sql"$field, e.attributes -> ${slickAttributePath(field)}"
        }
        concatSqlActions(
          sql"""JSON_OBJECT(
               '#${CompactEntitySerialization.VERSION_KEY}', e.attributes -> '$$.#${CompactEntitySerialization.VERSION_KEY}',
               '#${CompactEntitySerialization.ATTRS_KEY}', JSON_OBJECT(""",
          reduceSqlActionsWithDelim(fieldSqls.toSeq, sql","),
          sql"))"
        )
      case _ => sql"attributes"
    }

  private def fromActiveEntitiesOfTypeInWorkspace(workspaceId: UUID, entityType: String) =
    sql" from ENTITY e where e.workspace_id = $workspaceId and e.entity_type = $entityType and e.deleted = 0"

  private def filterTermsCondition(filterTerms: Seq[String], operator: FilterOperator) = {
    // note the lower casing for case insensitive search
    val filterClauses = filterTerms.map { filterTerm =>
      sql"""JSON_SEARCH(lower(e.attributes -> '#${CompactEntitySerialization.slickAttrsPath}'), 'one', ${'%' + filterTerm.toLowerCase + '%'})"""
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
      sql" #${SortDirections.toSql(entityQuery.sortDirection)}"
    )

  private def paginationClause(entityQuery: EntityQuery): SQLActionBuilder =
    sql" limit ${entityQuery.pageSize} offset ${entityQuery.offset}"

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
    sql"""insert into ENTITY_REFS(workspace_id, from_entity_type, from_name, to_entity_type, to_name)
         select e.workspace_id,
          e.entity_type, e.name,
          r.entity_type, r.name
         from ENTITY e, ENTITY_ATTRIBUTE_#$shardId ea, ENTITY r
         where ea.owner_id = e.id
         and e.workspace_id = $workspaceId
         and e.deleted = 0
         and ea.value_entity_ref is not null
         and ea.value_entity_ref = r.id;""".asUpdate

  // note this cleans up legacy attributes for soft-deleted entities as well as active entities
  def migrationDeleteLegacyReferences(workspaceId: UUID, shardId: String): ReadWriteAction[Int] =
    sql"""delete ea
         from ENTITY e, ENTITY_ATTRIBUTE_#$shardId ea
         where ea.owner_id = e.id
         and e.workspace_id = $workspaceId""".asUpdate

  // ====================================================================================================
  //  testing helpers
  // ====================================================================================================

  /** look up the types&names of all entities referenced by the given entity */
  @VisibleForTesting
  def getReferencesFrom(workspaceId: UUID, from: EntityPointer): ReadAction[Seq[EntityPointer]] =
    sql"""select to_entity_type, to_name
         from ENTITY_REFS
         where workspace_id = $workspaceId
         and from_entity_type = ${from.entityType}
         and from_name = ${from.entityName}""".as[EntityPointer]

  // return the ENTITY_KEYS row for a given entity
  // `execution plan: single row constant; fully indexed by primary key`
  @VisibleForTesting
  protected[slick] def getKeys(entityId: Long): ReadAction[Option[KeysRecord]] = {
    val query = sql"""select id, workspace_id, entity_type, attribute_keys, last_updated
            from ENTITY_KEYS
            where id = $entityId;""".as[KeysRecord]
    uniqueResult(query)
  }

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
}
