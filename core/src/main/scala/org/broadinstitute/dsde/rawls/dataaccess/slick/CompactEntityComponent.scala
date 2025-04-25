package org.broadinstitute.dsde.rawls.dataaccess.slick

import com.google.common.annotations.VisibleForTesting
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.entities.compact.CompactEntityUtils
import org.broadinstitute.dsde.rawls.model.{AttributeEntityReference, Entity}
import slick.jdbc.MySQLProfile.api._
import slick.jdbc._

import spray.json._

import java.util.UUID

trait CompactEntityComponent extends LazyLogging {
  this: DriverComponent =>

  /** low-level raw SQL queries for ENTITY. */
  object compactEntityQuery extends CompactEntityQuery(this)
}

class CompactEntityQuery(driverComponent: DriverComponent) extends RawSqlQuery with CompactEntityUtils {
  override val driver = driverComponent.driver
  import driverComponent.uniqueResult

  // read a json column from the db and translate into a JsValue
  implicit val GetJsValueResult: GetResult[JsValue] = GetResult(r => r.nextString().parseJson)

  // write a JsValue to the database by converting it to a string (the db column is still JSON)
  implicit object SetJsValueParameter extends SetParameter[JsValue] {
    def apply(v: JsValue, pp: PositionedParameters): Unit =
      pp.setString(v.compactPrint)
  }

  // select id, name, entity_type, workspace_id, record_version, deleted, deleted_date, attributes
  // into a JsonEntityRecord
  implicit val getJsonEntityRecord: GetResult[CompactEntityRecord] =
    GetResult(r => CompactEntityRecord(r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<))

  implicit val getJsonEntityRefRecord: GetResult[CompactEntityRefRecord] =
    GetResult(r => CompactEntityRefRecord(r.<<, r.<<, r.<<))

  implicit val getKeysRecord: GetResult[KeysRecord] =
    GetResult(r => KeysRecord(r.<<, r.<<, r.<<, r.<<, r.<<))

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
  def getEntity(workspaceId: UUID, entityType: String, entityName: String): ReadAction[Option[CompactEntityRecord]] = {
    val selectStatement: SQLActionBuilder =
      sql"""select id, name, entity_type, workspace_id, record_version, deleted, attributes
              from ENTITY
              where workspace_id = $workspaceId
              and entity_type = $entityType
              and name = $entityName
              and deleted = 0;"""

    uniqueResult(selectStatement.as[CompactEntityRecord])
  }

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
               from ENTITY
               where workspace_id = $workspaceId
               and deleted = 0
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

  // ====================================================================================================
  //  migration helpers
  //      methods in this section are only used for migrating data from legacy->compact format
  // ====================================================================================================

  // do not use this outside migrations; it does not change the record_version
  def migrationUpdateAttributes(workspaceId: UUID, entity: Entity): ReadWriteAction[Int] = {
    val attrsJson = toSql(entity.attributes)
    sql"""update ENTITY set attributes = $attrsJson
            where workspace_id = $workspaceId
              where workspace_id = $workspaceId
              and entity_type = ${entity.entityType}
              and name = ${entity.name}""".asUpdate
  }

  def migrationAddReferences(workspaceId: UUID, shardId: String): ReadWriteAction[Int] =
    sql"""insert into ENTITY_REFS(from_id, to_id)
         select e.id, ea.value_entity_ref
         from ENTITY e, ENTITY_ATTRIBUTE_#$shardId ea
         where ea.owner_id = e.id
         and e.workspace_id = $workspaceId
         and e.deleted = 0""".asUpdate

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
