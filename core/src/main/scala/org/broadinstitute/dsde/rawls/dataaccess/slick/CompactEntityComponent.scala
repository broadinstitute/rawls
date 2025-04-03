package org.broadinstitute.dsde.rawls.dataaccess.slick

import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._
import org.broadinstitute.dsde.rawls.model.{AttributeEntityReference, AttributeFormat, Entity}
import slick.jdbc.MySQLProfile.api._
import slick.jdbc._
import spray.json.DefaultJsonProtocol._
import spray.json._

import java.util.UUID

// TODO CORE-362: add Slick tables for use in DataAccess.truncateAll
trait CompactEntityComponent extends LazyLogging {
  this: DriverComponent =>

  // json codec for entity attributes
  implicit val attributeFormat: AttributeFormat = new AttributeFormat with CompactEntityAttributeListSerializer

  /** low-level raw SQL queries for ENTITY */
  object compactEntityQuery extends RawSqlQuery {
    val driver: JdbcProfile = CompactEntityComponent.this.driver

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
      */
    def createEntity(workspaceId: UUID, entity: Entity): ReadWriteAction[Int] = {
      val attributesJson: JsValue = entity.attributes.toJson

      sqlu"""insert into ENTITY(name, entity_type, workspace_id, record_version, deleted, attributes)
          values (${entity.name}, ${entity.entityType}, $workspaceId, 0, 0, $attributesJson)"""
    }

    /**
      * Read a single entity from the db
      */
    def getEntity(workspaceId: UUID,
                  entityType: String,
                  entityName: String
    ): ReadAction[Option[CompactEntityRecord]] = {
      val selectStatement: SQLActionBuilder =
        sql"""select id, name, entity_type, workspace_id, record_version, deleted, attributes
              from ENTITY where workspace_id = $workspaceId and entity_type = $entityType and name = $entityName"""

      uniqueResult(selectStatement.as[CompactEntityRecord])
    }

    /** Given a set of entity references, return the ids being referenced.
      * Ignores deleted entities.
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
              sql""" entity_type = $entityType and name in (""",
              entityNamesSql,
              sql") "
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
      */
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
    //  testing helpers
    // ====================================================================================================

    // return all reference targets for a given reference source
    def getReferencedIds(fromId: Long): ReadAction[Seq[Long]] =
      sql"""select to_id from ENTITY_REFS where from_id = $fromId;""".as[Long]

    // return the ENTITY_KEYS row for a given entity
    def getKeys(entityId: Long): ReadAction[Option[KeysRecord]] = {
      val query = sql"""select id, workspace_id, entity_type, attribute_keys, last_updated
            from ENTITY_KEYS
            where id = $entityId;""".as[KeysRecord]
      uniqueResult(query)
    }

  }

}
