package org.broadinstitute.dsde.rawls.dataaccess.slick

import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.model.Attributable.AttributeMap
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._
import org.broadinstitute.dsde.rawls.model._
import slick.jdbc._
import spray.json.DefaultJsonProtocol._
import spray.json._

import java.sql.Timestamp
import java.util.UUID
import scala.language.postfixOps

/**
  * model class for rows in the ENTITY table
  */
// TODO AJ-2008: handle the all_attribute_values column
case class JsonEntityRecord(id: Long,
                            name: String,
                            entityType: String,
                            workspaceId: UUID,
                            recordVersion: Long,
                            deleted: Boolean,
                            deletedDate: Option[Timestamp],
                            attributes: JsValue
) {
  def toEntity: Entity =
    Entity(name, entityType, attributes.convertTo[AttributeMap])
}

/**
  * companion object for constants, etc.
  */
object JsonEntityComponent {
  // the length of the all_attribute_values column, which is TEXT, minus a few bytes because i'm nervous
  val allAttributeValuesColumnSize = 65532
}

/**
  * Slick component for reading/writing JSON-based entities
  */
trait JsonEntityComponent {
  this: DriverComponent =>

  import slick.jdbc.MySQLProfile.api._

  // json codec for entity attributes
  implicit val attributeFormat: AttributeFormat = new AttributeFormat with PlainArrayAttributeListSerializer

  /**
    * SQL queries for working with the ENTITY table
    */
  object jsonEntityQuery extends RawSqlQuery {
    val driver: JdbcProfile = JsonEntityComponent.this.driver

    // read a json column from the db and translate into a JsValue
    implicit val GetJsValueResult: GetResult[JsValue] = GetResult(r => r.nextString().parseJson)

    // write a JsValue to the database by converting it to a string (the db column is still JSON)
    implicit object SetJsValueParameter extends SetParameter[JsValue] {
      def apply(v: JsValue, pp: PositionedParameters): Unit =
        pp.setString(v.compactPrint)
    }

    // select id, name, entity_type, workspace_id, record_version, deleted, deleted_date, attributes
    // into a JsonEntityRecord
    implicit val getJsonEntityRecord: GetResult[JsonEntityRecord] =
      GetResult(r => JsonEntityRecord(r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<))

    /**
      * Insert a single entity to the db
      */
    // TODO AJ-2008: return Entity instead of JsonEntityRecord?
    def createEntity(workspaceId: UUID, entity: Entity): ReadWriteAction[Int] = {
      val attributesJson: JsValue = entity.attributes.toJson

      sqlu"""insert into ENTITY(name, entity_type, workspace_id, record_version, deleted, attributes)
          values (${entity.name}, ${entity.entityType}, $workspaceId, 0, 0, $attributesJson)"""
    }

    /**
      * Update a single entity in the db
      */
    // TODO AJ-2008: return Entity instead of JsonEntityRecord?
    def updateEntity(workspaceId: UUID, entity: Entity, recordVersion: Long): ReadWriteAction[Int] = {
      val attributesJson: JsValue = entity.attributes.toJson

      sqlu"""update ENTITY set record_version = record_version+1, attributes = $attributesJson
            where workspace_id = $workspaceId and entity_type = ${entity.entityType} and name = ${entity.name}
            and record_version = $recordVersion;
          """
    }

    /**
      * Read a single entity from the db
      */
    // TODO AJ-2008: return Entity instead of JsonEntityRecord?
    def getEntity(workspaceId: UUID, entityType: String, entityName: String): ReadAction[Option[JsonEntityRecord]] = {
      val selectStatement: SQLActionBuilder =
        sql"""select id, name, entity_type, workspace_id, record_version, deleted, deleted_date, attributes
              from ENTITY where workspace_id = $workspaceId and entity_type = $entityType and name = $entityName"""

      uniqueResult(selectStatement.as[JsonEntityRecord])
    }

    /**
      * All entity types for the given workspace, with their counts of active entities
      */
    def typesAndCounts(workspaceId: UUID): ReadAction[Seq[(String, Int)]] =
      sql"""select entity_type, count(1) from ENTITY where workspace_id = $workspaceId and deleted = 0 group by entity_type"""
        .as[(String, Int)]

    /**
      * All attribute names for the given workspace, paired to their entity type
      * The ENTITY_KEYS table is automatically populated via triggers on the ENTITY table; see the db
      * to understand those triggers.
      */
    def typesAndAttributes(workspaceId: UUID): ReadAction[Seq[(String, String)]] =
      sql"""SELECT DISTINCT entity_type, json_key FROM ENTITY_KEYS,
              JSON_TABLE(attribute_keys, '$$[*]' COLUMNS(json_key VARCHAR(256) PATH '$$')) t
            where workspace_id = $workspaceId;"""
        .as[(String, String)]

    def queryEntities(workspaceId: UUID, entityType: String, queryParams: EntityQuery): ReadAction[Seq[Entity]] = {

      val offset = queryParams.pageSize * (queryParams.page - 1)

      // TODO AJ-2008: full-table text search
      // TODO AJ-2008: filter by column
      // TODO AJ-2008: arbitrary sorting
      // TODO AJ-2008: result projection
      // TODO AJ-2008: total/filtered counts

      sql"""select id, name, entity_type, workspace_id, record_version, deleted, deleted_date, attributes
              from ENTITY where workspace_id = $workspaceId and entity_type = $entityType
              order by name
              limit #${queryParams.pageSize}
              offset #$offset""".as[JsonEntityRecord].map(results => results.map(_.toEntity))
    }

    // TODO AJ-2008: retrieve many JsonEntityRecords by type/name pairs. Use JsonEntityRecords for access to the recordVersion
    def retrieve(workspaceId: UUID,
                 allMentionedEntities: Seq[AttributeEntityReference]
    ): ReadAction[Seq[JsonEntityRecord]] = {
      // group the entity type/name pairs by type
      val groupedReferences: Map[String, Seq[String]] = allMentionedEntities.groupMap(_.entityType)(_.entityName)

      // build select statements for each type
      val queryParts: Iterable[SQLActionBuilder] = groupedReferences.map {
        case (entityType: String, entityNames: Seq[String]) =>
          // build the "IN" clause values
          val entityNamesSql = reduceSqlActionsWithDelim(entityNames.map(name => sql"$name"), sql",")

          // TODO AJ-2008: check query plan for this and make sure it is properly using indexes
          concatSqlActions(
            sql"""select id, name, entity_type, workspace_id, record_version, deleted, deleted_date, attributes
                from ENTITY where workspace_id = $workspaceId and entity_type = $entityType
                and name in (""",
            entityNamesSql,
            sql")"
          )
      }

      // union the select statements together
      val unionQuery = reduceSqlActionsWithDelim(queryParts.toSeq, sql" union ")

      // execute
      unionQuery.as[JsonEntityRecord]
    }
  }

}
