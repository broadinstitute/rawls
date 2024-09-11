package org.broadinstitute.dsde.rawls.dataaccess.slick

import org.broadinstitute.dsde.rawls.RawlsException
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
)

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
    def createEntity(workspaceId: UUID, entity: Entity): ReadWriteAction[JsonEntityRecord] = {
      val attributesJson: JsValue = entity.attributes.toJson

      // create insert statement
      val insertStatement =
        sqlu"""insert into ENTITY(name, entity_type, workspace_id, record_version, deleted, attributes)
          values (${entity.name}, ${entity.entityType}, $workspaceId, 0, 0, $attributesJson)"""

      // execute insert statement
      insertStatement flatMap { _ =>
        // return the actually-saved entity
        // TODO AJ-2008: move this logic up to JsonEntityProvider
        getEntity(workspaceId, entity.entityType, entity.name)
      }
    }

    /**
      * Read a single entity from the db
      */
    // TODO AJ-2008: return Entity instead of JsonEntityRecord?
    def getEntity(workspaceId: UUID, entityType: String, entityName: String): ReadAction[JsonEntityRecord] = {
      val selectStatement: SQLActionBuilder =
        sql"""select id, name, entity_type, workspace_id, record_version, deleted, deleted_date, attributes
              from ENTITY where workspace_id = $workspaceId and entity_type = $entityType and name = $entityName"""

      // execute select statement
      selectStatement.as[JsonEntityRecord].map {
        case Seq()    => throw new RawlsException(s"Expected at least one result")
        case Seq(one) => one
        case tooMany  => throw new RawlsException(s"Expected 1 result but found ${tooMany.size}")
      }
    }

  }

}
