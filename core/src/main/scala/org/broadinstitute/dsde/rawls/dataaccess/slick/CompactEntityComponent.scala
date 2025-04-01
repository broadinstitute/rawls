package org.broadinstitute.dsde.rawls.dataaccess.slick

import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.model.Attributable.AttributeMap
import org.broadinstitute.dsde.rawls.model.{
  AttributeEntityReference,
  AttributeFormat,
  Entity,
  PlainArrayAttributeListSerializer
}
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._
import slick.jdbc.{GetResult, JdbcProfile, PositionedParameters, SQLActionBuilder, SetParameter}
import spray.json.DefaultJsonProtocol._
import spray.json._

import java.util.UUID
import slick.jdbc.MySQLProfile.api._

/**
  * model class for rows in the ENTITY table, used for high-level Slick operations
  */
case class CompactEntityRecord(id: Long,
                               name: String,
                               entityType: String,
                               workspaceId: UUID,
                               recordVersion: Long,
                               deleted: Boolean,
                               attributes: Option[String]
) {
  def toEntity: Entity =
    Entity(name, entityType, attributes.getOrElse("{}").parseJson.convertTo[AttributeMap])
}

/**
  * abbreviated model for rows in the ENTITY table when we don't need all the columns
  */
case class CompactEntityRefRecord(id: Long, name: String, entityType: String)

/**
  * model class for rows in the ENTITY_REFS table
  */
case class RefPointerRecord(fromId: Long, toId: Long)

trait CompactEntityComponent extends LazyLogging {
  this: DriverComponent =>

  // json codec for entity attributes
  implicit val attributeFormat: AttributeFormat = new AttributeFormat with PlainArrayAttributeListSerializer

  /** high-level Slick table for ENTITY */
  class CompactEntityTable(tag: Tag) extends Table[CompactEntityRecord](tag, "ENTITY") {
    def id = column[Long]("id", O.PrimaryKey, O.AutoInc)
    def name = column[String]("name", O.Length(254))
    def entityType = column[String]("entity_type", O.Length(254))
    def workspaceId = column[UUID]("workspace_id")
    def version = column[Long]("record_version")
    def deleted = column[Boolean]("deleted")
    def attributes = column[Option[String]]("attributes")

    def * =
      (id, name, entityType, workspaceId, version, deleted, attributes) <> (CompactEntityRecord.tupled,
                                                                            CompactEntityRecord.unapply
      )
  }

  /** high-level Slick table for ENTITY_REFS */
  class CompactEntityRefTable(tag: Tag) extends Table[RefPointerRecord](tag, "ENTITY_REFS") {
    def fromId = column[Long]("from_id")
    def toId = column[Long]("to_id")

    def * =
      (fromId, toId) <> (RefPointerRecord.tupled, RefPointerRecord.unapply)
  }

  /** high-level Slick query object for ENTITY */
  object compactEntitySlickQuery extends TableQuery(new CompactEntityTable(_)) {}

  /** high-level Slick query object for ENTITY_REFS */
  object compactEntityRefSlickQuery extends TableQuery(new CompactEntityRefTable(_)) {}

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

    /** Given a set of entity references, retrieve those entities */
    def getEntityRefs(workspaceId: UUID, refs: Set[AttributeEntityReference]): ReadAction[Seq[CompactEntityRefRecord]] =
      // short-circuit
      if (refs.isEmpty) {
        DBIO.successful(Seq.empty[CompactEntityRefRecord])
      } else {
        // group the entity type/name pairs by type
        val groupedReferences: Map[String, Set[String]] = refs.groupMap(_.entityType)(_.entityName)

        // build select statements for each type
        val queryParts: Iterable[SQLActionBuilder] = groupedReferences.map {
          case (entityType: String, entityNames: Set[String]) =>
            // build the "IN" clause values
            val entityNamesSql = reduceSqlActionsWithDelim(entityNames.map(name => sql"$name").toSeq, sql",")

            // TODO CORE-362: check query plan for this and make sure it is properly using indexes
            //   UNION query does use indexes for each select; but it also requires a temporary table to
            //   combine the results, and we can probably do better. `where (entity_type, name) in ((?, ?), (?, ?))
            //   looks like it works well
            // TODO CORE-362: include `where deleted=0`? Make that an argument?
            concatSqlActions(
              sql"""select id, name, entity_type
                from ENTITY where workspace_id = $workspaceId and entity_type = $entityType
                and name in (""",
              entityNamesSql,
              sql")"
            )
        }

        // union the select statements together
        val unionQuery = reduceSqlActionsWithDelim(queryParts.toSeq, sql" union ")

        // execute
        unionQuery.as[CompactEntityRefRecord](getJsonEntityRefRecord)
      }
  }

}
