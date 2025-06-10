package org.broadinstitute.dsde.rawls.dataaccess.slick

import slick.jdbc.MySQLProfile.api._

import java.sql.Timestamp
import java.util.UUID
import scala.annotation.unused

/**
  * High-level Slick component for building queries with the Slick DSL.
  *
  * Currently not used in runtime code, but is called from DataAccess.truncateAll for tests.
  */
trait CompactEntitySlickComponent {

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
    def workspaceId = column[UUID]("workspace_id")
    def fromEntityType = column[String]("from_entity_type")
    def fromName = column[String]("from_name")
    def toEntityType = column[String]("to_entity_type")
    def toName = column[String]("to_name")

    def * =
      (workspaceId, fromEntityType, fromName, toEntityType, toName) <> (RefPointerRecord.tupled,
                                                                        RefPointerRecord.unapply
      )
  }

  /** high-level Slick table for ENTITY_KEYS */
  class CompactEntityKeysTable(tag: Tag) extends Table[KeysRecord](tag, "ENTITY_KEYS") {
    def id = column[Long]("id")
    def workspaceId = column[UUID]("workspace_id")
    def entityType = column[String]("entity_type")
    def attributeKeys = column[String]("attribute_keys")
    def lastUpdated = column[Timestamp]("last_updated")

    def * =
      (id, workspaceId, entityType, attributeKeys, lastUpdated) <> (KeysRecord.tupled, KeysRecord.unapply)
  }

  /** high-level Slick query object for ENTITY */
  @unused
  object compactEntitySlickQuery extends TableQuery(new CompactEntityTable(_)) {}

  /** high-level Slick query object for ENTITY_REFS */
  @unused
  object compactEntityRefSlickQuery extends TableQuery(new CompactEntityRefTable(_)) {}

  /** high-level Slick query object for ENTITY_KEYS */
  @unused
  object compactEntityKeysSlickQuery extends TableQuery(new CompactEntityKeysTable(_)) {}
}
