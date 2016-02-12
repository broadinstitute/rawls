package org.broadinstitute.dsde.rawls.dataaccess.slick

import java.sql.Timestamp
import java.util.UUID

/**
 * Created by dvoet on 2/4/16.
 */
case class EntityRecord(id: Long, name: String, entityType: String, workspaceId: UUID)
case class EntityAttributeRecord(entityId: Long, attributeId: Long)

trait EntityComponent {
  this: DriverComponent with WorkspaceComponent with AttributeComponent =>

  import driver.api._

  class EntityTable(tag: Tag) extends Table[EntityRecord](tag, "ENTITY") {
    def id = column[Long]("id", O.PrimaryKey, O.AutoInc)
    def name = column[String]("name")
    def entityType = column[String]("entity_type")
    def workspaceId = column[UUID]("workspace_id")
    def workspace = foreignKey("FK_ENTITY_WORKSPACE", workspaceId, workspaceQuery)(_.id)
    def uniqueTypeName = index("idx_entity_type_name", (workspaceId, entityType, name), unique = true)

    def * = (id, name, entityType, workspaceId) <> (EntityRecord.tupled, EntityRecord.unapply)
  }

  class EntityAttributeTable(tag: Tag) extends Table[EntityAttributeRecord](tag, "ENTITY_ATTRIBUTE") {
    def attributeId = column[Long]("attribute_id", O.PrimaryKey, O.AutoInc)
    def entityId = column[Long]("entity_id")

    def entity = foreignKey("FK_ENT_ATTR_ENTITY", entityId, entityQuery)(_.id)
    def attribute = foreignKey("FK_ENT_ATTR_ATTRIBUTE", attributeId, attributeQuery)(_.id)

    def * = (entityId, attributeId) <> (EntityAttributeRecord.tupled, EntityAttributeRecord.unapply)
  }

  protected val entityQuery = TableQuery[EntityTable]
  protected val entityAttributeQuery = TableQuery[EntityAttributeTable]

  def findEntityByName(workspaceId: UUID, entityType: String, entityName: String): driver.api.Query[EntityTable, EntityRecord, Seq] = {
    entityQuery.filter(entRec => entRec.name === entityName && entRec.entityType === entityType && entRec.workspaceId === workspaceId)
  }

}
