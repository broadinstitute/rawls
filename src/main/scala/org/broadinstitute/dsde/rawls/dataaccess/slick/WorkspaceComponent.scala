package org.broadinstitute.dsde.rawls.dataaccess.slick

import java.sql.Timestamp
import java.util.UUID

import org.joda.time.DateTime

/**
 * Created by dvoet on 2/4/16.
 */
case class WorkspaceRecord(
  namespace: String,
  name: String,
  id: UUID,
  bucketName: String,
  createdDate: Timestamp,
  lastModified: Timestamp,
  createdBy: String,
  isLocked: Boolean
)
case class WorkspaceAttributeRecord(workspaceId: UUID, attributeId: Long)
case class WorkspaceAccessRecord(workspaceId: UUID, groupName: String, accessLevel: String)

trait WorkspaceComponent {
  this: DriverComponent with AttributeComponent with RawlsGroupComponent =>

  import driver.api._

  class WorkspaceTable(tag: Tag) extends Table[WorkspaceRecord](tag, "WORKSPACE") {
    def id = column[UUID]("id", O.PrimaryKey)
    def namespace = column[String]("namespace")
    def name = column[String]("name")
    def bucketName = column[String]("bucket_name")
    def createdDate = column[Timestamp]("created_date")
    def lastModified = column[Timestamp]("last_modified")
    def createdBy = column[String]("created_by")
    def isLocked = column[Boolean]("is_locked")

    def * = (namespace, name, id, bucketName, createdDate, lastModified, createdBy, isLocked) <> (WorkspaceRecord.tupled, WorkspaceRecord.unapply)
  }

  class WorkspaceAttributeTable(tag: Tag) extends Table[WorkspaceAttributeRecord](tag, "WORKSPACE_ATTRIBUTE") {
    def attributeId = column[Long]("attribute_id", O.PrimaryKey)
    def workspaceId = column[UUID]("workspace_id")

    def workspace = foreignKey("FK_WS_ATTR_WORKSPACE", workspaceId, workspaceQuery)(_.id)
    def attribute = foreignKey("FK_WS_ATTR_ATTRIBUTE", attributeId, attributeQuery)(_.id)

    def * = (workspaceId, attributeId) <> (WorkspaceAttributeRecord.tupled, WorkspaceAttributeRecord.unapply)
  }

  class WorkspaceAccessTable(tag: Tag) extends Table[WorkspaceAccessRecord](tag, "WORKSPACE_ACCESS") {
    def groupName = column[String]("group_name")
    def workspaceId = column[UUID]("workspace_id")
    def accessLevel = column[String]("access_level")

    def workspace = foreignKey("FK_WS_ACCESS_WORKSPACE", workspaceId, workspaceQuery)(_.id)
    def group = foreignKey("FK_WS_ACCESS_GROUP", groupName, rawlsGroupQuery)(_.groupName)

    def x = primaryKey("PK_WORKSPACE_ACCESS", (workspaceId, accessLevel))

    def * = (workspaceId, groupName, accessLevel) <> (WorkspaceAccessRecord.tupled, WorkspaceAccessRecord.unapply)
  }

  protected val workspaceQuery = TableQuery[WorkspaceTable]
  protected val workspaceAttributeQuery = TableQuery[WorkspaceAttributeTable]
  protected val workspaceAccessQuery = TableQuery[WorkspaceAccessTable]
}
