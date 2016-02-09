package org.broadinstitute.dsde.rawls.dataaccess.slick

case class GroupSubgroupsRecord(parentGroupName: String, childGroupName: String)

trait GroupSubgroupsComponent {
  this: DriverComponent
    with RawlsGroupComponent =>

  import driver.api._

  class GroupSubgroupsTable(tag: Tag) extends Table[GroupSubgroupsRecord](tag, "GROUP_SUBGROUPS") {
    def parentGroupName = column[String]("parentGroupName")
    def childGroupName = column[String]("childGroupName")

    def * = (parentGroupName, childGroupName) <> (GroupSubgroupsRecord.tupled, GroupSubgroupsRecord.unapply)

    def parentGroup = foreignKey("FK_GROUP_SUBGROUPS_PARENT", parentGroupName, rawlsGroupQuery)(_.groupName)
    def childGroup = foreignKey("FK_GROUP_SUBGROUPS_CHILD", childGroupName, rawlsGroupQuery)(_.groupName)
    def pk = primaryKey("PK_GROUP_SUBGROUPS", (parentGroupName, childGroupName))
  }

  protected val groupSubgroupsQuery = TableQuery[GroupSubgroupsTable]
}
