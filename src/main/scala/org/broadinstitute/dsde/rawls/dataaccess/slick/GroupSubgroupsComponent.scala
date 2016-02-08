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

  val groupSubgroupsQuery = TableQuery[GroupSubgroupsTable]

  def groupSubgroupsByParentQuery(groupName: String) = Compiled {
    groupSubgroupsQuery.filter(_.parentGroupName === groupName)
  }

  def groupSubgroupsByChildQuery(groupName: String) = Compiled {
    groupSubgroupsQuery.filter(_.childGroupName === groupName)
  }

  def saveGroupSubgroups(membership: GroupSubgroupsRecord) = {
    (groupSubgroupsQuery += membership) map { _ => membership }
  }

  def loadGroupSubgroupsByParent(groupName: String) = {
    groupSubgroupsByParentQuery(groupName).result
  }

  def loadGroupSubgroupsByChild(groupName: String) = {
    groupSubgroupsByChildQuery(groupName).result
  }

  def deleteGroupSubgroups(membership: GroupSubgroupsRecord) = {
    groupSubgroupsQuery.filter(q => q.parentGroupName === membership.parentGroupName && q.childGroupName === membership.childGroupName).delete
  }

  def deleteGroupSubgroupsByParent(groupName: String) = {
    groupSubgroupsByParentQuery(groupName).delete
  }

  def deleteGroupSubgroupsByChild(groupName: String) = {
    groupSubgroupsByChildQuery(groupName).delete
  }
}
