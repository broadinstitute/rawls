package org.broadinstitute.dsde.rawls.dataaccess.slick

case class SubgroupMembershipRecord(parentGroupName: String, childGroupName: String)

trait SubgroupMembershipComponent {
  this: DriverComponent
    with RawlsGroupComponent =>

  import driver.api._

  class SubgroupMembershipTable(tag: Tag) extends Table[SubgroupMembershipRecord](tag, "SUBGROUP_MEMBERSHIP") {
    def parentGroupName = column[String]("parentGroupName")
    def childGroupName = column[String]("childGroupName")

    def * = (parentGroupName, childGroupName) <> (SubgroupMembershipRecord.tupled, SubgroupMembershipRecord.unapply)

    def parentGroup = foreignKey("FK_SUBGROUP_MEMBERSHIP_PARENT", parentGroupName, rawlsGroupQuery)(_.groupName)
    def childGroup = foreignKey("FK_SUBGROUP_MEMBERSHIP_CHILD", childGroupName, rawlsGroupQuery)(_.groupName)
    def uniqueIdx = index("IDX_SUBGROUP_MEMBERSHIP_UNIQUE", (parentGroupName, childGroupName), unique = true)
  }

  val subgroupMembershipQuery = TableQuery[SubgroupMembershipTable]

  def subgroupMembershipByParentQuery(groupName: String) = Compiled {
    subgroupMembershipQuery.filter(_.parentGroupName === groupName)
  }

  def subgroupMembershipByChildQuery(groupName: String) = Compiled {
    subgroupMembershipQuery.filter(_.childGroupName === groupName)
  }

  def saveSubgroupMembership(membership: SubgroupMembershipRecord) = {
    (subgroupMembershipQuery += membership) map { _ => membership }
  }

  def loadSubgroupMembershipByParent(groupName: String) = {
    subgroupMembershipByParentQuery(groupName).result
  }

  def loadSubgroupMembershipByChild(groupName: String) = {
    subgroupMembershipByChildQuery(groupName).result
  }

  def deleteSubgroupMembership(membership: SubgroupMembershipRecord) = {
    subgroupMembershipQuery.filter(q => q.parentGroupName === membership.parentGroupName && q.childGroupName === membership.childGroupName).delete
  }

  def deleteSubgroupMembershipByParent(groupName: String) = {
    subgroupMembershipByParentQuery(groupName).delete
  }

  def deleteSubgroupMembershipByChild(groupName: String) = {
    subgroupMembershipByChildQuery(groupName).delete
  }
}
