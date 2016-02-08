package org.broadinstitute.dsde.rawls.dataaccess.slick

case class GroupUsersRecord(userSubjectId: String, groupName: String)

trait GroupUsersComponent {
  this: DriverComponent
    with RawlsUserComponent
    with RawlsGroupComponent =>

  import driver.api._

  class GroupUsersTable(tag: Tag) extends Table[GroupUsersRecord](tag, "GROUP_USERS") {
    def userSubjectId = column[String]("userSubjectId")
    def groupName = column[String]("groupName")

    def * = (userSubjectId, groupName) <> (GroupUsersRecord.tupled, GroupUsersRecord.unapply)

    def user = foreignKey("FK_GROUP_USERS_USER", userSubjectId, rawlsUserQuery)(_.userSubjectId)
    def group = foreignKey("FK_GROUP_USERS_GROUP", groupName, rawlsGroupQuery)(_.groupName)
    def pk = primaryKey("PK_GROUP_USERS", (userSubjectId, groupName))
  }

  val groupUsersQuery = TableQuery[GroupUsersTable]

  def groupUsersBySubjectIdQuery(userSubjectId: String) = Compiled {
    groupUsersQuery.filter(_.userSubjectId === userSubjectId)
  }

  def groupUsersByGroupNameQuery(groupName: String) = Compiled {
    groupUsersQuery.filter(_.groupName === groupName)
  }

  def saveGroupUsers(membership: GroupUsersRecord) = {
    (groupUsersQuery += membership) map { _ => membership }
  }

  def loadGroupUsersBySubjectId(userSubjectId: String) = {
    groupUsersBySubjectIdQuery(userSubjectId).result
  }

  def loadGroupUsersByGroupName(groupName: String) = {
    groupUsersByGroupNameQuery(groupName).result
  }

  def deleteGroupUsers(membership: GroupUsersRecord) = {
    groupUsersQuery.filter(q => q.userSubjectId === membership.userSubjectId && q.groupName === membership.groupName).delete
  }

  def deleteGroupUsersBySubjectId(userSubjectId: String) = {
    groupUsersBySubjectIdQuery(userSubjectId).delete
  }

  def deleteGroupUsersByGroupName(groupName: String) = {
    groupUsersByGroupNameQuery(groupName).delete
  }
}
