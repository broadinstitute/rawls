package org.broadinstitute.dsde.rawls.dataaccess.slick

case class UserMembershipRecord(userSubjectId: String, groupName: String)

trait UserMembershipComponent {
  this: DriverComponent
    with RawlsUserComponent
    with RawlsGroupComponent =>

  import driver.api._

  class UserMembershipTable(tag: Tag) extends Table[UserMembershipRecord](tag, "USER_MEMBERSHIP") {
    def userSubjectId = column[String]("userSubjectId")
    def groupName = column[String]("groupName")

    def * = (userSubjectId, groupName) <> (UserMembershipRecord.tupled, UserMembershipRecord.unapply)

    def user = foreignKey("FK_USER_MEMBERSHIP_USER", userSubjectId, rawlsUserQuery)(_.userSubjectId)
    def group = foreignKey("FK_USER_MEMBERSHIP_GROUP", groupName, rawlsGroupQuery)(_.groupName)
    def pk = primaryKey("PK_USER_MEMBERSHIP", (userSubjectId, groupName))
  }

  val userMembershipQuery = TableQuery[UserMembershipTable]

  def userMembershipBySubjectIdQuery(userSubjectId: String) = Compiled {
    userMembershipQuery.filter(_.userSubjectId === userSubjectId)
  }

  def userMembershipByGroupNameQuery(groupName: String) = Compiled {
    userMembershipQuery.filter(_.groupName === groupName)
  }

  def saveUserMembership(membership: UserMembershipRecord) = {
    (userMembershipQuery += membership) map { _ => membership }
  }

  def loadUserMembershipBySubjectId(userSubjectId: String) = {
    userMembershipBySubjectIdQuery(userSubjectId).result
  }

  def loadUserMembershipByGroupName(groupName: String) = {
    userMembershipByGroupNameQuery(groupName).result
  }

  def deleteUserMembership(membership: UserMembershipRecord) = {
    userMembershipQuery.filter(q => q.userSubjectId === membership.userSubjectId && q.groupName === membership.groupName).delete
  }

  def deleteUserMembershipBySubjectId(userSubjectId: String) = {
    userMembershipBySubjectIdQuery(userSubjectId).delete
  }

  def deleteUserMembershipByGroupName(groupName: String) = {
    userMembershipByGroupNameQuery(groupName).delete
  }
}
