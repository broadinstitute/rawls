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

    def * = (userSubjectId, groupName) <>(GroupUsersRecord.tupled, GroupUsersRecord.unapply)

    def user = foreignKey("FK_GROUP_USERS_USER", userSubjectId, rawlsUserQuery)(_.userSubjectId)
    def group = foreignKey("FK_GROUP_USERS_GROUP", groupName, rawlsGroupQuery)(_.groupName)
    def pk = primaryKey("PK_GROUP_USERS", (userSubjectId, groupName))
  }

  protected val groupUsersQuery = TableQuery[GroupUsersTable]
}