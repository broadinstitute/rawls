package org.broadinstitute.dsde.rawls.dataaccess.slick

case class ProjectUsersRecord(userSubjectId: String, projectName: String)

trait ProjectUsersComponent {
  this: DriverComponent
    with RawlsUserComponent
    with RawlsBillingProjectComponent =>

  import driver.api._

  class ProjectUsersTable(tag: Tag) extends Table[ProjectUsersRecord](tag, "PROJECT_USERS") {
    def userSubjectId = column[String]("userSubjectId")
    def projectName = column[String]("projectName")

    def * = (userSubjectId, projectName) <> (ProjectUsersRecord.tupled, ProjectUsersRecord.unapply)

    def user = foreignKey("FK_PROJECT_USERS_USER", userSubjectId, rawlsUserQuery)(_.userSubjectId)
    def project = foreignKey("FK_PROJECT_USERS_PROJECT", projectName, rawlsBillingProjectQuery)(_.projectName)
    def pk = primaryKey("PK_PROJECT_USERS", (userSubjectId, projectName))
  }

  protected val projectUsersQuery = TableQuery[ProjectUsersTable]
}
