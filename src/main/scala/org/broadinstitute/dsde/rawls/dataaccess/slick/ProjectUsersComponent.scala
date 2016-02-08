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

  val projectUsersQuery = TableQuery[ProjectUsersTable]

  def projectUsersBySubjectIdQuery(userSubjectId: String) = Compiled {
    projectUsersQuery.filter(_.userSubjectId === userSubjectId)
  }

  def projectUsersByProjectNameQuery(projectName: String) = Compiled {
    projectUsersQuery.filter(_.projectName === projectName)
  }

  def saveProjectUsers(membership: ProjectUsersRecord) = {
    (projectUsersQuery += membership) map { _ => membership }
  }

  def loadProjectUsersBySubjectId(userSubjectId: String) = {
    projectUsersBySubjectIdQuery(userSubjectId).result
  }

  def loadProjectUsersByProjectName(projectName: String) = {
    projectUsersByProjectNameQuery(projectName).result
  }

  def deleteProjectUsers(membership: ProjectUsersRecord) = {
    projectUsersQuery.filter(q => q.userSubjectId === membership.userSubjectId && q.projectName === membership.projectName).delete
  }

  def deleteProjectUsersBySubjectId(userSubjectId: String) = {
    projectUsersBySubjectIdQuery(userSubjectId).delete
  }

  def deleteProjectUsersByProjectName(projectName: String) = {
    projectUsersByProjectNameQuery(projectName).delete
  }
}
