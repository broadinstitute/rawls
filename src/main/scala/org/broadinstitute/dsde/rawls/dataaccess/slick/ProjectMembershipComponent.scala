package org.broadinstitute.dsde.rawls.dataaccess.slick

case class ProjectMembershipRecord(userSubjectId: String, projectName: String)

trait ProjectMembershipComponent {
  this: DriverComponent
    with RawlsUserComponent
    with RawlsBillingProjectComponent =>

  import driver.api._

  class ProjectMembershipTable(tag: Tag) extends Table[ProjectMembershipRecord](tag, "PROJECT_MEMBERSHIP") {
    def userSubjectId = column[String]("userSubjectId")
    def projectName = column[String]("projectName")

    def * = (userSubjectId, projectName) <> (ProjectMembershipRecord.tupled, ProjectMembershipRecord.unapply)

    def user = foreignKey("FK_PROJECT_MEMBERSHIP_USER", userSubjectId, rawlsUserQuery)(_.userSubjectId)
    def project = foreignKey("FK_PROJECT_MEMBERSHIP_PROJECT", projectName, rawlsBillingProjectQuery)(_.projectName)
    def pk = primaryKey("PK_PROJECT_MEMBERSHIP", (userSubjectId, projectName))
  }

  val projectMembershipQuery = TableQuery[ProjectMembershipTable]

  def projectMembershipBySubjectIdQuery(userSubjectId: String) = Compiled {
    projectMembershipQuery.filter(_.userSubjectId === userSubjectId)
  }

  def projectMembershipByProjectNameQuery(projectName: String) = Compiled {
    projectMembershipQuery.filter(_.projectName === projectName)
  }

  def saveProjectMembership(membership: ProjectMembershipRecord) = {
    (projectMembershipQuery += membership) map { _ => membership }
  }

  def loadProjectMembershipBySubjectId(userSubjectId: String) = {
    projectMembershipBySubjectIdQuery(userSubjectId).result
  }

  def loadProjectMembershipByProjectName(projectName: String) = {
    projectMembershipByProjectNameQuery(projectName).result
  }

  def deleteProjectMembership(membership: ProjectMembershipRecord) = {
    projectMembershipQuery.filter(q => q.userSubjectId === membership.userSubjectId && q.projectName === membership.projectName).delete
  }

  def deleteProjectMembershipBySubjectId(userSubjectId: String) = {
    projectMembershipBySubjectIdQuery(userSubjectId).delete
  }

  def deleteProjectMembershipByProjectName(projectName: String) = {
    projectMembershipByProjectNameQuery(projectName).delete
  }
}
