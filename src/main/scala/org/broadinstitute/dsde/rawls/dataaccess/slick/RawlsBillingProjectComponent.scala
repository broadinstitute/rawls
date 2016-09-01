package org.broadinstitute.dsde.rawls.dataaccess.slick

import org.broadinstitute.dsde.rawls.model.ProjectRoles.ProjectRole
import org.broadinstitute.dsde.rawls.model._

case class RawlsBillingProjectRecord(projectName: String, cromwellAuthBucketUrl: String, status: String)
case class ProjectUsersRecord(userSubjectId: String, projectName: String, role: String)

trait RawlsBillingProjectComponent {
  this: DriverComponent
    with RawlsUserComponent =>

  import driver.api._

  class RawlsBillingProjectTable(tag: Tag) extends Table[RawlsBillingProjectRecord](tag, "BILLING_PROJECT") {
    def projectName = column[String]("NAME", O.PrimaryKey, O.Length(254))
    def cromwellAuthBucketUrl = column[String]("CROMWELL_BUCKET_URL", O.Length(128))
    def status = column[String]("STATUS", O.Length(20))

    def * = (projectName, cromwellAuthBucketUrl, status) <> (RawlsBillingProjectRecord.tupled, RawlsBillingProjectRecord.unapply)
  }

  class ProjectUsersTable(tag: Tag) extends Table[ProjectUsersRecord](tag, "PROJECT_USERS") {
    def userSubjectId = column[String]("USER_SUBJECT_ID", O.Length(254))
    def projectName = column[String]("PROJECT_NAME", O.Length(254))
    def role = column[String]("ROLE", O.Length(20))

    def * = (userSubjectId, projectName, role) <> (ProjectUsersRecord.tupled, ProjectUsersRecord.unapply)

    def user = foreignKey("FK_PROJECT_USERS_USER", userSubjectId, rawlsUserQuery)(_.userSubjectId)
    def project = foreignKey("FK_PROJECT_USERS_PROJECT", projectName, rawlsBillingProjectQuery)(_.projectName)
    def pk = primaryKey("PK_PROJECT_USERS", (userSubjectId, projectName))
  }

  protected val projectUsersQuery = TableQuery[ProjectUsersTable]
  private type RawlsBillingProjectQuery = Query[RawlsBillingProjectTable, RawlsBillingProjectRecord, Seq]
  private type ProjectUsersQuery = Query[ProjectUsersTable, ProjectUsersRecord, Seq]

  object rawlsBillingProjectQuery extends TableQuery(new RawlsBillingProjectTable(_)) {

    def save(billingProject: RawlsBillingProject): WriteAction[RawlsBillingProject] = {
      validateUserDefinedString(billingProject.projectName.value)
      val projectInsert = rawlsBillingProjectQuery insertOrUpdate marshalBillingProject(billingProject)
      val ownerRecs = billingProject.owners.map(marshalProjectUsers(_, billingProject.projectName, ProjectRoles.Owner))

      // remove any owners that might have been put in the users list cause you can only be in 1
      val userRecs = (billingProject.users -- billingProject.owners).map(marshalProjectUsers(_, billingProject.projectName, ProjectRoles.User))
      val userInsert = projectUsersQuery ++= (ownerRecs ++ userRecs)

      projectInsert andThen findUsersByProjectName(billingProject.projectName.value).delete andThen userInsert map { _ => billingProject }
    }

    def updatesStatus(projectNames: Seq[RawlsBillingProjectName], newStatus: ProjectStatuses.ProjectStatus): WriteAction[Int] = {
      filter(_.projectName inSetBind(projectNames.map(_.value))).map(_.status).update(newStatus.toString)
    }

    def listProjectsWithStatus(status: ProjectStatuses.ProjectStatus): ReadAction[Seq[RawlsBillingProjectRecord]] = {
      filter(_.status === status.toString).result
    }

    def load(rawlsProjectName: RawlsBillingProjectName): ReadAction[Option[RawlsBillingProject]] = {
      val name = rawlsProjectName.value
      uniqueResult[RawlsBillingProjectRecord](findBillingProjectByName(name)).flatMap {
        case None => DBIO.successful(None)
        case Some(projectRec) =>
          for {
            users <- findUsersByProjectName(name).result
          } yield Option(unmarshalBillingProject(projectRec, users.toSet))
      }
    }

    def loadProjectUsersWithEmail(rawlsProjectName: RawlsBillingProjectName): ReadAction[Seq[RawlsBillingProjectMember]] = {
      val name = rawlsProjectName.value

      val query = for {
        projectUser <- projectUsersQuery if projectUser.projectName === name
        user <- rawlsUserQuery if user.userSubjectId === projectUser.userSubjectId
      } yield (user.userEmail, projectUser.role)

      query.result.map(_.map { case (email, role) =>
        RawlsBillingProjectMember(RawlsUserEmail(email), ProjectRoles.withName(role))
      })
    }

    def delete(billingProject: RawlsBillingProject): ReadWriteAction[Boolean] = {
      val name = billingProject.projectName.value
      val projectQuery = findBillingProjectByName(name)

      uniqueResult[RawlsBillingProjectRecord](projectQuery).flatMap {
        case None => DBIO.successful(false)
        case Some(projectRec) =>
          findUsersByProjectName(name).delete andThen projectQuery.delete map { count => count > 0 }
      }
    }

    def addUserToProject(userRef: RawlsUserRef, billingProjectName: RawlsBillingProjectName, role: ProjectRole): WriteAction[ProjectUsersRecord] = {
      val record = marshalProjectUsers(userRef, billingProjectName, role)
      projectUsersQuery insertOrUpdate record map { _ => record }
    }

    def removeUserFromProject(userRef: RawlsUserRef, billingProject: RawlsBillingProject): WriteAction[Boolean] = {
      val query = projectUsersQuery.filter(q => q.userSubjectId === userRef.userSubjectId.value && q.projectName === billingProject.projectName.value)
      query.delete.map { count => count > 0 }
    }

    def removeUserFromAllProjects(userRef: RawlsUserRef): WriteAction[Boolean] = {
      findProjectsByUserSubjectId(userRef.userSubjectId.value).delete.map { count => count > 0 }
    }

    def listUserProjects(rawlsUser: RawlsUserRef): ReadAction[Iterable[RawlsBillingProjectMembership]] = {
      val query = for {
        user <- projectUsersQuery if user.userSubjectId === rawlsUser.userSubjectId.value
        project <- rawlsBillingProjectQuery if project.projectName === user.projectName
      } yield {
        (user.role, project.projectName, project.status)
      }
      query.result.map { _.map {
        case (role, projectName, projectStatus) => RawlsBillingProjectMembership(RawlsBillingProjectName(projectName), ProjectRoles.withName(role), ProjectStatuses.withName(projectStatus))
      }}
    }

    def loadAllUsersWithProjects: ReadAction[Map[RawlsUser, Iterable[RawlsBillingProjectName]]] = {
      val usersAndProjects = for {
        (user, userProject) <- rawlsUserQuery joinLeft projectUsersQuery on (_.userSubjectId === _.userSubjectId)
      } yield (user, userProject.map(_.projectName))

      usersAndProjects.result.map { results =>
        results.groupBy(_._1) map {
          case (userRec, userAndProjectOps) =>
            val projects = userAndProjectOps.flatMap(_._2.map(RawlsBillingProjectName))
            rawlsUserQuery.unmarshalRawlsUser(userRec) -> projects
        }
      }
    }
    
    def hasOneOfProjectRole(projectName: RawlsBillingProjectName, user: RawlsUserRef, roles: Set[ProjectRole]): ReadAction[Boolean] = {
      findProjectUser(projectName, user, roles).length.result.map(_ > 0)
    }
    
    private def findProjectUser(projectName: RawlsBillingProjectName, user: RawlsUserRef, roles: Set[ProjectRole]) = {
      projectUsersQuery.filter(pu => pu.projectName === projectName.value &&
        pu.userSubjectId === user.userSubjectId.value &&
        pu.role.inSetBind(roles.map(_.toString)))
    }

    private def marshalBillingProject(billingProject: RawlsBillingProject): RawlsBillingProjectRecord = {
      RawlsBillingProjectRecord(billingProject.projectName.value, billingProject.cromwellAuthBucketUrl, billingProject.status.toString)
    }

    private def unmarshalBillingProject(projectRecord: RawlsBillingProjectRecord, userRecords: Set[ProjectUsersRecord]): RawlsBillingProject = {
      val userRefsByRole = userRecords.groupBy(rec => ProjectRoles.withName(rec.role)).map { case (role, records) =>
        role -> records.map { u => RawlsUserRef(RawlsUserSubjectId(u.userSubjectId)) }
      }
      RawlsBillingProject(RawlsBillingProjectName(projectRecord.projectName), userRefsByRole.getOrElse(ProjectRoles.Owner, Set.empty), userRefsByRole.getOrElse(ProjectRoles.User, Set.empty), projectRecord.cromwellAuthBucketUrl, ProjectStatuses.withName(projectRecord.status))
    }

    private def marshalProjectUsers(userRef: RawlsUserRef, projectName: RawlsBillingProjectName, role: ProjectRoles.ProjectRole): ProjectUsersRecord = {
      ProjectUsersRecord(userRef.userSubjectId.value, projectName.value, role.toString)
    }

    private def findBillingProjectByName(name: String): RawlsBillingProjectQuery = {
      filter(_.projectName === name)
    }

    private def findUsersByProjectName(name: String): ProjectUsersQuery = {
      projectUsersQuery.filter(_.projectName === name)
    }

    private def findProjectsByUserSubjectId(subjectId: String): ProjectUsersQuery = {
      projectUsersQuery.filter(_.userSubjectId === subjectId)
    }
  }
}

