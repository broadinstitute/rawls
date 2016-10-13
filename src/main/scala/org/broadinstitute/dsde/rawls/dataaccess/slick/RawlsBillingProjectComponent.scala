package org.broadinstitute.dsde.rawls.dataaccess.slick

import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.model.ProjectRoles.ProjectRole
import org.broadinstitute.dsde.rawls.model._

case class RawlsBillingProjectRecord(projectName: String, cromwellAuthBucketUrl: String, creationStatus: String)
case class RawlsBillingProjectGroupRecord(projectName: String, groupName: String, role: String)

trait RawlsBillingProjectComponent {
  this: DriverComponent
    with RawlsUserComponent
    with RawlsGroupComponent =>

  import driver.api._

  class RawlsBillingProjectTable(tag: Tag) extends Table[RawlsBillingProjectRecord](tag, "BILLING_PROJECT") {
    def projectName = column[String]("NAME", O.PrimaryKey, O.Length(254))
    def cromwellAuthBucketUrl = column[String]("CROMWELL_BUCKET_URL", O.Length(128))
    def creationStatus = column[String]("CREATION_STATUS", O.Length(20))

    def * = (projectName, cromwellAuthBucketUrl, creationStatus) <> (RawlsBillingProjectRecord.tupled, RawlsBillingProjectRecord.unapply)
  }

  class RawlsBillingProjectGroupTable(tag: Tag) extends Table[RawlsBillingProjectGroupRecord](tag, "BILLING_PROJECT_GROUP") {
    def projectName = column[String]("PROJECT_NAME", O.Length(254))
    def groupName = column[String]("GROUP_NAME", O.Length(254))
    def role = column[String]("PROJECT_ROLE", O.Length(254))

    def groupRef = foreignKey("FK_PROJECT_GROUP", groupName, rawlsGroupQuery)(_.groupName)
    def projectRef = foreignKey("FK_PROJECT_NAME", projectName, rawlsBillingProjectQuery)(_.projectName)
    def pk = primaryKey("PK_BILLING_PROJECT_GROUP", (projectName, groupName, role))

    def * = (projectName, groupName, role) <> (RawlsBillingProjectGroupRecord.tupled, RawlsBillingProjectGroupRecord.unapply)
  }

  protected val rawlsBillingProjectGroupQuery = TableQuery[RawlsBillingProjectGroupTable]

  private type RawlsBillingProjectQuery = Query[RawlsBillingProjectTable, RawlsBillingProjectRecord, Seq]
  private type RawlsBillingProjectGroupQuery = Query[RawlsBillingProjectGroupTable, RawlsBillingProjectGroupRecord, Seq]

  object rawlsBillingProjectQuery extends TableQuery(new RawlsBillingProjectTable(_)) {

    def create(billingProject: RawlsBillingProject): ReadWriteAction[RawlsBillingProject] = {
      uniqueResult(findBillingProjectByName(billingProject.projectName.value).result) flatMap {
        case Some(_) => throw new RawlsException(s"Cannot create billing project [${billingProject.projectName.value}] in database because it already exists")
        case None =>
          (rawlsBillingProjectQuery += marshalBillingProject(billingProject)) andThen
            (rawlsBillingProjectGroupQuery ++= billingProject.groups.map{ case (role, group) => RawlsBillingProjectGroupRecord(billingProject.projectName.value, group.groupName.value, role.toString)}).map { _ => billingProject }
      }
    }

    def updateCreationStatus(projectNames: Seq[RawlsBillingProjectName], newStatus: CreationStatuses.CreationStatus): WriteAction[Int] = {
      filter(_.projectName inSetBind(projectNames.map(_.value))).map(_.creationStatus).update(newStatus.toString)
    }

    def listProjectsWithCreationStatus(status: CreationStatuses.CreationStatus): ReadAction[Seq[RawlsBillingProjectRecord]] = {
      filter(_.creationStatus === status.toString).result
    }

    def load(rawlsProjectName: RawlsBillingProjectName): ReadAction[Option[RawlsBillingProject]] = {
      val projectName = rawlsProjectName.value
      uniqueResult[RawlsBillingProjectRecord](findBillingProjectByName(projectName)).flatMap {
        case None => DBIO.successful(None)
        case Some(projectRec) =>
          findBillingProjectGroups(projectName).result.flatMap { groups =>
            DBIO.sequence(groups.map { group =>
              for {
                loadedGroup <- rawlsGroupQuery.load(RawlsGroupRef(RawlsGroupName(group.groupName)))
              } yield (ProjectRoles.withName(group.role), loadedGroup.get)
            }) map { groups =>
              Option(unmarshalBillingProject(projectRec, groups.toMap))
            }
          }
      }
    }

    def loadProjectUsersWithEmail(rawlsProjectName: RawlsBillingProjectName): ReadAction[Seq[RawlsBillingProjectMember]] = {
      val ownerUsersQuery = findProjectUsersWithRole(rawlsProjectName, ProjectRoles.Owner)
      val userUsersQuery = findProjectUsersWithRole(rawlsProjectName, ProjectRoles.User)

      val ownerSubgroupsQuery = findProjectSubgroupsWithRole(rawlsProjectName, ProjectRoles.Owner)
      val userSubgroupsQuery = findProjectSubgroupsWithRole(rawlsProjectName, ProjectRoles.User)

      val owners = ownerUsersQuery union ownerSubgroupsQuery
      val users = userUsersQuery union userSubgroupsQuery

      (owners union users).result.map(_.map{ case(email, role) => RawlsBillingProjectMember(RawlsUserEmail(email), ProjectRoles.withName(role))})
    }

    def delete(billingProjectName: RawlsBillingProjectName): ReadWriteAction[Boolean] = {
      findBillingGroups(billingProjectName).result.flatMap { groupNames =>
        rawlsBillingProjectQuery.filter(_.projectName === billingProjectName.value).delete andThen
          DBIO.sequence(groupNames.map { groupName => rawlsGroupQuery.delete(RawlsGroupRef(RawlsGroupName(groupName)))}) map { results => results.size > 0 }
      }
    }

    def findBillingGroups(billingProjectName: RawlsBillingProjectName) = {
      rawlsBillingProjectGroupQuery.filter(_.projectName === billingProjectName.value).map(_.groupName)
    }

    def getProjectRoleFromEmail(billingProjectName: RawlsBillingProjectName, email: String): ReadAction[ProjectRoles.ProjectRole] = {
      rawlsGroupQuery.loadFromEmail(email) flatMap {
        case None => throw new RawlsException(s"Unable to determine status of ${email} on project ${billingProjectName}")
        case Some(ref) => ref match {
          case Left(user) => findProjectUser(billingProjectName, user, ProjectRoles.all).result map { projectWithRole =>
            projectWithRole.toMap.values match {
              case Seq(role) => ProjectRoles.withName(role)
              case _ => throw new RawlsException(s"Unable to determine status of ${email} on project ${billingProjectName}")
            }
          }
          case Right(group) => findProjectSubgroup(billingProjectName, group, ProjectRoles.all).result map { projectWithRole =>
            projectWithRole.toMap.values match {
              case Seq(role) => ProjectRoles.withName(role)
              case _ => throw new RawlsException(s"Unable to determine status of ${email} on project ${billingProjectName}")
            }
          }
        }
      }
    }

    def removeUserFromAllProjects(userRef: RawlsUserRef): WriteAction[Boolean] = {
      findProjectGroupsByUserSubjectId(userRef.userSubjectId.value).delete.map { count => count > 0 }
    }

    def listUserProjects(rawlsUser: RawlsUserRef): ReadAction[Iterable[RawlsBillingProjectMembership]] = {
      val query = for {
        group <- groupUsersQuery if group.userSubjectId === rawlsUser.userSubjectId.value
        groupsThatAreProjectRelated <- rawlsBillingProjectGroupQuery if groupsThatAreProjectRelated.groupName === group.groupName
        project <- rawlsBillingProjectQuery if project.projectName === groupsThatAreProjectRelated.projectName
      } yield (groupsThatAreProjectRelated.role, groupsThatAreProjectRelated.projectName, project.creationStatus)
      query.result.map { _.map {
        case (role, projectName, creationStatus) => RawlsBillingProjectMembership(RawlsBillingProjectName(projectName), ProjectRoles.withName(role), CreationStatuses.withName(creationStatus))
      }}
    }

    def loadAllUsersWithProjects: ReadAction[Map[RawlsUser, Iterable[RawlsBillingProjectName]]] = {
      val usersAndProjects = for {
        (project, user) <- rawlsBillingProjectQuery join rawlsBillingProjectGroupQuery on (_.projectName === _.projectName) join
          groupUsersQuery on (_._2.groupName === _.groupName) join rawlsUserQuery on (_._2.userSubjectId === _.userSubjectId)
      } yield (user, project._1._1)

      usersAndProjects.result.map { results =>
        results.groupBy(_._1) map {
          case (userRec, userAndProjectOps) =>
            val projects = userAndProjectOps.map(proj => RawlsBillingProjectName(proj._2.projectName))
            rawlsUserQuery.unmarshalRawlsUser(userRec) -> projects
        }
      }
    }

    def hasOneOfProjectRole(projectName: RawlsBillingProjectName, user: RawlsUserRef, roles: Set[ProjectRole]): ReadAction[Boolean] = {
      findProjectUser(projectName, user, roles).length.result.map(_ > 0)
    }

    private def findProjectUser(projectName: RawlsBillingProjectName, user: RawlsUserRef, roles: Set[ProjectRole]) = {
      for {
        group <- groupUsersQuery if group.userSubjectId === user.userSubjectId.value
        groupsThatAreProjectRelated <- rawlsBillingProjectGroupQuery if groupsThatAreProjectRelated.groupName === group.groupName
        project <- rawlsBillingProjectQuery if groupsThatAreProjectRelated.groupName === group.groupName && project.projectName === projectName.value && groupsThatAreProjectRelated.role.inSet(roles.map(_.toString))
      } yield (project, groupsThatAreProjectRelated.role)
    }

    private def findProjectSubgroup(projectName: RawlsBillingProjectName, groupRef: RawlsGroupRef, roles: Set[ProjectRole]) = {
      for {
        group <- groupSubgroupsQuery if group.parentGroupName === groupRef.groupName.value
        groupsThatAreProjectRelated <- rawlsBillingProjectGroupQuery if groupsThatAreProjectRelated.groupName === group.parentGroupName
        project <- rawlsBillingProjectQuery if groupsThatAreProjectRelated.groupName === group.parentGroupName && project.projectName === projectName.value && groupsThatAreProjectRelated.role.inSet(roles.map(_.toString))
      } yield (project, groupsThatAreProjectRelated.role)
    }

    private def findProjectUsersWithRole(projectName: RawlsBillingProjectName, role: ProjectRoles.ProjectRole) = {
      for {
        group <- rawlsBillingProjectGroupQuery if group.projectName === projectName.value && group.role === role.toString
        user <- groupUsersQuery if user.groupName === group.groupName
        email <- rawlsUserQuery if email.userSubjectId === user.userSubjectId
      } yield (email.userEmail, LiteralColumn(role.toString))
    }

    private def findProjectSubgroupsWithRole(projectName: RawlsBillingProjectName, role: ProjectRoles.ProjectRole) = {
      for {
        group <- rawlsBillingProjectGroupQuery if group.projectName === projectName.value && group.role === role.toString
        subGroup <- groupSubgroupsQuery if subGroup.parentGroupName === group.groupName
        email <- rawlsGroupQuery if email.groupName === subGroup.parentGroupName
      } yield (email.groupEmail, LiteralColumn(role.toString))
    }

    private def marshalBillingProject(billingProject: RawlsBillingProject): RawlsBillingProjectRecord = {
      RawlsBillingProjectRecord(billingProject.projectName.value, billingProject.cromwellAuthBucketUrl, billingProject.status.toString)
    }

    private def unmarshalBillingProject(projectRecord: RawlsBillingProjectRecord, groups: Map[ProjectRoles.ProjectRole, RawlsGroup]): RawlsBillingProject = {
      RawlsBillingProject(RawlsBillingProjectName(projectRecord.projectName), groups, projectRecord.cromwellAuthBucketUrl, CreationStatuses.withName(projectRecord.creationStatus))
    }

    private def findBillingProjectByName(name: String): RawlsBillingProjectQuery = {
      filter(_.projectName === name)
    }

    private def findBillingProjectGroups(name: String): RawlsBillingProjectGroupQuery = {
      rawlsBillingProjectGroupQuery filter (_.projectName === name)
    }

    private def findProjectGroupsByUserSubjectId(subjectId: String) = {
      for {
        group <- groupUsersQuery if group.userSubjectId === subjectId
        billingGroup <- rawlsBillingProjectGroupQuery if billingGroup.groupName === group.groupName
        membership <- groupUsersQuery if membership.groupName === billingGroup.groupName
      } yield membership
    }
  }
}

