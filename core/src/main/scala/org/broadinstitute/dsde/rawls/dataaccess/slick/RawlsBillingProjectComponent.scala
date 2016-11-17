package org.broadinstitute.dsde.rawls.dataaccess.slick

import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.model.ProjectRoles.ProjectRole
import org.broadinstitute.dsde.rawls.model._
import slick.dbio.Effect.Read

case class RawlsBillingProjectRecord(projectName: String, cromwellAuthBucketUrl: String, creationStatus: String, billingAccount: Option[String])
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
    def billingAccount = column[Option[String]]("BILLING_ACCOUNT", O.Length(100))

    def * = (projectName, cromwellAuthBucketUrl, creationStatus, billingAccount) <> (RawlsBillingProjectRecord.tupled, RawlsBillingProjectRecord.unapply)
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
      validateUserDefinedString(billingProject.projectName.value)
      uniqueResult(findBillingProjectByName(billingProject.projectName).result) flatMap {
        case Some(_) => throw new RawlsException(s"Cannot create billing project [${billingProject.projectName.value}] in database because it already exists")
        case None =>
          (rawlsBillingProjectQuery += marshalBillingProject(billingProject)) andThen
            (rawlsBillingProjectGroupQuery ++= billingProject.groups.map { case (role, group) =>
              RawlsBillingProjectGroupRecord(billingProject.projectName.value, group.groupName.value, role.toString)}).map { _ => billingProject }
      }
    }

    def updateCreationStatus(projectNames: Seq[RawlsBillingProjectName], newStatus: CreationStatuses.CreationStatus): WriteAction[Int] = {
      filter(_.projectName inSetBind(projectNames.map(_.value))).map(_.creationStatus).update(newStatus.toString)
    }

    def listProjectsWithCreationStatus(status: CreationStatuses.CreationStatus): ReadAction[Seq[RawlsBillingProject]] = {
      filter(_.creationStatus === status.toString).result.flatMap { projectRecords =>
        DBIO.sequence(projectRecords.map { projectRec => load(projectRec) })
      }
    }

    def load(projectName: RawlsBillingProjectName): ReadAction[Option[RawlsBillingProject]] = {
      uniqueResult[RawlsBillingProjectRecord](findBillingProjectByName(projectName)).flatMap {
        case None => DBIO.successful(None)
        case Some(projectRec) => load(projectRec).map(Option(_))
      }
    }

    def load(projectRec: RawlsBillingProjectRecord): ReadAction[RawlsBillingProject] = {
      findBillingProjectGroups(RawlsBillingProjectName(projectRec.projectName)).result.flatMap { groups =>
        DBIO.sequence(groups.map { group =>
          for {
            loadedGroup <- rawlsGroupQuery.load(RawlsGroupRef(RawlsGroupName(group.groupName)))
          } yield (ProjectRoles.withName(group.role), loadedGroup.get)
        }) map { groups =>
          unmarshalBillingProject(projectRec, groups.toMap)
        }
      }
    }

    /**
     * loads the immediate (i.e. not recursive) users and groups for a project
     * @param rawlsProjectName
     * @return
     */
    def loadDirectProjectMembersWithEmail(rawlsProjectName: RawlsBillingProjectName): ReadAction[Seq[RawlsBillingProjectMember]] = {
      val usersQuery = findDirectProjectUsers(rawlsProjectName)
      val subgroupsQuery = findDirectProjectSubgroups(rawlsProjectName)

      val membersQuery = usersQuery union subgroupsQuery

      membersQuery.result.map(_.map{ case(email, role) => RawlsBillingProjectMember(RawlsUserEmail(email), ProjectRoles.withName(role))})
    }

    def delete(billingProjectName: RawlsBillingProjectName): ReadWriteAction[Boolean] = {
      findBillingGroups(billingProjectName).result.flatMap { groupNames =>
        rawlsBillingProjectGroupQuery.filter(_.projectName === billingProjectName.value).delete andThen
          DBIO.sequence(groupNames.map { groupName => rawlsGroupQuery.delete(RawlsGroupRef(RawlsGroupName(groupName)))}) andThen
            rawlsBillingProjectQuery.filter(_.projectName === billingProjectName.value).delete map { count => count > 0 }
      }
    }

    private def findBillingGroups(billingProjectName: RawlsBillingProjectName) = {
      rawlsBillingProjectGroupQuery.filter(_.projectName === billingProjectName.value).map(_.groupName)
    }

    /**
     * Given a list of groups, only some of which may be associated to projects, figure out which groups
     * are associated to projects and the role for the group.
     *
     * @param groups
     * @return
     */
    def listProjectMembershipsForGroups(groups: Set[RawlsGroupRef]): ReadAction[Seq[RawlsBillingProjectMembership]] = {
      val query = for {
        projectGroup <- rawlsBillingProjectGroupQuery if (projectGroup.groupName.inSetBind(groups.map(_.groupName.value)))
        project <- rawlsBillingProjectQuery if (project.projectName === projectGroup.projectName)
      } yield (project, projectGroup)

      query.result.map(_.map { case (project, projectGroup) =>
        RawlsBillingProjectMembership(RawlsBillingProjectName(project.projectName), ProjectRoles.withName(projectGroup.role), CreationStatuses.withName(project.creationStatus))
      })
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

    /**
     * Checks that user has at least one of roles for the project specified by projectName. Will recurse through sub groups.
     * @param projectName
     * @param user
     * @param roles
     * @return
     */
    def hasOneOfProjectRole(projectName: RawlsBillingProjectName, user: RawlsUserRef, roles: Set[ProjectRole]): ReadAction[Boolean] = {
      val projectUsersAction = findBillingProjectGroupsForRoles(projectName, roles).result.flatMap { groups =>
        DBIO.sequence(groups.map { group =>
          rawlsGroupQuery.flattenGroupMembership(RawlsGroupRef(RawlsGroupName(group.groupName)))
        })
      }

      projectUsersAction.map { projectUsers =>
        projectUsers.flatten.contains(user)
      }
    }

    // does not recurse
    private def findDirectProjectUsers(projectName: RawlsBillingProjectName) = {
      for {
        projectGroup <- rawlsBillingProjectGroupQuery if projectGroup.projectName === projectName.value
        groupUser <- groupUsersQuery if groupUser.groupName === projectGroup.groupName
        user <- rawlsUserQuery if user.userSubjectId === groupUser.userSubjectId
      } yield (user.userEmail, projectGroup.role)
    }

    // does not recurse
    private def findDirectProjectSubgroups(projectName: RawlsBillingProjectName) = {
      for {
        projectGroup <- rawlsBillingProjectGroupQuery if projectGroup.projectName === projectName.value
        groupSubgroup <- groupSubgroupsQuery if groupSubgroup.parentGroupName === projectGroup.groupName
        subgroup <- rawlsGroupQuery if subgroup.groupName === groupSubgroup.parentGroupName
      } yield (subgroup.groupEmail, projectGroup.role)
    }

    private def marshalBillingProject(billingProject: RawlsBillingProject): RawlsBillingProjectRecord = {
      RawlsBillingProjectRecord(billingProject.projectName.value, billingProject.cromwellAuthBucketUrl, billingProject.status.toString, billingProject.billingAccount.map(_.value))
    }

    private def unmarshalBillingProject(projectRecord: RawlsBillingProjectRecord, groups: Map[ProjectRoles.ProjectRole, RawlsGroup]): RawlsBillingProject = {
      RawlsBillingProject(RawlsBillingProjectName(projectRecord.projectName), groups, projectRecord.cromwellAuthBucketUrl, CreationStatuses.withName(projectRecord.creationStatus), projectRecord.billingAccount.map(RawlsBillingAccountName))
    }

    private def findBillingProjectByName(name: RawlsBillingProjectName): RawlsBillingProjectQuery = {
      filter(_.projectName === name.value)
    }

    private def findBillingProjectGroups(name: RawlsBillingProjectName): RawlsBillingProjectGroupQuery = {
      rawlsBillingProjectGroupQuery filter (_.projectName === name.value)
    }

    private def findBillingProjectGroupsForRoles(name: RawlsBillingProjectName, roles: Set[ProjectRole]): RawlsBillingProjectGroupQuery = {
      rawlsBillingProjectGroupQuery filter { rec => rec.projectName === name.value && rec.role.inSetBind(roles.map(_.toString)) }
    }
  }
}

