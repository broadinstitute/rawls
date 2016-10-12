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
    def role = column[String]("PROJECT_ROLE", O.Length(20))

    def groupRef = foreignKey("FK_PROJECT_GROUP", groupName, rawlsGroupQuery)(_.groupName)
    def projectRef = foreignKey("FK_PROJECT_NAME", projectName, rawlsBillingProjectQuery)(_.projectName)
    def pk = primaryKey("PK_BILLING_PROJECT_GROUP", (projectName, groupName, role))

    def * = (projectName, groupName, role) <> (RawlsBillingProjectGroupRecord.tupled, RawlsBillingProjectGroupRecord.unapply)
  }

  protected val rawlsBillingProjectGroupQuery = TableQuery[RawlsBillingProjectGroupTable]

  private type RawlsBillingProjectQuery = Query[RawlsBillingProjectTable, RawlsBillingProjectRecord, Seq]
  private type RawlsBillingProjectGroupQuery = Query[RawlsBillingProjectGroupTable, RawlsBillingProjectGroupRecord, Seq]

  object rawlsBillingProjectQuery extends TableQuery(new RawlsBillingProjectTable(_)) {

    def toGoogleGroupName(groupName: RawlsGroupName) = s"GROUP_${groupName.value}@dev.test.firecloud.org" //todo: get this outta here

    def toBillingProjectGroupName(projectName: RawlsBillingProjectName, projectRole: ProjectRole) = s"PROJECT_${projectName.value}-${projectRole}" //todo: make this live in only one place

    def create(billingProjectName: RawlsBillingProjectName, cromwellBucket: String, status: CreationStatuses.CreationStatus, creators: Set[RawlsUserRef]): ReadWriteAction[RawlsBillingProject] = {
      uniqueResult(findBillingProjectByName(billingProjectName.value).result) flatMap {
        case Some(_) => throw new RawlsException(s"Cannot create billing project [${billingProjectName.value}] in database because it already exists")
        case None => //todo: rewrite this as a for-comp?
          val owner = RawlsGroupName(toBillingProjectGroupName(billingProjectName, ProjectRoles.Owner))
          val user = RawlsGroupName(toBillingProjectGroupName(billingProjectName, ProjectRoles.User))
          rawlsGroupQuery.save(RawlsGroup(owner, RawlsGroupEmail(toGoogleGroupName(owner)), creators, Set.empty)) flatMap { ownerGroup =>
            rawlsGroupQuery.save(RawlsGroup(user, RawlsGroupEmail(toGoogleGroupName(owner)), Set.empty, Set.empty)) flatMap { userGroup =>
              val foo = RawlsBillingProjectGroupRecord(billingProjectName.value, ownerGroup.groupName.value, ProjectRoles.Owner.toString)
              val bar = RawlsBillingProjectGroupRecord(billingProjectName.value, userGroup.groupName.value, ProjectRoles.User.toString)
              (rawlsBillingProjectGroupQuery ++= Seq(foo, bar)) flatMap { _ =>
                val billingProject = RawlsBillingProject(billingProjectName, ownerGroup, userGroup, cromwellBucket, status)
                (rawlsBillingProjectQuery += marshalBillingProject(billingProject)).map { _ => billingProject }
              }
            }
          }
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
              val stuff = groups.toMap
              Option(unmarshalBillingProject(projectRec, stuff(ProjectRoles.Owner), stuff(ProjectRoles.User)))
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

    def delete(billingProject: RawlsBillingProject): ReadWriteAction[Boolean] = {
      rawlsBillingProjectGroupQuery.filter(_.projectName === billingProject.projectName.value).delete andThen
        rawlsGroupQuery.delete(billingProject.owners) andThen rawlsGroupQuery.delete(billingProject.users) andThen
        rawlsBillingProjectQuery.filter(_.projectName === billingProject.projectName.value).delete map { count => count > 0 }
    }

    //this should probably end up unused too
    def addUserToProject(member: Either[RawlsUserRef, RawlsGroupRef], billingProjectName: RawlsBillingProjectName, role: ProjectRole): ReadWriteAction[RawlsGroup] = {
      rawlsGroupQuery.load(RawlsGroupRef(RawlsGroupName(toBillingProjectGroupName(billingProjectName, role)))) flatMap {
        case None => throw new RawlsException(s"Unable to save member ${member} as ${role} on project ${billingProjectName}")
        case Some(group) => member match {
          case Left(userToAdd) => rawlsGroupQuery.save(group.copy(users = group.users ++ Set(userToAdd)))
          case Right(groupToAdd) => rawlsGroupQuery.save(group.copy(subGroups = group.subGroups ++ Set(groupToAdd)))
        }
      }
    }

//    def removeUserFromProject(member: Either[RawlsUserRef, RawlsGroupRef], billingProjectName: RawlsBillingProjectName): ReadWriteAction[Boolean] = {
//      load(billingProjectName) flatMap {
//        case None => throw new RawlsException(s"Unable to remove ${member} from project ${billingProjectName}")
//        case Some(ref) => member match {
//          case Left(user) => rawlsGroupQuery.save(ref.owners.copy(users = ref.owners.users -- Set(user))) andThen rawlsGroupQuery.save(ref.users.copy(users = ref.users.users -- Set(user)))
//          case Right(group) => rawlsGroupQuery.save(ref.owners.copy(subGroups = ref.owners.subGroups -- Set(group))) andThen rawlsGroupQuery.save(ref.users.copy(subGroups = ref.users.subGroups -- Set(group)))
//        }
//      }
//    }.map{ _ => true } //todo: this is kinda BS right now. clean up

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
//      val usersAndProjects = for {
//        (user, userProject) <- rawlsUserQuery joinLeft projectUsersQuery on (_.userSubjectId === _.userSubjectId)
//      } yield (user, userProject.map(_.projectName))
//
//      usersAndProjects.result.map { results =>
//        results.groupBy(_._1) map {
//          case (userRec, userAndProjectOps) =>
//            val projects = userAndProjectOps.flatMap(_._2.map(RawlsBillingProjectName))
//            rawlsUserQuery.unmarshalRawlsUser(userRec) -> projects
//        }
//      }

      DBIO.successful(Map[RawlsUser, Iterable[RawlsBillingProjectName]]())
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

    private def unmarshalBillingProject(projectRecord: RawlsBillingProjectRecord, ownerGroup: RawlsGroup, userGroup: RawlsGroup): RawlsBillingProject = {
      RawlsBillingProject(RawlsBillingProjectName(projectRecord.projectName), ownerGroup, userGroup, projectRecord.cromwellAuthBucketUrl, CreationStatuses.withName(projectRecord.creationStatus))
    }

    private def findBillingProjectByName(name: String): RawlsBillingProjectQuery = {
      filter(_.projectName === name)
    }

    private def findBillingProjectGroups(name: String): RawlsBillingProjectGroupQuery = {
      rawlsBillingProjectGroupQuery filter (_.projectName === name)
    }

    //this probably won't end up being used
//    private def findUsersByProjectName(name: String): ReadAction[Map[RawlsUserRef, ProjectRoles.ProjectRole]] = {
//      uniqueResult(findBillingProjectByName(name).result) flatMap { bp =>
//        bp.getOrElse(throw new RawlsException(s"Unable to load billing project [${name}]"))
//
//        findBillingProjectGroups(name).result.flatMap { groups =>
//          DBIO.sequence(groups.map { group =>
//            for {
//              members <- rawlsGroupQuery.flattenGroupMembership(RawlsGroupRef(RawlsGroupName(group.groupName)))
//            } yield (members, ProjectRoles.withName(group.role))
//          }) map { membership =>
//            membership.flatMap { case (users, role) =>
//              users.map(_ -> role)
//            }.toMap
//          }
//        }
//      }
//    }

    private def findProjectGroupsByUserSubjectId(subjectId: String) = {
      for {
        group <- groupUsersQuery if group.userSubjectId === subjectId
        billingGroup <- rawlsBillingProjectGroupQuery if billingGroup.groupName === group.groupName
        billingProject <- rawlsBillingProjectQuery if billingProject.projectName === billingGroup.projectName
      } yield billingProject
    }
  }
}

