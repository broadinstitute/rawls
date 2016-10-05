package org.broadinstitute.dsde.rawls.dataaccess.slick

import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.model.ProjectRoles.ProjectRole
import org.broadinstitute.dsde.rawls.model._

case class RawlsBillingProjectRecord(projectName: String, cromwellAuthBucketUrl: String, creationStatus: String, ownerGroup: String, userGroup: String) //we can probably derive these so this is possible temp

trait RawlsBillingProjectComponent {
  this: DriverComponent
    with RawlsUserComponent
    with RawlsGroupComponent =>

  import driver.api._

  class RawlsBillingProjectTable(tag: Tag) extends Table[RawlsBillingProjectRecord](tag, "BILLING_PROJECT") {
    def projectName = column[String]("NAME", O.PrimaryKey, O.Length(254))
    def cromwellAuthBucketUrl = column[String]("CROMWELL_BUCKET_URL", O.Length(128))
    def creationStatus = column[String]("CREATION_STATUS", O.Length(20))
    def ownerGroup = column[String]("OWNER_GROUP", O.Length(254))
    def userGroup = column[String]("USER_GROUP", O.Length(254)) //feel like these should end up being ints (add column to group table?)

    def projectOwners = foreignKey("FK_PROJECT_OWNER_GROUP", ownerGroup, rawlsGroupQuery)(_.groupName)
    def projectUsers = foreignKey("FK_PROJECT_USER_GROUP", ownerGroup, rawlsGroupQuery)(_.groupName)

    def * = (projectName, cromwellAuthBucketUrl, creationStatus, projectOwners, projectUsers) <> (RawlsBillingProjectRecord.tupled, RawlsBillingProjectRecord.unapply)
  }

  private type RawlsBillingProjectQuery = Query[RawlsBillingProjectTable, RawlsBillingProjectRecord, Seq]

  object rawlsBillingProjectQuery extends TableQuery(new RawlsBillingProjectTable(_)) {

    //save is now create?
    def create(billingProjectName: RawlsBillingProjectName, cromwellBucket: String, status: CreationStatuses.CreationStatus, creators: Set[RawlsUserRef]): ReadWriteAction[RawlsBillingProject] = {
      uniqueResult(findBillingProjectByName(billingProjectName.value).result) flatMap {
        case Some(_) => throw new RawlsException(s"Cannot create billing project [${billingProjectName.value}] in database because it already exists")
        case None => //todo: rewrite this as a for-comp?
          rawlsGroupQuery.save(RawlsGroup(RawlsGroupName(toBillingProjectGroupName(billingProjectName, ProjectRoles.Owner)), RawlsGroupEmail("bar@bar.com"), creators, Set.empty)) flatMap { ownerGroup => //todo: real emails
            rawlsGroupQuery.save(RawlsGroup(RawlsGroupName(toBillingProjectGroupName(billingProjectName, ProjectRoles.User)), RawlsGroupEmail("bar@bar.com"), Set.empty, Set.empty)) flatMap { userGroup =>
              val billingProject = RawlsBillingProject(billingProjectName, ownerGroup, userGroup, cromwellBucket, status)
              (rawlsBillingProjectQuery += marshalBillingProject(billingProject)).map { _ => billingProject }
            }
          }
      }
    }

    //strictly update
    def save(billingProject: RawlsBillingProject): WriteAction[RawlsBillingProject] = {
      validateUserDefinedString(billingProject.projectName.value)
      val projectUpdate = rawlsBillingProjectQuery update marshalBillingProject(billingProject)

      val ownersUpdate = rawlsGroupQuery.save(billingProject.owners)

      // remove any owners that might have been put in the users list cause you can only be in 1
      val realUsers = (billingProject.users.users -- billingProject.owners.users)
      val realSubgroups = (billingProject.users.subGroups -- billingProject.owners.subGroups)
      val usersUpdate = rawlsGroupQuery.save(billingProject.users.copy(users = realUsers, subGroups = realSubgroups))

      projectUpdate andThen ownersUpdate andThen usersUpdate map { _ => billingProject }
    }

    def updateCreationStatus(projectNames: Seq[RawlsBillingProjectName], newStatus: CreationStatuses.CreationStatus): WriteAction[Int] = {
      filter(_.projectName inSetBind(projectNames.map(_.value))).map(_.creationStatus).update(newStatus.toString)
    }

    def listProjectsWithCreationStatus(status: CreationStatuses.CreationStatus): ReadAction[Seq[RawlsBillingProjectRecord]] = {
      filter(_.creationStatus === status.toString).result
    }

    def load(rawlsProjectName: RawlsBillingProjectName): ReadAction[Option[RawlsBillingProject]] = {
      val name = rawlsProjectName.value
      uniqueResult[RawlsBillingProjectRecord](findBillingProjectByName(name)).flatMap {
        case None => DBIO.successful(None)
        case Some(projectRec) =>
          for {
            ownerGroup <- rawlsGroupQuery.load(RawlsGroupRef(RawlsGroupName(projectRec.ownerGroup)))
            userGroup <- rawlsGroupQuery.load(RawlsGroupRef(RawlsGroupName(projectRec.userGroup)))
          } yield Option(unmarshalBillingProject(projectRec, ownerGroup.get, userGroup.get)) //todo: get rid of these gets
      }
    }

    def loadProjectUsersWithEmail(rawlsProjectName: RawlsBillingProjectName): ReadAction[Seq[RawlsBillingProjectMember]] = {
      val name = rawlsProjectName.value

      val query = for {
        group <- groupUsersQuery if group.userSubjectId === subjectId
        userGroup <- rawlsBillingProjectQuery if user.userGroup === group.groupName
//        userInGroup <- groupUsersQuery if userGroup.
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
          rawlsGroupQuery.delete(RawlsGroupRef(RawlsGroupName(projectRec.ownerGroup))) andThen
            rawlsGroupQuery.delete(RawlsGroupRef(RawlsGroupName(projectRec.userGroup))) andThen
            projectQuery.delete map { count => count > 0 }
      }
    }

    def toBillingProjectGroupName(projectName: RawlsBillingProjectName, projectRole: ProjectRole) = s"PROJECT_${projectName.value}-${projectRole}" //todo: make this live in only one place

    def addUserToProject(userRef: RawlsUserRef, billingProjectName: RawlsBillingProjectName, role: ProjectRole): ReadWriteAction[RawlsGroup] = {
      rawlsGroupQuery.load(RawlsGroupRef(RawlsGroupName(toBillingProjectGroupName(billingProjectName, role)))) flatMap {
        case None => throw new RawlsException(s"Unable to save user ${userRef} as ${role} on project ${billingProjectName}")
        case Some(group) => rawlsGroupQuery.save(group.copy(users = group.users ++ Set(userRef)))
      }
    }

    def removeUserFromProject(thing: Either[RawlsUserRef, RawlsGroupRef], billingProjectName: RawlsBillingProjectName): ReadWriteAction[Boolean] = {
      load(billingProjectName) flatMap {
        case None => throw new RawlsException(s"Unable to remove ${thing} from project ${billingProjectName}")
        case Some(z) => thing match {
          case Left(x) => rawlsGroupQuery.save(z.owners.copy(users = z.owners.users -- Set(x))) andThen rawlsGroupQuery.save(z.users.copy(users = z.users.users -- Set(x)))
          case Right(y) => rawlsGroupQuery.save(z.owners.copy(subGroups = z.owners.subGroups -- Set(y))) andThen rawlsGroupQuery.save(z.users.copy(subGroups = z.users.subGroups -- Set(y)))
        }
      }
    }.map{ _ => true } //todo: this is kinda BS right now. clean up

    def removeUserFromAllProjects(userRef: RawlsUserRef): WriteAction[Boolean] = {
      findProjectsByUserSubjectId(userRef.userSubjectId.value).delete.map { count => count > 0 }
    }

    def listUserProjects(rawlsUser: RawlsUserRef): ReadAction[Iterable[RawlsBillingProjectMembership]] = {
      val query = for {
        user <- projectUsersQuery if user.userSubjectId === rawlsUser.userSubjectId.value
        project <- rawlsBillingProjectQuery if project.projectName === user.projectName
      } yield {
        (user.role, project.projectName, project.creationStatus)
      }
      query.result.map { _.map {
        case (role, projectName, creationStatus) => RawlsBillingProjectMembership(RawlsBillingProjectName(projectName), ProjectRoles.withName(role), CreationStatuses.withName(creationStatus))
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


//      projectUsersQuery.filter(pu => pu.projectName === projectName.value &&
//        pu.userSubjectId === user.userSubjectId.value &&
//        pu.role.inSetBind(roles.map(_.toString)))
    }

    private def marshalBillingProject(billingProject: RawlsBillingProject): RawlsBillingProjectRecord = {
      RawlsBillingProjectRecord(billingProject.projectName.value, billingProject.cromwellAuthBucketUrl, billingProject.status.toString, billingProject.owners.groupName.value, billingProject.users.groupName.value)
    }

    private def unmarshalBillingProject(projectRecord: RawlsBillingProjectRecord, ownerGroup: RawlsGroup, userGroup: RawlsGroup): RawlsBillingProject = {
      RawlsBillingProject(RawlsBillingProjectName(projectRecord.projectName), ownerGroup, userGroup, projectRecord.cromwellAuthBucketUrl, CreationStatuses.withName(projectRecord.creationStatus))
    }

    private def findBillingProjectByName(name: String): RawlsBillingProjectQuery = {
      filter(_.projectName === name)
    }

    private def findUsersByProjectName(name: String): ReadAction[Map[RawlsUserRef, ProjectRoles.ProjectRole]] = {
      uniqueResult(findBillingProjectByName(name).result) flatMap { bp =>
        val billingProject = bp.getOrElse(throw new RawlsException(s"Unable to load billing project [${name}]"))

        rawlsGroupQuery.flattenGroupMembership(RawlsGroupRef(RawlsGroupName(billingProject.ownerGroup))) flatMap { owners =>
          rawlsGroupQuery.flattenGroupMembership(RawlsGroupRef(RawlsGroupName(billingProject.userGroup))) map { users =>
            (owners.map(owner => owner -> ProjectRoles.Owner) ++ users.map(user => user -> ProjectRoles.User)).toMap
          }
        }
      }
    }

    private def findProjectsByUserSubjectId(subjectId: String) = {
      val queryForOwnership = for {
        group <- groupUsersQuery if group.userSubjectId === subjectId
        owner <- rawlsBillingProjectQuery if owner.ownerGroup === group.groupName
      } yield (ProjectRoles.Owner.toString, owner.projectName, owner.creationStatus)

      val queryForMembership = for {
        group <- groupUsersQuery if group.userSubjectId === subjectId
        user <- rawlsBillingProjectQuery if user.userGroup === group.groupName
      } yield (ProjectRoles.User.toString, user.projectName, user.creationStatus)

      val f = (queryForOwnership union queryForMembership).result.map { _.map { //todo: explain plan...
        case (role, projectName, creationStatus) => RawlsBillingProjectMembership(RawlsBillingProjectName(projectName), ProjectRoles.withName(role), CreationStatuses.withName(creationStatus))
      }}
    }
  }
}

