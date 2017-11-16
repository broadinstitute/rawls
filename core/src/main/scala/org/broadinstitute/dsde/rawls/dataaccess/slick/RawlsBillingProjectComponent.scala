package org.broadinstitute.dsde.rawls.dataaccess.slick

import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.dataaccess.jndi.JndiDirectoryDAO
import org.broadinstitute.dsde.rawls.model.ProjectRoles.ProjectRole
import org.broadinstitute.dsde.rawls.model._

case class RawlsBillingProjectRecord(projectName: String, cromwellAuthBucketUrl: String, creationStatus: String, billingAccount: Option[String], message: Option[String])
case class RawlsBillingProjectGroupRecord(projectName: String, groupName: String, role: String)
case class RawlsBillingProjectOperationRecord(projectName: String, operationName: String, operationId: String, done: Boolean, errorMessage: Option[String], api: String)

trait RawlsBillingProjectComponent {
  this: DriverComponent
    with JndiDirectoryDAO =>

  import driver.api._

  class RawlsBillingProjectTable(tag: Tag) extends Table[RawlsBillingProjectRecord](tag, "BILLING_PROJECT") {
    def projectName = column[String]("NAME", O.PrimaryKey, O.Length(254))
    def cromwellAuthBucketUrl = column[String]("CROMWELL_BUCKET_URL", O.Length(128))
    def creationStatus = column[String]("CREATION_STATUS", O.Length(20))
    def billingAccount = column[Option[String]]("BILLING_ACCOUNT", O.Length(100))
    def message = column[Option[String]]("MESSAGE")

    def * = (projectName, cromwellAuthBucketUrl, creationStatus, billingAccount, message) <> (RawlsBillingProjectRecord.tupled, RawlsBillingProjectRecord.unapply)
  }

  class RawlsBillingProjectGroupTable(tag: Tag) extends Table[RawlsBillingProjectGroupRecord](tag, "BILLING_PROJECT_GROUP") {
    def projectName = column[String]("PROJECT_NAME", O.Length(254))
    def groupName = column[String]("GROUP_NAME", O.Length(254))
    def role = column[String]("PROJECT_ROLE", O.Length(254))

    def pk = primaryKey("PK_BILLING_PROJECT_GROUP", (projectName, groupName, role))

    def * = (projectName, groupName, role) <> (RawlsBillingProjectGroupRecord.tupled, RawlsBillingProjectGroupRecord.unapply)
  }

  class RawlsBillingProjectOperationTable(tag: Tag) extends Table[RawlsBillingProjectOperationRecord](tag, "BILLING_PROJECT_OPERATION") {
    def projectName = column[String]("PROJECT_NAME", O.Length(254))
    def operationName = column[String]("OPERATION_NAME", O.Length(254))
    def operationId = column[String]("OPERATION_ID", O.Length(254))
    def done = column[Boolean]("DONE")
    def errorMessage = column[Option[String]]("ERROR_MESSAGE")
    def api = column[String]("API")

    def pk = primaryKey("PK_BILLING_PROJECT_OPERATION", (projectName, operationName))

    def * = (projectName, operationName, operationId, done, errorMessage, api) <> (RawlsBillingProjectOperationRecord.tupled, RawlsBillingProjectOperationRecord.unapply)
  }

  protected val rawlsBillingProjectGroupQuery = TableQuery[RawlsBillingProjectGroupTable]
  protected val rawlsBillingProjectOperationQuery = TableQuery[RawlsBillingProjectOperationTable]

  private type RawlsBillingProjectQuery = Query[RawlsBillingProjectTable, RawlsBillingProjectRecord, Seq]
  private type RawlsBillingProjectGroupQuery = Query[RawlsBillingProjectGroupTable, RawlsBillingProjectGroupRecord, Seq]

  object rawlsBillingProjectQuery extends TableQuery(new RawlsBillingProjectTable(_)) {

    //already updated
    def create(billingProject: RawlsBillingProject): ReadWriteAction[RawlsBillingProject] = {
      validateUserDefinedString(billingProject.projectName.value)
      uniqueResult(findBillingProjectByName(billingProject.projectName).result) flatMap {
        case Some(_) => throw new RawlsException(s"Cannot create billing project [${billingProject.projectName.value}] in database because it already exists")
        case None => (rawlsBillingProjectQuery += marshalBillingProject(billingProject)).map { _ => billingProject }
      }
    }

    //no changes necessary
    def updateBillingProjects(projects: Traversable[RawlsBillingProject]): WriteAction[Seq[Int]] = {
      DBIO.sequence(projects.map(project => rawlsBillingProjectQuery.filter(_.projectName === project.projectName.value).update(marshalBillingProject(project))).toSeq)
    }

    //no changes necessary
    def listProjectsWithCreationStatus(status: CreationStatuses.CreationStatus): ReadWriteAction[Seq[RawlsBillingProject]] = {
      filter(_.creationStatus === status.toString).result.flatMap { projectRecords =>
        DBIO.sequence(projectRecords.map { projectRec => load(projectRec) })
      }
    }

    def load(projectName: RawlsBillingProjectName): ReadWriteAction[Option[RawlsBillingProject]] = {
      uniqueResult[RawlsBillingProjectRecord](findBillingProjectByName(projectName)).flatMap {
        case None => DBIO.successful(None)
        case Some(projectRec) => load(projectRec).map(Option(_))
      }
    }

    //requires changes
    def load(projectRec: RawlsBillingProjectRecord): ReadWriteAction[RawlsBillingProject] = {
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

    //requires changes
    def delete(billingProjectName: RawlsBillingProjectName): ReadWriteAction[Boolean] = {
      findBillingGroups(billingProjectName).result.flatMap { groupNames =>
        rawlsBillingProjectGroupQuery.filter(_.projectName === billingProjectName.value).delete andThen
          DBIO.sequence(groupNames.map { groupName => rawlsGroupQuery.delete(RawlsGroupRef(RawlsGroupName(groupName)))}) andThen
            rawlsBillingProjectQuery.filter(_.projectName === billingProjectName.value).delete map { count => count > 0 }
      }
    }

    //requires changes
    private def findBillingGroups(billingProjectName: RawlsBillingProjectName) = {
      rawlsBillingProjectGroupQuery.filter(_.projectName === billingProjectName.value).map(_.groupName)
    }

    def getBillingProjectDetails(projectNames: Set[RawlsBillingProjectName]): ReadAction[Map[String, (CreationStatuses.CreationStatus, Option[String])]] = {
      val query = for {
        project <- rawlsBillingProjectQuery if (project.projectName.inSetBind(projectNames.map(_.value)))
      } yield project

      query.result.map(_.map { project =>
        project.projectName -> (CreationStatuses.withName(project.creationStatus), project.message)
      }.toMap)
    }

    /**
     * Checks that user has at least one of roles for the project specified by projectName. Will recurse through sub groups.
     * @param projectName
     * @param user
     * @param roles
     * @return
     */
    def hasOneOfProjectRole(projectName: RawlsBillingProjectName, user: RawlsUserRef, roles: Set[ProjectRole]): ReadWriteAction[Boolean] = {
      val projectUsersAction = findBillingProjectGroupsForRoles(projectName, roles).result.flatMap { groups =>
        DBIO.sequence(groups.map { group =>
          rawlsGroupQuery.isGroupMember(RawlsGroupRef(RawlsGroupName(group.groupName)), user)
        })
      }

      projectUsersAction.map { isMembers => isMembers.contains(true) }
    }

    def insertOperations(operations: Seq[RawlsBillingProjectOperationRecord]): WriteAction[Unit] = {
      (rawlsBillingProjectOperationQuery ++= operations).map(_ => ())
    }

    def updateOperations(operations: Seq[RawlsBillingProjectOperationRecord]): WriteAction[Seq[Int]] = {
      DBIO.sequence(operations.map(rec => rawlsBillingProjectOperationQuery.filter(x => x.projectName === rec.projectName && x.operationName === rec.operationName).update(rec)))
    }

    def loadOperationsForProjects(projectNames: Seq[RawlsBillingProjectName]): ReadAction[Seq[RawlsBillingProjectOperationRecord]] = {
      rawlsBillingProjectOperationQuery.filter(_.projectName.inSetBind(projectNames.map(_.value))).result
    }

    private def marshalBillingProject(billingProject: RawlsBillingProject): RawlsBillingProjectRecord = {
      RawlsBillingProjectRecord(billingProject.projectName.value, billingProject.cromwellAuthBucketUrl, billingProject.status.toString, billingProject.billingAccount.map(_.value), billingProject.message)
    }

    private def unmarshalBillingProject(projectRecord: RawlsBillingProjectRecord, groups: Map[ProjectRoles.ProjectRole, RawlsGroup]): RawlsBillingProject = {
      RawlsBillingProject(RawlsBillingProjectName(projectRecord.projectName), groups, projectRecord.cromwellAuthBucketUrl, CreationStatuses.withName(projectRecord.creationStatus), projectRecord.billingAccount.map(RawlsBillingAccountName), projectRecord.message)
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

