package org.broadinstitute.dsde.rawls.dataaccess.slick

import java.sql.Timestamp
import java.util.{Date, UUID}

import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.model.WorkspaceAccessLevels.WorkspaceAccessLevel
import org.broadinstitute.dsde.rawls.model._
import org.joda.time.DateTime
import slick.dbio.Effect.Read
import org.broadinstitute.dsde.rawls.dataaccess.SlickWorkspaceContext
import org.broadinstitute.dsde.rawls.model.Attributable.AttributeMap

/**
 * Created by dvoet on 2/4/16.
 */
case class WorkspaceRecord(
  namespace: String,
  name: String,
  id: UUID,
  bucketName: String,
  createdDate: Timestamp,
  lastModified: Timestamp,
  createdBy: String,
  isLocked: Boolean,
  realmGroupName: Option[String],
  recordVersion: Long
)

case class WorkspaceAccessRecord(workspaceId: UUID, groupName: String, accessLevel: String, isRealmAcl: Boolean)

/** result class for workspaceQuery.findAssociatedGroupsToIntersect, target = group1 intersect group2 */
case class GroupsToIntersect(target: RawlsGroupRef, group1: RawlsGroupRef, group2: RawlsGroupRef)

case class WorkspaceUserShareRecord(workspaceId: UUID, subjectId: String)
case class WorkspaceGroupShareRecord(workspaceId: UUID, groupName: String)

case class PendingWorkspaceAccessRecord(
  workspaceId: UUID,
  userEmail: String,
  originSubjectId: String,
  inviteDate: Timestamp,
  accessLevel: String
)

trait WorkspaceComponent {
  this: DriverComponent
    with AttributeComponent
    with RawlsGroupComponent
    with RawlsUserComponent
    with EntityComponent
    with SubmissionComponent
    with WorkflowComponent
    with MethodConfigurationComponent =>

  import driver.api._

  class WorkspaceTable(tag: Tag) extends Table[WorkspaceRecord](tag, "WORKSPACE") {
    def id = column[UUID]("id", O.PrimaryKey)
    def namespace = column[String]("namespace", O.Length(254))
    def name = column[String]("name", O.Length(254))
    def bucketName = column[String]("bucket_name", O.Length(128))
    def createdDate = column[Timestamp]("created_date", O.SqlType("TIMESTAMP(6)"), O.Default(defaultTimeStamp))
    def lastModified = column[Timestamp]("last_modified", O.SqlType("TIMESTAMP(6)"), O.Default(defaultTimeStamp))
    def createdBy = column[String]("created_by", O.Length(254))
    def isLocked = column[Boolean]("is_locked")
    def realmGroupName = column[Option[String]]("realm_group_name", O.Length(254))
    def recordVersion = column[Long]("record_version")

    def uniqueNamespaceName = index("IDX_WS_UNIQUE_NAMESPACE_NAME", (namespace, name), unique = true)
    def realm = foreignKey("FK_WS_REALM_GROUP", realmGroupName, rawlsGroupQuery)(_.groupName.?)

    def * = (namespace, name, id, bucketName, createdDate, lastModified, createdBy, isLocked, realmGroupName, recordVersion) <> (WorkspaceRecord.tupled, WorkspaceRecord.unapply)
  }

  class WorkspaceAccessTable(tag: Tag) extends Table[WorkspaceAccessRecord](tag, "WORKSPACE_ACCESS") {
    def groupName = column[String]("group_name", O.Length(254))
    def workspaceId = column[UUID]("workspace_id")
    def accessLevel = column[String]("access_level", O.Length(254))
    def isRealmAcl = column[Boolean]("is_realm_acl")

    def workspace = foreignKey("FK_WS_ACCESS_WORKSPACE", workspaceId, workspaceQuery)(_.id)
    def group = foreignKey("FK_WS_ACCESS_GROUP", groupName, rawlsGroupQuery)(_.groupName)

    def accessPrimaryKey = primaryKey("PK_WORKSPACE_ACCESS", (workspaceId, accessLevel, isRealmAcl))

    def * = (workspaceId, groupName, accessLevel, isRealmAcl) <> (WorkspaceAccessRecord.tupled, WorkspaceAccessRecord.unapply)
  }

  class PendingWorkspaceAccessTable(tag: Tag) extends Table[PendingWorkspaceAccessRecord](tag, "PENDING_WORKSPACE_ACCESS") {
    def workspaceId = column[UUID]("workspace_id")
    def userEmail = column[String]("user_email", O.Length(254))
    def originSubjectId = column[String]("origin_subject_id", O.Length(254))
    def inviteDate = column[Timestamp]("invite_date", O.SqlType("TIMESTAMP(6)"), O.Default(defaultTimeStamp))
    def accessLevel = column[String]("access_level", O.Length(254))

    def workspace = foreignKey("FK_PENDING_WS_ACCESS_WORKSPACE", workspaceId, workspaceQuery)(_.id)
    def originUser = foreignKey("FK_PENDING_WS_ACCESS_ORIGIN_USER", originSubjectId, rawlsUserQuery)(_.userSubjectId)

    def pendingAccessPrimaryKey = primaryKey("PK_PENDING_WORKSPACE_ACCESS", (workspaceId, userEmail)) //only allow one invite per user per workspace

    def * = (workspaceId, userEmail, originSubjectId, inviteDate, accessLevel) <> (PendingWorkspaceAccessRecord.tupled, PendingWorkspaceAccessRecord.unapply)
  }

  class WorkspaceUserShareTable(tag: Tag) extends Table[WorkspaceUserShareRecord](tag, "WORKSPACE_USER_SHARE") {
    def workspaceId = column[UUID]("workspace_id")
    def userSubjectId = column[String]("user_subject_id")

    def workspace = foreignKey("FK_USER_SHARE_PERMS_WS", workspaceId, workspaceQuery)(_.id)
    def user = foreignKey("FK_USER_SHARE_PERMS_USER", userSubjectId, rawlsUserQuery)(_.userSubjectId)

    def * = (workspaceId, userSubjectId) <> (WorkspaceUserShareRecord.tupled, WorkspaceUserShareRecord.unapply)
  }

  class WorkspaceGroupShareTable(tag: Tag) extends Table[WorkspaceGroupShareRecord](tag, "WORKSPACE_GROUP_SHARE") {
    def workspaceId = column[UUID]("workspace_id")
    def groupName = column[String]("group_name")

    def workspace = foreignKey("FK_GROUP_SHARE_PERMS_WS", workspaceId, workspaceQuery)(_.id)
    def group = foreignKey("FK_GROUP_SHARE_PERMS_GROUP", groupName, rawlsGroupQuery)(_.groupName)

    def * = (workspaceId, groupName) <> (WorkspaceGroupShareRecord.tupled, WorkspaceGroupShareRecord.unapply)
  }

  protected val workspaceAccessQuery = TableQuery[WorkspaceAccessTable]
  protected val pendingWorkspaceAccessQuery = TableQuery[PendingWorkspaceAccessTable]
  protected val workspaceUserShareQuery = TableQuery[WorkspaceUserShareTable]
  protected val workspaceGroupShareQuery = TableQuery[WorkspaceGroupShareTable]

  object workspaceQuery extends TableQuery(new WorkspaceTable(_)) {
    private type WorkspaceQueryType = driver.api.Query[WorkspaceTable, WorkspaceRecord, Seq]

    def listAll(): ReadAction[Seq[Workspace]] = {
      loadWorkspaces(workspaceQuery)
    }

    def listWithAttribute(attrName: AttributeName, attrValue: AttributeValue): ReadAction[Seq[Workspace]] = {
      loadWorkspaces(getWorkspacesWithAttribute(attrName, attrValue))
    }

    def save(workspace: Workspace): ReadWriteAction[Workspace] = {
      validateUserDefinedString(workspace.namespace)
      validateUserDefinedString(workspace.name)
      workspace.attributes.keys.foreach { attrName =>
        validateUserDefinedString(attrName.name)
        validateAttributeName(attrName, Attributable.workspaceEntityType)
      }

      uniqueResult[WorkspaceRecord](findByIdQuery(UUID.fromString(workspace.workspaceId))) flatMap {
        case None =>
          (workspaceQuery += marshalNewWorkspace(workspace)) andThen
            insertOrUpdateAccessRecords(workspace) andThen
            upsertAttributes(workspace) andThen
            updateLastModified(UUID.fromString(workspace.workspaceId))
        case Some(workspaceRecord) =>
          insertOrUpdateAccessRecords(workspace) andThen
            upsertAttributes(workspace) andThen
            optimisticLockUpdate(workspaceRecord) andThen
            updateLastModified(UUID.fromString(workspace.workspaceId))
      } map { _ => workspace }
    }

    private def insertOrUpdateAccessRecords(workspace: Workspace): WriteAction[Unit] = {
      val id = UUID.fromString(workspace.workspaceId)
      val accessRecords = workspace.accessLevels.map { case (accessLevel, group) => WorkspaceAccessRecord(id, group.groupName.value, accessLevel.toString, false) }
      val realmAclRecords = workspace.realmACLs.map { case (accessLevel, group) => WorkspaceAccessRecord(id, group.groupName.value, accessLevel.toString, true) }
      DBIO.seq((accessRecords ++ realmAclRecords).map { workspaceAccessQuery insertOrUpdate }.toSeq: _*)
    }

    private def upsertAttributes(workspace: Workspace) = {
      val workspaceId = UUID.fromString(workspace.workspaceId)

      val entityRefs = workspace.attributes.collect { case (_, ref: AttributeEntityReference) => ref }
      val entityRefListMembers = workspace.attributes.collect { case (_, refList: AttributeEntityReferenceList) => refList.list}.flatten
      val entitiesToLookup = (entityRefs ++ entityRefListMembers)

      def insertScratchAttributes(attributeRecs: Seq[WorkspaceAttributeRecord])(transactionId: String): ReadWriteAction[Unit] = {
        workspaceAttributeScratchQuery.batchInsertAttributes(attributeRecs, transactionId)
      }

      entityQuery.lookupEntitiesByNames(workspaceId, entitiesToLookup) flatMap { entityRecords =>
        val entityIdsByRef = entityRecords.map(rec => AttributeEntityReference(rec.entityType, rec.name) -> rec.id).toMap
        val attributesToSave = workspace.attributes flatMap { attr => workspaceAttributeQuery.marshalAttribute(workspaceId, attr._1, attr._2, entityIdsByRef) }

        workspaceAttributeQuery.findByOwnerQuery(Seq(workspaceId)).result flatMap { existingAttributes =>
          workspaceAttributeQuery.upsertAction(attributesToSave, existingAttributes, insertScratchAttributes)
        }
      }
    }

    private def optimisticLockUpdate(originalRec: WorkspaceRecord): ReadWriteAction[Int] = {
      findByIdAndRecordVersionQuery(originalRec.id, originalRec.recordVersion) update originalRec.copy(recordVersion = originalRec.recordVersion + 1) map {
        case 0 => throw new RawlsConcurrentModificationException(s"could not update $originalRec because its record version has changed")
        case success => success
      }
    }

    def findByName(workspaceName: WorkspaceName): ReadAction[Option[Workspace]] = {
      loadWorkspace(findByNameQuery(workspaceName))
    }

    def findById(workspaceId: String): ReadAction[Option[Workspace]] = {
      loadWorkspace(findByIdQuery(UUID.fromString(workspaceId)))
    }

    def listByIds(workspaceIds: Seq[UUID]): ReadAction[Seq[Workspace]] = {
      loadWorkspaces(findByIdsQuery(workspaceIds))
    }

    def delete(workspaceName: WorkspaceName): ReadWriteAction[Boolean] = {
      uniqueResult[WorkspaceRecord](findByNameQuery(workspaceName)).flatMap {
        case None => DBIO.successful(false)
        case Some(workspaceRecord) =>
          workspaceAttributes(findByIdQuery(workspaceRecord.id)).result.flatMap(recs => DBIO.seq(deleteWorkspaceAttributes(recs.map(_._2)))) flatMap { _ =>
            //should we be deleting ALL workspace-related things inside of this method?
            workspaceAccessQuery.filter(_.workspaceId === workspaceRecord.id).delete
          } flatMap { _ =>
            findByIdQuery(workspaceRecord.id).delete
          } map { count =>
            count > 0
          }
      }
    }

    def updateLastModified(workspaceId: UUID) = {
      val currentTime = new Timestamp(new Date().getTime)
      findByIdQuery(workspaceId).map(_.lastModified).update(currentTime)
    }

    def updateLastModified(workspaceName: WorkspaceName) = {
      val currentTime = new Timestamp(new Date().getTime)
      findByNameQuery(workspaceName).map(_.lastModified).update(currentTime)
    }

    def lock(workspaceName: WorkspaceName): ReadWriteAction[Int] = {
      findByNameQuery(workspaceName).map(_.isLocked).update(true)
    }

    def unlock(workspaceName: WorkspaceName): ReadWriteAction[Int] = {
      findByNameQuery(workspaceName).map(_.isLocked).update(false)
    }

    def saveInvite(workspaceId: UUID, originUser: String, invite: WorkspaceACLUpdate): ReadWriteAction[WorkspaceACLUpdate] = {
      pendingWorkspaceAccessQuery insertOrUpdate(marshalWorkspaceInvite(workspaceId, originUser, invite)) map { _ => invite }
    }

    def removeInvite(workspaceId: UUID, userEmail: String): ReadWriteAction[Boolean] = {
      pendingWorkspaceAccessQuery.filter(rec => rec.workspaceId === workspaceId && rec.userEmail === userEmail).delete.map { count => count == 1 }
    }

    def getInvites(workspaceId: UUID): ReadAction[Seq[(String, WorkspaceAccessLevel)]] = {
      (pendingWorkspaceAccessQuery.filter(_.workspaceId === workspaceId).map(rec => (rec.userEmail, rec.accessLevel))).result.map(_.map {case (email, access) =>
        (email, WorkspaceAccessLevels.withName(access))
      })
    }

    def findWorkspaceInvitesForUser(userEmail: RawlsUserEmail): ReadAction[Seq[(WorkspaceName, WorkspaceAccessLevel)]] = {
      (pendingWorkspaceAccessQuery.filter(_.userEmail === userEmail.value) join workspaceQuery on (_.workspaceId === _.id)).map(rec => (rec._2.namespace, rec._2.name, rec._1.accessLevel)).result.map { namesWithAccessLevel =>
        namesWithAccessLevel.map{ case (namespace, name, accessLevel) => (WorkspaceName(namespace, name), WorkspaceAccessLevels.withName(accessLevel))}
      }
    }

    def deleteWorkspaceInvitesForUser(userEmail: RawlsUserEmail) = {
      (pendingWorkspaceAccessQuery.filter(_.userEmail === userEmail.value)).delete
    }

    def deleteWorkspaceInvites(workspaceId: UUID) = {
      findWorkspaceInvitesQuery(workspaceId).delete
    }

    def insertUserSharePermissions(workspaceId: UUID, userRefs: Seq[RawlsUserRef]) = {
      val recordsToInsert = userRefs.map(userRef => WorkspaceUserShareRecord(workspaceId, userRef.userSubjectId.value))
      (workspaceUserShareQuery ++= recordsToInsert)
    }

    def insertGroupSharePermissions(workspaceId: UUID, groupRefs: Seq[RawlsGroupRef]) = {
      val recordsToInsert = groupRefs.map(groupRef => WorkspaceGroupShareRecord(workspaceId, groupRef.groupName.value))
      (workspaceGroupShareQuery ++= recordsToInsert)
    }

    def deleteUserSharePermissions(workspaceId: UUID, userRefs: Seq[RawlsUserRef]) = {
      val usersToRemove = userRefs.map(_.userSubjectId.value)
      (workspaceUserShareQuery.filter(rec => rec.workspaceId === workspaceId && rec.userSubjectId.inSet(usersToRemove))).delete
    }

    def deleteGroupSharePermissions(workspaceId: UUID, groupRefs: Seq[RawlsGroupRef]) = {
      val groupsToRemove = groupRefs.map(_.groupName.value)
      (workspaceGroupShareQuery.filter(rec => rec.workspaceId === workspaceId && rec.groupName.inSet(groupsToRemove))).delete
    }

    def deleteWorkspaceSharePermissions(workspaceId: UUID) = {
      workspaceUserShareQuery.filter(_.workspaceId === workspaceId).delete andThen
        workspaceGroupShareQuery.filter(_.workspaceId === workspaceId).delete
    }

    def getUserSharePermissions(subjectId: RawlsUserSubjectId, workspaceContext: SlickWorkspaceContext): ReadAction[Boolean] = {
      workspaceUserShareQuery.filter(rec => (rec.userSubjectId === subjectId.value && rec.workspaceId === workspaceContext.workspaceId)).countDistinct.result.map(rows => rows > 0) flatMap { hasSharePermission =>
        if(hasSharePermission) DBIO.successful(hasSharePermission)
        else rawlsGroupQuery.listGroupsForUser(RawlsUserRef(subjectId)).flatMap { userGroups =>
          val groupNames = userGroups.map(_.groupName.value)
          workspaceGroupShareQuery.filter(rec => ((rec.workspaceId === workspaceContext.workspaceId) && (rec.groupName).inSet(groupNames))).countDistinct.result.map(rows => rows > 0)
        }
      }
    }

    def listEmailsAndAccessLevel(workspaceContext: SlickWorkspaceContext): ReadAction[Seq[(String, WorkspaceAccessLevel, Boolean)]] = {
      val accessAndUserEmail = (for {
        access <- workspaceAccessQuery if (access.workspaceId === workspaceContext.workspaceId && access.isRealmAcl === false)
        group <- rawlsGroupQuery if (access.groupName === group.groupName)
        userGroup <- groupUsersQuery if (group.groupName === userGroup.groupName)
        user <- rawlsUserQuery if (user.userSubjectId === userGroup.userSubjectId)
      } yield (access, user)).map { case (access, user) => (access.accessLevel, user.userEmail, user.userSubjectId) }

      val accessAndSubGroupEmail = (for {
        access <- workspaceAccessQuery if (access.workspaceId === workspaceContext.workspaceId && access.isRealmAcl === false)
        group <- rawlsGroupQuery if (access.groupName === group.groupName)
        subGroupGroup <- groupSubgroupsQuery if (group.groupName === subGroupGroup.parentGroupName)
        subGroup <- rawlsGroupQuery if (subGroup.groupName === subGroupGroup.childGroupName)
      } yield (access, subGroup)).map { case (access, subGroup) => (access.accessLevel, subGroup.groupEmail, subGroup.groupName) }

      /*  The left join here is important. Since we are only going to store share-permission records for users and groups that have been
          explicitly granted that ability, we want to be able to return all users and groups even if they don't have share permissions.
          After the join, a null column equates to false, and a non-null column equates to true. This conversion is done with permission.isDefined
       */
      val userShareQuery = accessAndUserEmail.joinLeft(workspaceUserShareQuery.filter(_.workspaceId === workspaceContext.workspaceId)).on(_._3 === _.userSubjectId).map { case ((accessLevel, userEmail, _), shareRecord) => (accessLevel, userEmail, shareRecord.isDefined) }
      val groupShareQuery = accessAndSubGroupEmail.joinLeft(workspaceGroupShareQuery.filter(_.workspaceId === workspaceContext.workspaceId)).on(_._3 === _.groupName).map { case ((accessLevel, groupEmail, _), shareRecord) => (accessLevel, groupEmail, shareRecord.isDefined) }

      for {
        userResults <- userShareQuery.result
        groupResults <- groupShareQuery.result
      } yield {
        (userResults ++ groupResults).map { case (access, email, hasSharePermission) =>
          val accessLevel = WorkspaceAccessLevels.withName(access)
          (email, accessLevel, hasSharePermission)
        }
      }
    }

    def deleteWorkspaceAccessReferences(workspaceId: UUID) = {
      workspaceAccessQuery.filter(_.workspaceId === workspaceId).delete
    }

    def deleteWorkspaceEntitiesAndAttributes(workspaceId: UUID) = {
      entityQuery.DeleteEntityAttributesQuery.deleteAction(workspaceId) andThen {
        entityQuery.filter(_.workspaceId === workspaceId).delete
      } andThen
        updateLastModified(workspaceId)
    }

    def deleteWorkspaceSubmissions(workspaceId: UUID) = {
      submissionQuery.DeleteSubmissionQuery.deleteAction(workspaceId) andThen {
        submissionQuery.filter(_.workspaceId === workspaceId).delete
      } andThen
        updateLastModified(workspaceId)
    }

    def deleteWorkspaceMethodConfigs(workspaceId: UUID) = {
      methodConfigurationQuery.DeleteMethodConfigurationQuery.deleteAction(workspaceId) andThen {
        methodConfigurationQuery.filter(_.workspaceId === workspaceId).delete
      } andThen
        updateLastModified(workspaceId)
    }

    def getAuthorizedRealms(workspaceIds: Seq[String], user: RawlsUserRef): ReadAction[Seq[Option[RawlsGroupRef]]] = {
      val realmQuery = for {
        workspace <- workspaceQuery if workspace.id.inSetBind(workspaceIds.map(UUID.fromString))
      } yield workspace.realmGroupName

      realmQuery.result flatMap { allRealms =>
        val flatRealms = allRealms.flatten.toSet
        DBIO.sequence(flatRealms.toSeq.map { realm =>
          val groupRef = RawlsGroupRef(RawlsGroupName(realm))
          rawlsGroupQuery.loadGroupIfMember(groupRef, user) flatMap {
            case None => DBIO.successful(None)
            case Some(_) => DBIO.successful(Some(groupRef))
          }
        })
      }
    }

    /**
      * Lists all workspaces with a particular attribute name/value pair.
      *
      * ** Note: This is an inefficient query.  It performs a full scan of the attribute table since
      *    there is no index on attribute name and/or value.  This method is only being used in one
      *    place; if you find yourself needing this for other things, consider adding such an index.
      * @param attrName
      * @param attrValue
      * @return
      */
    def getWorkspacesWithAttribute(attrName: AttributeName, attrValue: AttributeValue) = {
      for {
        attribute <- workspaceAttributeQuery.queryByAttribute(attrName, attrValue)
        workspace <- workspaceQuery if workspace.id === attribute.ownerId
      } yield workspace
    }

    def listAccessGroupMemberEmails(workspaceIds: Seq[UUID], accessLevel: WorkspaceAccessLevel): ReadAction[Map[UUID, Seq[String]]] = {
      val accessQuery = workspaceAccessQuery.filter(access => access.workspaceId.inSetBind(workspaceIds) && access.isRealmAcl === false && access.accessLevel === accessLevel.toString)

      val subGroupEmailQuery = accessQuery join rawlsGroupQuery.subGroupEmailsQuery on {
        case (wsAccess, (parentGroupName, subGroupEmail)) => wsAccess.groupName === parentGroupName } map {
        case (wsAccess, (parentGroupName, subGroupEmail)) => (wsAccess.workspaceId, subGroupEmail) }

      val userEmailQuery = accessQuery join rawlsGroupQuery.userEmailsQuery on {
        case (wsAccess, (parentGroupName, userEmail)) => wsAccess.groupName === parentGroupName } map {
        case (wsAccess, (parentGroupName, userEmail)) => (wsAccess.workspaceId, userEmail) }

      (subGroupEmailQuery union userEmailQuery).result.map { wsIdAndEmails =>
        wsIdAndEmails.groupBy { case (wsId, email) => wsId }.
          mapValues(_.map { case (wsId, email) => email }) }
    }

    def loadAccessGroup(workspaceName: WorkspaceName, accessLevel: WorkspaceAccessLevel) = {
      val query = for {
        workspace <- workspaceQuery if (workspace.namespace === workspaceName.namespace && workspace.name === workspaceName.name)
        accessGroup <- workspaceAccessQuery if (accessGroup.workspaceId === workspace.id && accessGroup.accessLevel === accessLevel.toString && accessGroup.isRealmAcl === false)
      } yield accessGroup.groupName

      uniqueResult(query.result).map(name => RawlsGroupRef(RawlsGroupName(name.getOrElse(throw new RawlsException(s"Unable to load ${accessLevel} access group for workspace ${workspaceName}")))))
    }

    /**
     * gets the submission stats (last workflow failed date, last workflow success date, running submission count)
     * for each workspace
     *
     * @param workspaceIds the workspace ids to query for
     * @return WorkspaceSubmissionStats keyed by workspace id
     */
    def listSubmissionSummaryStats(workspaceIds: Seq[UUID]): ReadAction[Map[UUID, WorkspaceSubmissionStats]] = {
      // workflow date query: select workspaceId, workflow.status, max(workflow.statusLastChangedDate) ... group by workspaceId, workflow.status
      val workflowDatesQuery = for {
        submissions <- submissionQuery if submissions.workspaceId.inSetBind(workspaceIds)
        workflows <- workflowQuery if submissions.id === workflows.submissionId
      } yield (submissions.workspaceId, workflows.status, workflows.statusLastChangedDate)

      val workflowDatesGroupedQuery = workflowDatesQuery.groupBy { case (wsId, status, _) => (wsId, status) }.
        map { case ((wsId, wfStatus), records) => (wsId, wfStatus, records.map { case (_, _, lastChanged) => lastChanged }.max) }

      // running submission query: select workspaceId, count(1) ... where submissions.status === Submitted group by workspaceId
      val runningSubmissionsQuery = (for {
        submissions <- submissionQuery if submissions.workspaceId.inSetBind(workspaceIds) && submissions.status.inSetBind(SubmissionStatuses.activeStatuses.map(_.toString))
      } yield submissions).groupBy(_.workspaceId).map { case (wfId, submissions) => (wfId, submissions.length)}

      for {
        workflowDates <- workflowDatesGroupedQuery.result
        runningSubmissions <- runningSubmissionsQuery.result
      } yield {
        val workflowDatesByWorkspaceByStatus: Map[UUID, Map[String, Option[Timestamp]]] = groupByWorkspaceIdThenStatus(workflowDates)
        val runningSubmissionCountByWorkspace: Map[UUID, Int] = groupByWorkspaceId(runningSubmissions)

        workspaceIds.map { wsId =>
          val (lastFailedDate, lastSuccessDate) = workflowDatesByWorkspaceByStatus.get(wsId) match {
            case None => (None, None)
            case Some(datesByStatus) =>
              (datesByStatus.getOrElse(WorkflowStatuses.Failed.toString, None), datesByStatus.getOrElse(WorkflowStatuses.Succeeded.toString, None))
          }
          wsId -> WorkspaceSubmissionStats(
            lastSuccessDate.map(t => new DateTime(t.getTime)),
            lastFailedDate.map(t => new DateTime(t.getTime)),
            runningSubmissionCountByWorkspace.getOrElse(wsId, 0)
          )
        }
      } toMap
    }

    private def workspaceAttributes(lookup: WorkspaceQueryType) = for {
      workspaceAttrRec <- workspaceAttributeQuery if workspaceAttrRec.ownerId.in(lookup.map(_.id))
    } yield (workspaceAttrRec.ownerId, workspaceAttrRec)

    private def workspaceAttributesWithReferences(lookup: WorkspaceQueryType) = {
      workspaceAttributes(lookup) joinLeft entityQuery on (_._2.valueEntityRef === _.id)
    }

    private def deleteWorkspaceAttributes(attributeRecords: Seq[WorkspaceAttributeRecord]) = {
      workspaceAttributeQuery.deleteAttributeRecords(attributeRecords)
    }

    private def findByNameQuery(workspaceName: WorkspaceName): WorkspaceQueryType = {
      filter(rec => rec.namespace === workspaceName.namespace && rec.name === workspaceName.name)
    }

    private def findWorkspaceInvitesQuery(workspaceId: UUID) = {
      pendingWorkspaceAccessQuery.filter(_.workspaceId === workspaceId)
    }

    def findByIdQuery(workspaceId: UUID): WorkspaceQueryType = {
      filter(_.id === workspaceId)
    }

    def findByIdAndRecordVersionQuery(workspaceId: UUID, recordVersion: Long): WorkspaceQueryType = {
      filter(w => w.id === workspaceId && w.recordVersion === recordVersion)
    }

    def findByIdsQuery(workspaceIds: Seq[UUID]): WorkspaceQueryType = {
      filter(_.id.inSetBind(workspaceIds))
    }

    def listPermissionPairsForGroups(groups: Set[RawlsGroupRef]): ReadAction[Seq[WorkspacePermissionsPair]] = {
      val query = for {
        accessLevel <- workspaceAccessQuery if (accessLevel.groupName.inSetBind(groups.map(_.groupName.value)) && accessLevel.isRealmAcl === false)
        workspace <- workspaceQuery if (workspace.id === accessLevel.workspaceId)
      } yield (workspace, accessLevel)
      query.result.map(_.map { case (workspace, accessLevel) => WorkspacePermissionsPair(workspace.id.toString(), WorkspaceAccessLevels.withName(accessLevel.accessLevel)) })
    }

    /**
     * Find the intersection groups that need to be recomputed given the change in group and the groups that
     * should be used in the intersection
     * @param group group that has changed to trigger the recompute
     * @return
     */
    def findAssociatedGroupsToIntersect(group: RawlsGroupRef): ReadAction[Set[(GroupsToIntersect)]] = {
      def findWorkspacesForGroups(groups: Set[RawlsGroupRecord]) = {
        for {
          workspaceAccess <- workspaceAccessQuery if workspaceAccess.groupName.inSetBind(groups.map(_.groupName)) && workspaceAccess.isRealmAcl === false
          workspace <- workspaceQuery if workspaceAccess.workspaceId === workspace.id && workspace.realmGroupName.isDefined
        } yield workspace
      }

      def findWorkspacesForRealms(groups: Set[RawlsGroupRecord]) = {
        for {
          workspace <- workspaceQuery if workspace.realmGroupName.inSetBind(groups.map(_.groupName)) && workspace.realmGroupName.isDefined
        } yield workspace
      }

      for {
        groupRecs <- rawlsGroupQuery.findGroupByName(group.groupName.value).result
        // the group in question may be an access group for a workspace, get the workspace access rec if it is
        workspaceAccessRec <- workspaceAccessQuery.filter(workspaceAccess => workspaceAccess.groupName === group.groupName.value && workspaceAccess.isRealmAcl === false).result.headOption
        allGroups <- rawlsGroupQuery.listParentGroupsRecursive(groupRecs.toSet, groupRecs.toSet)
        workspaceRecsForGroups <- findWorkspacesForGroups(allGroups).result
        workspaceRecsForRealms <- findWorkspacesForRealms(allGroups).result
        realmedWorkspaceRecs = (workspaceRecsForGroups ++ workspaceRecsForRealms)
        accessGroupRecs <- workspaceAccessQuery.filter(rec => rec.workspaceId.inSetBind(realmedWorkspaceRecs.map(_.id))).result
      } yield {
        val indexedAccessGroups = accessGroupRecs.map(rec => (rec.workspaceId, rec.accessLevel, rec.isRealmAcl) -> RawlsGroupRef(RawlsGroupName(rec.groupName))).toMap

        val groupsToIntersect = for {
          workspaceRec <- realmedWorkspaceRecs
          // the if clause below makes sure if the group in question is an access group we don't do the intersection
          // for other access groups of the workspace. The intersections are not required because an access group should
          // not be a sub group of another access group for the same workspace. But more importantly this prevents a
          // concurrency issue. Without this guard changing workspace acls can cause multiple concurrent intersections
          // for all the access groups of a workspace.
          accessLevel <- WorkspaceAccessLevels.groupAccessLevelsAscending if workspaceAccessRec.isEmpty || workspaceAccessRec.get.workspaceId != workspaceRec.id || (workspaceRec.id == workspaceAccessRec.get.workspaceId && accessLevel == WorkspaceAccessLevels.withName(workspaceAccessRec.get.accessLevel))
        } yield {
          GroupsToIntersect(
            indexedAccessGroups((workspaceRec.id, accessLevel.toString, true)),
            indexedAccessGroups((workspaceRec.id, accessLevel.toString, false)),
            RawlsGroupRef(RawlsGroupName(workspaceRec.realmGroupName.get)))
        }
        groupsToIntersect.toSet
      }
    }

    def findWorkspaceUsersAndAccessLevel(workspaceId: UUID): ReadAction[Set[(Either[RawlsUserRef, RawlsGroupRef], (WorkspaceAccessLevel, Boolean))]] = {
      val userQuery = for {
        access <- workspaceAccessQuery if access.workspaceId === workspaceId && access.isRealmAcl === false
        user <- groupUsersQuery if user.groupName === access.groupName
      } yield {
        (user.userSubjectId, access.accessLevel)
      }

      val subGroupQuery = for {
        access <- workspaceAccessQuery if access.workspaceId === workspaceId && access.isRealmAcl === false
        subGroup <- groupSubgroupsQuery if subGroup.parentGroupName === access.groupName
      } yield {
        (subGroup.childGroupName, access.accessLevel)
      }

      val usersWithSharePermission = userQuery.joinLeft(workspaceUserShareQuery).on(_._1 === _.userSubjectId).map { case ((subjectId, accessLevel), hasSharePermission) =>
        (subjectId, accessLevel, hasSharePermission.isDefined)
      }
      val groupsWithSharePermission = subGroupQuery.joinLeft(workspaceGroupShareQuery).on(_._1 === _.groupName).map { case ((groupName, accessLevel), hasSharePermission) =>
        (groupName, accessLevel, hasSharePermission.isDefined)
      }

      for {
        users <- usersWithSharePermission.result.map {
          _.map { case (subjectId, accessLevel, hasSharePermission) =>
            (Left(RawlsUserRef(RawlsUserSubjectId(subjectId))), (WorkspaceAccessLevels.withName(accessLevel), hasSharePermission)) }
        }
        subGroups <- groupsWithSharePermission.result.map {
          _.map { case (groupName, accessLevel, hasSharePermission) =>
            (Right(RawlsGroupRef(RawlsGroupName(groupName))), (WorkspaceAccessLevels.withName(accessLevel), hasSharePermission)) }
        }
      } yield {
        (users ++ subGroups).toSet
      }
    }

    private def loadWorkspace(lookup: WorkspaceQueryType): DBIOAction[Option[Workspace], NoStream, Read] = {
      uniqueResult(loadWorkspaces(lookup))
    }

    private def loadWorkspaces(lookup: WorkspaceQueryType): DBIOAction[Seq[Workspace], NoStream, Read] = {
      for {
        workspaceRecs <- lookup.result
        workspaceAttributeRecs <- workspaceAttributesWithReferences(lookup).result
        workspaceAccessGroupRecs <- workspaceAccessQuery.filter(_.workspaceId.in(lookup.map(_.id))).result
      } yield {
        val attributesByWsId = workspaceAttributeQuery.unmarshalAttributes(workspaceAttributeRecs)
        val workspaceGroupsByWsId = workspaceAccessGroupRecs.groupBy(_.workspaceId).map{ case(workspaceId, accessRecords) => (workspaceId, unmarshalRawlsGroupRefs(accessRecords)) }
        workspaceRecs.map { workspaceRec =>
          val workspaceGroups = workspaceGroupsByWsId.getOrElse(workspaceRec.id, WorkspaceGroups(Map.empty[WorkspaceAccessLevel, RawlsGroupRef], Map.empty[WorkspaceAccessLevel, RawlsGroupRef]))
          unmarshalWorkspace(workspaceRec, attributesByWsId.getOrElse(workspaceRec.id, Map.empty), workspaceGroups.accessGroups, workspaceGroups.realmAcls)
        }
      }
    }

    private def marshalWorkspaceInvite(workspaceId: UUID, originUser: String, invite: WorkspaceACLUpdate) = {
      PendingWorkspaceAccessRecord(workspaceId, invite.email, originUser, new Timestamp(DateTime.now.getMillis), invite.accessLevel.toString)
    }

    private def marshalNewWorkspace(workspace: Workspace) = {
      WorkspaceRecord(workspace.namespace, workspace.name, UUID.fromString(workspace.workspaceId), workspace.bucketName, new Timestamp(workspace.createdDate.getMillis), new Timestamp(workspace.lastModified.getMillis), workspace.createdBy, workspace.isLocked, workspace.realm.map(_.groupName.value), 0)
    }

    private def unmarshalWorkspace(workspaceRec: WorkspaceRecord, attributes: AttributeMap, accessGroups: Map[WorkspaceAccessLevel, RawlsGroupRef], realmACLs: Map[WorkspaceAccessLevel, RawlsGroupRef]): Workspace = {
      val realm = workspaceRec.realmGroupName.map(name => RawlsGroupRef(RawlsGroupName(name)))
      Workspace(workspaceRec.namespace, workspaceRec.name, realm, workspaceRec.id.toString, workspaceRec.bucketName, new DateTime(workspaceRec.createdDate), new DateTime(workspaceRec.lastModified), workspaceRec.createdBy, attributes, accessGroups, realmACLs, workspaceRec.isLocked)
    }

    private def unmarshalRawlsGroupRefs(workspaceAccessRecords: Seq[WorkspaceAccessRecord]) = {
      def toGroupMap(recs: Seq[WorkspaceAccessRecord]) =
        recs.map(rec => WorkspaceAccessLevels.withName(rec.accessLevel) -> RawlsGroupRef(RawlsGroupName(rec.groupName))).toMap

      val (realmAclRecs, accessGroupRecs) = workspaceAccessRecords.partition(_.isRealmAcl)
      WorkspaceGroups(toGroupMap(realmAclRecs), toGroupMap(accessGroupRecs))
    }
  }

  private def groupByWorkspaceId(runningSubmissions: Seq[(UUID, Int)]): Map[UUID, Int] = {
    runningSubmissions.groupBy{ case (wsId, count) => wsId }.mapValues { case Seq((_, count)) => count }
  }

  private def groupByWorkspaceIdThenStatus(workflowDates: Seq[(UUID, String, Option[Timestamp])]): Map[UUID, Map[String, Option[Timestamp]]] = {
    workflowDates.groupBy { case (wsId, _, _) => wsId }.mapValues(_.groupBy { case (_, status, _) => status }.mapValues { case Seq((_, _, timestamp)) => timestamp })
  }
}

private case class WorkspaceGroups(
  realmAcls: Map[WorkspaceAccessLevels.WorkspaceAccessLevel, RawlsGroupRef],
  accessGroups:  Map[WorkspaceAccessLevels.WorkspaceAccessLevel, RawlsGroupRef])