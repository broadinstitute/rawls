package org.broadinstitute.dsde.rawls.dataaccess.slick

import java.sql.Timestamp
import java.util.UUID

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

  protected val workspaceAccessQuery = TableQuery[WorkspaceAccessTable]

  object workspaceQuery extends TableQuery(new WorkspaceTable(_)) {
    private type WorkspaceQueryType = driver.api.Query[WorkspaceTable, WorkspaceRecord, Seq]

    def listAll(): ReadAction[Seq[Workspace]] = {
      loadWorkspaces(workspaceQuery)
    }

    def listWithAttribute(attributeMap: AttributeMap): ReadAction[Seq[Workspace]] = {
      loadWorkspaces(getWorkspacesWithAttribute(attributeMap))
    }

    def save(workspace: Workspace): ReadWriteAction[Workspace] = {
      validateUserDefinedString(workspace.namespace)
      validateUserDefinedString(workspace.name)
      workspace.attributes.keys.foreach { attrName =>
        validateUserDefinedString(attrName.name)
        validateAttributeName(attrName)
      }

      uniqueResult[WorkspaceRecord](findByIdQuery(UUID.fromString(workspace.workspaceId))) flatMap {
        case None =>
          (workspaceQuery += marshalNewWorkspace(workspace)) andThen
            insertOrUpdateAccessRecords(workspace) andThen
            upsertAttributes(workspace)
        case Some(workspaceRecord) =>
          insertOrUpdateAccessRecords(workspace) andThen
            upsertAttributes(workspace) andThen
            optimisticLockUpdate(workspaceRecord)
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

      def insertScratchAttributes(transactionId: String): ReadWriteAction[Unit] = {
        val entityRefs = workspace.attributes.collect { case (_, ref: AttributeEntityReference) => ref }
        val entityRefListMembers = workspace.attributes.collect { case (_, refList: AttributeEntityReferenceList) => refList.list}.flatten
        val entitiesToLookup = (entityRefs ++ entityRefListMembers)

        entityQuery.lookupEntitiesByNames(workspaceId, entitiesToLookup) flatMap { entityRecords =>
          val entityIdsByRef = entityRecords.map(rec => AttributeEntityReference(rec.entityType, rec.name) -> rec.id).toMap
          workspaceAttributeScratchQuery.batchInsertAttributes(workspace.attributes.map(attr => workspaceAttributeQuery.marshalAttribute(workspaceId, attr._1, attr._2, entityIdsByRef)).flatten.toSeq, transactionId)
        }
      }

      //this is really only ever going to be one id but in order to be generic we pretend it's a list
      workspaceAttributeQuery.AlterAttributesUsingScratchTableQueries.upsertAction(Seq(workspaceId), insertScratchAttributes)
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

    def lock(workspaceName: WorkspaceName): ReadWriteAction[Int] = {
      findByNameQuery(workspaceName).map(_.isLocked).update(true)
    }

    def unlock(workspaceName: WorkspaceName): ReadWriteAction[Int] = {
      findByNameQuery(workspaceName).map(_.isLocked).update(false)
    }
    
    def listEmailsAndAccessLevel(workspaceContext: SlickWorkspaceContext): ReadAction[Seq[(String, WorkspaceAccessLevel)]] = {
      val accessAndUserEmail = (for {
        access <- workspaceAccessQuery if (access.workspaceId === workspaceContext.workspaceId && access.isRealmAcl === false)
        group <- rawlsGroupQuery if (access.groupName === group.groupName)
        userGroup <- groupUsersQuery if (group.groupName === userGroup.groupName)
        user <- rawlsUserQuery if (user.userSubjectId === userGroup.userSubjectId)
      } yield (access, user)).map { case (access, user) => (access.accessLevel, user.userEmail) }

      val accessAndSubGroupEmail = (for {
        access <- workspaceAccessQuery if (access.workspaceId === workspaceContext.workspaceId && access.isRealmAcl === false)
        group <- rawlsGroupQuery if (access.groupName === group.groupName)
        subGroupGroup <- groupSubgroupsQuery if (group.groupName === subGroupGroup.parentGroupName)
        subGroup <- rawlsGroupQuery if (subGroup.groupName === subGroupGroup.childGroupName)
      } yield (access, subGroup)).map { case (access, subGroup) => (access.accessLevel, subGroup.groupEmail) }

      (accessAndUserEmail union accessAndSubGroupEmail).result.map(_.map { case (access, email) => 
        (email, WorkspaceAccessLevels.withName(access))
      })
    }

    def deleteWorkspaceAccessReferences(workspaceId: UUID) = {
      workspaceAccessQuery.filter(_.workspaceId === workspaceId).delete
    }

    def deleteWorkspaceEntitiesAndAttributes(workspaceId: UUID) = {
      entityQuery.DeleteEntityAttributesQuery.deleteAction(workspaceId) andThen {
        entityQuery.filter(_.workspaceId === workspaceId).delete
      }
    }

    def deleteWorkspaceSubmissions(workspaceId: UUID) = {
      submissionQuery.DeleteSubmissionQuery.deleteAction(workspaceId) andThen {
        submissionQuery.filter(_.workspaceId === workspaceId).delete
      }
    }

    def deleteWorkspaceMethodConfigs(workspaceId: UUID) = {
      methodConfigurationQuery.DeleteMethodConfigurationQuery.deleteAction(workspaceId) andThen {
        methodConfigurationQuery.filter(_.workspaceId === workspaceId).delete
      }
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

    def getWorkspacesWithAttribute(attributeMap: AttributeMap) = {
      val (attrName, attrValue) = attributeMap.head
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

    def findWorkspacesForGroup(group: RawlsGroupRef): ReadAction[Seq[Workspace]] = {
      val byAccessGroupAction: ReadAction[Seq[WorkspaceRecord]] = for {
        groupRecs <- rawlsGroupQuery.findGroupByName(group.groupName.value).result
        allGroups <- rawlsGroupQuery.listParentGroupsRecursive(groupRecs.toSet, groupRecs.toSet)
        workspaceRecs <- findWorkspacesForGroups(allGroups).result
      } yield workspaceRecs

      val byRealmAction: ReadAction[Seq[WorkspaceRecord]] = for {
        groupRecs <- rawlsGroupQuery.findGroupByName(group.groupName.value).result
        allGroups <- rawlsGroupQuery.listParentGroupsRecursive(groupRecs.toSet, groupRecs.toSet)
        workspaceRecs <- findWorkspacesForRealms(allGroups).result
      } yield workspaceRecs

      val workspaceRecs = for {
        byAccessGroup <- byAccessGroupAction
        byRealm <- byRealmAction
      } yield (byAccessGroup.toSet ++ byRealm).toSeq

      workspaceRecs.flatMap(recs => loadWorkspaces(findByIdsQuery(recs.map(_.id))))
    }

    private def findWorkspacesForGroups(groups: Set[RawlsGroupRecord]) = {
      for {
        workspaceAccess <- workspaceAccessQuery if (workspaceAccess.groupName.inSetBind(groups.map(_.groupName))  && workspaceAccess.isRealmAcl === false)
        workspace <- workspaceQuery if (workspaceAccess.workspaceId === workspace.id)
      } yield workspace
    }

    private def findWorkspacesForRealms(groups: Set[RawlsGroupRecord]) = {
      for {
        workspace <- workspaceQuery if (workspace.realmGroupName.inSetBind(groups.map(_.groupName)))
      } yield workspace
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