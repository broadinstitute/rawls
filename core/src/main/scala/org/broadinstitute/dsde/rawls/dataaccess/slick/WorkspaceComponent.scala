package org.broadinstitute.dsde.rawls.dataaccess.slick

import java.sql.Timestamp
import java.util.{Date, UUID}

import cats.instances.int._
import cats.instances.option._
import cats.{Monoid, MonoidK}
import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.dataaccess.SlickWorkspaceContext
import org.broadinstitute.dsde.rawls.model.Attributable.AttributeMap
import org.broadinstitute.dsde.rawls.model.WorkspaceAccessLevels.WorkspaceAccessLevel
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.util.CollectionUtils
import org.joda.time.DateTime
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
  recordVersion: Long) {
  def toWorkspaceName: WorkspaceName = WorkspaceName(namespace, name)
}

trait WorkspaceComponent {
  this: DriverComponent
    with AttributeComponent
    with EntityComponent
    with SubmissionComponent
    with WorkflowComponent
    with MethodConfigurationComponent
    with ManagedGroupComponent =>

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
    def recordVersion = column[Long]("record_version")

    def uniqueNamespaceName = index("IDX_WS_UNIQUE_NAMESPACE_NAME", (namespace, name), unique = true)

    def * = (namespace, name, id, bucketName, createdDate, lastModified, createdBy, isLocked, recordVersion) <> (WorkspaceRecord.tupled, WorkspaceRecord.unapply)
  }

  object workspaceQuery extends TableQuery(new WorkspaceTable(_)) {
    private type WorkspaceQueryType = driver.api.Query[WorkspaceTable, WorkspaceRecord, Seq]

    def listAll(): ReadAction[Seq[Workspace]] = {
      loadWorkspaces(workspaceQuery)
    }

    def getTags(queryString: Option[String]): ReadAction[Seq[WorkspaceTag]] = {
      val tags = workspaceAttributeQuery.findUniqueStringsByNameQuery(AttributeName.withTagsNS, queryString).result
      tags map(_.map { rec =>
          WorkspaceTag(rec._1, rec._2)
        })
    }

    def listWithAttribute(attrName: AttributeName, attrValue: AttributeValue): ReadAction[Seq[Workspace]] = {
      loadWorkspaces(getWorkspacesWithAttribute(attrName, attrValue))
    }

    def save(workspace: Workspace): ReadWriteAction[Workspace] = {
      validateUserDefinedString(workspace.namespace)
      validateWorkspaceName(workspace.name)
      workspace.attributes.keys.foreach { attrName =>
        validateUserDefinedString(attrName.name)
        validateAttributeName(attrName, Attributable.workspaceEntityType)
      }

      uniqueResult[WorkspaceRecord](findByIdQuery(UUID.fromString(workspace.workspaceId))) flatMap {
        case None =>
          (workspaceQuery += marshalNewWorkspace(workspace)) andThen
            rewriteAttributes(workspace) andThen
            updateLastModified(UUID.fromString(workspace.workspaceId))
        case Some(workspaceRecord) =>
          rewriteAttributes(workspace) andThen
            optimisticLockUpdate(workspaceRecord) andThen
            updateLastModified(UUID.fromString(workspace.workspaceId))
      } map { _ => workspace }
    }

    private def rewriteAttributes(workspace: Workspace) = {
      val workspaceId = UUID.fromString(workspace.workspaceId)

      val entityRefs = workspace.attributes.collect { case (_, ref: AttributeEntityReference) => ref }
      val entityRefListMembers = workspace.attributes.collect { case (_, refList: AttributeEntityReferenceList) => refList.list }.flatten
      val entitiesToLookup = (entityRefs ++ entityRefListMembers).toSet

      entityQuery.getEntityRecords(workspaceId, entitiesToLookup) flatMap { entityRecords =>
        val entityIdsByRef = entityRecords.map(e => e.toReference -> e.id).toMap
        val attributesToSave = workspace.attributes flatMap { attr => workspaceAttributeQuery.marshalAttribute(workspaceId, attr._1, attr._2, entityIdsByRef) }

        workspaceAttributeQuery.findByOwnerQuery(Seq(workspaceId)).result flatMap { existingAttributes =>
          workspaceAttributeQuery.rewriteAttrsAction(attributesToSave, existingAttributes, workspaceAttributeScratchQuery.insertScratchAttributes)
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
          workspaceAttributes(findByIdQuery(workspaceRecord.id)).result.flatMap(recs => DBIO.seq(deleteWorkspaceAttributes(recs.map(_._2)))) andThen {
            //should we be deleting ALL workspace-related things inside of this method?
            workspaceAccessQuery.filter(_.workspaceId === workspaceRecord.id).delete
          } andThen {
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

    def listWorkspacesInAuthDomain(authDomainRef: ManagedGroupRef): ReadAction[Seq[WorkspaceName]] = {
      for {
        workspaceIds <- findWorkspaceIdsInAuthDomainGroup(authDomainRef).result
        workspaces <- findByIdsQuery(workspaceIds).result
      } yield workspaces.map(ws => WorkspaceName(ws.namespace, ws.name))
    }

    def saveInvite(workspaceId: UUID, originUser: String, invite: WorkspaceACLUpdate): ReadWriteAction[WorkspaceACLUpdate] = {
      pendingWorkspaceAccessQuery insertOrUpdate marshalWorkspaceInvite(workspaceId, originUser, invite) map { _ => invite }
    }

    def removeInvite(workspaceId: UUID, userEmail: String): ReadWriteAction[Boolean] = {
      pendingWorkspaceAccessQuery.filter(rec => rec.workspaceId === workspaceId && rec.userEmail === userEmail).delete.map { count => count == 1 }
    }

    def getInvites(workspaceId: UUID): ReadAction[Seq[(String, WorkspaceAccessLevel)]] = {
      pendingWorkspaceAccessQuery.filter(_.workspaceId === workspaceId).map(rec => (rec.userEmail, rec.accessLevel)).result.map(_.map {case (email, access) =>
        (email, WorkspaceAccessLevels.withName(access))
      })
    }

    def findWorkspaceInvitesForUser(userEmail: RawlsUserEmail): ReadAction[Seq[(WorkspaceName, WorkspaceAccessLevel)]] = {
      (pendingWorkspaceAccessQuery.filter(_.userEmail === userEmail.value) join workspaceQuery on (_.workspaceId === _.id)).map(rec => (rec._2.namespace, rec._2.name, rec._1.accessLevel)).result.map { namesWithAccessLevel =>
        namesWithAccessLevel.map{ case (namespace, name, accessLevel) => (WorkspaceName(namespace, name), WorkspaceAccessLevels.withName(accessLevel))}
      }
    }

    def deleteWorkspaceInvitesForUser(userEmail: RawlsUserEmail) = {
      pendingWorkspaceAccessQuery.filter(_.userEmail === userEmail.value).delete
    }

    def deleteWorkspaceInvites(workspaceId: UUID) = {
      findWorkspaceInvitesQuery(workspaceId).delete
    }

    private def insertAuthDomainRecords(workspace: Workspace): WriteAction[Int] = {
      val id = UUID.fromString(workspace.workspaceId)
      val authDomainRecords = workspace.authorizationDomain.map(_.membersGroupName.value).map(groupName => WorkspaceAuthDomainRecord(id, groupName))

      (workspaceAuthDomainQuery ++= authDomainRecords).map(_.sum)
    }

    def deleteWorkspaceAuthDomainRecords(workspaceId: UUID) = {
      findAuthDomainGroupsForWorkspace(workspaceId).delete
    }

    def deleteWorkspaceAccessReferences(workspaceId: UUID): WriteAction[Int] = {
      workspaceAccessQuery.filter(_.workspaceId === workspaceId).delete
    }

    def getWorkspaceId(workspaceName: WorkspaceName): ReadAction[Option[UUID]] = {
      uniqueResult(workspaceQuery.findByNameQuery(workspaceName).result).map(x => x.map(_.id))
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

    def loadAccessGroup(workspaceName: WorkspaceName, accessLevel: WorkspaceAccessLevel) = {
      val query = for {
        workspace <- workspaceQuery if workspace.namespace === workspaceName.namespace && workspace.name === workspaceName.name
        accessGroup <- workspaceAccessQuery if accessGroup.workspaceId === workspace.id && accessGroup.accessLevel === accessLevel.toString && accessGroup.isAuthDomainAcl === false
      } yield accessGroup.groupName

      uniqueResult(query.result).map(name => RawlsGroupRef(RawlsGroupName(name.getOrElse(throw new RawlsException(s"Unable to load ${accessLevel} access group for workspace ${workspaceName}")))))
    }

    /**
     * gets the submission stats (last submission failed date, last submission success date, running submission count)
     * for each workspace
     *
     * @param workspaceIds the workspace ids to query for
     * @return WorkspaceSubmissionStats keyed by workspace id
     */
    def listSubmissionSummaryStats(workspaceIds: Seq[UUID]): ReadAction[Map[UUID, WorkspaceSubmissionStats]] = {
      // submission date query:
      //
      // select workspaceId, case when subFailed > 0 then 'Failed' else 'Succeeded' end as status, max(subEndDate) as subEndDate
      // from (
      //   select submission.id, submission.workspaceId, max(case when w.status = 'Failed' then 1 else 0 end) as subFailed, max(w.status_last_changed) as subEndDate
      //   from submission
      //   join workflow on workflow.submissionId = submission.id
      //   where submission.workspaceId in (:workspaceIds)
      //   and status in ('Succeeded', 'Failed')
      //   group by 1, 2) v
      // group by 1, 2
      //
      // Explanation:
      // - inner query gets 2 things per (workspace, submission):
      // -- whether or not the submission contains any failed workflows
      // -- the most recent workflow status change date
      // - outer query gets the most recent workflow status change date per (workspace, submissionStatus), where submissionStatus is:
      // -- Failed if the submission contains failed workflows
      // -- Succeeded if the submission does not contain failed workflows

      val innerSubmissionDateQuery = (
        for {
          submissions <- submissionQuery if submissions.workspaceId.inSetBind(workspaceIds)
          workflows <- workflowQuery if submissions.id === workflows.submissionId
        } yield (submissions.id, submissions.workspaceId, workflows.status, workflows.statusLastChangedDate)
      ).filter { case (_, _, status, _) =>
        status === WorkflowStatuses.Failed.toString || status === WorkflowStatuses.Succeeded.toString
      }.map { case (submissionId, workspaceId, status, statusLastChangedDate) =>
        (submissionId, workspaceId, Case If (status === WorkflowStatuses.Failed.toString) Then 1 Else 0, statusLastChangedDate)
      }.groupBy { case (submissionId, workspaceId, _, _) =>
        (submissionId, workspaceId)
      }.map { case ((submissionId, workspaceId), recs) =>
        (submissionId, workspaceId, recs.map(_._3).sum, recs.map(_._4).max)
      }

      val outerSubmissionDateQuery = innerSubmissionDateQuery.map { case (_, workspaceId, numFailures, maxDate) =>
        (workspaceId, Case If (numFailures > 0) Then WorkflowStatuses.Failed.toString Else WorkflowStatuses.Succeeded.toString, maxDate)
      }.groupBy { case (workspaceId, status, _) =>
        (workspaceId, status)
      }.map { case ((workspaceId, status), recs) =>
        (workspaceId, status, recs.map(_._3).max)
      }

      // running submission query: select workspaceId, count(1) ... where submissions.status === Submitted group by workspaceId
      val runningSubmissionsQuery = (for {
        submissions <- submissionQuery if submissions.workspaceId.inSetBind(workspaceIds) && submissions.status.inSetBind(SubmissionStatuses.activeStatuses.map(_.toString))
      } yield submissions).groupBy(_.workspaceId).map { case (wfId, submissions) => (wfId, submissions.length)}

      for {
        submissionDates <- outerSubmissionDateQuery.result
        runningSubmissions <- runningSubmissionsQuery.result
      } yield {
        val submissionDatesByWorkspaceByStatus: Map[UUID, Map[String, Option[Timestamp]]] = groupByWorkspaceIdThenStatus(submissionDates)
        val runningSubmissionCountByWorkspace: Map[UUID, Int] = groupByWorkspaceId(runningSubmissions)

        workspaceIds.map { wsId =>
          val (lastFailedDate, lastSuccessDate) = submissionDatesByWorkspaceByStatus.get(wsId) match {
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
      workspaceAttributeQuery.deleteAttributeRecordsById(attributeRecords.map(_.id))
    }

    private def findByNameQuery(workspaceName: WorkspaceName): WorkspaceQueryType = {
      filter(rec => (rec.namespace === workspaceName.namespace) && (rec.name === workspaceName.name))
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

    def findWorkspaceIdsInAuthDomainGroup(groupRef: ManagedGroupRef) = {
      workspaceAuthDomainQuery.filter(_.groupName === groupRef.membersGroupName.value).map(_.workspaceId).distinct
    }

    def findAuthDomainGroupsForWorkspace(workspaceId: UUID) = {
      workspaceAuthDomainQuery.filter(_.workspaceId === workspaceId)
    }

    def listPermissionPairsForGroups(groups: Set[RawlsGroupRef]): ReadAction[Seq[WorkspacePermissionsPair]] = {
      val query = for {
        accessLevel <- workspaceAccessQuery if accessLevel.groupName.inSetBind(groups.map(_.groupName.value)) && accessLevel.isAuthDomainAcl === false
        workspace <- workspaceQuery if workspace.id === accessLevel.workspaceId
      } yield (workspace, accessLevel)
      query.result.map(_.map { case (workspace, accessLevel) => WorkspacePermissionsPair(workspace.id.toString, WorkspaceAccessLevels.withName(accessLevel.accessLevel)) })
    }

    def findWorkspaceUsersAndGroupsWithCatalog(workspaceId: UUID): ReadAction[(Set[RawlsUserRef], Set[RawlsGroupRef])] = {

      val usersWithCatalogPermission = workspaceUserCatalogQuery.filter(rec => rec.workspaceId === workspaceId)
      val groupsWithCatalogPermission = workspaceGroupCatalogQuery.filter(rec => rec.workspaceId === workspaceId)

      for {
        users <- usersWithCatalogPermission.result.map(_.map { rec =>RawlsUserRef(RawlsUserSubjectId(rec.subjectId)) } )
        groups <- groupsWithCatalogPermission.result.map(_.map { rec => RawlsGroupRef(RawlsGroupName(rec.groupName)) } )
      } yield {
        (users.toSet, groups.toSet)
      }
    }

    private def loadWorkspace(lookup: WorkspaceQueryType): ReadAction[Option[Workspace]] = {
      uniqueResult(loadWorkspaces(lookup))
    }

    private def loadWorkspaces(lookup: WorkspaceQueryType): ReadAction[Seq[Workspace]] = {
      for {
        workspaceRecs <- lookup.result
        workspaceAuthDomains <- workspaceAuthDomainQuery.filter(_.workspaceId.in(lookup.map(_.id))).result
        workspaceAttributeRecs <- workspaceAttributesWithReferences(lookup).result
        workspaceAccessGroupRecs <- workspaceAccessQuery.filter(_.workspaceId.in(lookup.map(_.id))).result
      } yield {
        val attributesByWsId = workspaceAttributeQuery.unmarshalAttributes(workspaceAttributeRecs)
        val authDomainsByWsId = workspaceAuthDomains.groupBy(_.workspaceId).map{ case(workspaceId, authDomainRecords) => (workspaceId, authDomainRecords.map(x => x.groupName)) }
        val workspaceGroupsByWsId = workspaceAccessGroupRecs.groupBy(_.workspaceId).map{ case(workspaceId, accessRecords) => (workspaceId, unmarshalRawlsGroupRefs(accessRecords)) }
        workspaceRecs.map { workspaceRec =>
          val workspaceGroups = workspaceGroupsByWsId.getOrElse(workspaceRec.id, WorkspaceGroups(Map.empty[WorkspaceAccessLevel, RawlsGroupRef], Map.empty[WorkspaceAccessLevel, RawlsGroupRef]))
          unmarshalWorkspace(workspaceRec, authDomainsByWsId.getOrElse(workspaceRec.id, Seq.empty), attributesByWsId.getOrElse(workspaceRec.id, Map.empty), workspaceGroups.accessGroups, workspaceGroups.authDomainAcls)
        }
      }
    }

    private def marshalWorkspaceInvite(workspaceId: UUID, originUser: String, invite: WorkspaceACLUpdate) = {
      PendingWorkspaceAccessRecord(workspaceId, invite.email, originUser, new Timestamp(DateTime.now.getMillis), invite.accessLevel.toString)
    }

    private def marshalNewWorkspace(workspace: Workspace) = {
      WorkspaceRecord(workspace.namespace, workspace.name, UUID.fromString(workspace.workspaceId), workspace.bucketName, new Timestamp(workspace.createdDate.getMillis), new Timestamp(workspace.lastModified.getMillis), workspace.createdBy, workspace.isLocked, 0)
    }

    private def unmarshalWorkspace(workspaceRec: WorkspaceRecord, authDomain: Seq[String], attributes: AttributeMap, accessGroups: Map[WorkspaceAccessLevel, RawlsGroupRef], authDomainACLs: Map[WorkspaceAccessLevel, RawlsGroupRef]): Workspace = {
      val authDomainRefs = authDomain.map(name => ManagedGroupRef(RawlsGroupName(name)))
      Workspace(workspaceRec.namespace, workspaceRec.name, authDomainRefs.toSet, workspaceRec.id.toString, workspaceRec.bucketName, new DateTime(workspaceRec.createdDate), new DateTime(workspaceRec.lastModified), workspaceRec.createdBy, attributes, accessGroups, authDomainACLs, workspaceRec.isLocked)
    }

    private def unmarshalRawlsGroupRefs(workspaceAccessRecords: Seq[WorkspaceAccessRecord]) = {
      def toGroupMap(recs: Seq[WorkspaceAccessRecord]) =
        recs.map(rec => WorkspaceAccessLevels.withName(rec.accessLevel) -> RawlsGroupRef(RawlsGroupName(rec.groupName))).toMap

      val (authDomainAclRecs, accessGroupRecs) = workspaceAccessRecords.partition(_.isAuthDomainAcl)
      WorkspaceGroups(toGroupMap(authDomainAclRecs), toGroupMap(accessGroupRecs))
    }
  }

  private def groupByWorkspaceId(runningSubmissions: Seq[(UUID, Int)]): Map[UUID, Int] = {
    CollectionUtils.groupPairs(runningSubmissions)
  }

  private def groupByWorkspaceIdThenStatus(workflowDates: Seq[(UUID, String, Option[Timestamp])]): Map[UUID, Map[String, Option[Timestamp]]] = {
    // The function groupTriples, called below, transforms a Seq((T1, T2, T3)) to a Map(T1 -> Map(T2 -> T3)).
    // It does this by calling foldMap, which in turn requires a monoid for T3. In our case, T3 is an Option[Timestamp],
    // so we need to provide an implicit monoid for Option[Timestamp].
    //
    // There isn't really a sane monoid implementation for Timestamp (what would you do, add them?). Thankfully
    // it turns out that the UUID/String pairs in workflowDates are always unique, so it doesn't matter what the
    // monoid does because it'll never be used to combine two Option[Timestamp]s. It just needs to be provided in
    // order to make the compiler happy.
    //
    // To do this, we use the universal monoid for Option, MonoidK[Option]. Note that the inner Option takes no type
    // parameter: MonoidK doesn't care about the type inside Option, it just calls orElse on the Option for its "combine"
    // operator. Finally, the call to algebra[Timestamp] turns a MonoidK[Option] into a Monoid[Option[Timestamp]] by
    // leaving the monoid implementation alone (so it still calls orElse) and poking the Timestamp type into the Option.

    implicit val optionTimestampMonoid: Monoid[Option[Timestamp]] = MonoidK[Option].algebra[Timestamp]
    CollectionUtils.groupTriples(workflowDates)
  }
}

private case class WorkspaceGroups(
  authDomainAcls: Map[WorkspaceAccessLevels.WorkspaceAccessLevel, RawlsGroupRef],
  accessGroups:  Map[WorkspaceAccessLevels.WorkspaceAccessLevel, RawlsGroupRef])
