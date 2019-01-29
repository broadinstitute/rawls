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
  workflowCollection: Option[String],
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
    with MethodConfigurationComponent =>

  import driver.api._

  class WorkspaceTable(tag: Tag) extends Table[WorkspaceRecord](tag, "WORKSPACE") {
    def id = column[UUID]("id", O.PrimaryKey)
    def namespace = column[String]("namespace", O.Length(254))
    def name = column[String]("name", O.Length(254))
    def bucketName = column[String]("bucket_name", O.Length(128))
    def workflowCollection = column[Option[String]]("workflow_collection", O.Length(255))
    def createdDate = column[Timestamp]("created_date", O.SqlType("TIMESTAMP(6)"), O.Default(defaultTimeStamp))
    def lastModified = column[Timestamp]("last_modified", O.SqlType("TIMESTAMP(6)"), O.Default(defaultTimeStamp))
    def createdBy = column[String]("created_by", O.Length(254))
    def isLocked = column[Boolean]("is_locked")
    def recordVersion = column[Long]("record_version")

    def uniqueNamespaceName = index("IDX_WS_UNIQUE_NAMESPACE_NAME", (namespace, name), unique = true)

    def * = (namespace, name, id, bucketName, workflowCollection, createdDate, lastModified, createdBy, isLocked, recordVersion) <> (WorkspaceRecord.tupled, WorkspaceRecord.unapply)
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
          //should we be deleting ALL workspace-related things inside of this method?
          workspaceAttributes(findByIdQuery(workspaceRecord.id)).result.flatMap(recs => DBIO.seq(deleteWorkspaceAttributes(recs.map(_._2)))) andThen {
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

    def findByIdQuery(workspaceId: UUID): WorkspaceQueryType = {
      filter(_.id === workspaceId)
    }

    def findByIdAndRecordVersionQuery(workspaceId: UUID, recordVersion: Long): WorkspaceQueryType = {
      filter(w => w.id === workspaceId && w.recordVersion === recordVersion)
    }

    def findByIdsQuery(workspaceIds: Seq[UUID]): WorkspaceQueryType = {
      filter(_.id.inSetBind(workspaceIds))
    }

    private def loadWorkspace(lookup: WorkspaceQueryType): ReadAction[Option[Workspace]] = {
      uniqueResult(loadWorkspaces(lookup))
    }

    private def loadWorkspaces(lookup: WorkspaceQueryType): ReadAction[Seq[Workspace]] = {
      for {
        workspaceRecs <- lookup.result
        workspaceAttributeRecs <- workspaceAttributesWithReferences(lookup).result
      } yield {
        val attributesByWsId = workspaceAttributeQuery.unmarshalAttributes(workspaceAttributeRecs)
        workspaceRecs.map { workspaceRec =>
          unmarshalWorkspace(workspaceRec, attributesByWsId.getOrElse(workspaceRec.id, Map.empty))
        }
      }
    }

    private def marshalNewWorkspace(workspace: Workspace) = {
      WorkspaceRecord(workspace.namespace, workspace.name, UUID.fromString(workspace.workspaceId), workspace.bucketName, workspace.workflowCollectionName, new Timestamp(workspace.createdDate.getMillis), new Timestamp(workspace.lastModified.getMillis), workspace.createdBy, workspace.isLocked, 0)
    }

    private def unmarshalWorkspace(workspaceRec: WorkspaceRecord, attributes: AttributeMap): Workspace = {
      Workspace(workspaceRec.namespace, workspaceRec.name, workspaceRec.id.toString, workspaceRec.bucketName, workspaceRec.workflowCollection, new DateTime(workspaceRec.createdDate), new DateTime(workspaceRec.lastModified), workspaceRec.createdBy, attributes, workspaceRec.isLocked)
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
