package org.broadinstitute.dsde.rawls.dataaccess.slick

import java.sql.Timestamp
import java.util.{Date, UUID}

import cats.instances.int._
import cats.instances.option._
import cats.{Monoid, MonoidK}
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.model.Attributable.AttributeMap
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.model.WorkspaceSubmissionStats
import org.broadinstitute.dsde.rawls.util.CollectionUtils
import org.joda.time.DateTime
import slick.jdbc.{GetResult, JdbcProfile}

import scala.language.postfixOps

import com.google.common.base.CaseFormat


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

  /** raw/optimized SQL queries for working with workspace attributes
    */
  object workspaceAttributesRawSqlQuery extends RawSqlQuery {
    val driver: JdbcProfile = WorkspaceComponent.this.driver

    // convenience case class to simplify signatures
    case class WorkspaceAttributeWithReference(attribute: WorkspaceAttributeRecord,
                                               entityRec: Option[EntityRecord])

    // tells slick how to convert a result row from a raw sql query to an instance of WorkspaceAttributeWithReference
    implicit val getAttributeWithReferenceResult:GetResult[WorkspaceAttributeWithReference] = GetResult { r =>
      // note that the number and order of all the r.<< match precisely with the select clause of workspaceAttributesWithReferences
      val attrRec: WorkspaceAttributeRecord = WorkspaceAttributeRecord(r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<)

      val entityIdOption: Option[Long] = r.<<
      val entityRecOption = entityIdOption.map(id => EntityRecord(id, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<))

      WorkspaceAttributeWithReference(attrRec, entityRecOption)
    }

    /** direct replacement for WorkspaceComponent.workspaceAttributesWithReferences, now with optimized SQL
      *
      * @param workspaceIds the workspaces for which to query attributes
      * @param attributeSpecs which attributes to return
      * @return attributes found in the db, with optional entity values
      */
    def workspaceAttributesWithReferences(workspaceIds: Seq[UUID], attributeSpecs: WorkspaceAttributeSpecs): ReadAction[Seq[( (UUID, WorkspaceAttributeRecord), Option[EntityRecord] )]] = {
      // if workspaceIds is empty, or the caller explicitly specified no attributes, short-circuit and return nothing
      if (workspaceIds.isEmpty || (!attributeSpecs.all && attributeSpecs.attrsToSelect.isEmpty)) {
        DBIO.successful(Seq())
      } else {
        // we have workspaceIds, and we will be retrieving attributes.
        val workspaceIdList = reduceSqlActionsWithDelim(workspaceIds.map { id => sql"$id" })

        // does this need "AND WORKSPACE_ATTRIBUTE.deleted = FALSE"?
        // the method this replaced did not specify that, so we also don't specify it here.
        val startSql =
          sql"""select
            a.id, a.owner_id, a.namespace, a.name, a.value_string, a.value_number, a.value_boolean, a.value_json, a.value_entity_ref, a.list_index, a.list_length, a.deleted, a.deleted_date,
            e_ref.id, e_ref.name, e_ref.entity_type, e_ref.workspace_id, e_ref.record_version, e_ref.deleted, e_ref.deleted_date
            from WORKSPACE_ATTRIBUTE a
            left outer join ENTITY e_ref on a.value_entity_ref = e_ref.id
            where a.owner_id in ("""

        val endSql = attributeSpecs match {
          case specs if specs.all =>
            // user supplied a filter but explicitly told us to get all attributes; so, don't add to the where clause
            sql""")"""
          case specs if specs.attrsToSelect.nonEmpty =>
            // user requested specific attributes. include them in the where clause.
            val attrNamespaceNameTuples = reduceSqlActionsWithDelim(specs.attrsToSelect.map {
                attrName => sql"(${attrName.namespace}, ${attrName.name})"
            })
            concatSqlActions(sql") and (a.namespace, a.name) in (", attrNamespaceNameTuples, sql")")
          case _ =>
            // this case should never happen, because of the short-circuits at the beginning of the method.
            throw new RawlsException(s"encountered unexpected attributeSpecs: $attributeSpecs")
        }

        val sqlResult = concatSqlActions(startSql, workspaceIdList, endSql).as[WorkspaceAttributeWithReference]

        // this implementation is a refactor of a previous impl that returns a Seq[ ( (UUID, WorkspaceAttributeRecord), Option[EntityRecord] ) ]
        // it's an awkward signature, but we'll return exactly that here:
        sqlResult.map { attrsWithRefs => attrsWithRefs.map { attrWithRef =>
          ( (attrWithRef.attribute.ownerId, attrWithRef.attribute), attrWithRef.entityRec)
        }}
      }
    }

  }

  object workspaceQuery extends TableQuery(new WorkspaceTable(_)) with RawSqlQuery with LazyLogging  {
    private type WorkspaceQueryType = driver.api.Query[WorkspaceTable, WorkspaceRecord, Seq]
    val driver: JdbcProfile = WorkspaceComponent.this.driver


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

    def findByName(workspaceName: WorkspaceName, attributeSpecs: Option[WorkspaceAttributeSpecs] = None): ReadAction[Option[Workspace]] = {
      loadWorkspace(findByNameQuery(workspaceName), attributeSpecs)
    }

    def findById(workspaceId: String): ReadAction[Option[Workspace]] = {
      loadWorkspace(findByIdQuery(UUID.fromString(workspaceId)))
    }

    def countByNamespace(namespaceName: RawlsBillingProjectName): ReadAction[Int] = {
      findByNamespaceQuery(namespaceName).size.result
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

    private def workspaceAttributes(lookup: WorkspaceQueryType, attributeSpecs: Option[WorkspaceAttributeSpecs] = None) = for {
      workspaceAttrRec <- workspaceAttributeQuery if workspaceAttrRec.ownerId.in(lookup.map(_.id))
    } yield (workspaceAttrRec.ownerId, workspaceAttrRec)

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

    private def findByNamespaceQuery(namespaceName: RawlsBillingProjectName): WorkspaceQueryType = {
      filter(rec => (rec.namespace === namespaceName.value))
    }

    private def loadWorkspace(lookup: WorkspaceQueryType, attributeSpecs: Option[WorkspaceAttributeSpecs] = None): ReadAction[Option[Workspace]] = {
        uniqueResult(loadWorkspaces(lookup, attributeSpecs))
    }

    private def loadWorkspaces(lookup: WorkspaceQueryType, attributeSpecsOption: Option[WorkspaceAttributeSpecs] = None): ReadAction[Seq[Workspace]] = {
      // if the caller did not supply a WorkspaceAttributeSpecs, default to retrieving all attributes
      val attributeSpecs = attributeSpecsOption.getOrElse(WorkspaceAttributeSpecs(all=true))

      for {
        workspaceRecs <- lookup.result
        workspaceAttributeRecs <- workspaceAttributesRawSqlQuery.workspaceAttributesWithReferences(workspaceRecs.map(_.id), attributeSpecs)
      } yield {
        val attributesByWsId = workspaceAttributeQuery.unmarshalAttributes(workspaceAttributeRecs)
        workspaceRecs.map { workspaceRec =>
          unmarshalWorkspace(workspaceRec, attributesByWsId.getOrElse(workspaceRec.id, Map.empty))
        }
      }
    }


    // result structure from workspace and attribute list raw sql
    case class WorkspaceAndAttributesRecord(workspaceRecord: WorkspaceRecord,
                                            attributeRecord: Option[WorkspaceAttributeRecord],
                                            entityRef: Option[EntityRecord],
                                            submissionStats: WorkspaceSubmissionStats)

    //implicit val getWorkspaceSubmissionStatsResult = GetResult { r => WorkspaceSubmissionStats(r.<<, r.<<, r.<<) }
    // tells slick how to convert a result row from a raw sql query to an instance of WorkspaceAndAttributesResult
    implicit val getWorkspaceAndAttributesResult = GetResult { r =>
      val workspaceRecordResult = WorkspaceRecord(r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<)

      val attributeIdOption: Option[Long] = r.<<
      val attributeRecOption = attributeIdOption.map(id => WorkspaceAttributeRecord(id, workspaceRecordResult.id, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<))

      val entityIdOption: Option[Long] = r.<<
      val entityRecOption = entityIdOption.map(id => EntityRecord(id, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<))

      val submissionStats = WorkspaceSubmissionStats(r.<<, r.<<, r.<<)

      WorkspaceAndAttributesRecord(workspaceRecordResult, attributeRecOption, entityRecOption, submissionStats)
    }

    def listWorkspaces(workspaceIds: Seq[UUID], workspaceQuery: WorkspaceQuery): ReadAction[(Int, Int, Seq[(Workspace, WorkspaceSubmissionStats)])] = {
      val lw = loadWorkspaces(workspaceIds, workspaceQuery)
      lw.map { case (unfilteredCount, filteredCount, workspaceAndAttributesRecords) =>
        val wsAttRec = workspaceAndAttributesRecords
        val allWorkspaceAndSubmissionStatsRecords = workspaceAndAttributesRecords.map(rec => (rec.workspaceRecord, rec.submissionStats)).distinct

        val workspacesWithAttributes = workspaceAndAttributesRecords.collect {
          case WorkspaceAndAttributesRecord(workspaceRecord, Some(attributeRecord), entityRefOption, submissionStats) => (((workspaceRecord.id, attributeRecord), entityRefOption), submissionStats)
        }

        val attributesByWorkspaceId = workspaceAttributeQuery.unmarshalAttributes(workspacesWithAttributes.map(_._1))

        val workspacesAndSubmissionStats = allWorkspaceAndSubmissionStatsRecords.map { case (workspaceRec, submissionStats) =>
          (unmarshalWorkspace(workspaceRec, attributesByWorkspaceId.getOrElse(workspaceRec.id, Map.empty)), submissionStats)
        }
        println("STUFF " + unfilteredCount + filteredCount + workspacesAndSubmissionStats)
        (unfilteredCount, filteredCount, workspacesAndSubmissionStats)

      }
    }


    def loadWorkspaces(workspaceIds: Seq[UUID], workspaceQuery: WorkspaceQuery): ReadAction[(Int, Int, Seq[WorkspaceAndAttributesRecord])] = {
      println("inside loadWorkspaces: " + workspaceIds + " " + workspaceQuery)

      def workspaceUUIDList = {
        val sqlWhere = sql" WHERE "
        val sqlWhereClause = if (workspaceIds.isEmpty) sql" FALSE "
        else {
          concatSqlActions( sql" w.id in ", sql"(", reduceSqlActionsWithDelim( workspaceIds.map { id => sql" UNHEX(REPLACE(${id.toString}, '-','')) "} ), sql")")
        }
        concatSqlActions(sqlWhere, sqlWhereClause)
      }

      def lastSubmissionStatusFilter = workspaceQuery.lastSubmissionStatuses.map { statuses =>
        val statusSql = reduceSqlActionsWithDelim( statuses.map { status => sql"${status.toString}"} )
        concatSqlActions(sql" AND last_submission_status in (", statusSql, sql")")
      }.getOrElse(sql" ")

      def workspaceNamespaceFilter = workspaceQuery.billingProject.map { namespace =>
        sql" AND w.namespace = ${ namespace } " }.getOrElse(sql" ")

      def workspaceNameFilter = workspaceQuery.workspaceName.map { workspaceName =>
        sql" AND w.name = ${ workspaceName } " }.getOrElse(sql" ")

      def tagFilter = workspaceQuery.tags.map { tags =>
        val tagSql = reduceSqlActionsWithDelim( tags.map { tag => sql"${tag}"})
        concatSqlActions(sql"AND wa.value_string in (", tagSql, sql") ")
      }.getOrElse(sql" ")

      // This is different from namespace and workspace name, which do an exact match
      def searchNamespaceAndNameFilter = {
        workspaceQuery.searchTerm.map { searchTerm =>
          val sqlSearchTerm = '%' + searchTerm + '%'
          sql" AND (w.namespace LIKE '#${sqlSearchTerm}' OR w.name LIKE '#${sqlSearchTerm}' ) "
        }.getOrElse(sql" ")
      }


      def ordering(prefix: String) = {
        // ToDo: Check that sortField is defined sortDirection is defaulted to ascending
        def addPrefix(str: String) = prefix + "." + str
        val sortField = addPrefix(CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, workspaceQuery.sortField))
        val sortDirection = SortDirections.toSql(workspaceQuery.sortDirection)
        val defaultSortOrdering = Seq("name", "last_modified", "created_by").map(prefix + "." + _)
        if (defaultSortOrdering.contains(sortField)) {
          val sortOrdering = defaultSortOrdering.filterNot( _ == sortField)
          val sqlOrdering = reduceSqlActionsWithDelim( sortOrdering.map { field => sql" ${field} "})
          concatSqlActions(sql" ORDER BY #${sortField} #${sortDirection}, ", sqlOrdering)
        } else {
          val sqlOrdering = reduceSqlActionsWithDelim(defaultSortOrdering.map { field => sql" ${field} "})
          concatSqlActions(sql" ORDER BY ", sqlOrdering)
        }
      }

      def limitOffset = {
        val offset = (workspaceQuery.page - 1) * workspaceQuery.pageSize
        sql" limit ${workspaceQuery.pageSize} offset ${offset}"
      }

      val sqlString =
        sql"""
              SELECT ws.namespace,
                     ws.name,
                     ws.id,
                     ws.bucket_name,
                     ws.workflow_collection,
                     ws.created_date,
                     ws.last_modified,
                     ws.created_by,
                     ws.is_locked,
                     ws.record_version,
                     wa.id,
                     wa.namespace,
                     wa.name,
                     wa.value_string,
                     wa.value_number,
                     wa.value_boolean,
                     wa.VALUE_JSON,
                     wa.value_entity_ref,
                     wa.list_index,
                     wa.list_length,
                     wa.deleted,
                     wa.deleted_date,
                     e_ref.id,
                     e_ref.name,
                     e_ref.entity_type,
                     e_ref.workspace_id,
                     e_ref.record_version,
                     e_ref.deleted,
                     e_ref.deleted_date,
                     last_succeeded,
                     last_failed,
                     running_submission_count
              FROM
           """

      val sqlInnerSelect =
        sql"""
          SELECT w.namespace,
                 w.name,
                 w.id,
                 w.bucket_name,
                 w.workflow_collection,
                 w.created_date,
                 w.last_modified,
                 w.created_by,
                 w.is_locked,
                 w.record_version,
                 sub_stats.last_succeeded,
                 sub_stats.last_failed,
                 sub_stats.running_submission_count,
                 last_submission_status
          FROM WORKSPACE w
           """

      val wsAttSubmissionJoins = sql""" LEFT OUTER JOIN WORKSPACE_ATTRIBUTE wa on w.id = wa.owner_id """

      val substatssql = sql""" LEFT OUTER JOIN (SELECT workspace_id,
                                                       submission_id,
                                                       workspace_name,
                                                       submission_status,
                                                       workflow_status,
                                                       MAX(if (workflow_status = '#${WorkflowStatuses.Succeeded.toString}', max_date, null)) AS last_succeeded,
                                                       MAX(if (workflow_status = '#${WorkflowStatuses.Failed.toString}', max_date, null)) AS last_failed,
                                                       MAX(submission_active_count) AS running_submission_count,
                                                       (CASE
                                                         when MAX(submission_active_count) > 0 then 'Running'
                                                         when MAX(if (workflow_status = 'Succeeded', max_date, 0)) > MAX(if (workflow_status = 'Failed', max_date, 0)) then 'Succeeded'
                                                         when MAX(if (workflow_status = 'Succeeded', max_date, 0)) < MAX(if (workflow_status = 'Failed', max_date, 0)) then 'Failed'
                                                         else null END) AS last_submission_status
                                                FROM
                                                    (SELECT w.id AS workspace_id,
                                                            s.id AS submission_id,
                                                            w.name AS workspace_name,
                                                            s.STATUS AS submission_status,
                                                            wf.STATUS AS workflow_status,
                                                            MAX(wf.STATUS_LAST_CHANGED) as max_date,
                                                            SUM(s.STATUS in ('Accepted', 'Evaluating', 'Submitting', 'Submitted', 'Aborting')) AS submission_active_count
                                                     FROM WORKSPACE w
                                                              LEFT OUTER JOIN SUBMISSION s on w.id = s.WORKSPACE_ID
                                                              LEFT OUTER JOIN WORKFLOW wf on s.ID = wf.SUBMISSION_ID
                                                     GROUP BY s.id, w.id, wf.status) innerS
                                                GROUP BY workspace_id
                                                ORDER BY workspace_name) sub_stats ON sub_stats.workspace_id = w.id """

      val fromAndJoins = sql"""
                         LEFT OUTER JOIN WORKSPACE_ATTRIBUTE wa on ws.id = wa.owner_id
                         LEFT OUTER JOIN ENTITY e_ref on wa.value_entity_ref = e_ref.id """
      for {
        filteredCount <- {
          val thing = concatSqlActions(sql"SELECT count(1) FROM (SELECT w.id FROM WORKSPACE w ", wsAttSubmissionJoins, substatssql, workspaceUUIDList, lastSubmissionStatusFilter, workspaceNamespaceFilter, workspaceNameFilter, tagFilter, searchNamespaceAndNameFilter, sql" GROUP BY w.id ", sql") ws").as[Int]
          println("filtered Count: " + thing.statements)
          thing
        }
//        unfilteredCount <- {
          // TODO: should this actually do the select in case there are UUIDs that don't have a record in the workspace table
//          val thing = concatSqlActions(sql"SELECT count(1) FROM (SELECT * FROM WORKSPACE w ", workspaceUUIDList, sql") ws").as[Int]
//          println("SQL " + concatSqlActions(sql"SELECT count(1) FROM (SELECT * FROM WORKSPACE w ", workspaceUUIDList, sql") ws"))
//          val thing  = workspaceIds.length
//          println("unfiltered Count: " + thing)
//          thing
//        }
        page <- {
          val thing = concatSqlActions(sqlString, sql" (", sqlInnerSelect, wsAttSubmissionJoins, substatssql, workspaceUUIDList, lastSubmissionStatusFilter, workspaceNamespaceFilter, workspaceNameFilter, tagFilter, searchNamespaceAndNameFilter, sql" GROUP BY w.id", ordering("w"),  limitOffset, sql")  ws ", fromAndJoins, ordering("ws")).as[WorkspaceAndAttributesRecord]
          println("page: " + thing.statements)
          println("page STRING: " + thing.toString)
          thing
        }
      } yield {
        println("HI")
        println("loadWorkspace: hi " +  workspaceIds.length + " " + filteredCount.head + " " + page.toString)
        (workspaceIds.length, filteredCount.head, page)
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
