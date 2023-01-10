package org.broadinstitute.dsde.rawls.dataaccess.slick

import cats.implicits.catsSyntaxOptionId
import cats.instances.int._
import cats.instances.option._
import cats.{Monoid, MonoidK}
import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.model.Attributable.AttributeMap
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.util.CollectionUtils
import org.joda.time.DateTime
import slick.jdbc.{GetResult, JdbcProfile}

import java.sql.Timestamp
import java.util.{Date, UUID}
import scala.language.postfixOps

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
  recordVersion: Long,
  workspaceVersion: String,
  googleProjectId: String,
  googleProjectNumber: Option[String],
  currentBillingAccountOnGoogleProject: Option[String],
  errorMessage: Option[String],
  completedCloneWorkspaceFileTransfer: Option[Timestamp],
  workspaceType: String
) {
  def toWorkspaceName: WorkspaceName = WorkspaceName(namespace, name)
}

object WorkspaceRecord {
  def fromWorkspace(workspace: Workspace): WorkspaceRecord =
    WorkspaceRecord(
      workspace.namespace,
      workspace.name,
      UUID.fromString(workspace.workspaceId),
      workspace.bucketName,
      workspace.workflowCollectionName,
      new Timestamp(workspace.createdDate.getMillis),
      new Timestamp(workspace.lastModified.getMillis),
      workspace.createdBy,
      workspace.isLocked,
      0,
      workspace.workspaceVersion.value,
      workspace.googleProjectId.value,
      workspace.googleProjectNumber.map(_.value),
      workspace.currentBillingAccountOnGoogleProject.map(_.value),
      workspace.errorMessage,
      workspace.completedCloneWorkspaceFileTransfer.map(dateTime => new Timestamp(dateTime.getMillis)),
      workspaceType = workspace.workspaceType.toString
    )

  def toWorkspace(workspaceRec: WorkspaceRecord): Workspace =
    Workspace(
      workspaceRec.namespace,
      workspaceRec.name,
      workspaceRec.id.toString,
      workspaceRec.bucketName,
      workspaceRec.workflowCollection,
      new DateTime(workspaceRec.createdDate),
      new DateTime(workspaceRec.lastModified),
      workspaceRec.createdBy,
      Map.empty,
      workspaceRec.isLocked,
      WorkspaceVersions.fromStringThrows(workspaceRec.workspaceVersion),
      GoogleProjectId(workspaceRec.googleProjectId),
      workspaceRec.googleProjectNumber.map(GoogleProjectNumber),
      workspaceRec.currentBillingAccountOnGoogleProject.map(RawlsBillingAccountName),
      workspaceRec.errorMessage,
      workspaceRec.completedCloneWorkspaceFileTransfer.map(timestamp => new DateTime(timestamp)),
      WorkspaceType.withName(workspaceRec.workspaceType)
    )

  def toWorkspace(workspaceRec: WorkspaceRecord, attributes: AttributeMap): Workspace =
    Workspace(
      workspaceRec.namespace,
      workspaceRec.name,
      workspaceRec.id.toString,
      workspaceRec.bucketName,
      workspaceRec.workflowCollection,
      new DateTime(workspaceRec.createdDate),
      new DateTime(workspaceRec.lastModified),
      workspaceRec.createdBy,
      attributes,
      workspaceRec.isLocked,
      WorkspaceVersions.fromStringThrows(workspaceRec.workspaceVersion),
      GoogleProjectId(workspaceRec.googleProjectId),
      workspaceRec.googleProjectNumber.map(GoogleProjectNumber),
      workspaceRec.currentBillingAccountOnGoogleProject.map(RawlsBillingAccountName),
      workspaceRec.errorMessage,
      workspaceRec.completedCloneWorkspaceFileTransfer.map(timestamp => new DateTime(timestamp)),
      WorkspaceType.withName(workspaceRec.workspaceType)
    )
}

trait WorkspaceComponent {
  this: DriverComponent
    with AttributeComponent
    with RawlsBillingProjectComponent
    with EntityComponent
    with SubmissionComponent
    with WorkflowComponent
    with MethodConfigurationComponent
    with RawlsBillingProjectComponent =>

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
    def workspaceVersion = column[String]("workspace_version")
    def googleProjectId = column[String]("google_project_id")
    def googleProjectNumber = column[Option[String]]("google_project_number")
    def currentBillingAccountOnGoogleProject =
      column[Option[String]]("billing_account_on_google_project", O.Length(254))
    def errorMessage = column[Option[String]]("error_message")
    def completedCloneWorkspaceFileTransfer = column[Option[Timestamp]]("completed_clone_workspace_file_transfer")
    def workspaceType = column[String]("workspace_type")

    def uniqueNamespaceName = index("IDX_WS_UNIQUE_NAMESPACE_NAME", (namespace, name), unique = true)

    def * = (namespace,
             name,
             id,
             bucketName,
             workflowCollection,
             createdDate,
             lastModified,
             createdBy,
             isLocked,
             recordVersion,
             workspaceVersion,
             googleProjectId,
             googleProjectNumber,
             currentBillingAccountOnGoogleProject,
             errorMessage,
             completedCloneWorkspaceFileTransfer,
             workspaceType
    ) <> ((WorkspaceRecord.apply _).tupled, WorkspaceRecord.unapply)
  }

  /** raw/optimized SQL queries for working with workspace attributes
    */
  object workspaceAttributesRawSqlQuery extends RawSqlQuery {
    val driver: JdbcProfile = WorkspaceComponent.this.driver

    // convenience case class to simplify signatures
    case class WorkspaceAttributeWithReference(attribute: WorkspaceAttributeRecord, entityRec: Option[EntityRecord])

    // tells slick how to convert a result row from a raw sql query to an instance of WorkspaceAttributeWithReference
    implicit val getAttributeWithReferenceResult: GetResult[WorkspaceAttributeWithReference] = GetResult { r =>
      // note that the number and order of all the r.<< match precisely with the select clause of workspaceAttributesWithReferences
      val attrRec: WorkspaceAttributeRecord =
        WorkspaceAttributeRecord(r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<)

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
    def workspaceAttributesWithReferences(workspaceIds: Seq[UUID],
                                          attributeSpecs: WorkspaceAttributeSpecs
    ): ReadAction[Seq[((UUID, WorkspaceAttributeRecord), Option[EntityRecord])]] =
      // if workspaceIds is empty, or the caller explicitly specified no attributes, short-circuit and return nothing
      if (workspaceIds.isEmpty || (!attributeSpecs.all && attributeSpecs.attrsToSelect.isEmpty)) {
        DBIO.successful(Seq())
      } else {
        // we have workspaceIds, and we will be retrieving attributes.
        val workspaceIdList = reduceSqlActionsWithDelim(workspaceIds.map(id => sql"$id"))

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
            val attrNamespaceNameTuples = reduceSqlActionsWithDelim(specs.attrsToSelect.map { attrName =>
              sql"(${attrName.namespace}, ${attrName.name})"
            })
            concatSqlActions(sql") and (a.namespace, a.name) in (", attrNamespaceNameTuples, sql")")
          case _ =>
            // this case should never happen, because of the short-circuits at the beginning of the method.
            throw new RawlsException(s"encountered unexpected attributeSpecs: $attributeSpecs")
        }

        val sqlResult = concatSqlActions(startSql, workspaceIdList, endSql).as[WorkspaceAttributeWithReference]

        // this implementation is a refactor of a previous impl that returns a Seq[ ( (UUID, WorkspaceAttributeRecord), Option[EntityRecord] ) ]
        // it's an awkward signature, but we'll return exactly that here:
        sqlResult.map { attrsWithRefs =>
          attrsWithRefs.map { attrWithRef =>
            ((attrWithRef.attribute.ownerId, attrWithRef.attribute), attrWithRef.entityRec)
          }
        }
      }

  }

  type WorkspaceQueryType = driver.api.Query[WorkspaceTable, WorkspaceRecord, Seq]

  object workspaceQuery extends TableQuery(new WorkspaceTable(_)) {

    def listAll(): ReadAction[Seq[Workspace]] =
      loadWorkspaces(workspaceQuery)

    def listWithBillingProject(billingProject: RawlsBillingProjectName): ReadAction[Seq[Workspace]] =
      workspaceQuery.withBillingProject(billingProject).read

    def getTags(queryString: Option[String],
                limit: Option[Int] = None,
                ownerIds: Option[Seq[UUID]] = None
    ): ReadAction[Seq[WorkspaceTag]] = {
      val tags = workspaceAttributeQuery
        .findUniqueStringsByNameQuery(AttributeName.withTagsNS, queryString, limit, ownerIds)
        .result
      tags map (_.map { rec =>
        WorkspaceTag(rec._1, rec._2)
      })
    }

    def listWithAttribute(attrName: AttributeName, attrValue: AttributeValue): ReadAction[Seq[Workspace]] =
      loadWorkspaces(getWorkspacesWithAttribute(attrName, attrValue))

    /**
      * Creates or updates the provided Workspace.  First queries the database to see if a Workspace record already
      * exists with the same workspaceId.  If yes, then the existing Workspace record will be updated, otherwise a new
      * Workspace record will be created.
      * @param workspace
      * @return The updated or created Workspace
      */
    def createOrUpdate(workspace: Workspace): ReadWriteAction[Workspace] = {
      validateUserDefinedString(workspace.namespace)
      validateWorkspaceName(workspace.name)
      workspace.attributes.keys.foreach { attrName =>
        validateUserDefinedString(attrName.name)
        validateAttributeName(attrName, Attributable.workspaceEntityType)
      }

      uniqueResult[WorkspaceRecord](findByIdQuery(UUID.fromString(workspace.workspaceId))) flatMap {
        case None =>
          (workspaceQuery += WorkspaceRecord.fromWorkspace(workspace)) andThen
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
      val entityRefListMembers = workspace.attributes.collect { case (_, refList: AttributeEntityReferenceList) =>
        refList.list
      }.flatten
      val entitiesToLookup = (entityRefs ++ entityRefListMembers).toSet

      entityQuery.getEntityRecords(workspaceId, entitiesToLookup) flatMap { entityRecords =>
        val entityIdsByRef = entityRecords.map(e => e.toReference -> e.id).toMap
        val attributesToSave = workspace.attributes flatMap { attr =>
          workspaceAttributeQuery.marshalAttribute(workspaceId, attr._1, attr._2, entityIdsByRef)
        }

        workspaceAttributeQuery.findByOwnerQuery(Seq(workspaceId)).result flatMap { existingAttributes =>
          workspaceAttributeQuery.rewriteAttrsAction(attributesToSave,
                                                     existingAttributes,
                                                     workspaceAttributeTempQuery.insertScratchAttributes
          )
        }
      }
    }

    private def optimisticLockUpdate(originalRec: WorkspaceRecord): ReadWriteAction[Int] =
      findByIdAndRecordVersionQuery(originalRec.id, originalRec.recordVersion) update originalRec.copy(recordVersion =
        originalRec.recordVersion + 1
      ) map {
        case 0 =>
          throw new RawlsConcurrentModificationException(
            s"could not update $originalRec because its record version has changed"
          )
        case success => success
      }

    def findByName(workspaceName: WorkspaceName,
                   attributeSpecs: Option[WorkspaceAttributeSpecs] = None
    ): ReadAction[Option[Workspace]] =
      loadWorkspace(findByNameQuery(workspaceName), attributeSpecs)

    def findById(workspaceId: String): ReadAction[Option[Workspace]] =
      loadWorkspace(findByIdQuery(UUID.fromString(workspaceId)))

    def findByIdOrFail(workspaceId: String): ReadAction[Workspace] = findById(workspaceId) map {
      _.getOrElse(throw new RawlsException(s"""No workspace found matching id "$workspaceId"."""))
    }

    def listByIds(workspaceIds: Seq[UUID],
                  attributeSpecs: Option[WorkspaceAttributeSpecs] = None
    ): ReadAction[Seq[Workspace]] =
      loadWorkspaces(findByIdsQuery(workspaceIds), attributeSpecs)

    def listByNamespaces(namespaceNames: Seq[RawlsBillingProjectName]): ReadAction[Seq[Workspace]] =
      loadWorkspaces(findByNamespacesQuery(namespaceNames))

    def countByNamespace(namespaceName: RawlsBillingProjectName): ReadAction[Int] =
      findByNamespaceQuery(namespaceName).size.result

    def delete(workspaceName: WorkspaceName): ReadWriteAction[Boolean] =
      uniqueResult[WorkspaceRecord](findByNameQuery(workspaceName)).flatMap {
        case None                  => DBIO.successful(false)
        case Some(workspaceRecord) =>
          // should we be deleting ALL workspace-related things inside of this method?
          workspaceAttributes(findByIdQuery(workspaceRecord.id)).result.flatMap(recs =>
            DBIO.seq(deleteWorkspaceAttributes(recs.map(_._2)))
          ) andThen {
            findByIdQuery(workspaceRecord.id).delete
          } map { count =>
            count > 0
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

    def updateGoogleProjectNumber(workspaceIds: Seq[UUID], googleProjectNumber: GoogleProjectNumber): WriteAction[Int] =
      findByIdsQuery(workspaceIds).map(_.googleProjectNumber).update(Option(googleProjectNumber.value))

    def getWorkspaceId(workspaceName: WorkspaceName): ReadAction[Option[UUID]] =
      uniqueResult(workspaceQuery.findByNameQuery(workspaceName).result).map(x => x.map(_.id))

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
    def getWorkspacesWithAttribute(attrName: AttributeName, attrValue: AttributeValue) =
      for {
        attribute <- workspaceAttributeQuery.queryByAttribute(attrName, attrValue)
        workspace <- workspaceQuery if workspace.id === attribute.ownerId
      } yield workspace

    def getWorkspacesInPerimeter(servicePerimeterName: ServicePerimeterName): ReadAction[Seq[Workspace]] = {
      val workspaces = for {
        billingProject <- rawlsBillingProjectQuery.withServicePerimeter(servicePerimeterName.some)
        workspace <- workspaceQuery if workspace.namespace === billingProject.projectName
      } yield workspace

      loadWorkspaces(workspaces)
    }

    def updateCompletedCloneWorkspaceFileTransfer(workspaceId: UUID): WriteAction[Int] = {
      val currentTime = new Timestamp(new Date().getTime)
      findByIdQuery(workspaceId).map(_.completedCloneWorkspaceFileTransfer).update(Option(currentTime))
    }

    def deleteAllWorkspaceBillingAccountErrorMessagesInBillingProject(
      namespace: RawlsBillingProjectName
    ): WriteAction[Int] =
      findByNamespaceQuery(namespace).map(_.errorMessage).update(None)

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
        (submissionId,
         workspaceId,
         Case If (status === WorkflowStatuses.Failed.toString) Then 1 Else 0,
         statusLastChangedDate
        )
      }.groupBy { case (submissionId, workspaceId, _, _) =>
        (submissionId, workspaceId)
      }.map { case ((submissionId, workspaceId), recs) =>
        (submissionId, workspaceId, recs.map(_._3).sum, recs.map(_._4).max)
      }

      val outerSubmissionDateQuery = innerSubmissionDateQuery
        .map { case (_, workspaceId, numFailures, maxDate) =>
          (workspaceId,
           Case If (numFailures > 0) Then WorkflowStatuses.Failed.toString Else WorkflowStatuses.Succeeded.toString,
           maxDate
          )
        }
        .groupBy { case (workspaceId, status, _) =>
          (workspaceId, status)
        }
        .map { case ((workspaceId, status), recs) =>
          (workspaceId, status, recs.map(_._3).max)
        }

      // running submission query: select workspaceId, count(1) ... where submissions.status === Submitted group by workspaceId
      val runningSubmissionsQuery = (for {
        submissions <- submissionQuery if submissions.workspaceId
          .inSetBind(workspaceIds) && submissions.status.inSetBind(SubmissionStatuses.activeStatuses.map(_.toString))
      } yield submissions).groupBy(_.workspaceId).map { case (wfId, submissions) => (wfId, submissions.length) }

      for {
        submissionDates <- outerSubmissionDateQuery.result
        runningSubmissions <- runningSubmissionsQuery.result
      } yield {
        val submissionDatesByWorkspaceByStatus: Map[UUID, Map[String, Option[Timestamp]]] =
          groupByWorkspaceIdThenStatus(submissionDates)
        val runningSubmissionCountByWorkspace: Map[UUID, Int] = groupByWorkspaceId(runningSubmissions)

        workspaceIds.map { wsId =>
          val (lastFailedDate, lastSuccessDate) = submissionDatesByWorkspaceByStatus.get(wsId) match {
            case None => (None, None)
            case Some(datesByStatus) =>
              (datesByStatus.getOrElse(WorkflowStatuses.Failed.toString, None),
               datesByStatus.getOrElse(WorkflowStatuses.Succeeded.toString, None)
              )
          }
          wsId -> WorkspaceSubmissionStats(
            lastSuccessDate.map(t => new DateTime(t.getTime)),
            lastFailedDate.map(t => new DateTime(t.getTime)),
            runningSubmissionCountByWorkspace.getOrElse(wsId, 0)
          )
        }
      } toMap
    }

    private def workspaceAttributes(lookup: WorkspaceQueryType,
                                    attributeSpecs: Option[WorkspaceAttributeSpecs] = None
    ) = for {
      workspaceAttrRec <- workspaceAttributeQuery if workspaceAttrRec.ownerId.in(lookup.map(_.id))
    } yield (workspaceAttrRec.ownerId, workspaceAttrRec)

    private def deleteWorkspaceAttributes(attributeRecords: Seq[WorkspaceAttributeRecord]) =
      workspaceAttributeQuery.deleteAttributeRecordsById(attributeRecords.map(_.id))

    private def findByNameQuery(workspaceName: WorkspaceName): WorkspaceQueryType =
      filter(rec => (rec.namespace === workspaceName.namespace) && (rec.name === workspaceName.name))

    def findByIdQuery(workspaceId: UUID): WorkspaceQueryType =
      workspaceQuery.withWorkspaceId(workspaceId)

    def findByIdAndRecordVersionQuery(workspaceId: UUID, recordVersion: Long): WorkspaceQueryType =
      filter(w => w.id === workspaceId && w.recordVersion === recordVersion)

    def findByIdsQuery(workspaceIds: Seq[UUID]): WorkspaceQueryType =
      filter(_.id.inSetBind(workspaceIds))

    private def findByNamespaceQuery(namespaceName: RawlsBillingProjectName): WorkspaceQueryType =
      workspaceQuery.withBillingProject(namespaceName)

    private def findByNamespacesQuery(namespaceNames: Seq[RawlsBillingProjectName]): WorkspaceQueryType =
      filter(_.namespace.inSetBind(namespaceNames.map(_.value)))

    private def loadWorkspace(lookup: WorkspaceQueryType,
                              attributeSpecs: Option[WorkspaceAttributeSpecs] = None
    ): ReadAction[Option[Workspace]] =
      uniqueResult(loadWorkspaces(lookup, attributeSpecs))

    private def loadWorkspaces(lookup: WorkspaceQueryType,
                               attributeSpecsOption: Option[WorkspaceAttributeSpecs] = None
    ): ReadAction[Seq[Workspace]] = {
      // if the caller did not supply a WorkspaceAttributeSpecs, default to retrieving all attributes
      val attributeSpecs = attributeSpecsOption.getOrElse(WorkspaceAttributeSpecs(all = true))

      for {
        workspaceRecs <- lookup.result
        workspaceAttributeRecs <- workspaceAttributesRawSqlQuery.workspaceAttributesWithReferences(
          workspaceRecs.map(_.id),
          attributeSpecs
        )
      } yield {
        val attributesByWsId = workspaceAttributeQuery.unmarshalAttributes(workspaceAttributeRecs)
        workspaceRecs.map { workspaceRec =>
          WorkspaceRecord.toWorkspace(workspaceRec, attributesByWsId.getOrElse(workspaceRec.id, Map.empty))
        }
      }
    }
  }

  implicit class WorkspaceExtensions(query: WorkspaceQueryType) {

    def read: ReadAction[Seq[Workspace]] =
      for {
        records <- query.result
      } yield records.map(WorkspaceRecord.toWorkspace)

    // filters
    def withWorkspaceId(workspaceId: UUID): WorkspaceQueryType =
      query.filter(_.id === workspaceId)

    def withBillingProject(projectName: RawlsBillingProjectName): WorkspaceQueryType =
      query.filter(_.namespace === projectName.value)

    def withGoogleProjectId(googleProjectId: GoogleProjectId): WorkspaceQueryType =
      query.filter(_.googleProjectId === googleProjectId.value)

    def withoutGoogleProjectId(googleProjectId: GoogleProjectId): WorkspaceQueryType =
      query.filter(_.googleProjectId =!= googleProjectId.value)

    // setters
    def setCurrentBillingAccountOnGoogleProject(billingAccount: Option[RawlsBillingAccountName]): WriteAction[Int] =
      query.map(_.currentBillingAccountOnGoogleProject).update(billingAccount.map(_.value))

    def setBillingAccountErrorMessage(message: Option[String]): WriteAction[Int] =
      query.map(_.errorMessage).update(message)

    def lock: WriteAction[Boolean] =
      setIsLocked(true)

    def unlock: WriteAction[Boolean] =
      setIsLocked(false)

    def setIsLocked(isLocked: Boolean): WriteAction[Boolean] =
      query.map(_.isLocked).filter(_ =!= isLocked).update(isLocked).map(_ > 0)
  }

  private def groupByWorkspaceId(runningSubmissions: Seq[(UUID, Int)]): Map[UUID, Int] =
    CollectionUtils.groupPairs(runningSubmissions)

  private def groupByWorkspaceIdThenStatus(
    workflowDates: Seq[(UUID, String, Option[Timestamp])]
  ): Map[UUID, Map[String, Option[Timestamp]]] = {
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

private case class WorkspaceGroups(authDomainAcls: Map[WorkspaceAccessLevels.WorkspaceAccessLevel, RawlsGroupRef],
                                   accessGroups: Map[WorkspaceAccessLevels.WorkspaceAccessLevel, RawlsGroupRef]
)
