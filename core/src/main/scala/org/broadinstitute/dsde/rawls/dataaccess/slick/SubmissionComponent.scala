package org.broadinstitute.dsde.rawls.dataaccess.slick

import java.sql.Timestamp
import java.util.UUID

import nl.grons.metrics.scala.Counter
import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.dataaccess.SlickWorkspaceContext
import org.broadinstitute.dsde.rawls.dataaccess.jndi.JndiDirectoryDAO
import org.broadinstitute.dsde.rawls.metrics.RawlsInstrumented._
import org.broadinstitute.dsde.rawls.model.SubmissionStatuses.SubmissionStatus
import org.broadinstitute.dsde.rawls.model.WorkflowStatuses.WorkflowStatus
import org.broadinstitute.dsde.rawls.model._
import org.joda.time.DateTime
import slick.driver.JdbcDriver
import slick.jdbc.GetResult

/**
 * Created by mbemis on 2/18/16.
 */

case class SubmissionRecord(id: UUID,
                            workspaceId: UUID,
                            submissionDate: Timestamp,
                            submitterId: String,
                            methodConfigurationId: Long,
                            submissionEntityId: Long,
                            status: String,
                            useCallCache: Boolean,
                            workflowFailureMode: Option[String]
                           )

case class SubmissionValidationRecord(id: Long,
                                      workflowId: Long,
                                      errorText: Option[String],
                                      inputName: String
                                     )

case class SubmissionAuditStatusRecord(id: Long, submissionId: UUID, status: String, timestamp: Timestamp)

trait SubmissionComponent {
  this: DriverComponent
    with JndiDirectoryDAO
    with MethodConfigurationComponent
    with EntityComponent
    with AttributeComponent
    with WorkflowComponent
    with WorkspaceComponent =>

  import driver.api._

  class SubmissionTable(tag: Tag) extends Table[SubmissionRecord](tag, "SUBMISSION") {
    def id = column[UUID]("ID", O.PrimaryKey)
    def workspaceId = column[UUID]("WORKSPACE_ID")
    def submissionDate = column[Timestamp]("DATE_SUBMITTED", O.SqlType("TIMESTAMP(6)"), O.Default(defaultTimeStamp))
    def submitterId = column[String]("SUBMITTER", O.Length(254))
    def methodConfigurationId = column[Long]("METHOD_CONFIG_ID")
    def submissionEntityId = column[Long]("ENTITY_ID")
    def status = column[String]("STATUS", O.Length(32))
    def useCallCache = column[Boolean]("USE_CALL_CACHE")
    def workflowFailureMode = column[Option[String]]("WORKFLOW_FAILURE_MODE", O.Length(32))

    def * = (id, workspaceId, submissionDate, submitterId, methodConfigurationId, submissionEntityId, status, useCallCache, workflowFailureMode) <> (SubmissionRecord.tupled, SubmissionRecord.unapply)

    def workspace = foreignKey("FK_SUB_WORKSPACE", workspaceId, workspaceQuery)(_.id)
    def methodConfiguration = foreignKey("FK_SUB_METHOD_CONFIG", methodConfigurationId, methodConfigurationQuery)(_.id)
    def submissionEntity = foreignKey("FK_SUB_ENTITY", submissionEntityId, entityQuery)(_.id)
  }

  class SubmissionValidationTable(tag: Tag) extends Table[SubmissionValidationRecord](tag, "SUBMISSION_VALIDATION") {
    def id = column[Long]("id", O.PrimaryKey, O.AutoInc)
    def workflowId = column[Long]("WORKFLOW_ID")
    def errorText = column[Option[String]]("ERROR_TEXT")
    def inputName = column[String]("INPUT_NAME")

    def * = (id, workflowId, errorText, inputName) <> (SubmissionValidationRecord.tupled, SubmissionValidationRecord.unapply)

    def workflow = foreignKey("FK_SUB_VALIDATION_WF", workflowId, workflowQuery)(_.id)
  }

  // this table records the timestamp and status of every submission, each time a submission changes status.
  // it is populated via triggers on the SUBMISSION table. We never write to it from Scala; we only read.
  class SubmissionAuditStatusTable(tag: Tag) extends Table[SubmissionAuditStatusRecord](tag, "AUDIT_SUBMISSION_STATUS") {
    def id = column[Long]("id", O.PrimaryKey, O.AutoInc)
    def submissionId = column[UUID]("submission_id")
    def status = column[String]("status", O.Length(32))
    def timestamp = column[Timestamp]("timestamp", O.SqlType("TIMESTAMP(6)"), O.Default(defaultTimeStamp))

    def * = (id, submissionId, status, timestamp) <> (SubmissionAuditStatusRecord.tupled, SubmissionAuditStatusRecord.unapply)

    def statusIndex = index("IDX_AUDIT_SUBMISSION_STATUS_SUBMISSION_ID", submissionId)
  }

  protected val submissionValidationQuery = TableQuery[SubmissionValidationTable]
  protected val submissionAuditStatusQuery = TableQuery[SubmissionAuditStatusTable]

  object submissionQuery extends TableQuery(new SubmissionTable(_)) {

    type SubmissionQueryType = Query[SubmissionTable, SubmissionRecord, Seq]

    /*
      the core methods
     */

    /* gets a submission */
    def get(workspaceContext: SlickWorkspaceContext, submissionId: String): ReadAction[Option[Submission]] = {
      loadSubmission(UUID.fromString(submissionId))
    }

    /* lists all submissions in a workspace */
    def list(workspaceContext: SlickWorkspaceContext): ReadAction[Seq[Submission]] = {
      findByWorkspaceId(workspaceContext.workspaceId).result.flatMap(recs => DBIO.sequence(recs.map{ rec =>
        loadSubmission(rec.id) map(sub => sub.get)
      }))
    }

    def listWithSubmitter(workspaceContext: SlickWorkspaceContext): ReadWriteAction[Seq[SubmissionListResponse]] = {
      val query = for {
        submissionRec <- findByWorkspaceId(workspaceContext.workspaceId)
        methodConfigRec <- methodConfigurationQuery if (submissionRec.methodConfigurationId === methodConfigRec.id)
        entityRec <- entityQuery if (submissionRec.submissionEntityId === entityRec.id)
      } yield (submissionRec, methodConfigRec, entityRec)

      for {
        workflowStates <- GatherStatusesForWorkspaceSubmissionsQuery.gatherWorkflowStatuses(workspaceContext.workspaceId)
        states = workflowStates.groupBy(_.submissionId)
        recs <- query.result
        users <- rawlsUserQuery.load(recs.map { case (submissionRec, _, _) => RawlsUserRef(RawlsUserSubjectId(submissionRec.submitterId)) })
      } yield {
        val usersById = users.map(u => u.userSubjectId -> u).toMap

        recs.map { case (submissionRec, methodConfigRec, entityRec) =>
          val user = usersById(RawlsUserSubjectId(submissionRec.submitterId))
          val config = methodConfigurationQuery.unmarshalMethodConfig(methodConfigRec, Map.empty, Map.empty, Map.empty)
          val subStatuses = states.getOrElse(submissionRec.id, Seq.empty).map(x => x.workflowStatus -> x.count).toMap

          new SubmissionListResponse(unmarshalSubmission(submissionRec, config, entityRec.toReference, Seq.empty), user, subStatuses)
        }
      }
    }

    def countAllStatuses: ReadAction[Map[String, Int]] = {
      groupBy(s => s.status).map { case (status, submissions) => (status, submissions.length) }.result map { r =>
        r.toMap
      }
    }

    def countByStatus(workspaceContext: SlickWorkspaceContext): ReadAction[Map[String, Int]] = {
      filter(_.workspaceId === workspaceContext.workspaceId).groupBy(s => s.status).map { case (status, submissions) =>
        (status, submissions.length)
      }.result map { result =>
        result.toMap
      }
    }

    /* creates a submission and associated workflows in a workspace */
    def create(workspaceContext: SlickWorkspaceContext, submission: Submission)(implicit submissionStatusCounter: SubmissionStatus => Counter, wfStatusCounter: WorkflowStatus => Counter): ReadWriteAction[Submission] = {

      def saveSubmissionWorkflows(workflows: Seq[Workflow]) = {
        workflowQuery.createWorkflows(workspaceContext, UUID.fromString(submission.submissionId), workflows)
      }

      uniqueResult[SubmissionRecord](findById(UUID.fromString(submission.submissionId))) flatMap {
        case None =>
          val configIdAction = uniqueResult[Long](methodConfigurationQuery.findActiveByName(
            workspaceContext.workspaceId, submission.methodConfigurationNamespace, submission.methodConfigurationName).map(_.id))

          loadSubmissionEntityId(workspaceContext.workspaceId, submission.submissionEntity) flatMap { entityId =>
            configIdAction flatMap { configId =>
              submissionStatusCounter(submission.status).countDBResult {
                (submissionQuery += marshalSubmission(workspaceContext.workspaceId, submission, entityId, configId.get))
              }
            } andThen
              saveSubmissionWorkflows(submission.workflows)
          }
        case Some(_) =>
          throw new RawlsException(s"A submission already exists by the id [${submission.submissionId}]")
      }
    } map { _ => submission }

    def updateSubmissionWorkspace(submissionId: UUID) = {
      uniqueResult[SubmissionRecord](findById(submissionId)) flatMap {
        case None => DBIO.successful(None)
        case Some(submissionRec) => workspaceQuery.updateLastModified(submissionRec.workspaceId)
      }
    }

    /* updates the status of a submission */
    def updateStatus(submissionId: UUID, newStatus: SubmissionStatus)(implicit submissionStatusCounter: SubmissionStatus => Counter) = {
      updateSubmissionWorkspace(submissionId) andThen
        submissionStatusCounter(newStatus).countDBResult {
          findById(submissionId).map(_.status).update(newStatus.toString)
        }
    }

    /* deletes a submission and all associated records */
    def delete(workspaceContext: SlickWorkspaceContext, submissionId: String): ReadWriteAction[Boolean] = {
      uniqueResult[SubmissionRecord](findById(UUID.fromString(submissionId))) flatMap {
        case None =>
          DBIO.successful(false)
        case Some(submissionRec) =>
          updateSubmissionWorkspace(UUID.fromString(submissionId)) andThen
            deleteSubmissionAction(UUID.fromString(submissionId)).map(_ > 0)
      }
    }

    /* admin action: lists all active submissions in the system */
    def listAllActiveSubmissions(): ReadAction[Seq[ActiveSubmission]] = {
      findActiveSubmissions.result.flatMap(recs => DBIO.sequence(recs.map{ rec =>
        loadActiveSubmission(rec.id)
      }))
    }

    def listAllActiveSubmissionIdsWithWorkspace(): ReadAction[Seq[(UUID, WorkspaceName)]] = {
      val query = findActiveSubmissions join workspaceQuery on(_.workspaceId === _.id)
      val result = query.map{ case (sub, ws) => (sub.id, ws.namespace, ws.name) }.result
      result.map(rows => rows.map { case (subId, wsNs, wsName) => (subId, WorkspaceName(wsNs, wsName)) } )
    }

    private def deleteSubmissionAction(submissionId: UUID): ReadWriteAction[Int] = {
      val workflowDeletes = workflowQuery.filter(_.submissionId === submissionId).result flatMap { result =>
        DBIO.seq(result.map(wf => workflowQuery.deleteWorkflowAction(wf.id)).toSeq:_*)
      }

      workflowDeletes andThen
        submissionQuery.filter(_.id === submissionId).delete
    }

    /*
      the finder methods
     */

    def findByWorkspaceId(workspaceId: UUID): SubmissionQueryType = {
      filter(rec => rec.workspaceId === workspaceId)
    }

    def findById(submissionId: UUID): SubmissionQueryType = {
      filter(rec => rec.id === submissionId)
    }

    def findByWorkspaceAndId(workspaceId: UUID, submissionId: UUID): SubmissionQueryType = {
      filter(rec => rec.workspaceId === workspaceId && rec.id === submissionId)
    }

    def findBySubmitter(submitterId: String): SubmissionQueryType = {
      filter(rec => rec.submitterId === submitterId)
    }

    def findActiveSubmissions: SubmissionQueryType = {
      filter(rec => rec.status inSetBind(SubmissionStatuses.activeStatuses.map(_.toString)))
    }

    /*
      the load methods
     */
    def loadSubmission(submissionId: UUID): ReadAction[Option[Submission]] = {
      uniqueResult[SubmissionRecord](findById(submissionId)) flatMap {
        case None => DBIO.successful(None)
        case Some(submissionRec) =>
          for {
            config <- methodConfigurationQuery.loadMethodConfigurationById(submissionRec.methodConfigurationId)
            workflows <- loadSubmissionWorkflows(submissionRec.id)
            entity <- loadSubmissionEntity(submissionRec.submissionEntityId)
          } yield Option(unmarshalSubmission(submissionRec, config.get, entity, workflows))
      }
    }

    def loadActiveSubmission(submissionId: UUID): ReadAction[ActiveSubmission] = {
      uniqueResult[SubmissionRecord](findById(submissionId)) flatMap { rec =>
        for {
          config <- methodConfigurationQuery.loadMethodConfigurationById(rec.get.methodConfigurationId)
          workflows <- loadSubmissionWorkflows(rec.get.id)
          entity <- loadSubmissionEntity(rec.get.submissionEntityId)
          workspace <- workspaceQuery.findById(rec.get.workspaceId.toString)
        } yield unmarshalActiveSubmission(rec.get, workspace.get, config.get, entity, workflows)
      }
    }

    def loadSubmissionWorkflowsWithIds(submissionId: UUID): ReadAction[Seq[(Long, Workflow)]] = {
      // get all workflows for this submission, with their entity and messages:
      val workflows = WorkflowAndMessagesRawSqlQuery.action(submissionId)

      // get all input resolutions for all workflows in this submission
      // we *could* do this in the same query as above, but since workflow:message and workflow:inputResolution
      // are both one-to-many, this would cause a cartesian product
      val inputResolutions = WorkflowAndInputResolutionRawSqlQuery.action(submissionId)

      inputResolutions.flatMap { resolutions =>
        workflows.map { recs =>

          // first, prepare all the input resolutions
          val groupedResolutions = resolutions.groupBy { _.workflowRecord.id }

          // group the workflow/entity/message records by workflowId, which creates a map keyed by workflowId
          val recordsByWorkflowId = recs.groupBy { _.workflowRecord.id }

          // now that we're grouped, process each workflow's records, translating into a Workflow object
          recordsByWorkflowId.map { case (workflowId, record) =>
            // because we are grouped by workflow id, and because workflow:entity is an inner join, we know that the
            // workflow and entity records are the same throughout this sequence. We can safely take the head
            // for each of them.
            val wr: WorkflowRecord = record.head.workflowRecord // first in the record
            val er: EntityRecord = record.head.entityRecord // second in the record

            // but, the workflow messages are all different - and may not exist (i.e. be None) due to the outer join.
            // translate any/all messages that exist into a Seq[AttributeString]
            val messages: Seq[AttributeString] = record.map { _.messageRecord }.collect { case Some(wm) => AttributeString(wm.message) }

            // attach the input resolutions to the workflow object
            val workflowResolutions: Seq[SubmissionValidationValue] = groupedResolutions.get(wr.id).map { seq =>

              //collect up the workflow resolution results
              val resolutions = seq.collect {
                case WorkflowAndInputResolutionRawSqlQuery.WorkflowInputResolutionListResult(workflow, Some(resolution), attribute) =>
                  (resolution, attribute)
              }
              workflowQuery.unmarshalOneWorkflowInputs(resolutions, workflowId)

            }.getOrElse(Seq.empty)

            // create the Workflow object, now that we've processed all records for this workflow.
            (wr.id, Workflow(wr.externalId,
              WorkflowStatuses.withName(wr.status),
              new DateTime(wr.statusLastChangedDate.getTime),
              er.toReference,
              workflowResolutions.sortBy(_.inputName), //enforce consistent sorting
              messages
            ))
          }.toSeq
        }
      }
    }

    def loadSubmissionWorkflows(submissionId: UUID): ReadAction[Seq[Workflow]] = {
      loadSubmissionWorkflowsWithIds(submissionId) map( _ map { case (workflowId, workflow) => workflow } )
    }

    def loadSubmissionEntity(entityId: Long): ReadAction[AttributeEntityReference] = {
      uniqueResult[EntityRecord](entityQuery.findEntityById(entityId)).map { rec =>
        unmarshalEntity(rec.getOrElse(throw new RawlsException(s"entity with id $entityId does not exist")))
      }
    }

    def loadSubmissionEntityId(workspaceId: UUID, entityRef: AttributeEntityReference): ReadAction[Long] = {
      val idOp: ReadAction[Option[Long]] = uniqueResult(entityQuery.findEntityByName(workspaceId, entityRef.entityType, entityRef.entityName).map(_.id))
      idOp.map(id => id.getOrElse(throw new RawlsException(s"$entityRef not found")))
    }

    def getMethodConfigOutputExpressions(submissionId: UUID): ReadAction[Map[String, String]] = {
      val query = for {
        submission <- submissionQuery if submission.id === submissionId
        methodConfigOutputs <- methodConfigurationOutputQuery if methodConfigOutputs.methodConfigId === submission.methodConfigurationId
      } yield (methodConfigOutputs.key, methodConfigOutputs.value)

      query.result.map(_.toMap)
    }

    def getSubmissionWorkflowStatusCounts(submissionId: UUID): ReadAction[Map[String, Int]] = {
      val query = for {
        workflow <- workflowQuery if workflow.submissionId === submissionId
      } yield (workflow.status)

      query.result.map(wfs => wfs.groupBy(identity).mapValues(_.size))
    }

    def confirmInWorkspace(workspaceId: UUID, submissionId: UUID): ReadAction[Option[Unit]] = {
      uniqueResult[Unit](findByWorkspaceAndId(workspaceId, submissionId).map(_ => ()))
    }

    /*
      the marshal/unmarshal methods
     */

    private def marshalSubmission(workspaceId: UUID, submission: Submission, entityId: Long, configId: Long): SubmissionRecord = {
      SubmissionRecord(
        UUID.fromString(submission.submissionId),
        workspaceId,
        new Timestamp(submission.submissionDate.toDate.getTime),
        submission.submitter.userSubjectId.value,
        configId,
        entityId,
        submission.status.toString,
        submission.useCallCache,
        submission.workflowFailureMode.map(_.toString))
    }

    private def unmarshalSubmission(submissionRec: SubmissionRecord, config: MethodConfiguration, entity: AttributeEntityReference, workflows: Seq[Workflow]): Submission = {
      Submission(
        submissionRec.id.toString,
        new DateTime(submissionRec.submissionDate.getTime),
        RawlsUserRef(RawlsUserSubjectId(submissionRec.submitterId)),
        config.namespace,
        config.name,
        entity,
        workflows.toList.sortBy(wf => wf.workflowEntity.entityName),
        SubmissionStatuses.withName(submissionRec.status),
        submissionRec.useCallCache,
        WorkflowFailureModes.withNameOpt(submissionRec.workflowFailureMode))
    }

    private def unmarshalActiveSubmission(submissionRec: SubmissionRecord, workspace: Workspace, config: MethodConfiguration, entity: AttributeEntityReference, workflows: Seq[Workflow]): ActiveSubmission = {
      ActiveSubmission(workspace.namespace, workspace.name,
        unmarshalSubmission(
          submissionRec,
          config,
          entity,
          workflows.toList.sortBy(wf => wf.workflowEntity.entityName))
      )
    }

    private def unmarshalEntity(entityRec: EntityRecord): AttributeEntityReference = entityRec.toReference

    private object WorkflowAndMessagesRawSqlQuery extends RawSqlQuery {
      val driver: JdbcDriver = SubmissionComponent.this.driver
      case class WorkflowMessagesListResult(workflowRecord: WorkflowRecord, entityRecord: EntityRecord, messageRecord: Option[WorkflowMessageRecord])

      implicit val getWorkflowMessagesListResult = GetResult { r =>
        val workflowRec = WorkflowRecord(r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<)
        val entityRec = EntityRecord(workflowRec.workflowEntityId, r.<<, r.<<, r.<<, r.<<, None, r.<<, r.<<)

        val messageOption: Option[String] = r.<<

        WorkflowMessagesListResult(workflowRec, entityRec, messageOption.map(WorkflowMessageRecord(workflowRec.id, _)))
      }

      def action(submissionId: UUID) = {
        sql"""select w.ID, w.EXTERNAL_ID, w.SUBMISSION_ID, w.STATUS, w.STATUS_LAST_CHANGED, w.ENTITY_ID, w.record_version, w.EXEC_SERVICE_KEY,
        e.name, e.entity_type, e.workspace_id, e.record_version, e.deleted, e.deleted_date,
        m.MESSAGE
        from WORKFLOW w
        join ENTITY e on w.ENTITY_ID = e.id
        left outer join WORKFLOW_MESSAGE m on m.workflow_id = w.id
        where w.submission_id = ${submissionId}""".as[WorkflowMessagesListResult]
      }
    }

    private object WorkflowAndInputResolutionRawSqlQuery extends RawSqlQuery {
      val driver: JdbcDriver = SubmissionComponent.this.driver
      case class WorkflowInputResolutionListResult(workflowRecord: WorkflowRecord, submissionValidationRec: Option[SubmissionValidationRecord], submissionAttributeRec: Option[SubmissionAttributeRecord])

      implicit val getWorkflowInputResolutionListResult = GetResult { r =>
        val workflowRec = WorkflowRecord(r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<)
        val (submissionValidation, attribute) = r.nextLongOption() match {
          case Some(submissionValidationId) =>
            ( Option(SubmissionValidationRecord(submissionValidationId, workflowRec.id, r.<<, r.<<)),
              r.nextLongOption().map(SubmissionAttributeRecord(_, submissionValidationId, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<)) )
          case None => (None, None)
        }
        WorkflowInputResolutionListResult(workflowRec, submissionValidation, attribute)
      }

      def action(submissionId: UUID) = {
        sql"""select w.ID, w.EXTERNAL_ID, w.SUBMISSION_ID, w.STATUS, w.STATUS_LAST_CHANGED, w.ENTITY_ID, w.record_version, w.EXEC_SERVICE_KEY,
              sv.id, sv.ERROR_TEXT, sv.INPUT_NAME,
              sa.id, sa.namespace, sa.name, sa.value_string, sa.value_number, sa.value_boolean, sa.value_json, sa.value_entity_ref, sa.list_index, sa.list_length, sa.deleted, sa.deleted_date
        from WORKFLOW w
        left outer join SUBMISSION_VALIDATION sv on sv.workflow_id = w.id
        left outer join SUBMISSION_ATTRIBUTE sa on sa.owner_id = sv.id
        where w.submission_id = ${submissionId}""".as[WorkflowInputResolutionListResult]
      }
    }

    // performs actual deletion (not hiding) of everything that depends on a submission
    object SubmissionDependenciesDeletionQuery extends RawSqlQuery {
      val driver: JdbcDriver = SubmissionComponent.this.driver

      def deleteAction(workspaceId: UUID): WriteAction[Seq[Int]] = {

        def deleteSubmissionAttributes(workflowTable: String, columnId: String) = {
          sqlu""" delete t from SUBMISSION_ATTRIBUTE t
                 inner join SUBMISSION_VALIDATION sv on sv.id=t.owner_id
                 inner join #$workflowTable w on w.id=sv.#$columnId
                 inner join SUBMISSION s on s.id=w.submission_id
                 where s.workspace_id=$workspaceId
            """
        }

        def deleteFromTable(tableFrom: String, workflowTable: String, columnId: String) = {
          sqlu"""delete t from #$tableFrom t
                 inner join #$workflowTable w on w.id=t.#$columnId
                 inner join SUBMISSION s on w.submission_id=s.id
                 where s.workspace_id=$workspaceId
              """
        }

        DBIO.seq(
          deleteSubmissionAttributes("WORKFLOW", "workflow_id"),
          deleteFromTable("WORKFLOW_MESSAGE", "WORKFLOW", "workflow_id"),
          deleteFromTable("SUBMISSION_VALIDATION", "WORKFLOW", "workflow_id")
        ) andThen {
          DBIO.sequence(Seq("WORKFLOW") map { workflow_table =>
            // delete workflows
            sqlu"""delete w from #$workflow_table w
                   inner join SUBMISSION s on w.submission_id=s.id
                   where s.workspace_id=$workspaceId
            """
          })
        }
      }
    }

    // performs actual deletion (not hiding) of submission
    def deleteFromDb(workspaceId: UUID): WriteAction[Int] = {
      SubmissionDependenciesDeletionQuery.deleteAction(workspaceId) andThen
        submissionQuery.filter(_.workspaceId === workspaceId).delete
    }

    object SubmissionStatisticsQueries extends RawSqlQuery {
      val driver: JdbcDriver = SubmissionComponent.this.driver

      def countSubmissionsPerUserQuery(startDate: String, endDate: String) = {
        sql"""select min(count), max(count), avg(count), stddev(count)
                from (select count(1) as count from SUBMISSION
                where DATE_SUBMITTED between $startDate and $endDate
                group by SUBMITTER) as counts""".as[SummaryStatistics].head
      }

      def submissionRunTimeQuery(startDate: String, endDate: String) = {
        sql"""select min(seconds), max(seconds), avg(seconds), stddev(seconds)
                from (
                  select TIMESTAMPDIFF(SECOND, s.DATE_SUBMITTED, a.timestamp) as seconds
                    from SUBMISSION s join AUDIT_SUBMISSION_STATUS a on s.ID=a.submission_id
                    where s.STATUS in ("Done") and a.STATUS in ("Done")
                    and a.timestamp between $startDate and $endDate
                ) as runtimes""".as[SummaryStatistics].head
      }

      def countSubmissionsInWindow(startDate: String, endDate: String) = {
        sql"""select count(1) from SUBMISSION
                where DATE_SUBMITTED between $startDate and $endDate""".as[SingleStatistic].head
      }

      def countUsersWhoSubmittedInWindow(startDate: String, endDate: String) = {
        sql"""select count(distinct SUBMITTER) from SUBMISSION
                where DATE_SUBMITTED between $startDate and $endDate""".as[SingleStatistic].head
      }
    }

    object GatherStatusesForWorkspaceSubmissionsQuery extends RawSqlQuery {
      val driver: JdbcDriver = SubmissionComponent.this.driver

      implicit val getSubmissionWorkflowStatusResponse = GetResult { r =>
        SubmissionWorkflowStatusResponse(r.<<, r.<<, r.<<)
      }

      def gatherWorkflowStatuses(workspaceId: UUID) = {
        sql"""select s.id, w.status, count(*) from SUBMISSION s
                join WORKFLOW w
                on s.id = w.submission_id
                where s.workspace_id = $workspaceId
                group by s.id, w.status""".as[SubmissionWorkflowStatusResponse]
      }
    }
  }
}
