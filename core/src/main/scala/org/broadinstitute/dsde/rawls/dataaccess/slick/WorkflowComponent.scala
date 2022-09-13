package org.broadinstitute.dsde.rawls.dataaccess.slick

import cats.instances.int._
import cats.instances.list._
import cats.instances.map._
import cats.syntax.foldable._
import nl.grons.metrics4.scala.Counter
import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.dataaccess.ExecutionServiceId
import org.broadinstitute.dsde.rawls.model.SubmissionStatuses.SubmissionStatus
import org.broadinstitute.dsde.rawls.model.WorkflowStatuses.WorkflowStatus
import org.broadinstitute.dsde.rawls.model._
import org.joda.time.DateTime
import slick.dbio.Effect.Write
import slick.jdbc.JdbcProfile

import java.sql.Timestamp
import java.util.UUID

/**
 * Created by mbemis on 2/18/16.
 */

case class WorkflowRecord(id: Long,
                          externalId: Option[String],
                          submissionId: UUID,
                          status: String,
                          statusLastChangedDate: Timestamp,
                          workflowEntityId: Option[Long],
                          recordVersion: Long,
                          executionServiceKey: Option[String],
                          externalEntityId: Option[String]
)

case class WorkflowMessageRecord(workflowId: Long, message: String)

case class WorkflowAuditStatusRecord(id: Long, workflowId: Long, status: String, timestamp: Timestamp)

case class WorkflowId(id: Long)

trait WorkflowComponent {
  this: DriverComponent with EntityComponent with SubmissionComponent with AttributeComponent =>

  import driver.api._

  class WorkflowTable(tag: Tag) extends Table[WorkflowRecord](tag, "WORKFLOW") {
    def id = column[Long]("ID", O.PrimaryKey, O.AutoInc)
    def externalId = column[Option[String]]("EXTERNAL_ID", O.SqlType("CHAR(36)"))
    def submissionId = column[UUID]("SUBMISSION_ID")
    def status = column[String]("STATUS", O.Length(32))
    def statusLastChangedDate =
      column[Timestamp]("STATUS_LAST_CHANGED", O.SqlType("TIMESTAMP(6)"), O.Default(defaultTimeStamp))
    def workflowEntityId = column[Option[Long]]("ENTITY_ID")
    def version = column[Long]("record_version")
    def executionServiceKey = column[Option[String]]("EXEC_SERVICE_KEY")
    def externalEntityId = column[Option[String]]("EXTERNAL_ENTITY_ID")

    def * = (id,
             externalId,
             submissionId,
             status,
             statusLastChangedDate,
             workflowEntityId,
             version,
             executionServiceKey,
             externalEntityId
    ) <> (WorkflowRecord.tupled, WorkflowRecord.unapply)

    def submission = foreignKey("FK_WF_SUB", submissionId, submissionQuery)(_.id)
    def workflowEntity = foreignKey("FK_WF_ENTITY", workflowEntityId, entityQuery)(_.id.?)

    def uniqueWorkflowEntity = index("idx_workflow_entity", (submissionId, workflowEntityId), unique = true)
    def statusIndex = index("idx_workflow_status", status)
    def executionServiceKeyIndex = index("idx_workflow_exec_service_key", executionServiceKey)
  }

  class WorkflowMessageTable(tag: Tag) extends Table[WorkflowMessageRecord](tag, "WORKFLOW_MESSAGE") {
    def workflowId = column[Long]("WORKFLOW_ID")
    def message = column[String]("MESSAGE")

    def * = (workflowId, message) <> (WorkflowMessageRecord.tupled, WorkflowMessageRecord.unapply)

    def workflow = foreignKey("FK_WF_MSG_WF", workflowId, workflowQuery)(_.id)
  }

  // this table records the timestamp and status of every workflow, each time a workflow changes status.
  // it is populated via triggers on the WORKFLOW table. We never write to it from Scala; we only read.
  class WorkflowAuditStatusTable(tag: Tag) extends Table[WorkflowAuditStatusRecord](tag, "AUDIT_WORKFLOW_STATUS") {
    def id = column[Long]("id", O.PrimaryKey, O.AutoInc)
    def workflowId = column[Long]("workflow_id")
    def status = column[String]("status", O.Length(32))
    def timestamp = column[Timestamp]("timestamp", O.SqlType("TIMESTAMP(6)"), O.Default(defaultTimeStamp))

    def * = (id, workflowId, status, timestamp) <> (WorkflowAuditStatusRecord.tupled, WorkflowAuditStatusRecord.unapply)

    def statusIndex = index("IDX_AUDIT_WORKFLOW_STATUS_WORKFLOW_ID", workflowId)
  }

  protected val workflowMessageQuery = TableQuery[WorkflowMessageTable]

  object workflowQuery extends TableQuery(new WorkflowTable(_)) {
    type WorkflowQueryType = Query[WorkflowTable, WorkflowRecord, Seq]
    type WorkflowQueryWithInputResolutions = Query[(SubmissionValidationTable, SubmissionAttributeTable),
                                                   (SubmissionValidationRecord, SubmissionAttributeRecord),
                                                   Seq
    ]

    def get(workspaceContext: Workspace,
            submissionId: String,
            entityType: String,
            entityName: String
    ): ReadAction[Option[Workflow]] =
      loadWorkflow(
        findWorkflowByEntity(UUID.fromString(submissionId), workspaceContext.workspaceIdAsUUID, entityType, entityName)
      )

    def get(id: Long): ReadAction[Option[Workflow]] =
      loadWorkflow(findWorkflowById(id))

    def getByExternalId(externalId: String, submissionId: String): ReadAction[Option[Workflow]] =
      loadWorkflow(findWorkflowByExternalIdAndSubmissionId(externalId, UUID.fromString(submissionId)))

    def getExecutionServiceIdByExternalId(externalId: String, submissionId: String): ReadAction[Option[String]] =
      uniqueResult[WorkflowRecord](
        findWorkflowByExternalIdAndSubmissionId(externalId, UUID.fromString(submissionId))
      ) map {
        case Some(wr) => wr.executionServiceKey
        case _        => None
      }

    def delete(id: Long): ReadWriteAction[Boolean] =
      deleteWorkflowAction(id).map(_ > 0)

    def createWorkflows(workspaceContext: Workspace,
                        submissionId: UUID,
                        workflows: Seq[Workflow],
                        externalEntityInfo: Option[ExternalEntityInfo]
    )(implicit wfStatusCounter: WorkflowStatus => Option[Counter]): ReadWriteAction[Seq[Workflow]] = {
      def insertWorkflowRecs(submissionId: UUID,
                             workflows: Seq[Workflow],
                             localEntityRecs: Seq[EntityRecord]
      ): ReadWriteAction[Map[Option[AttributeEntityReference], WorkflowRecord]] = {
        val localEntityRecsMap = localEntityRecs.map(e => e.toReference -> e.id).toMap
        val recsToInsert = workflows.map { workflow =>
          val entityId = workflow.workflowEntity.flatMap(localEntityRecsMap.get)
          // if entity does not exist locally (i.e. we don't have an id) then workflowEntity must be external
          val externalEntity = if (entityId.isEmpty) workflow.workflowEntity.map(_.entityName) else None
          marshalNewWorkflow(submissionId, workflow, entityId, externalEntity)
        }

        val insertedRecQuery = for {
          (workflowRec, workflowEntityRec) <- findWorkflowsBySubmissionId(
            submissionId
          ) joinLeft entityQuery on (_.workflowEntityId === _.id)
        } yield (workflowRec, workflowEntityRec)

        insertInBatches(workflowQuery, recsToInsert).map { rows =>
          recsToInsert.foreach(wf => wfStatusCounter(WorkflowStatuses.withName(wf.status)).foreach(_ += 1))
          rows
        } andThen {
          externalEntityInfo match {
            case None =>
              insertedRecQuery.result.map(_.map { case (workflowRec, workflowEntityRec) =>
                workflowEntityRec.map(_.toReference) -> workflowRec
              }.toMap)

            case Some(info) =>
              insertedRecQuery.result.map(_.map { case (workflowRec, _) =>
                workflowRec.externalEntityId.map(AttributeEntityReference(info.rootEntityType, _)) -> workflowRec
              }.toMap)
          }

        }
      }

      def insertInputResolutionRecs(submissionId: UUID,
                                    workflows: Seq[Workflow],
                                    workflowRecsByEntity: Map[Option[AttributeEntityReference], WorkflowRecord]
      ): ReadWriteAction[Map[(Option[AttributeEntityReference], String), SubmissionValidationRecord]] = {
        val inputResolutionRecs = for {
          workflow <- workflows
          inputResolution <- workflow.inputResolutions
        } yield marshalInputResolution(inputResolution, workflowRecsByEntity(workflow.workflowEntity).id)

        val insertedRecQuery = for {
          workflowRec <- findWorkflowsBySubmissionId(submissionId)
          insertedInputResolutionRec <- submissionValidationQuery
          if insertedInputResolutionRec.workflowId === workflowRec.id
        } yield insertedInputResolutionRec

        val entityRefsByWorkflowId = workflowRecsByEntity.map { case (entityRef, workflowRec) =>
          workflowRec.id -> entityRef
        }

        insertInBatches(submissionValidationQuery, inputResolutionRecs) andThen
          insertedRecQuery.result.map(_.map { insertedInputResolutionRec =>
            val ref = entityRefsByWorkflowId(insertedInputResolutionRec.workflowId)
            val name = insertedInputResolutionRec.inputName
            (ref, name) -> insertedInputResolutionRec
          }.toMap)
      }

      def insertInputResolutionAttributes(
        workflows: Seq[Workflow],
        inputResolutionRecs: Map[(Option[AttributeEntityReference], String), SubmissionValidationRecord]
      ): WriteAction[Int] = {
        val attributes = for {
          workflow <- workflows
          inputResolution <- workflow.inputResolutions
          attribute <- inputResolution.value
        } yield (workflow, inputResolution, attribute)

        // each attribute may be marshalled into one or more records
        // so including the marshal step in the above yield would result in a Seq[Seq[]]

        val attributeRecs = attributes flatMap { case (workflow, inputResolution, attribute) =>
          val ownerId = inputResolutionRecs(workflow.workflowEntity, inputResolution.inputName).id

          // note: the concept of namespace does not apply to Submission "attributes" because they are really input names
          val inputAttrName = AttributeName.withDefaultNS(inputResolution.inputName)
          submissionAttributeQuery.marshalAttribute(ownerId, inputAttrName, attribute, Map.empty)
        }

        submissionAttributeQuery.batchInsertAttributes(attributeRecs)
      }

      def insertMessages(workflows: Seq[Workflow],
                         workflowRecsByEntity: Map[Option[AttributeEntityReference], WorkflowRecord]
      ) = {
        val messageRecs = for {
          workflow <- workflows
          message <- workflow.messages
        } yield WorkflowMessageRecord(workflowRecsByEntity(workflow.workflowEntity).id, message.value)

        insertInBatches(workflowMessageQuery, messageRecs)
      }

      val entitiesWithMultipleWorkflows = workflows
        .groupBy(_.workflowEntity)
        .filter { case (entityRef, workflowsForEntity) => workflowsForEntity.size > 1 }
        .keys
      if (entitiesWithMultipleWorkflows.nonEmpty) {
        throw new RawlsException(
          s"Each workflow in a submission must have a unique entity. Entities [${entitiesWithMultipleWorkflows.mkString(", ")}] have multiple workflows"
        )
      }

      def maybeLookupLocalEntities =
        externalEntityInfo match {
          case Some(_) =>
            // externalEntityInfo exists: don't lookup any entities because they are not local
            DBIO.successful(Seq.empty)
          case None =>
            // externalEntityInfo does not exist: lookup local entities
            entityQuery.getEntityRecords(workspaceContext.workspaceIdAsUUID, workflows.flatMap(_.workflowEntity).toSet)
        }

      for {
        localEntityRecs <- maybeLookupLocalEntities
        workflowRecsByEntity <- insertWorkflowRecs(submissionId, workflows, localEntityRecs)
        inputResolutionRecs <- insertInputResolutionRecs(submissionId, workflows, workflowRecsByEntity)
        _ <- insertInputResolutionAttributes(workflows, inputResolutionRecs)
        _ <- insertMessages(workflows, workflowRecsByEntity)
      } yield workflows
    }

    def saveMessages(messages: Seq[AttributeString], workflowId: Long) =
      workflowMessageQuery ++= messages.map(message => WorkflowMessageRecord(workflowId, message.value))

    def updateStatus(workflow: WorkflowRecord, newStatus: WorkflowStatus)(implicit
      wfStatusCounter: WorkflowStatus => Option[Counter]
    ): ReadWriteAction[Int] =
      batchUpdateStatus(Seq(workflow), newStatus)

    // input: old workflow records, and the status that we want to apply to all of them
    def batchUpdateStatus(workflows: Seq[WorkflowRecord], newStatus: WorkflowStatus)(implicit
      wfStatusCounter: WorkflowStatus => Option[Counter]
    ): ReadWriteAction[Int] =
      if (workflows.isEmpty) {
        DBIO.successful(0)
      } else {
        UpdateWorkflowStatusRawSql.actionForWorkflowRecs(workflows, newStatus) map { rows =>
          if (rows.head == workflows.size) {
            workflows.size
          } else {
            throw new RawlsConcurrentModificationException(
              s"could not update ${workflows.size - rows.head} workflows because their record version(s) have changed"
            )
          }
        } map { result =>
          wfStatusCounter(newStatus).foreach(_ += result)
          result
        }
      }

    def batchUpdateStatusAndExecutionServiceKey(workflows: Seq[WorkflowRecord],
                                                newStatus: WorkflowStatus,
                                                execServiceId: ExecutionServiceId
    )(implicit wfStatusCounter: WorkflowStatus => Option[Counter]): ReadWriteAction[Int] =
      if (workflows.isEmpty) {
        DBIO.successful(0)
      } else {
        UpdateWorkflowStatusAndExecutionIdRawSql.actionForWorkflowRecs(workflows, newStatus, execServiceId) map {
          rows =>
            if (rows.head == workflows.size)
              workflows.size
            else
              throw new RawlsConcurrentModificationException(
                s"could not update ${workflows.size - rows.head} workflows because their record version(s) have changed"
              )
        } map { result =>
          wfStatusCounter(newStatus).foreach(_ += result)
          result
        }
      }

    def updateWorkflowRecord(
      workflowRecord: WorkflowRecord
    )(implicit wfStatusCounter: WorkflowStatus => Option[Counter]): WriteAction[Int] =
      findWorkflowByIdAndVersion(workflowRecord.id, workflowRecord.recordVersion)
        .update(
          workflowRecord.copy(statusLastChangedDate = new Timestamp(System.currentTimeMillis()),
                              recordVersion = workflowRecord.recordVersion + 1
          )
        )
        .map { result =>
          wfStatusCounter(WorkflowStatuses.withName(workflowRecord.status)).foreach(_ += result)
          result
        }

    // Not instrumented because there is no workspace/submission context for this method.
    // It is actually only called on Rawls boot to reset Launching workflows to Queued.
    def batchUpdateStatus(currentStatus: WorkflowStatuses.WorkflowStatus,
                          newStatus: WorkflowStatuses.WorkflowStatus
    ): WriteAction[Int] =
      UpdateWorkflowStatusRawSql.actionForCurrentStatus(currentStatus, newStatus)

    def batchUpdateWorkflowsOfStatus(submissionId: UUID,
                                     currentStatus: WorkflowStatus,
                                     newStatus: WorkflowStatuses.WorkflowStatus
    )(implicit wfStatusCounter: WorkflowStatus => Option[Counter]): WriteAction[Int] =
      UpdateWorkflowStatusRawSql.actionForCurrentStatusAndSubmission(submissionId, currentStatus, newStatus).map {
        result =>
          wfStatusCounter(newStatus).foreach(_ += result)
          result
      }

    def deleteWorkflowAction(id: Long) =
      deleteWorkflowAttributes(id) andThen
        deleteMessagesAndInputs(id) andThen
        findWorkflowById(id).delete

    def findActiveWorkflowsWithoutExternalIds(workspaceContext: Workspace): ReadAction[Seq[WorkflowRecord]] =
      findActiveWorkflows(workspaceContext).filter(!_.externalId.isDefined).result

    def findActiveWorkflowsWithExternalIds(workspaceContext: Workspace): ReadAction[Seq[WorkflowRecord]] =
      findActiveWorkflows(workspaceContext).filter(_.externalId.isDefined).result

    def findActiveWorkflows(workspaceContext: Workspace): WorkflowQueryType =
      findWorkflowsByWorkspace(workspaceContext).filter(
        _.status inSetBind ((WorkflowStatuses.queuedStatuses ++ WorkflowStatuses.runningStatuses) map { _.toString })
      )

    def deleteWorkflowAttributes(id: Long) =
      findInputResolutionsByWorkflowId(id).result flatMap { validations =>
        submissionAttributeQuery.filter(_.ownerId inSetBind (validations.map(_.id))).delete
      }

    def loadWorkflow(query: WorkflowQueryType): ReadAction[Option[Workflow]] =
      uniqueResult[WorkflowRecord](query) flatMap {
        case None =>
          DBIO.successful(None)
        case Some(rec) =>
          loadWorkflow(rec)
      }

    def loadWorkflow(rec: WorkflowRecord): ReadAction[Option[Workflow]] =
      for {
        entity <- DBIO.sequenceOption(loadWorkflowEntity(rec))
        inputResolutions <- loadInputResolutions(rec.id)
        messages <- loadWorkflowMessages(rec.id)
      } yield Option(unmarshalWorkflow(rec, entity, inputResolutions, messages))

    private def loadWorkflowEntity(rec: WorkflowRecord): Option[ReadAction[AttributeEntityReference]] =
      (rec.workflowEntityId, rec.externalEntityId) match {
        case (Some(entityId), _) => Option(submissionQuery.loadEntity(entityId))
        case (_, Some(externalId)) =>
          Option(uniqueResult[SubmissionRecord](submissionQuery.findById(rec.submissionId)).map { subOpt =>
            val entityRefOpt = for {
              sub <- subOpt
              entityType <- sub.rootEntityType
            } yield AttributeEntityReference(entityType, externalId)
            entityRefOpt.getOrElse(
              throw new RawlsException(
                s"externalEntityId exists on workflow ${rec.id} but rootEntityType not defined on submission"
              )
            )
          })
        case _ => None
      }

    def listWorkflowRecsForSubmissionAndStatuses(submissionId: UUID,
                                                 statuses: WorkflowStatuses.WorkflowStatus*
    ): ReadAction[Seq[WorkflowRecord]] =
      findWorkflowsBySubmissionId(submissionId).filter(_.status inSetBind (statuses.map(_.toString))).result

    def countWorkflowsForSubmissionByQueueStatus(submissionId: UUID): ReadAction[Map[String, Int]] = {
      val groupedSeq = findWorkflowsBySubmissionId(submissionId)
        .groupBy(_.status)
        .map { case (status, recs) => (status, recs.length) }
        .result
      groupedSeq.map(_.toMap)
    }

    def countAllStatuses: ReadAction[Map[String, Int]] =
      groupBy(_.status).map { case (status, workflows) => status -> workflows.length }.result map { _.toMap }

    def countWorkflowsByQueueStatus: ReadAction[Map[String, Int]] = {
      val groupedSeq =
        findQueuedAndRunningWorkflows.groupBy(_.status).map { case (status, recs) => (status, recs.length) }.result
      groupedSeq.map(_.toMap)
    }

    def countWorkflowsByQueueStatusByUser: ReadAction[Map[String, Map[String, Int]]] = {
      // Run query for workflow counts, grouping by submitter and workflow status.
      // The query returns a Seq[(userEmail, workflowStatus, count)].
      val userWorkflowQuery = for {
        workflow <- findQueuedAndRunningWorkflows
        submission <- submissionQuery if workflow.submissionId === submission.id
      } yield (submission, workflow)

      val groupedSeq = userWorkflowQuery
        .groupBy { case (submission, workflow) =>
          (submission.submitterId, workflow.status)
        }
        .map { case ((email, status), recs) =>
          (email, status, recs.length)
        }
        .result

      // Convert the Seq[(String, String, Int)] from the database to a nested Map[String, Map[String, Int]].
      //
      // There's some cats magic going on here. Basically we're using the `Foldable` typeclass instance
      // for List to invoke the `foldMap` method on the database result set. `foldMap` maps every A value
      // into B and then combines them using the given Monoid[B] instance. In this case our B is a
      // Map[String, Map[String, Int]]. Cats provides a Monoid instance for Map out of the box which groups
      // the keys and sums the values, which is exactly what we want in this case.
      //
      // For more information on Foldable (and cats in general), see:
      // - http://typelevel.org/cats/typeclasses/foldable.html
      // - https://www.scala-exercises.org/cats/foldable
      //
      // For reference, an implementation using the standard library might look something like this:
      //  groupedSeq.map(_.groupBy(_._1).
      //    mapValues(_.groupBy(_._2).
      //      mapValues { case Seq((_, _, n)) => n }))

      groupedSeq.map(_.toList.foldMap { case (user, workflow, n) =>
        Map(user -> Map(workflow -> n))
      })
    }

    def countWorkflowsAheadOfUserInQueue(userInfo: UserInfo): ReadAction[Int] =
      getFirstQueuedWorkflow(userInfo.userEmail.value) flatMap { optRec =>
        val query = optRec match {
          case Some(workflow) => findWorkflowsQueuedBefore(workflow.statusLastChangedDate)
          case _              => findQueuedWorkflows(Seq.empty, Seq.empty)
        }
        query.length.result
      }

    def getFirstQueuedWorkflow(submitter: String): ReadAction[Option[WorkflowRecord]] = {
      val query = for {
        submission <- submissionQuery.filter(_.submitterId === submitter)
        workflows <- filter(_.status === WorkflowStatuses.Queued.toString).filter(_.submissionId === submission.id)
      } yield workflows

      uniqueResult(query.sortBy(_.statusLastChangedDate).take(1).result)
    }

    def listWorkflowRecsForSubmission(submissionId: UUID): ReadAction[Seq[WorkflowRecord]] =
      findWorkflowsBySubmissionId(submissionId).result

    def loadWorkflowMessages(workflowId: Long): ReadAction[Seq[AttributeString]] =
      findWorkflowMessagesById(workflowId).result.map(unmarshalWorkflowMessages)

    def loadInputResolutions(workflowId: Long): ReadAction[Seq[SubmissionValidationValue]] = {
      val inputResolutionsAttributeJoin =
        findInputResolutionsByWorkflowId(workflowId) join submissionAttributeQuery on (_.id === _.ownerId)
      unmarshalInputResolutions(inputResolutionsAttributeJoin)
    }

    /**
     * Lists the submitter ids that have more workflows in statuses than count
     *
     * @param count
     * @param statuses
     * @return seq of tuples, first element being the submitter id, second the workflow count
     */
    def listSubmittersWithMoreWorkflowsThan(count: Int,
                                            statuses: Seq[WorkflowStatuses.WorkflowStatus]
    ): ReadAction[Seq[(String, Int)]] = {
      val query = for {
        workflows <- this if workflows.status inSetBind (statuses.map(_.toString))
        submission <- submissionQuery if workflows.submissionId === submission.id
      } yield (submission.submitterId, workflows)

      query
        .groupBy { case (submitter, workflows) => submitter }
        .map { case (submitter, workflows) => submitter -> workflows.length }
        .filter { case (submitter, workflowCount) => workflowCount > count }
        .result
    }

    def countWorkflows(statuses: Seq[WorkflowStatuses.WorkflowStatus]): ReadAction[Int] =
      filter(_.status inSetBind (statuses.map(_.toString))).length.result

    /*
      the find methods
     */

    def findWorkflowById(id: Long): WorkflowQueryType =
      filter(_.id === id)

    def findWorkflowByIdAndVersion(id: Long, recordVersion: Long): WorkflowQueryType =
      filter(rec => rec.id === id && rec.version === recordVersion)

    def findWorkflowByIds(ids: Traversable[Long]): WorkflowQueryType =
      filter(_.id inSetBind ids)

    def findWorkflowByExternalIdAndSubmissionId(externalId: String, submissionId: UUID): WorkflowQueryType =
      filter(wf => wf.externalId === externalId && wf.submissionId === submissionId)

    def findWorkflowsForAbort(submissionId: UUID): WorkflowQueryType = {
      val statuses: Traversable[String] = WorkflowStatuses.abortableStatuses map (_.toString)
      filter(wf => wf.submissionId === submissionId && wf.externalId.isDefined && wf.status.inSetBind(statuses))
    }

    def findWorkflowByEntityId(submissionId: UUID, entityId: Long): WorkflowQueryType =
      filter(rec => rec.submissionId === submissionId && rec.workflowEntityId === entityId)

    def findWorkflowByEntity(submissionId: UUID,
                             workspaceId: UUID,
                             entityType: String,
                             entityName: String
    ): WorkflowQueryType =
      for {
        entity <- entityQuery.findEntityByName(workspaceId, entityType, entityName)
        workflow <- workflowQuery if workflow.submissionId === submissionId && workflow.workflowEntityId === entity.id
      } yield workflow

    def findQueuedWorkflows(excludedSubmitters: Seq[String],
                            excludedSubmissionStatuses: Seq[SubmissionStatus]
    ): WorkflowQueryType = {
      val queuedWorkflows = filter(_.status === WorkflowStatuses.Queued.toString)
      val query = if (excludedSubmitters.isEmpty && excludedSubmissionStatuses.isEmpty) {
        queuedWorkflows
      } else {
        val excludedSubmittersQuery = if (excludedSubmitters.nonEmpty) {
          submissionQuery.filterNot(_.submitterId.inSetBind(excludedSubmitters))
        } else submissionQuery

        val filteredSubmissionsQuery = if (excludedSubmissionStatuses.nonEmpty) {
          excludedSubmittersQuery.filterNot(_.status.inSetBind(excludedSubmissionStatuses.map(_.toString)))
        } else excludedSubmittersQuery

        for {
          workflows <- queuedWorkflows
          submission <- filteredSubmissionsQuery if submission.id === workflows.submissionId
        } yield workflows
      }

      query.sortBy(_.id)
    }

    def findWorkflowsQueuedBefore(lastChangedDate: Timestamp): WorkflowQueryType =
      filter(wf => wf.status === WorkflowStatuses.Queued.toString && wf.statusLastChangedDate < lastChangedDate)

    def findWorkflowMessagesById(workflowId: Long) =
      workflowMessageQuery filter (_.workflowId === workflowId)

    def findInputResolutionsByWorkflowId(workflowId: Long) =
      submissionValidationQuery filter (_.workflowId === Option(workflowId))

    def findWorkflowsBySubmissionId(submissionId: UUID): WorkflowQueryType =
      filter(rec => rec.submissionId === submissionId)

    def findWorkflowsByWorkspace(workspaceContext: Workspace): WorkflowQueryType =
      for {
        sub <- submissionQuery.findByWorkspaceId(workspaceContext.workspaceIdAsUUID)
        wf <- filter(w => w.submissionId === sub.id)
      } yield wf

    def findQueuedAndRunningWorkflows: WorkflowQueryType =
      filter(rec =>
        rec.status inSetBind ((WorkflowStatuses.queuedStatuses ++ WorkflowStatuses.runningStatuses) map { _.toString })
      )

    /*
      the marshal and unmarshal methods
     */

    def marshalNewWorkflow(submissionId: UUID,
                           workflow: Workflow,
                           entityId: Option[Long],
                           externalEntityId: Option[String]
    ): WorkflowRecord =
      WorkflowRecord(
        0,
        workflow.workflowId,
        submissionId,
        workflow.status.toString,
        new Timestamp(workflow.statusLastChangedDate.toDate.getTime),
        entityId,
        0,
        None,
        externalEntityId
      )

    private def unmarshalWorkflow(workflowRec: WorkflowRecord,
                                  entity: Option[AttributeEntityReference],
                                  inputResolutions: Seq[SubmissionValidationValue],
                                  messages: Seq[AttributeString]
    ): Workflow =
      Workflow(
        workflowRec.externalId,
        WorkflowStatuses.withName(workflowRec.status),
        new DateTime(workflowRec.statusLastChangedDate.getTime),
        entity,
        inputResolutions,
        messages
      )

    private def marshalInputResolution(value: SubmissionValidationValue, workflowId: Long): SubmissionValidationRecord =
      SubmissionValidationRecord(
        0,
        workflowId,
        value.error,
        value.inputName
      )

    // Unmarshal all input resolutions for a single workflow. Assumes that everything in wfInputResolutionRecs has the same workflowId.
    def unmarshalOneWorkflowInputs(
      wfInputResolutionRecs: Seq[(SubmissionValidationRecord, Option[SubmissionAttributeRecord])],
      workflowId: Long
    ): Seq[SubmissionValidationValue] = {

      // collect up the workflow resolution results by input
      val resolutionsByInput = wfInputResolutionRecs.groupBy { case (resolution, attribute) => resolution.inputName }

      // unmarshalAttributes will unmarshal multiple workflow attributes at once, but it expects all the attribute records to be real and not options.
      // To get around this, we split by input, so that each input is successful (or not) individually.
      val submissionValues = resolutionsByInput map {
        case (inputName, recTuples: Seq[(SubmissionValidationRecord, Option[SubmissionAttributeRecord])]) =>
          val attr = if (recTuples.forall { case (submissionRec, attrRecOpt) => attrRecOpt.isDefined }) {
            // all attributes are real
            val attrRecsWithRefs = recTuples collect { case (rec, Some(attrOpt)) => ((workflowId, attrOpt), None) }

            // note: the concept of namespace does not apply to Submission "attributes" because they are really input names
            val inputAttrName = AttributeName.withDefaultNS(inputName)
            Option(submissionAttributeQuery.unmarshalAttributes(attrRecsWithRefs)(workflowId)(inputAttrName))
          } else {
            None
          }
          // assuming that the first elem has the error here
          SubmissionValidationValue(attr, recTuples.head._1.errorText, inputName)
      }
      submissionValues.toSeq
    }

    private def unmarshalInputResolutions(
      resolutions: WorkflowQueryWithInputResolutions
    ): ReadAction[Seq[SubmissionValidationValue]] =
      resolutions.result map { (inputResolutionRecords: Seq[(SubmissionValidationRecord, SubmissionAttributeRecord)]) =>
        val submissionRecs = inputResolutionRecords map {
          // recast to option
          case (valRec, attrRec) => (valRec, Option(attrRec))
        }

        if (submissionRecs.isEmpty) Seq.empty
        else unmarshalOneWorkflowInputs(submissionRecs, submissionRecs.head._1.workflowId)
      }

    private def unmarshalWorkflowMessages(workflowMsgRecs: Seq[WorkflowMessageRecord]): Seq[AttributeString] =
      workflowMsgRecs.map(rec => AttributeString(rec.message))

    private def unmarshalEntity(entityRec: EntityRecord): AttributeEntityReference = entityRec.toReference

    private def deleteMessagesAndInputs(id: Long): DBIOAction[Int, NoStream, Write with Write] =
      findWorkflowMessagesById(id).delete andThen
        findInputResolutionsByWorkflowId(id).delete
  }

  private object UpdateWorkflowStatusRawSql extends RawSqlQuery {
    val driver: JdbcProfile = WorkflowComponent.this.driver

    private def update(newStatus: WorkflowStatus) =
      sql"update WORKFLOW set status = ${newStatus.toString}, status_last_changed = ${new Timestamp(System.currentTimeMillis())}, record_version = record_version + 1, rawls_hostname = ${hostname} "

    def actionForWorkflowRecs(workflows: Seq[WorkflowRecord], newStatus: WorkflowStatus) = {
      val where = sql"where ("
      val workflowTuples = reduceSqlActionsWithDelim(workflows.map { case wf =>
                                                       sql"(id = ${wf.id} AND record_version = ${wf.recordVersion})"
                                                     },
                                                     sql" OR "
      )
      concatSqlActions(update(newStatus), where, workflowTuples, sql")").as[Int]
    }

    def actionForCurrentStatus(currentStatus: WorkflowStatuses.WorkflowStatus,
                               newStatus: WorkflowStatuses.WorkflowStatus
    ): WriteAction[Int] =
      concatSqlActions(update(newStatus), sql"where status = ${currentStatus.toString}").as[Int].map(_.head)

    def actionForCurrentStatusAndSubmission(submissionId: UUID,
                                            currentStatus: WorkflowStatus,
                                            newStatus: WorkflowStatuses.WorkflowStatus
    ): WriteAction[Int] =
      concatSqlActions(update(newStatus),
                       sql"where status = ${currentStatus.toString} and submission_id = ${submissionId}"
      ).as[Int].map(_.head)
  }

  private object UpdateWorkflowStatusAndExecutionIdRawSql extends RawSqlQuery {
    val driver: JdbcProfile = WorkflowComponent.this.driver

    private def update(newStatus: WorkflowStatus, executionServiceId: ExecutionServiceId) =
      sql"update WORKFLOW set status = ${newStatus.toString}, exec_service_key = ${executionServiceId.id}, status_last_changed = ${new Timestamp(System.currentTimeMillis())}, record_version = record_version + 1, rawls_hostname = ${hostname} "

    def actionForWorkflowRecs(workflows: Seq[WorkflowRecord],
                              newStatus: WorkflowStatus,
                              executionServiceId: ExecutionServiceId
    ) = {
      val where = sql"where ("
      val workflowTuples = reduceSqlActionsWithDelim(workflows.map { case wf =>
                                                       sql"(id=${wf.id} and record_version=${wf.recordVersion})"
                                                     },
                                                     sql" OR "
      )
      concatSqlActions(update(newStatus, executionServiceId), where, workflowTuples, sql")").as[Int]
    }

  }

  object workflowAuditStatusQuery extends TableQuery(new WorkflowAuditStatusTable(_)) {

    def queueTimeMostRecentSubmittedWorkflow: ReadAction[Long] =
      uniqueResult[WorkflowAuditStatusRecord](
        workflowAuditStatusQuery
          .filter(_.status === WorkflowStatuses.Submitted.toString)
          .sortBy(_.timestamp.desc)
          .take(1)
      ).flatMap {
        case Some(mostRecentlySubmitted) =>
          // we could query specifically for the time this workflow was Queued, but we'll just use the earliest time
          // for resiliency, in case the workflow somehow skipped Queued status.
          uniqueResult[WorkflowAuditStatusRecord](
            workflowAuditStatusQuery
              .filter(_.workflowId === mostRecentlySubmitted.workflowId)
              .sortBy(_.timestamp.asc)
              .take(1)
          ).map {
            case Some(earliestForThisWorkflow) =>
              mostRecentlySubmitted.timestamp.getTime - earliestForThisWorkflow.timestamp.getTime
            case _ => 0L
          }
        case _ => DBIO.successful(0L)
      }

    // this method used only inside unit tests. At runtime, the table is populated only via triggers.
    def save(wasr: WorkflowAuditStatusRecord): WriteAction[WorkflowAuditStatusRecord] = {
      (workflowAuditStatusQuery returning workflowAuditStatusQuery.map(_.id)) += wasr
    } map { newid => wasr.copy(id = newid) }
  }

}
