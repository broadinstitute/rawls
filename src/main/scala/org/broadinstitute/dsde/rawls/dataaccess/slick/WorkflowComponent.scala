package org.broadinstitute.dsde.rawls.dataaccess.slick

import java.sql.Timestamp
import java.util.UUID

import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.dataaccess.SlickWorkspaceContext
import org.broadinstitute.dsde.rawls.model._
import org.joda.time.DateTime
import slick.dbio.Effect.{Read, Write}

/**
 * Created by mbemis on 2/18/16.
 */

case class WorkflowRecord(id: Long,
                          externalId: String,
                          submissionId: UUID,
                          status: String,
                          statusLastChangedDate: Timestamp,
                          workflowEntityId: Option[Long]
                         )

case class WorkflowMessageRecord(workflowId: Long, message: String)

case class WorkflowFailureRecord(id: Long,
                                 submissionId: UUID,
                                 entityId: Option[Long]
                                )

case class WorkflowErrorRecord(workflowFailureId: Long, errorText: String)

case class WorkflowAuditStatusRecord(id: Long, workflowId: Long, status: String, timestamp: Timestamp)

case class WorkflowId(id: Long)
case class WorkflowFailureId(id: Long)

trait WorkflowComponent {
  this: DriverComponent
    with EntityComponent
    with SubmissionComponent
    with AttributeComponent =>

  import driver.api._

  class WorkflowTable(tag: Tag) extends Table[WorkflowRecord](tag, "WORKFLOW") {
    def id = column[Long]("ID", O.PrimaryKey, O.AutoInc)
    def externalid = column[String]("EXTERNAL_ID")
    def submissionId = column[UUID]("SUBMISSION_ID")
    def status = column[String]("STATUS")
    def statusLastChangedDate = column[Timestamp]("STATUS_LAST_CHANGED", O.Default(defaultTimeStamp))
    def workflowEntityId = column[Option[Long]]("ENTITY_ID")

    def * = (id, externalid, submissionId, status, statusLastChangedDate, workflowEntityId) <> (WorkflowRecord.tupled, WorkflowRecord.unapply)

    def submission = foreignKey("FK_WF_SUB", submissionId, submissionQuery)(_.id)
    def workflowEntity = foreignKey("FK_WF_ENTITY", workflowEntityId, entityQuery)(_.id.?)

    def uniqueWorkflowEntity = index("idx_workflow_entity", (submissionId, workflowEntityId), unique = true)
}

  class WorkflowMessageTable(tag: Tag) extends Table[WorkflowMessageRecord](tag, "WORKFLOW_MESSAGE") {
    def workflowId = column[Long]("WORKFLOW_ID")
    def message = column[String]("MESSAGE")

    def * = (workflowId, message) <> (WorkflowMessageRecord.tupled, WorkflowMessageRecord.unapply)

    def workflow = foreignKey("FK_WF_MSG_WF", workflowId, workflowQuery)(_.id)
  }

  class WorkflowFailureTable(tag: Tag) extends Table[WorkflowFailureRecord](tag, "WORKFLOW_FAILURE") {
    def id = column[Long]("ID", O.PrimaryKey, O.AutoInc)
    def submissionId = column[UUID]("SUBMISSION_ID")
    def entityId = column[Option[Long]]("ENTITY_ID")

    def * = (id, submissionId, entityId) <> (WorkflowFailureRecord.tupled, WorkflowFailureRecord.unapply)

    def submission = foreignKey("FK_WF_FAILURE_SUB", submissionId, submissionQuery)(_.id)
    def entity = foreignKey("FK_WF_FAILURE_ENTITY", entityId, entityQuery)(_.id.?)
  }

  class WorkflowErrorTable(tag: Tag) extends Table[WorkflowErrorRecord](tag, "WORKFLOW_ERROR") {
    def workflowFailureId = column[Long]("WORKFLOW_FAILURE_ID")
    def errorText = column[String]("ERROR_TEXT")

    def * = (workflowFailureId, errorText) <> (WorkflowErrorRecord.tupled, WorkflowErrorRecord.unapply)

    def workflowFailure = foreignKey("FK_WF_ERR_FAILURE", workflowFailureId, workflowFailureQuery)(_.id)
  }

  // this table records the timestamp and status of every workflow, each time a workflow changes status.
  // it is populated via triggers on the WORKFLOW table. We never write to it from Scala; we only read.
  class WorkflowAuditStatusTable(tag: Tag) extends Table[WorkflowAuditStatusRecord](tag, "AUDIT_WORKFLOW_STATUS") {
    def id = column[Long]("id", O.PrimaryKey, O.AutoInc)
    def workflowId = column[Long]("workflow_id")
    def status = column[String]("status")
    def timestamp = column[Timestamp]("timestamp", O.Default(defaultTimeStamp))

    def * = (id, workflowId, status, timestamp) <> (WorkflowAuditStatusRecord.tupled, WorkflowAuditStatusRecord.unapply)
  }

  protected val workflowMessageQuery = TableQuery[WorkflowMessageTable]
  protected val workflowErrorQuery = TableQuery[WorkflowErrorTable]

  object workflowQuery extends TableQuery(new WorkflowTable(_)) {

    type WorkflowQueryType = Query[WorkflowTable, WorkflowRecord, Seq]
    type WorkflowQueryWithInputResolutions = Query[(SubmissionValidationTable, SubmissionAttributeTable), (SubmissionValidationRecord, SubmissionAttributeRecord), Seq]

    def get(workspaceContext: SlickWorkspaceContext, submissionId: String, entityType: String, entityName: String): ReadAction[Option[Workflow]] = {
      loadWorkflow(findWorkflowByEntity(UUID.fromString(submissionId), workspaceContext.workspaceId, entityType, entityName))
    }

    def get(id: Long): ReadAction[Option[Workflow]] = {
      loadWorkflow(findWorkflowById(id))
    }
    
    def getWithWorkflowIds(workspaceContext: SlickWorkspaceContext, submissionId: String): ReadAction[Seq[(Long, Workflow)]] = {
      workflowQuery.filter(_.submissionId === UUID.fromString(submissionId)).result.flatMap { records =>
        DBIO.sequence(records.map { rec =>
          for {
            entity <- loadWorkflowEntity(rec.workflowEntityId)
            inputResolutions <- loadInputResolutions(rec.id)
            messages <- loadWorkflowMessages(rec.id)
          } yield((rec.id, unmarshalWorkflow(rec, entity, inputResolutions, messages)))
        })
      }
    }

    def getByExternalId(externalId: String, submissionId: String): ReadAction[Option[Workflow]] = {
      loadWorkflow(findWorkflowByExternalIdAndSubmissionId(externalId, UUID.fromString(submissionId)))
    }

    def delete(id: Long): ReadWriteAction[Boolean] = {
      deleteWorkflowAction(id).map(_ > 0)
    }

    def save(workspaceContext: SlickWorkspaceContext, submissionId: UUID, workflow: Workflow): ReadWriteAction[Workflow] = {

      submissionQuery.loadSubmissionEntityId(workspaceContext.workspaceId, workflow.workflowEntity) flatMap { eId =>
        ( (workflowQuery returning workflowQuery.map(_.id)) += marshalWorkflow(submissionId, workflow, eId)) flatMap { workflowId =>
          saveInputResolutions(workspaceContext, workflow.inputResolutions, Left(WorkflowId(workflowId))) andThen
          saveMessages(workflow.messages, workflowId)
        }
      }
    } map { _ => workflow }

    def saveInputResolutions(workspaceContext: SlickWorkspaceContext, values: Seq[SubmissionValidationValue], parentId: Either[WorkflowId, WorkflowFailureId]) = {
        DBIO.seq(values.map { case (v) =>
          v.value match {
            case None => (submissionValidationQuery += marshalInputResolution(v, parentId))
            case Some(attr) =>
              ((submissionValidationQuery returning submissionValidationQuery.map(_.id)) += marshalInputResolution(v, parentId)) flatMap { validationId =>
                DBIO.sequence(submissionAttributeQuery.insertAttributeRecords(validationId, v.inputName, attr, workspaceContext.workspaceId))
              }
          }
        }: _*)
    }

    def saveMessages(messages: Seq[AttributeString], workflowId: Long) = {
      workflowMessageQuery ++= messages.map { message => WorkflowMessageRecord(workflowId, message.value) }
    }

    def update(workspaceContext: SlickWorkspaceContext, submissionId: UUID, workflow: Workflow): ReadWriteAction[Workflow] = {
      uniqueResult[WorkflowRecord](findWorkflowByEntity(submissionId, workspaceContext.workspaceId, workflow.workflowEntity.get.entityType, workflow.workflowEntity.get.entityName)) flatMap {
        case None => throw new RawlsException(s"workflow does not exist: ${workflow}")
        case Some(rec) =>
          deleteWorkflowAttributes(rec.id) andThen
          deleteMessagesAndInputs(rec.id) andThen
          saveInputResolutions(workspaceContext, workflow.inputResolutions, Left(WorkflowId(rec.id))) andThen
          saveMessages(workflow.messages, rec.id) andThen
          findWorkflowById(rec.id).map(u => (u.status, u.statusLastChangedDate)).update((workflow.status.toString, new Timestamp(workflow.statusLastChangedDate.toDate.getTime)))
      }
    } map { _ => workflow }


    def deleteWorkflowAction(id: Long) = {
      deleteWorkflowAttributes(id) andThen
        deleteMessagesAndInputs(id) andThen
        findWorkflowById(id).delete
    }

    def deleteWorkflowErrors(submissionId: UUID) = {
      findInactiveWorkflows(submissionId).result flatMap { result =>
        DBIO.seq(result.map(f => workflowQuery.findWorkflowErrorsByWorkflowFailureId(f.id).delete).toSeq:_*)
      }
    }

    def deleteWorkflowFailures(submissionId: UUID) = {
      submissionQuery.filter(_.id === submissionId).result flatMap { result =>
        DBIO.seq(result.map(sub => workflowQuery.findInactiveWorkflows(submissionId).delete).toSeq:_*)
      }
    }

    //gather all workflowIds and workflowFailureIds in a submission and delete their attributes
    def deleteSubmissionAttributes(submissionId: UUID) = {
      val workflows = workflowQuery.filter(_.submissionId === submissionId).result
      val workflowFailures = workflowFailureQuery.filter(_.submissionId === submissionId).result

      workflows flatMap { workflowRecs =>
        val workflowIds = workflowRecs.map(_.id)
        workflowFailures flatMap { workflowFailureRecs =>
          val workflowFailureIds = workflowFailureRecs.map(_.id)
          DBIO.sequence(workflowIds.map(deleteWorkflowAttributes) ++ workflowFailureIds.map(deleteWorkflowFailureAttributes))
        }
      }
    }

    def deleteWorkflowAttributes(id: Long) = {
      findInputResolutionsByWorkflowId(id).result flatMap { validations =>
        submissionAttributeQuery.filter(_.ownerId inSetBind(validations.map(_.id))).delete
      }
    }

    def deleteWorkflowFailureAttributes(id: Long) = {
      findInputResolutionsByFailureId(id).result flatMap { validations =>
        submissionAttributeQuery.filter(_.ownerId inSetBind(validations.map(_.id))).delete
      }
    }

    def loadWorkflow(query: WorkflowQueryType): ReadAction[Option[Workflow]] = {
      uniqueResult[WorkflowRecord](query) flatMap {
        case None =>
          DBIO.successful(None)
        case Some(rec) =>
          for {
            entity <- loadWorkflowEntity(rec.workflowEntityId)
            inputResolutions <- loadInputResolutions(rec.id)
            messages <- loadWorkflowMessages(rec.id)
          } yield {
            Option(unmarshalWorkflow(rec, entity, inputResolutions, messages))
          }
      }
    }

    def listWorkflowRecsForSubmissionAndStatuses(submissionId: UUID, statuses: WorkflowStatuses.WorkflowStatus*): ReadAction[Seq[WorkflowRecord]] = {
      findWorkflowsBySubmissionId(submissionId).filter(_.status inSet(statuses.map(_.toString))).result
    }

    def countWorkflowsByQueueStatus: ReadAction[Map[String, Int]] = {
      val groupedSeq = findQueuedAndRunningWorkflows.groupBy(_.status).map { case (status, recs) => (status, recs.length) }.result
      groupedSeq.map(_.toMap)
    }

    def listWorkflowRecsForSubmission(submissionId: UUID): ReadAction[Seq[WorkflowRecord]] = {
      findWorkflowsBySubmissionId(submissionId).result
    }

    private def loadWorkflowEntity(entityId: Option[Long]): ReadAction[Option[AttributeEntityReference]] = {
      entityId match {
        case None => DBIO.successful(None)
        case Some(id) => uniqueResult[EntityRecord](entityQuery.findEntityById(id)).flatMap { rec =>
          DBIO.successful(unmarshalEntity(rec))
        }
      }
    }

    def loadWorkflowMessages(workflowId: Long): ReadAction[Seq[AttributeString]] = {
      findWorkflowMessagesById(workflowId).result.map(unmarshalWorkflowMessages)
    }

    def loadInputResolutions(workflowId: Long): ReadAction[Seq[SubmissionValidationValue]] = {
      val inputResolutionsAttributeJoin = findInputResolutionsByWorkflowId(workflowId) join submissionAttributeQuery on (_.id === _.ownerId)
      unmarshalInputResolutions(inputResolutionsAttributeJoin)
    }

    def loadFailedInputResolutions(failureId: Long): ReadAction[Seq[SubmissionValidationValue]] = {
      val failedInputResolutionsAttributeJoin = findInputResolutionsByFailureId(failureId) joinLeft submissionAttributeQuery on (_.id === _.ownerId)
      failedInputResolutionsAttributeJoin.result.map { resolutions => resolutions map {
        case (validation, attr) => unmarshalInputResolution(validation, attr)
      }}
    }

    def loadWorkflowFailureMessages(workflowFailureId: Long): ReadAction[Seq[AttributeString]] = {
      findWorkflowErrorsByWorkflowFailureId(workflowFailureId).result.map(unmarshalWorkflowErrors)
    }

    def loadWorkflowFailures(submissionId: UUID): ReadAction[Seq[WorkflowFailure]] = {
      findInactiveWorkflows(submissionId).result.flatMap(recs => DBIO.sequence(recs.map { rec =>
        loadWorkflowFailure(rec)
      }))
    }

    def loadWorkflowFailure(workflowFailure: WorkflowFailureRecord): ReadAction[WorkflowFailure] = {
      for {
        entity <- submissionQuery.loadSubmissionEntity(workflowFailure.entityId)
        inputResolutions <- loadFailedInputResolutions(workflowFailure.id)
        failureMessages <- loadWorkflowFailureMessages(workflowFailure.id)
      } yield(unmarshalInactiveWorkflow(entity.get, inputResolutions, failureMessages))
    }

    /*
      the find methods
     */

    def findWorkflowById(id: Long): WorkflowQueryType = {
      filter(_.id === id)
    }

    def findWorkflowByIds(ids: Traversable[Long]): WorkflowQueryType = {
      filter(_.id inSetBind(ids))
    }

    def findWorkflowByExternalIdAndSubmissionId(externalId: String, submissionId: UUID): WorkflowQueryType = {
      filter(wf => wf.externalid === externalId && wf.submissionId === submissionId)
    }

    def findWorkflowByEntityId(submissionId: UUID, entityId: Long): WorkflowQueryType = {
      filter(rec => rec.submissionId === submissionId && rec.workflowEntityId === entityId)
    }

    def findWorkflowByEntity(submissionId: UUID, workspaceId: UUID, entityType: String, entityName: String): WorkflowQueryType = {
      for {
        entity <- entityQuery.findEntityByName(workspaceId, entityType, entityName)
        workflow <- workflowQuery if (workflow.submissionId === submissionId && workflow.workflowEntityId === entity.id)
      } yield workflow
    }

    def findWorkflowMessagesById(workflowId: Long) = {
      (workflowMessageQuery filter (_.workflowId === workflowId))
    }

    def findInputResolutionsByWorkflowId(workflowId: Long) = {
      (submissionValidationQuery filter (_.workflowId === Option(workflowId)))
    }

    def findInputResolutionsByFailureId(failureId: Long) = {
      (submissionValidationQuery filter (_.workflowFailureId === Option(failureId)))
    }

    def findWorkflowsBySubmissionId(submissionId: UUID): WorkflowQueryType = {
      filter(rec => rec.submissionId === submissionId)
    }

    def findWorkflowErrorsByWorkflowFailureId(workflowFailureId: Long) = {
      (workflowErrorQuery filter(_.workflowFailureId === workflowFailureId))
    }

    def findInactiveWorkflows(submissionId: UUID) = {
      (workflowFailureQuery filter (_.submissionId === submissionId))
    }

    def findQueuedAndRunningWorkflows: WorkflowQueryType = {
      filter(rec => rec.status inSetBind((WorkflowStatuses.queuedStatuses ++ WorkflowStatuses.runningStatuses) map { _.toString }))
    }

    /*
      the marshal and unmarshal methods
     */

    def marshalWorkflow(submissionId: UUID, workflow: Workflow, entityId: Option[Long]): WorkflowRecord = {
      WorkflowRecord(
        0,
        workflow.workflowId,
        submissionId,
        workflow.status.toString,
        new Timestamp(workflow.statusLastChangedDate.toDate.getTime),
        entityId
      )
    }

    private def unmarshalWorkflow(workflowRec: WorkflowRecord, entity: Option[AttributeEntityReference], inputResolutions: Seq[SubmissionValidationValue], messages: Seq[AttributeString]): Workflow = {
      Workflow(
        workflowRec.externalId,
        WorkflowStatuses.withName(workflowRec.status),
        new DateTime(workflowRec.statusLastChangedDate.getTime),
        entity,
        inputResolutions,
        messages
      )
    }

    private def marshalInputResolution(value: SubmissionValidationValue, parentId: Either[WorkflowId, WorkflowFailureId]): SubmissionValidationRecord = {
      SubmissionValidationRecord(
        0,
        parentId.left.toOption.map{_.id},
        parentId.right.toOption.map{_.id},
        value.error,
        value.inputName
      )
    }

    private def unmarshalInputResolution(validationRec: SubmissionValidationRecord, value: Option[SubmissionAttributeRecord]): SubmissionValidationValue = {
      value match {
        case Some(attr) => SubmissionValidationValue(Some(submissionAttributeQuery.unmarshalAttributes(Seq(((attr.id, attr), None)))(attr.id).values.head), validationRec.errorText, validationRec.inputName)
        case None => SubmissionValidationValue(None, validationRec.errorText, validationRec.inputName)
      }
    }

    private def unmarshalInputResolutions(resolutions: WorkflowQueryWithInputResolutions) = {
      resolutions.result map { inputResolutionRecord =>
        inputResolutionRecord map { case (inputResolution, attribute) =>
          unmarshalInputResolution(inputResolution, Option(attribute))
        }
      }
    }

    private def unmarshalWorkflowMessages(workflowMsgRecs: Seq[WorkflowMessageRecord]): Seq[AttributeString] = {
      workflowMsgRecs.map(rec => AttributeString(rec.message))
    }

    private def unmarshalWorkflowErrors(workflowErrorRecs: Seq[WorkflowErrorRecord]): Seq[AttributeString] = {
      workflowErrorRecs.map(rec => AttributeString(rec.errorText))
    }

    private def unmarshalEntity(entityRec: Option[EntityRecord]): Option[AttributeEntityReference] = {
      entityRec.map(entity =>
        AttributeEntityReference(entity.entityType, entity.name)
      )
    }

    private def unmarshalInactiveWorkflow(entity: AttributeEntityReference, inputResolutions: Seq[SubmissionValidationValue], errors: Seq[AttributeString]): WorkflowFailure = {
      WorkflowFailure(entity.entityName, entity.entityType, inputResolutions, errors)
    }

    private def deleteMessagesAndInputs(id: Long): DBIOAction[Int, NoStream, Write with Write] = {
//    attributeQuery.filter(_.id in (findWorkflowMessagesById(id))).delete andThen
        findWorkflowMessagesById(id).delete andThen
          findInputResolutionsByWorkflowId(id).delete
    }

    def deleteFailureResolutions(workflowFailureId: Long): DBIOAction[Int, NoStream, Read with Write] = {
      deleteWorkflowFailureAttributes(workflowFailureId) andThen
        findInputResolutionsByFailureId(workflowFailureId).delete
    }
  }

  object workflowFailureQuery extends TableQuery(new WorkflowFailureTable(_)) {
    type WorkflowFailureQueryType = Query[WorkflowFailureTable, WorkflowFailureRecord, Seq]

    def save(workspaceContext: SlickWorkspaceContext, submissionId: UUID, wff: WorkflowFailure): ReadWriteAction[WorkflowFailure] = {

      for {
        wfEntityId <- uniqueResult[EntityRecord](entityQuery.findEntityByName(workspaceContext.workspaceId, wff.entityType, wff.entityName))
        failureId <- (workflowFailureQuery returning workflowFailureQuery.map(_.id)) += submissionQuery.marshalWorkflowFailure(wfEntityId.map{_.id}, submissionId)
        inputResolutions <- workflowQuery.saveInputResolutions(workspaceContext, wff.inputResolutions, Right(WorkflowFailureId(failureId)))
        messageInserts <- DBIO.sequence(wff.errors.map(message => workflowErrorQuery += WorkflowErrorRecord(failureId, message.value)))
      } yield failureId

    } map { _ => wff }


  }

  object workflowAuditStatusQuery extends TableQuery(new WorkflowAuditStatusTable(_)) {

    def queueTimeMostRecentSubmittedWorkflow: ReadAction[Long] = {
      // for the most-recently submitted workflow, find the time it was submitted and the earliest time we have recorded.
      // we could query specifically for the time this workflow was Queued, but we'll just use the earliest time
      // for resiliency, in case the workflow somehow skipped Queued status.
      val timingQuery = for {
        lastSubmitted <- workflowAuditStatusQuery.filter(_.status === WorkflowStatuses.Submitted.toString).sortBy(_.timestamp.desc).take(1)
        earliestSeen <- workflowAuditStatusQuery.sortBy(_.timestamp.asc).take(1) if earliestSeen.workflowId === lastSubmitted.workflowId
      } yield (lastSubmitted.timestamp, earliestSeen.timestamp)

      timingQuery.result.headOption map {
        case Some((s:Timestamp, e:Timestamp)) => s.getTime - e.getTime
        case _ => 0L
      }
    }

    // this method used only inside unit tests. At runtime, the table is populated only via triggers.
    def save(wasr: WorkflowAuditStatusRecord): WriteAction[WorkflowAuditStatusRecord] = {
      (workflowAuditStatusQuery returning workflowAuditStatusQuery.map(_.id)) += wasr
    } map { newid => wasr.copy(id = newid)}
  }


}
