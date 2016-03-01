package org.broadinstitute.dsde.rawls.dataaccess.slick

import java.sql.Timestamp
import java.util.UUID

import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.dataaccess.SlickWorkspaceContext
import org.broadinstitute.dsde.rawls.model._
import org.joda.time.DateTime

/**
 * Created by mbemis on 2/18/16.
 */

case class WorkflowRecord(id: UUID,
                          submissionId: UUID,
                          status: String,
                          statusLastChangedDate: Timestamp,
                          workflowEntityId: Option[Long]
                         )

case class WorkflowMessageRecord(workflowId: UUID, message: String)

case class WorkflowFailureRecord(id: Long,
                                 submissionId: UUID,
                                 entityId: Option[Long]
                                )

case class WorkflowErrorRecord(workflowFailureId: Long, errorText: String)

trait WorkflowComponent {
  this: DriverComponent
    with EntityComponent
    with SubmissionComponent
    with AttributeComponent =>

  import driver.api._

  class WorkflowTable(tag: Tag) extends Table[WorkflowRecord](tag, "WORKFLOW") {
    def id = column[UUID]("ID", O.PrimaryKey)
    def submissionId = column[UUID]("SUBMISSION_ID")
    def status = column[String]("STATUS")
    def statusLastChangedDate = column[Timestamp]("STATUS_LAST_CHANGED")
    def workflowEntityId = column[Option[Long]]("ENTITY_ID")

    def * = (id, submissionId, status, statusLastChangedDate, workflowEntityId) <> (WorkflowRecord.tupled, WorkflowRecord.unapply)

    def submission = foreignKey("FK_WF_SUB", submissionId, submissionQuery)(_.id)
    def workflowEntity = foreignKey("FK_WF_ENTITY", workflowEntityId, entityQuery)(_.id.?)
  }

  class WorkflowMessageTable(tag: Tag) extends Table[WorkflowMessageRecord](tag, "WORKFLOW_MESSAGE") {
    def workflowId = column[UUID]("WORKFLOW_ID")
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

  protected val workflowMessageQuery = TableQuery[WorkflowMessageTable]
  protected val workflowFailureQuery = TableQuery[WorkflowFailureTable]
  protected val workflowErrorQuery = TableQuery[WorkflowErrorTable]

  object workflowQuery extends TableQuery(new WorkflowTable(_)) {

    type WorkflowQueryType = Query[WorkflowTable, WorkflowRecord, Seq]
    type WorkflowQueryWithInputResolutions = Query[(SubmissionValidationTable, AttributeTable), (SubmissionValidationRecord, AttributeRecord), Seq]

    def get(workspaceContext: SlickWorkspaceContext, submissionId: String, workflowId: String): ReadAction[Option[Workflow]] = {
      loadWorkflow(UUID.fromString(submissionId), UUID.fromString(workflowId))
    }

    def delete(workspaceContext: SlickWorkspaceContext, submissionId: String, workflowId: String): ReadWriteAction[Boolean] = {
      uniqueResult[WorkflowRecord](findWorkflowById(UUID.fromString(submissionId), UUID.fromString(workflowId))) flatMap {
        case None => DBIO.successful(false)
        case Some(rec) => {
          findWorkflowMessagesById(rec.id).delete andThen
            findInputResolutionsByWorkflowId(rec.id).delete andThen
            findWorkflowById(UUID.fromString(submissionId), rec.id).delete
        } map { count =>
          count > 0
        }
      }
    }

    def save(workspaceContext: SlickWorkspaceContext, submissionId: String, workflow: Workflow): ReadWriteAction[Workflow] = {

      def saveInputResolutions(values: Seq[SubmissionValidationValue], workflowId: UUID) = {
        DBIO.seq(values.map { case (v) =>
          v.value match {
            case None => (submissionValidationQuery += marshalInputResolution(v, None, Some(workflowId)))
            case Some(attr) =>
              DBIO.sequence(attributeQuery.insertAttributeRecords(v.inputName, attr, workspaceContext.workspaceId)) flatMap { ids =>
                (submissionValidationQuery += marshalInputResolution(v, Some(ids.head), Some(workflowId)))
              }
          }
        }.toSeq: _*)
      }

      submissionQuery.loadSubmissionEntityId(workspaceContext.workspaceId, workflow.workflowEntity) flatMap { eId =>
        (workflowQuery += marshalWorkflow(UUID.fromString(submissionId), workflow, eId)) andThen
          saveInputResolutions(workflow.inputResolutions, UUID.fromString(workflow.workflowId))
      }
    } map { _ => workflow }

    def update(workspaceContext: SlickWorkspaceContext, submissionId: String, workflow: Workflow): ReadWriteAction[Workflow] = {
      uniqueResult[WorkflowRecord](findWorkflowById(UUID.fromString(submissionId), UUID.fromString(workflow.workflowId))) flatMap {
        case None => throw new RawlsException(s"workflow does not exist: ${workflow}")
        case Some(rec) =>
          delete(workspaceContext, submissionId, workflow.workflowId) andThen
            save(workspaceContext, submissionId, workflow)
      }
    } map { _ => workflow }

    def loadWorkflow(submissionId: UUID, workflowId: UUID): ReadAction[Option[Workflow]] = {
      uniqueResult[WorkflowRecord](findWorkflowById(submissionId, workflowId)) flatMap {
        case None => DBIO.successful(None)
        case Some(rec) =>
          for {
            entity <- loadWorkflowEntity(rec.workflowEntityId)
            inputResolutions <- loadInputResolutions(rec.id)
            messages <- loadWorkflowMessages(rec.id)
          } yield(Option(unmarshalWorkflow(rec, entity, inputResolutions, messages)))
      }
    }

    private def loadWorkflowEntity(entityId: Option[Long]): ReadAction[Option[AttributeEntityReference]] = {
      entityId match {
        case None => DBIO.successful(None)
        case Some(id) => uniqueResult[EntityRecord](entityQuery.findEntityById(id)).flatMap { rec =>
          DBIO.successful(unmarshalEntity(rec))
        }
      }
    }

    def loadWorkflowMessages(workflowId: UUID): ReadAction[Seq[AttributeString]] = {
      findWorkflowMessagesById(workflowId).result.map(unmarshalWorkflowMessages)
    }

    def loadInputResolutions(workflowId: UUID): ReadAction[Seq[SubmissionValidationValue]] = {
      val inputResolutionsAttributeJoin = findInputResolutionsByWorkflowId(workflowId) join attributeQuery on (_.valueId === _.id)
      unmarshalInputResolutions(inputResolutionsAttributeJoin)
    }

    def loadFailedInputResolutions(failureId: Long): ReadAction[Seq[SubmissionValidationValue]] = {
      val failedInputResolutionsAttributeJoin = findInputResolutionsByFailureId(failureId) join attributeQuery on (_.valueId === _.id)
      unmarshalInputResolutions(failedInputResolutionsAttributeJoin)
    }

    def loadWorkflowFailureMessages(workflowFailureId: Long): ReadAction[Seq[AttributeString]] = {
      findWorkflowErrorsByWorkflowId(workflowFailureId).result.map(unmarshalWorkflowErrors)
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

    def findWorkflowById(submissionId: UUID, workflowId: UUID) = {
      filter(rec => rec.submissionId === submissionId && rec.id === workflowId)
    }

    def findWorkflowMessagesById(workflowId: UUID) = {
      (workflowMessageQuery filter (_.workflowId === workflowId))
    }

    def findInputResolutionsByWorkflowId(workflowId: UUID) = {
      (submissionValidationQuery filter (_.workflowId === Option(workflowId)))
    }

    def findInputResolutionsByFailureId(failureId: Long) = {
      (submissionValidationQuery filter (_.workflowFailureId === Option(failureId)))
    }

    def findWorkflowsBySubmissionId(submissionId: UUID): WorkflowQueryType = {
      filter(rec => rec.submissionId === submissionId)
    }

    def findWorkflowErrorsByWorkflowId(workflowId: Long) = {
      (workflowErrorQuery filter(_.workflowFailureId === workflowId))
    }

    def findInactiveWorkflows(submissionId: UUID) = {
      (workflowFailureQuery filter (_.submissionId === submissionId))
    }

    /*
      the marshal and unmarshal methods
     */

    def marshalWorkflow(submissionId: UUID, workflow: Workflow, entityId: Option[Long]): WorkflowRecord = {
      WorkflowRecord(
        UUID.fromString(workflow.workflowId),
        submissionId,
        workflow.status.toString,
        new Timestamp(workflow.statusLastChangedDate.toDate.getTime),
        entityId
      )
    }

    private def unmarshalWorkflow(workflowRec: WorkflowRecord, entity: Option[AttributeEntityReference], inputResolutions: Seq[SubmissionValidationValue], messages: Seq[AttributeString]): Workflow = {
      Workflow(
        workflowRec.id.toString,
        WorkflowStatuses.withName(workflowRec.status),
        new DateTime(workflowRec.statusLastChangedDate.getTime),
        entity,
        inputResolutions,
        messages
      )
    }

    private def marshalInputResolution(value: SubmissionValidationValue, valueId: Option[Long], workflowId: Option[UUID]): SubmissionValidationRecord = {
      SubmissionValidationRecord(
        workflowId,
        None,
        valueId,
        value.error,
        value.inputName
      )
    }

    private def unmarshalInputResolution(validationRec: SubmissionValidationRecord, value: Option[AttributeRecord]): SubmissionValidationValue = {
      value match {
        case Some(attr) => SubmissionValidationValue(Some(attributeQuery.unmarshalAttributes(Seq((attr, None))).values.head), validationRec.errorText, validationRec.inputName)
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

  }
}
