package org.broadinstitute.dsde.rawls.dataaccess.slick

import java.sql.Timestamp
import java.util.UUID
import org.joda.time.DateTime

/**
 * Created by mbemis on 2/18/16.
 */

case class SubmissionRecord(id: UUID,
                            workspaceId: UUID,
                            submissionDate: Timestamp,
                            submitterId: String,
                            methodConfigurationId: Long,
                            submissionEntityId: Option[Long],
                            status: String
                           )

case class SubmissionValidationRecord(workflowId: Option[UUID],
                                      workflowFailureId: Option[Long],
                                      valueId: Option[Long],
                                      errorText: Option[String],
                                      inputName: String
                                     )

trait SubmissionComponent {
  this: DriverComponent
    with RawlsUserComponent
    with MethodConfigurationComponent
    with EntityComponent
    with AttributeComponent
    with WorkflowComponent
    with WorkspaceComponent =>

  import driver.api._

  class SubmissionTable(tag: Tag) extends Table[SubmissionRecord](tag, "SUBMISSION") {
    def id = column[UUID]("ID", O.PrimaryKey)
    def workspaceId = column[UUID]("WORKSPACE_ID")
    def submissionDate = column[Timestamp]("DATE_SUBMITTED")
    def submitterId = column[String]("SUBMITTER")
    def methodConfigurationId = column[Long]("METHOD_CONFIG_ID")
    def submissionEntityId = column[Option[Long]]("ENTITY_ID")
    def status = column[String]("STATUS")

    def * = (id, workspaceId, submissionDate, submitterId, methodConfigurationId, submissionEntityId, status) <> (SubmissionRecord.tupled, SubmissionRecord.unapply)

    def workspace = foreignKey("FK_SUB_WORKSPACE", workspaceId, workspaceQuery)(_.id)
    def submitter = foreignKey("FK_SUB_SUBMITTER", submitterId, rawlsUserQuery)(_.userSubjectId)
    def methodConfiguration = foreignKey("FK_SUB_METHOD_CONFIG", methodConfigurationId, methodConfigurationQuery)(_.id)
    def submissionEntity = foreignKey("FK_SUB_ENTITY", submissionEntityId, entityQuery)(_.id.?)
  }

  class SubmissionValidationTable(tag: Tag) extends Table[SubmissionValidationRecord](tag, "SUBMISSION_VALIDATION") {
    /*
      TODO: add a constraint to ensure that one of workflowId / workflowFailureId are present
     */
    def workflowId = column[Option[UUID]]("WORKFLOW_ID")
    def workflowFailureId = column[Option[Long]]("WORKFLOW_FAILURE_ID")
    def valueId = column[Option[Long]]("VALUE_ID")
    def errorText = column[Option[String]]("ERROR_TEXT")
    def inputName = column[String]("INPUT_NAME")

    def * = (workflowId, workflowFailureId, valueId, errorText, inputName) <> (SubmissionValidationRecord.tupled, SubmissionValidationRecord.unapply)

    def workflow = foreignKey("FK_SUB_VALIDATION_WF", workflowId, workflowQuery)(_.id.?)
    def workflowFailure = foreignKey("FK_SUB_VALIDATION_FAIL", workflowFailureId, workflowFailureQuery)(_.id.?)
    def value = foreignKey("FK_SUB_VALIDATION_VAL", valueId, attributeQuery)(_.id.?)
  }

  protected val submissionQuery = TableQuery[SubmissionTable]
  protected val submissionValidationQuery = TableQuery[SubmissionValidationTable]

}
