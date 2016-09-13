package org.broadinstitute.dsde.rawls.model

import org.broadinstitute.dsde.rawls.model.ExecutionJsonSupport.OutputType
import org.broadinstitute.dsde.rawls.model.SubmissionStatuses.SubmissionStatus
import org.broadinstitute.dsde.rawls.model.WorkflowStatuses.WorkflowStatus
import UserAuthJsonSupport._

import spray.json._
import org.joda.time.DateTime
import org.broadinstitute.dsde.rawls.RawlsException

import scala.util.{Failure, Success, Try}

/**
 * @author tsharpe
 */
// this is what a client sends to have us create a submission

// Request for a submission
case class SubmissionRequest(
  methodConfigurationNamespace: String,
  methodConfigurationName: String,
  entityType: String,
  entityName: String,
  expression: Option[String]
)

// Cromwell's response to workflow submission
case class ExecutionServiceStatus(
  id: String,
  status: String
)

case class ExecutionServiceFailure(status: String, message: String, errors: Option[JsValue])

// Cromwell's response to workflow validation
case class ExecutionServiceValidation(
  valid: Boolean,
  error: String
)

case class UnsupportedOutputType(json: JsValue)

case class ExecutionServiceOutputs(
  id: String,
  outputs: Map[String, OutputType]
)

case class ExecutionServiceLogs(
 id: String,
 logs: Map[String, Seq[ExecutionServiceCallLogs]]
)

// cromwell.engine.backend.CallLogs
case class ExecutionServiceCallLogs(
  stdout: String,
  stderr: String,
  backendLogs: Option[Map[String, String]] = None
)

case class ExecutionServiceWorkflowOptions(
  jes_gcs_root: String,
  google_project: String,
  account_name: String,
  refresh_token: String,
  auth_bucket: String,
  workflow_log_dir: String,
  defaultRuntimeOptions: Option[JsValue]
)

// Status of a successfully started workflow
case class Workflow(
  workflowId: Option[String],
  status: WorkflowStatus,
  statusLastChangedDate: DateTime,
  workflowEntity: AttributeEntityReference,
  inputResolutions: Seq[SubmissionValidationValue],
  messages: Seq[AttributeString] = Seq.empty
)

case class TaskOutput(
  logs: Option[Seq[ExecutionServiceCallLogs]],
  outputs: Option[Map[String, OutputType]]
)

case class WorkflowOutputs(
  workflowId: String,
  tasks: Map[String, TaskOutput]
)

// Status of a submission
case class Submission(
  submissionId: String,
  submissionDate: DateTime,
  submitter: RawlsUserRef,
  methodConfigurationNamespace: String,
  methodConfigurationName: String,
  submissionEntity: AttributeEntityReference,
  workflows: Seq[Workflow],
  status: SubmissionStatus
)

case class SubmissionStatusResponse(
  submissionId: String,
  submissionDate: DateTime,
  submitter: String,
  methodConfigurationNamespace: String,
  methodConfigurationName: String,
  submissionEntity: AttributeEntityReference,
  workflows: Seq[Workflow],
  status: SubmissionStatus
) {
  def this(submission: Submission, rawlsUser: RawlsUser) = this(submission.submissionId, submission.submissionDate, rawlsUser.userEmail.value, submission.methodConfigurationNamespace, submission.methodConfigurationName, submission.submissionEntity, submission.workflows, submission.status)
}

case class SubmissionListResponse(
  submissionId: String,
  submissionDate: DateTime,
  submitter: String,
  methodConfigurationNamespace: String,
  methodConfigurationName: String,
  submissionEntity: AttributeEntityReference,
  status: SubmissionStatus
) {
  def this(submission: Submission, rawlsUser: RawlsUser) = this(submission.submissionId, submission.submissionDate, rawlsUser.userEmail.value, submission.methodConfigurationNamespace, submission.methodConfigurationName, submission.submissionEntity, submission.status)
}

// method configuration input parameter, it's name and the associated expression from the method config
case class SubmissionValidationInput(
  wdlName: String,
  expression: String
)

// common values for all the entities -- the entity type and the input descriptions
case class SubmissionValidationHeader(
  entityType: String,
  inputExpressions: Seq[SubmissionValidationInput] // size of Seq is nInputs
)

// result of an expression parse
case class SubmissionValidationValue(
  value: Option[Attribute],
  error: Option[String],
  inputName: String
)

// the results of parsing each of the inputs for one entity
case class SubmissionValidationEntityInputs(
  entityName: String,
  inputResolutions: Seq[SubmissionValidationValue] // size of Seq is nInputs
)

// the results of parsing each input for each entity
case class SubmissionValidationReport(
  request: SubmissionRequest,
  header: SubmissionValidationHeader,
  validEntities: Seq[SubmissionValidationEntityInputs], // entities for which parsing all inputs succeeded
  invalidEntities: Seq[SubmissionValidationEntityInputs] // entities for which parsing at least 1 of the inputs failed
)

// the results of creating a submission
case class SubmissionReport(
  request: SubmissionRequest,
  submissionId: String,
  submissionDate: DateTime,
  submitter: String,
  status: SubmissionStatus,
  header: SubmissionValidationHeader,
  workflows: Seq[SubmissionValidationEntityInputs]
)

case class CallMetadata(
  inputs: Map[String, Attribute],
  executionStatus: String,
  backend: Option[String],
  backendStatus: Option[String],
  backendLogs: Option[Map[String, Attribute]],
  outputs: Option[Map[String, OutputType]],
  start: Option[DateTime],
  end: Option[DateTime],
  jobId: Option[String],
  returnCode: Option[Int],
  shardIndex: Int,
  stdout: Option[String],
  stderr: Option[String]
)

case class ExecutionMetadata
(
  id: String,
  status: String,
  submission: DateTime,
  start: Option[DateTime],
  end: Option[DateTime],
  inputs: Map[String, Attribute],
  outputs: Option[Map[String, OutputType]],
  calls: Map[String, Seq[CallMetadata]]
)

case class ActiveSubmission
(
  workspaceNamespace: String,
  workspaceName: String,
  submission: Submission
)

case class WorkflowQueueStatusResponse
(
  estimatedQueueTimeMS: Long, // milliseconds to drain queue
  workflowsBeforeNextUserWorkflow: Int,
  workflowCountsByStatus: Map[String, Int]
)

object ExecutionJsonSupport extends JsonSupport {
  type OutputType = Either[Attribute, UnsupportedOutputType]

  implicit object WorkflowStatusFormat extends RootJsonFormat[WorkflowStatus] {
    override def write(obj: WorkflowStatus): JsValue = JsString(obj.toString)
    override def read(json: JsValue): WorkflowStatus = json match {
      case JsString(name) => WorkflowStatuses.withName(name)
      case x => throw new DeserializationException("invalid value: " + x)
    }
  }

  implicit object SubmissionStatusFormat extends RootJsonFormat[SubmissionStatus] {
    override def write(obj: SubmissionStatus): JsValue = JsString(obj.toString)
    override def read(json: JsValue): SubmissionStatus = json match {
      case JsString(name) => SubmissionStatuses.withName(name)
      case x => throw new DeserializationException("invalid value: " + x)
    }
  }

  implicit object ExecutionOutputFormat extends RootJsonFormat[OutputType] {
    override def write(obj: OutputType): JsValue = obj match {
      case Left(attribute) => AttributeFormat.write(attribute)
      case Right(UnsupportedOutputType(json)) => json
    }

    override def read(json: JsValue): OutputType = {
      Try { AttributeFormat.read(json) } match {
        case Success(attribute) => Left(attribute)
        case Failure(e: DeserializationException) => Right(UnsupportedOutputType(json))
        case Failure(t) => throw t
      }
    }
  }

  implicit val SubmissionRequestFormat = jsonFormat5(SubmissionRequest)

  implicit val ExecutionServiceStatusFormat = jsonFormat2(ExecutionServiceStatus)

  implicit val ExecutionServiceFailureFormat = jsonFormat3(ExecutionServiceFailure)

  implicit val ExecutionServiceValidationFormat = jsonFormat2(ExecutionServiceValidation)

  implicit val ExecutionServiceOutputsFormat = jsonFormat2(ExecutionServiceOutputs)

  implicit val ExecutionServiceCallLogsFormat = jsonFormat3(ExecutionServiceCallLogs)

  implicit val ExecutionServiceLogsFormat = jsonFormat2(ExecutionServiceLogs)

  implicit val ExecutionServiceWorkflowOptionsFormat = jsonFormat7(ExecutionServiceWorkflowOptions)

  implicit val TaskOutputFormat = jsonFormat2(TaskOutput)

  implicit val WorkflowOutputsFormat = jsonFormat2(WorkflowOutputs)

  implicit val SubmissionValidationInputFormat = jsonFormat2(SubmissionValidationInput)

  implicit val SubmissionValidationHeaderFormat = jsonFormat2(SubmissionValidationHeader)

  implicit val SubmissionValidationValueFormat = jsonFormat3(SubmissionValidationValue)

  implicit val SubmissionValidationEntityInputsFormat = jsonFormat2(SubmissionValidationEntityInputs)

  implicit val SubmissionValidationReportFormat = jsonFormat4(SubmissionValidationReport)

  implicit val WorkflowFormat = jsonFormat6(Workflow)

  implicit val SubmissionFormat = jsonFormat8(Submission)

  implicit val SubmissionReportFormat = jsonFormat7(SubmissionReport)

  implicit val SubmissionStatusResponseFormat = jsonFormat8(SubmissionStatusResponse)

  implicit val SubmissionListResponseFormat = jsonFormat7(SubmissionListResponse)

  implicit val CallMetadataFormat = jsonFormat13(CallMetadata)

  implicit val ExecutionMetadataFormat = jsonFormat8(ExecutionMetadata)

  implicit val ActiveSubmissionFormat = jsonFormat3(ActiveSubmission)

  implicit val WorkflowQueueStatusResponseFormat = jsonFormat3(WorkflowQueueStatusResponse)
}

trait RawlsEnumeration[T <: RawlsEnumeration[T]] { self: T =>
  def toString: String
  def withName(name:String): T
}

object WorkflowStatuses {
  val queuedStatuses: Seq[WorkflowStatus] = Seq(Queued, Launching)
  val runningStatuses: Seq[WorkflowStatus] = Seq(Submitted, Running, Aborting)
  val terminalStatuses: Seq[WorkflowStatus] = Seq(Failed, Succeeded, Aborted, Unknown)

  sealed trait WorkflowStatus extends RawlsEnumeration[WorkflowStatus] {
    def isDone = {
      terminalStatuses.contains(this)
    }
    override def toString = getClass.getSimpleName.stripSuffix("$")
    override def withName(name: String) = WorkflowStatuses.withName(name)
  }

  def withName(name: String): WorkflowStatus = {
    name match {
      case "Queued" => Queued
      case "Launching" => Launching
      case "Submitted" => Submitted
      case "Running" => Running
      case "Failed" => Failed
      case "Succeeded" => Succeeded
      case "Aborting" => Aborting
      case "Aborted" => Aborted
      case "Unknown" => Unknown
      case _ => throw new RawlsException(s"invalid WorkflowStatus [${name}]")
    }
  }

  case object Queued extends WorkflowStatus
  case object Launching extends WorkflowStatus
  case object Submitted extends WorkflowStatus
  case object Running extends WorkflowStatus
  case object Failed extends WorkflowStatus
  case object Succeeded extends WorkflowStatus
  case object Aborting extends WorkflowStatus
  case object Aborted extends WorkflowStatus
  case object Unknown extends WorkflowStatus
}


object SubmissionStatuses {
  val activeStatuses: Seq[SubmissionStatus] = Seq(Accepted, Evaluating, Submitting, Submitted, Aborting)

  sealed trait SubmissionStatus extends RawlsEnumeration[SubmissionStatus] {
    def isDone = { this == Done }
    override def toString = getClass.getSimpleName.stripSuffix("$")
    override def withName(name: String) = SubmissionStatuses.withName(name)
  }

  def withName(name: String): SubmissionStatus = {
    name match {
      case "Accepted" => Accepted
      case "Evaluating" => Evaluating
      case "Submitting" => Submitting
      case "Submitted" => Submitted
      case "Aborting" => Aborting
      case "Aborted" => Aborted
      case "Done" => Done
      case _ => throw new RawlsException(s"invalid SubmissionStatus [${name}]")
    }
  }

  case object Accepted extends SubmissionStatus
  case object Evaluating extends SubmissionStatus
  case object Submitting extends SubmissionStatus
  case object Submitted extends SubmissionStatus
  case object Aborting extends SubmissionStatus
  case object Aborted extends SubmissionStatus
  case object Done extends SubmissionStatus
}
