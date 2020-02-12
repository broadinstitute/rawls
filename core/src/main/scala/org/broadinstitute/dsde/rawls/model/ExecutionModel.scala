package org.broadinstitute.dsde.rawls.model

import java.util.UUID

import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.model.ExecutionJsonSupport.{OutputType, StatusCounts, StatusCountsByUser}
import org.broadinstitute.dsde.rawls.model.SubmissionStatuses.SubmissionStatus
import org.broadinstitute.dsde.rawls.model.WorkflowFailureModes.WorkflowFailureMode
import org.broadinstitute.dsde.rawls.model.WorkflowStatuses.WorkflowStatus
import org.broadinstitute.dsde.workbench.model.{ValueObject, ValueObjectFormat, WorkbenchEmail}
import org.broadinstitute.dsde.workbench.model.WorkbenchIdentityJsonSupport._
import org.joda.time.DateTime
import spray.json._

import scala.util.{Failure, Success, Try}

/**
 * @author tsharpe
 */
// this is what a client sends to have us create a submission

// Request for a submission
case class SubmissionRequest(
  methodConfigurationNamespace: String,
  methodConfigurationName: String,
  entityType: Option[String],
  entityName: Option[String],
  expression: Option[String],
  useCallCache: Boolean,
  deleteIntermediateOutputFiles: Boolean,
  workflowFailureMode: Option[String] = None
)

// Cromwell's response to workflow submission
case class ExecutionServiceStatus(
  id: String,
  status: String
)

case class ExecutionServiceFailure(status: String, message: String, errors: Option[JsArray])

case class ExecutionServiceVersion(cromwell: String)

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
 calls: Option[Map[String, Seq[ExecutionServiceCallLogs]]]
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
  google_compute_service_account: String,
  user_service_account_json: String,
  auth_bucket: String,
  final_workflow_log_dir: String,
  default_runtime_attributes: Option[JsValue],
  read_from_cache: Boolean,
  delete_intermediate_output_files: Boolean,
  backend: CromwellBackend,
  workflow_failure_mode: Option[WorkflowFailureMode] = None,
  google_labels: Map[String, String] = Map.empty
)

// current possible backends are "JES" and "PAPIv2" but this is subject to change in the future
final case class CromwellBackend(value: String) extends ValueObject

case class ExecutionServiceLabelResponse(
  id: String,
  labels: Map[String, String]
)

// Status of a successfully started workflow
case class Workflow(
  workflowId: Option[String],
  status: WorkflowStatus,
  statusLastChangedDate: DateTime,
  workflowEntity: Option[AttributeEntityReference],
  inputResolutions: Seq[SubmissionValidationValue],
  messages: Seq[AttributeString] = Seq.empty,
  cost: Option[Float] = None
)

case class TaskOutput(
  logs: Option[Seq[ExecutionServiceCallLogs]],
  outputs: Option[Map[String, OutputType]]
)

case class WorkflowOutputs(
  workflowId: String,
  tasks: Map[String, TaskOutput]
)

case class WorkflowCost(
  workflowId: String,
  cost: Option[Float]
)

// Status of a submission
case class Submission(
  submissionId: String,
  submissionDate: DateTime,
  submitter: WorkbenchEmail,
  methodConfigurationNamespace: String,
  methodConfigurationName: String,
  submissionEntity: Option[AttributeEntityReference],
  workflows: Seq[Workflow],
  status: SubmissionStatus,
  useCallCache: Boolean,
  deleteIntermediateOutputFiles: Boolean,
  workflowFailureMode: Option[WorkflowFailureMode] = None,
  cost: Option[Float] = None
)

case class SubmissionStatusResponse(
  submissionId: String,
  submissionDate: DateTime,
  submitter: String,
  methodConfigurationNamespace: String,
  methodConfigurationName: String,
  submissionEntity: Option[AttributeEntityReference],
  workflows: Seq[Workflow],
  status: SubmissionStatus,
  useCallCache: Boolean,
  deleteIntermediateOutputFiles: Boolean,
  workflowFailureMode: Option[WorkflowFailureMode] = None,
  cost: Option[Float] = None
)
object SubmissionStatusResponse {
  def apply(submission: Submission): SubmissionStatusResponse =
    SubmissionStatusResponse(
      submissionId = submission.submissionId,
      submissionDate = submission.submissionDate,
      submitter = submission.submitter.value,
      methodConfigurationNamespace = submission.methodConfigurationNamespace,
      methodConfigurationName = submission.methodConfigurationName,
      submissionEntity = submission.submissionEntity,
      workflows = submission.workflows,
      status = submission.status,
      useCallCache = submission.useCallCache,
      deleteIntermediateOutputFiles = submission.deleteIntermediateOutputFiles,
      workflowFailureMode = submission.workflowFailureMode,
      cost = submission.cost
    )
}

case class SubmissionListResponse(
  submissionId: String,
  submissionDate: DateTime,
  submitter: String,
  methodConfigurationNamespace: String,
  methodConfigurationName: String,
  submissionEntity: Option[AttributeEntityReference],
  status: SubmissionStatus,
  workflowStatuses: StatusCounts,
  useCallCache: Boolean,
  deleteIntermediateOutputFiles: Boolean,
  workflowFailureMode: Option[WorkflowFailureMode] = None,
  workflowIds: Option[Seq[String]],
  cost: Option[Float] = None
)
object SubmissionListResponse {
  def apply(submission: Submission, workflowIds: Option[Seq[String]], workflowStatuses: StatusCounts): SubmissionListResponse =
    SubmissionListResponse(
      submissionId = submission.submissionId,
      submissionDate = submission.submissionDate,
      submitter = submission.submitter.value,
      methodConfigurationNamespace = submission.methodConfigurationNamespace,
      methodConfigurationName = submission.methodConfigurationName,
      submissionEntity = submission.submissionEntity,
      status = submission.status,
      workflowStatuses = workflowStatuses,
      useCallCache = submission.useCallCache,
      deleteIntermediateOutputFiles = submission.deleteIntermediateOutputFiles,
      workflowFailureMode = submission.workflowFailureMode,
      workflowIds = workflowIds
    )
}

// method configuration input parameter, it's name and the associated expression from the method config
case class SubmissionValidationInput(
  wdlName: String,
  expression: String
)

// common values for all the entities -- the entity type and the input descriptions
case class SubmissionValidationHeader(
  entityType: Option[String],
  inputExpressions: Set[SubmissionValidationInput] // size of Set is nInputs
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
  inputResolutions: Set[SubmissionValidationValue] // size of Seq is nInputs
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

case class ExecutionEvent(
  description: String,
  startTime: DateTime,
  endTime: DateTime
)

final case class MetadataParams(
  includeKeys: Set[String] = Set.empty[String],
  excludeKeys: Set[String] = Set.empty[String],
  expandSubWorkflows: Boolean = false
)

case class CallMetadata(
  inputs: JsObject,
  executionStatus: String,
  executionEvents: Option[Seq[ExecutionEvent]],
  backend: Option[String],
  backendStatus: Option[String],
  backendLogs: Option[JsObject],
  outputs: Option[JsObject],
  start: Option[DateTime],
  end: Option[DateTime],
  jobId: Option[String],
  returnCode: Option[Int],
  shardIndex: Int,
  stdout: Option[String],
  stderr: Option[String]
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
  workflowCountsByStatus: StatusCounts
)

case class WorkflowQueueStatusByUserResponse
(
  statuses: StatusCounts,
  users: StatusCountsByUser,
  maxActiveWorkflowsTotal: Int,
  maxActiveWorkflowsPerUser: Int
)

case class SubmissionWorkflowStatusResponse(
                                             submissionId: UUID,
                                             workflowId: Option[String],
                                             workflowStatus: String,
                                             count: Int)

//noinspection TypeAnnotation,ScalaUnusedSymbol
class ExecutionJsonSupport extends JsonSupport {
  import spray.json.DefaultJsonProtocol._

  type OutputType = Either[Attribute, UnsupportedOutputType]
  // status string -> count
  type StatusCounts = Map[String, Int]
  // user email -> status counts
  type StatusCountsByUser = Map[String, StatusCounts]

  implicit override val attributeFormat = new AttributeFormat with PlainArrayAttributeListSerializer

  implicit val WorkflowStatusFormat = rawlsEnumerationFormat(WorkflowStatuses.withName)

  implicit val SubmissionStatusFormat = rawlsEnumerationFormat(SubmissionStatuses.withName)

  implicit val WorkflowFailureModeFormat = rawlsEnumerationFormat(WorkflowFailureModes.withName)

  implicit val CromwellBackendFormat = ValueObjectFormat(CromwellBackend)

  implicit object ExecutionOutputFormat extends RootJsonFormat[OutputType] {
    override def write(obj: OutputType): JsValue = obj match {
      case Left(attribute) => attributeFormat.write(attribute)
      case Right(UnsupportedOutputType(json)) => json
    }

    override def read(json: JsValue): OutputType = {
      Try { attributeFormat.read(json) } match {
        case Success(attribute) => Left(attribute)
        case Failure(e: DeserializationException) => Right(UnsupportedOutputType(json))
        case Failure(t) => throw t
      }
    }
  }

  implicit val SubmissionRequestFormat = jsonFormat8(SubmissionRequest)

  implicit val ExecutionEventFormat = jsonFormat3(ExecutionEvent)

  implicit val ExecutionServiceStatusFormat = jsonFormat2(ExecutionServiceStatus)

  implicit val ExecutionServiceFailureFormat = jsonFormat3(ExecutionServiceFailure)

  implicit val ExecutionServiceVersionFormat = jsonFormat1(ExecutionServiceVersion)

  implicit val ExecutionServiceValidationFormat = jsonFormat2(ExecutionServiceValidation)

  implicit val ExecutionServiceOutputsFormat = jsonFormat2(ExecutionServiceOutputs)

  implicit val ExecutionServiceCallLogsFormat = jsonFormat3(ExecutionServiceCallLogs)

  implicit val ExecutionServiceLogsFormat = jsonFormat2(ExecutionServiceLogs)

  implicit val ExecutionServiceWorkflowOptionsFormat = jsonFormat13(ExecutionServiceWorkflowOptions)

  implicit val ExecutionServiceLabelResponseFormat = jsonFormat2(ExecutionServiceLabelResponse)

  implicit val TaskOutputFormat = jsonFormat2(TaskOutput)

  implicit val WorkflowOutputsFormat = jsonFormat2(WorkflowOutputs)

  implicit val WorkflowCostFormat = jsonFormat2(WorkflowCost)

  implicit val SubmissionValidationInputFormat = jsonFormat2(SubmissionValidationInput)

  implicit val SubmissionValidationHeaderFormat = jsonFormat2(SubmissionValidationHeader)

  implicit val SubmissionValidationValueFormat = jsonFormat3(SubmissionValidationValue)

  implicit val SubmissionValidationEntityInputsFormat = jsonFormat2(SubmissionValidationEntityInputs)

  implicit val SubmissionValidationReportFormat = jsonFormat4(SubmissionValidationReport)

  implicit val WorkflowFormat = jsonFormat7(Workflow)

  implicit val SubmissionFormat = jsonFormat12(Submission)

  implicit val SubmissionReportFormat = jsonFormat7(SubmissionReport)

  implicit val SubmissionStatusResponseFormat = jsonFormat12(SubmissionStatusResponse.apply)

  implicit val SubmissionListResponseFormat = jsonFormat13(SubmissionListResponse.apply)

  implicit val MetadataParamsFormat = jsonFormat3(MetadataParams)

  implicit val CallMetadataFormat = jsonFormat14(CallMetadata)

  implicit val ActiveSubmissionFormat = jsonFormat3(ActiveSubmission)

  implicit val WorkflowQueueStatusResponseFormat = jsonFormat3(WorkflowQueueStatusResponse)

  implicit object WorkflowQueueStatusByUserResponseFormat extends RootJsonFormat[WorkflowQueueStatusByUserResponse] {
    def write(r: WorkflowQueueStatusByUserResponse) = JsObject(
      "statuses" -> r.statuses.toJson,
      // add 1 layer of nesting to `users` to include the `statuses` key
      "users" -> JsObject {
        r.users.map { case (u, s) =>
          u -> JsObject("statuses" -> s.toJson)
        }
      },
      "maxActiveWorkflowsTotal" -> r.maxActiveWorkflowsTotal.toJson,
      "maxActiveWorkflowsPerUser" -> r.maxActiveWorkflowsPerUser.toJson
    )

    def read(value: JsValue) = {
      value.asJsObject.getFields("statuses", "users", "maxActiveWorkflowsTotal", "maxActiveWorkflowsPerUser") match {
        case Seq(statuses @ JsObject(_), users @ JsObject(_), JsNumber(maxActiveWorkflowsTotal), JsNumber(maxActiveWorkflowsPerUser)) =>
          WorkflowQueueStatusByUserResponse(
            statuses.convertTo[StatusCounts],
            // remove 1 layer of nesting from `users` to remove the middle `statuses` map
            users.convertTo[Map[String, StatusCountsByUser]].flatMap { case (k, v) =>
              v.values.map(v2 => k -> v2)
            },
            maxActiveWorkflowsTotal.intValue,
            maxActiveWorkflowsPerUser.intValue)
      }
    }
  }
}

//noinspection TypeAnnotation,RedundantBlock
object WorkflowStatuses {
  val allStatuses: Seq[WorkflowStatus] = Seq(Queued, Launching, Submitted, Running, Aborting, Failed, Succeeded, Aborted, Unknown)
  val queuedStatuses: Seq[WorkflowStatus] = Seq(Queued, Launching)
  val runningStatuses: Seq[WorkflowStatus] = Seq(Submitted, Running, Aborting)
  val terminalStatuses: Seq[WorkflowStatus] = Seq(Failed, Succeeded, Aborted, Unknown)
  val abortableStatuses: Seq[WorkflowStatus] = Seq(Submitted, Running)

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


//noinspection TypeAnnotation,RedundantBlock
object SubmissionStatuses {
  val activeStatuses: Seq[SubmissionStatus] = Seq(Accepted, Evaluating, Submitting, Submitted, Aborting)
  val terminalStatuses: Seq[SubmissionStatus] = Seq(Aborted, Done)
  val allStatuses: Seq[SubmissionStatus] = Seq(Accepted, Evaluating, Submitting, Submitted, Aborting, Aborted, Done)

  sealed trait SubmissionStatus extends RawlsEnumeration[SubmissionStatus] {
    def isTerminated = {
      terminalStatuses.contains(this)
    }
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

//noinspection TypeAnnotation,RedundantBlock
object WorkflowFailureModes {
  val allWorkflowFailureModes = List(ContinueWhilePossible, NoNewCalls)

  sealed trait WorkflowFailureMode extends RawlsEnumeration[WorkflowFailureMode] {
    override def toString = getClass.getSimpleName.stripSuffix("$")
    override def withName(name: String) = WorkflowFailureModes.withName(name)
  }

  def withName(name: String): WorkflowFailureMode = {
    name match {
      case "ContinueWhilePossible" => ContinueWhilePossible
      case "NoNewCalls" => NoNewCalls
      case _ => throw new RawlsException(s"Invalid WorkflowFailureMode [${name}]. Possible values: ${allWorkflowFailureModes.mkString(", ")}")
    }
  }

  def withNameOpt(name: Option[String]): Option[WorkflowFailureMode] = {
    name.flatMap(n => Try(withName(n)).toOption)
  }

  case object ContinueWhilePossible extends WorkflowFailureMode
  case object NoNewCalls extends WorkflowFailureMode
}

object ExecutionJsonSupport extends ExecutionJsonSupport
