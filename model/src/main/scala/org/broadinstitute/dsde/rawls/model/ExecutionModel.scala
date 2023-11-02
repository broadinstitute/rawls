package org.broadinstitute.dsde.rawls.model

import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.model.ExecutionJsonSupport.{OutputType, StatusCounts, StatusCountsByUser}
import org.broadinstitute.dsde.rawls.model.SubmissionRetryStatuses.RetryStatus
import org.broadinstitute.dsde.rawls.model.SubmissionStatuses.SubmissionStatus
import org.broadinstitute.dsde.rawls.model.WorkflowFailureModes.WorkflowFailureMode
import org.broadinstitute.dsde.rawls.model.WorkflowStatuses.{Aborted, Failed, WorkflowStatus}
import org.broadinstitute.dsde.workbench.model.WorkbenchIdentityJsonSupport._
import org.broadinstitute.dsde.workbench.model.{ValueObject, ValueObjectFormat, WorkbenchEmail}
import org.joda.time.DateTime
import spray.json._

import java.util.UUID
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
  useReferenceDisks: Boolean = false,
  memoryRetryMultiplier: Double = 1.0,
  workflowFailureMode: Option[String] = None,
  userComment: Option[String] = None,
  ignoreEmptyOutputs: Boolean = false,
  monitoringScript: Option[String] = None,
  monitoringImage: Option[String] = None,
  monitoringImageScript: Option[String] = None
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

// https://cromwell.readthedocs.io/en/stable/wf_options/Google/
case class ExecutionServiceWorkflowOptions(
  jes_gcs_root: String,
  google_project: String,
  account_name: String,
  google_compute_service_account: String,
  user_service_account_json: String,
  final_workflow_log_dir: String,
  default_runtime_attributes: Option[JsValue],
  read_from_cache: Boolean,
  delete_intermediate_output_files: Boolean,
  use_reference_disks: Boolean,
  memory_retry_multiplier: Double,
  backend: CromwellBackend,
  workflow_failure_mode: Option[WorkflowFailureMode] = None,
  google_labels: Map[String, String] = Map.empty,
  ignore_empty_outputs: Boolean = false,
  monitoring_script: Option[String] = None,
  monitoring_image: Option[String] = None,
  monitoring_image_script: Option[String] = None
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

case class ExternalEntityInfo(dataStoreId: String, rootEntityType: String)

// Status of a submission
case class Submission(
  submissionId: String,
  submissionDate: DateTime,
  submitter: WorkbenchEmail,
  methodConfigurationNamespace: String,
  methodConfigurationName: String,
  submissionEntity: Option[AttributeEntityReference],
  submissionRoot: String,
  workflows: Seq[Workflow],
  status: SubmissionStatus,
  useCallCache: Boolean,
  deleteIntermediateOutputFiles: Boolean,
  useReferenceDisks: Boolean = false,
  memoryRetryMultiplier: Double = 1.0,
  workflowFailureMode: Option[WorkflowFailureMode] = None,
  cost: Option[Float] = None,
  externalEntityInfo: Option[ExternalEntityInfo] = None,
  userComment: Option[String] = None,
  ignoreEmptyOutputs: Boolean = false,
  monitoringScript: Option[String] = None,
  monitoringImage: Option[String] = None,
  monitoringImageScript: Option[String] = None
)

case class SubmissionListResponse(
  submissionId: String,
  submissionDate: DateTime,
  submitter: String,
  methodConfigurationNamespace: String,
  methodConfigurationName: String,
  methodConfigurationDeleted: Boolean,
  submissionEntity: Option[AttributeEntityReference],
  status: SubmissionStatus,
  workflowStatuses: StatusCounts,
  useCallCache: Boolean,
  submissionRoot: String,
  deleteIntermediateOutputFiles: Boolean,
  workflowFailureMode: Option[WorkflowFailureMode] = None,
  workflowIds: Option[Seq[String]],
  cost: Option[Float] = None,
  externalEntityInfo: Option[ExternalEntityInfo] = None,
  userComment: Option[String] = None
)

object SubmissionListResponse {
  def apply(submission: Submission,
            workflowIds: Option[Seq[String]],
            workflowStatuses: StatusCounts,
            methodConfigurationDeleted: Boolean
  ): SubmissionListResponse =
    SubmissionListResponse(
      submissionId = submission.submissionId,
      submissionDate = submission.submissionDate,
      submitter = submission.submitter.value,
      methodConfigurationNamespace = submission.methodConfigurationNamespace,
      methodConfigurationName = submission.methodConfigurationName,
      methodConfigurationDeleted = methodConfigurationDeleted,
      submissionEntity = submission.submissionEntity,
      status = submission.status,
      workflowStatuses = workflowStatuses,
      useCallCache = submission.useCallCache,
      deleteIntermediateOutputFiles = submission.deleteIntermediateOutputFiles,
      submissionRoot = submission.submissionRoot,
      workflowFailureMode = submission.workflowFailureMode,
      workflowIds = workflowIds,
      externalEntityInfo = submission.externalEntityInfo,
      userComment = submission.userComment
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
  inputExpressions: Set[SubmissionValidationInput], // size of Set is nInputs
  entityStoreId: Option[String]
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

case class RetriedSubmissionReport(
  originalSubmissionId: String,
  submissionId: String,
  submissionDate: DateTime,
  submitter: String,
  status: SubmissionStatus,
  retryType: RetryStatus,
  workflows: Seq[Workflow]
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

case class ActiveSubmission(
  workspaceNamespace: String,
  workspaceName: String,
  submission: Submission
)

case class WorkflowQueueStatusResponse(
  estimatedQueueTimeMS: Long, // milliseconds to drain queue
  workflowsBeforeNextUserWorkflow: Int,
  workflowCountsByStatus: StatusCounts
)

case class WorkflowQueueStatusByUserResponse(
  statuses: StatusCounts,
  users: StatusCountsByUser,
  maxActiveWorkflowsTotal: Int,
  maxActiveWorkflowsPerUser: Int
)

case class SubmissionWorkflowStatusResponse(submissionId: UUID,
                                            workflowId: Option[String],
                                            workflowStatus: String,
                                            count: Int
)

case class UserCommentUpdateOperation(userComment: String)

//noinspection TypeAnnotation,ScalaUnusedSymbol
trait ExecutionJsonSupport extends JsonSupport {
  import spray.json.DefaultJsonProtocol._

  type OutputType = Either[Attribute, UnsupportedOutputType]
  // status string -> count
  type StatusCounts = Map[String, Int]
  // user email -> status counts
  type StatusCountsByUser = Map[String, StatusCounts]

  implicit override val attributeFormat = new AttributeFormat with PlainArrayAttributeListSerializer

  implicit val WorkflowStatusFormat = rawlsEnumerationFormat(WorkflowStatuses.withName)

  implicit val SubmissionStatusFormat = rawlsEnumerationFormat(SubmissionStatuses.withName)

  implicit val SubmissionRetryStatusesFormat = rawlsEnumerationFormat(SubmissionRetryStatuses.withName)

  implicit val WorkflowFailureModeFormat = rawlsEnumerationFormat(WorkflowFailureModes.withName)

  implicit val CromwellBackendFormat = ValueObjectFormat(CromwellBackend)

  implicit object ExecutionOutputFormat extends RootJsonFormat[OutputType] {
    override def write(obj: OutputType): JsValue = obj match {
      case Left(attribute)                    => attributeFormat.write(attribute)
      case Right(UnsupportedOutputType(json)) => json
    }

    override def read(json: JsValue): OutputType =
      Try(attributeFormat.read(json)) match {
        case Success(attribute)                   => Left(attribute)
        case Failure(e: DeserializationException) => Right(UnsupportedOutputType(json))
        case Failure(t)                           => throw t
      }
  }

  /*
  Copied from:
  - https://www.codersbistro.com/blog/default-values-with-spray-json-marshalling/
  - https://gist.github.com/agemooij/f2e4075a193d166355f8
  If you find a better way of implementing this (*cough* circe?) please add a link

  NOTE:
  - Until REST API endpoint versioning across the many repositories / consumers is figured out, DO NOT DELETE ANY FIELDS
      IN THE REQUEST, only deprecate and/or default!
  - We are operating under an assumption for clients: even if the API version doesn't change, clients will hopefully
      consume-and-discard any additional fields that appear in the returned json. Therefore response classes do not
      require this custom spray-json treatment, at the moment.
   */
  implicit object SubmissionRequestFormat extends RootJsonFormat[SubmissionRequest] {

    override def write(obj: SubmissionRequest): JsValue =
      JsObject(
        List(
          Option("methodConfigurationNamespace" -> obj.methodConfigurationNamespace.toJson),
          Option("methodConfigurationName" -> obj.methodConfigurationName.toJson),
          obj.entityType.map("entityType" -> _.toJson),
          obj.entityName.map("entityName" -> _.toJson),
          obj.expression.map("expression" -> _.toJson),
          Option("useCallCache" -> obj.useCallCache.toJson),
          Option("deleteIntermediateOutputFiles" -> obj.deleteIntermediateOutputFiles.toJson),
          Option("useReferenceDisks" -> obj.useReferenceDisks.toJson),
          Option("memoryRetryMultiplier" -> obj.memoryRetryMultiplier.toJson),
          obj.workflowFailureMode.map("workflowFailureMode" -> _.toJson),
          Option("userComment" -> obj.userComment.toJson),
          Option("ignoreEmptyOutputs" -> obj.ignoreEmptyOutputs.toJson),
          Option("monitoringScript" -> obj.monitoringScript.toJson),
          Option("monitoringImage" -> obj.monitoringImage.toJson),
          Option("monitoringImageScript" -> obj.monitoringImageScript.toJson)
        ).flatten: _*
      )

    override def read(json: JsValue): SubmissionRequest = {
      val fields = json.asJsObject.fields

      SubmissionRequest(
        // All new fields below this line MUST have defaults or be wrapped in Option[]!
        // https://broadworkbench.atlassian.net/browse/QA-1031
        // NOTE: All fields are optional in firecloud-orchestration's copy of this class,
        // so in that project one can just adjust the function call to jsonFormat*N*.
        methodConfigurationNamespace = fields("methodConfigurationNamespace").convertTo[String],
        methodConfigurationName = fields("methodConfigurationName").convertTo[String],
        // We are flatMapping these because `null` is a valid input provided by the UI
        entityType = fields.get("entityType").flatMap(_.convertTo[Option[String]]),
        entityName = fields.get("entityName").flatMap(_.convertTo[Option[String]]),
        expression = fields.get("expression").flatMap(_.convertTo[Option[String]]),
        useCallCache = fields("useCallCache").convertTo[Boolean],
        deleteIntermediateOutputFiles = fields.get("deleteIntermediateOutputFiles").fold(false)(_.convertTo[Boolean]),
        useReferenceDisks = fields.get("useReferenceDisks").fold(false)(_.convertTo[Boolean]),
        memoryRetryMultiplier = fields.get("memoryRetryMultiplier").fold(1.0)(_.convertTo[Double]),
        workflowFailureMode = fields.get("workflowFailureMode").flatMap(_.convertTo[Option[String]]),
        userComment = fields.get("userComment").flatMap(_.convertTo[Option[String]]),
        ignoreEmptyOutputs = fields.get("ignoreEmptyOutputs").fold(false)(_.convertTo[Boolean]),
        monitoringScript = fields.get("monitoringScript").flatMap(_.convertTo[Option[String]]),
        monitoringImage = fields.get("monitoringImage").flatMap(_.convertTo[Option[String]]),
        monitoringImageScript = fields.get("monitoringImageScript").flatMap(_.convertTo[Option[String]])
        // All new fields above this line MUST have defaults or be wrapped in Option[]!
      )
    }
  }

  implicit val ExecutionEventFormat = jsonFormat3(ExecutionEvent)

  implicit val ExecutionServiceStatusFormat = jsonFormat2(ExecutionServiceStatus)

  implicit val ExecutionServiceFailureFormat = jsonFormat3(ExecutionServiceFailure)

  implicit val ExecutionServiceVersionFormat = jsonFormat1(ExecutionServiceVersion)

  implicit val ExecutionServiceValidationFormat = jsonFormat2(ExecutionServiceValidation)

  implicit val ExecutionServiceOutputsFormat = jsonFormat2(ExecutionServiceOutputs)

  implicit val ExecutionServiceCallLogsFormat = jsonFormat3(ExecutionServiceCallLogs)

  implicit val ExecutionServiceLogsFormat = jsonFormat2(ExecutionServiceLogs)

  implicit val ExecutionServiceWorkflowOptionsFormat = jsonFormat18(ExecutionServiceWorkflowOptions)

  implicit val ExecutionServiceLabelResponseFormat = jsonFormat2(ExecutionServiceLabelResponse)

  implicit val TaskOutputFormat = jsonFormat2(TaskOutput)

  implicit val WorkflowOutputsFormat = jsonFormat2(WorkflowOutputs)

  implicit val WorkflowCostFormat = jsonFormat2(WorkflowCost)

  implicit val SubmissionValidationInputFormat = jsonFormat2(SubmissionValidationInput)

  implicit val SubmissionValidationHeaderFormat = jsonFormat3(SubmissionValidationHeader)

  implicit val SubmissionValidationValueFormat = jsonFormat3(SubmissionValidationValue)

  implicit val SubmissionValidationEntityInputsFormat = jsonFormat2(SubmissionValidationEntityInputs)

  implicit val SubmissionValidationReportFormat = jsonFormat4(SubmissionValidationReport)

  implicit val WorkflowFormat = jsonFormat7(Workflow)

  implicit val ExternalEntityInfoFormat = jsonFormat2(ExternalEntityInfo)

  implicit val SubmissionFormat = jsonFormat21(Submission)

  implicit val SubmissionRetryFormat = jsonFormat1(SubmissionRetry)

  implicit val SubmissionReportFormat = jsonFormat7(SubmissionReport)

  implicit val RetriedSubmissionReportFormat = jsonFormat7(RetriedSubmissionReport)

  implicit val SubmissionListResponseFormat = jsonFormat17(SubmissionListResponse.apply)

  implicit val MetadataParamsFormat = jsonFormat3(MetadataParams)

  implicit val CallMetadataFormat = jsonFormat14(CallMetadata)

  implicit val ActiveSubmissionFormat = jsonFormat3(ActiveSubmission)

  implicit val WorkflowQueueStatusResponseFormat = jsonFormat3(WorkflowQueueStatusResponse)

  implicit val UserCommentUpdateOperationFormat = jsonFormat1(UserCommentUpdateOperation)

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

    def read(value: JsValue) =
      value.asJsObject.getFields("statuses", "users", "maxActiveWorkflowsTotal", "maxActiveWorkflowsPerUser") match {
        case Seq(statuses @ JsObject(_),
                 users @ JsObject(_),
                 JsNumber(maxActiveWorkflowsTotal),
                 JsNumber(maxActiveWorkflowsPerUser)
            ) =>
          WorkflowQueueStatusByUserResponse(
            statuses.convertTo[StatusCounts],
            // remove 1 layer of nesting from `users` to remove the middle `statuses` map
            users.convertTo[Map[String, StatusCountsByUser]].flatMap { case (k, v) =>
              v.values.map(v2 => k -> v2)
            },
            maxActiveWorkflowsTotal.intValue,
            maxActiveWorkflowsPerUser.intValue
          )
      }
  }
}

case class SubmissionRetry(
  retryType: RetryStatus
)

//noinspection TypeAnnotation,RedundantBlock
object WorkflowStatuses {
  val allStatuses: Seq[WorkflowStatus] =
    Seq(Queued, Launching, Submitted, Running, Aborting, Failed, Succeeded, Aborted, Unknown)
  val queuedStatuses: Seq[WorkflowStatus] = Seq(Queued, Launching)
  val runningStatuses: Seq[WorkflowStatus] = Seq(Submitted, Running, Aborting)
  val terminalStatuses: Seq[WorkflowStatus] = Seq(Failed, Succeeded, Aborted, Unknown)
  val abortableStatuses: Seq[WorkflowStatus] = Seq(Submitted, Running)

  sealed trait WorkflowStatus extends RawlsEnumeration[WorkflowStatus] {
    def isDone =
      terminalStatuses.contains(this)
    override def toString = getClass.getSimpleName.stripSuffix("$")
    override def withName(name: String) = WorkflowStatuses.withName(name)
  }

  def withName(name: String): WorkflowStatus =
    name match {
      case "Queued"    => Queued
      case "Launching" => Launching
      case "Submitted" => Submitted
      case "Running"   => Running
      case "Failed"    => Failed
      case "Succeeded" => Succeeded
      case "Aborting"  => Aborting
      case "Aborted"   => Aborted
      case "Unknown"   => Unknown
      case _           => throw new RawlsException(s"invalid WorkflowStatus [${name}]")
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

object SubmissionRetryStatuses {
  sealed trait RetryStatus extends RawlsEnumeration[RetryStatus] {
    def filterWorkflows(workflows: Seq[Workflow]) =
      this match {
        case RetryFailed           => workflows.filter(wf => wf.status.equals(Failed))
        case RetryAborted          => workflows.filter(wf => wf.status.equals(Aborted))
        case RetryFailedAndAborted => workflows.filter(wf => wf.status.equals(Failed) || wf.status.equals(Aborted))
      }
    override def toString = getClass.getSimpleName.stripSuffix("$")
    override def withName(name: String) = SubmissionRetryStatuses.withName(name)
  }

  def withName(name: String): RetryStatus =
    name match {
      case "Failed"           => RetryFailed
      case "Aborted"          => RetryAborted
      case "FailedAndAborted" => RetryFailedAndAborted
      case _                  => throw new RawlsException(s"invalid WorkflowStatus [${name}]")
    }

  case object RetryFailed extends RetryStatus
  case object RetryAborted extends RetryStatus
  case object RetryFailedAndAborted extends RetryStatus
}

//noinspection TypeAnnotation,RedundantBlock
object SubmissionStatuses {
  val activeStatuses: Seq[SubmissionStatus] = Seq(Accepted, Evaluating, Submitting, Submitted, Aborting)
  val terminalStatuses: Seq[SubmissionStatus] = Seq(Aborted, Done)
  val allStatuses: Seq[SubmissionStatus] = Seq(Accepted, Evaluating, Submitting, Submitted, Aborting, Aborted, Done)

  sealed trait SubmissionStatus extends RawlsEnumeration[SubmissionStatus] {
    def isTerminated =
      terminalStatuses.contains(this)
    override def toString = getClass.getSimpleName.stripSuffix("$")
    override def withName(name: String) = SubmissionStatuses.withName(name)
  }

  def withName(name: String): SubmissionStatus =
    name match {
      case "Accepted"   => Accepted
      case "Evaluating" => Evaluating
      case "Submitting" => Submitting
      case "Submitted"  => Submitted
      case "Aborting"   => Aborting
      case "Aborted"    => Aborted
      case "Done"       => Done
      case _            => throw new RawlsException(s"invalid SubmissionStatus [${name}]")
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

  def withName(name: String): WorkflowFailureMode =
    name match {
      case "ContinueWhilePossible" => ContinueWhilePossible
      case "NoNewCalls"            => NoNewCalls
      case _ =>
        throw new RawlsException(
          s"Invalid WorkflowFailureMode [${name}]. Possible values: ${allWorkflowFailureModes.mkString(", ")}"
        )
    }

  def withNameOpt(name: Option[String]): Option[WorkflowFailureMode] =
    name.flatMap(n => Try(withName(n)).toOption)

  case object ContinueWhilePossible extends WorkflowFailureMode
  case object NoNewCalls extends WorkflowFailureMode
}

object ExecutionJsonSupport extends ExecutionJsonSupport
