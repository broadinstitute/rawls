package org.broadinstitute.dsde.rawls.model

import org.broadinstitute.dsde.rawls.model.SubmissionStatuses.SubmissionStatus
import org.broadinstitute.dsde.rawls.model.WorkflowStatuses.WorkflowStatus

import scala.annotation.meta.field
import spray.json._
import com.wordnik.swagger.annotations.{ApiModel, ApiModelProperty}
import org.joda.time.DateTime
import org.broadinstitute.dsde.rawls.{RawlsException, VertexProperty}

/**
 * @author tsharpe
 */

// this is what a client sends to have us create a submission

// Request for a submission
@ApiModel(value = "SubmissionRequest")
case class SubmissionRequest(
  @(ApiModelProperty@field)(required = true, value = "Namespace of the method configuration to execute.")
  methodConfigurationNamespace: String,
  @(ApiModelProperty@field)(required = true, value = "Name of the method configuration to execute.")
  methodConfigurationName: String,
  @(ApiModelProperty@field)(required = true, value = "Type of root entity for expression.")
  entityType: String,
  @(ApiModelProperty@field)(required = true, value = "Name of root entity for expression.")
  entityName: String,
  @(ApiModelProperty@field)(required = false, value = "Expression that resolves to one or more entities matching the entity type in the method configuration.")
  expression: Option[String]
)

// Cromwell's response to workflow submission (not decorated with annotations because it's not part of our API)
case class ExecutionServiceStatus(
  id: String,
  status: String
)

// Status of a successfully started workflow
@ApiModel(value = "Workflow")
case class Workflow(
  @(ApiModelProperty@field)("Workspace namespace")
  workspaceNamespace: String,
  @(ApiModelProperty@field)("Workspace name")
  workspaceName: String,
  @(ApiModelProperty@field)("Workflow unique identifier") @(VertexProperty@field)
  id: String,
  @(ApiModelProperty@field)("Workflow status") @(VertexProperty@field)
  status: WorkflowStatuses.WorkflowStatus,
  @(ApiModelProperty@field)("Status last-changed date") @(VertexProperty@field)
  statusLastChangedDate: DateTime,
  @(ApiModelProperty@field)("Entity name") @(VertexProperty@field)
  entityName: String,
  @(ApiModelProperty@field)("Entity type") @(VertexProperty@field)
  entityType: String
)

// Encapsulating errors for workflows that failed to start
@ApiModel(value = "WorkflowFailure")
case class WorkflowFailure(
  @(ApiModelProperty@field)("Workspace namespace")
  workspaceNamespace: String,
  @(ApiModelProperty@field)("Workspace name")
  workspaceName: String,
  @(ApiModelProperty@field)("Entity name") @(VertexProperty@field)
  entityName: String,
  @(ApiModelProperty@field)("Entity type") @(VertexProperty@field)
  entityType: String,
  @(ApiModelProperty@field)("List of errors starting this workflow") @(VertexProperty@field)
  errors: Seq[String]
)

// Status of a submission
@ApiModel(value = "Submission")
case class Submission(
  @(ApiModelProperty@field)("SubmissionRequest unique identifier") @(VertexProperty@field)
  id: String,
  @(ApiModelProperty@field)("SubmissionRequest date") @(VertexProperty@field)
  submissionDate: DateTime,
  @(ApiModelProperty@field)("Workspace namespace")
  workspaceNamespace: String,
  @(ApiModelProperty@field)("Workspace name")
  workspaceName: String,
  @(ApiModelProperty@field)("Method configuration namespace") @(VertexProperty@field)
  methodConfigurationNamespace: String,
  @(ApiModelProperty@field)("Method configuration name") @(VertexProperty@field)
  methodConfigurationName: String,
  @(ApiModelProperty@field)("Entity type") @(VertexProperty@field)
  entityType: String,
  @(ApiModelProperty@field)("Status of Workflow(s)")
  workflows: Seq[Workflow],
  @(ApiModelProperty@field)("Workflows that failed to start")
  notstarted: Seq[WorkflowFailure],
  @(ApiModelProperty@field)("Status") @(VertexProperty@field)
  status: SubmissionStatuses.SubmissionStatus
)

object ExecutionJsonSupport extends JsonSupport {

  implicit val SubmissionRequestFormat = jsonFormat5(SubmissionRequest)

  implicit val ExecutionServiceStatusFormat = jsonFormat2(ExecutionServiceStatus)

  implicit val WorkflowFormat = jsonFormat7(Workflow)

  implicit val WorkflowFailureFormat = jsonFormat5(WorkflowFailure)

  implicit val SubmissionFormat = jsonFormat10(Submission)

  implicit object WorkflowStatusFormat extends RootJsonFormat[WorkflowStatuses.WorkflowStatus] {
    override def write(obj: WorkflowStatus): JsValue = JsString(obj.toString)
    override def read(json: JsValue): WorkflowStatus = json match {
      case JsString(name) => WorkflowStatuses.withName(name)
      case x => throw new DeserializationException("invalid value: " + x)
    }
  }

  implicit object SubmissionStatusFormat extends RootJsonFormat[SubmissionStatuses.SubmissionStatus] {
    override def write(obj: SubmissionStatus): JsValue = JsString(obj.toString)
    override def read(json: JsValue): SubmissionStatus = json match {
      case JsString(name) => SubmissionStatuses.withName(name)
      case x => throw new DeserializationException("invalid value: " + x)
    }
  }
}

object WorkflowStatuses {
  val terminalStatuses: Seq[WorkflowStatus] = Seq(Failed, Succeeded, Unknown)

  sealed trait WorkflowStatus {
    def isDone = {
      terminalStatuses.contains(this)
    }
    override def toString = getClass.getSimpleName.stripSuffix("$")
  }

  def withName(name: String): WorkflowStatus = {
    name match {
      case "Submitted" => Submitted
      case "Running" => Running
      case "Failed" => Failed
      case "Succeeded" => Succeeded
      case "Unknown" => Unknown
      case _ => throw new RawlsException(s"invalid WorkflowStatus [${name}]")
    }
  }

  case object Submitted extends WorkflowStatus
  case object Running extends WorkflowStatus
  case object Failed extends WorkflowStatus
  case object Succeeded extends WorkflowStatus
  case object Unknown extends WorkflowStatus
}

object SubmissionStatuses {
  sealed trait SubmissionStatus {
    override def toString = getClass.getSimpleName.stripSuffix("$")
  }

  def withName(name: String): SubmissionStatus = {
    name match {
      case "Submitted" => Submitted
      case "Done" => Done
      case _ => throw new RawlsException(s"invalid SubmissionStatus [${name}]")
    }
  }

  case object Submitted extends SubmissionStatus
  case object Done extends SubmissionStatus
}
