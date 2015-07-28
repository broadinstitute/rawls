package org.broadinstitute.dsde.rawls.model

import org.broadinstitute.dsde.rawls.model.SubmissionStatuses.SubmissionStatus
import org.broadinstitute.dsde.rawls.model.WorkflowStatuses.WorkflowStatus

import scala.annotation.meta.field
import spray.json._
import org.joda.time.DateTime
import org.broadinstitute.dsde.rawls.{RawlsException, VertexProperty}

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

// Cromwell's response to workflow submission (not decorated with annotations because it's not part of our API)
case class ExecutionServiceStatus(
  id: String,
  status: String
)

// Status of a successfully started workflow
case class Workflow(
  workspaceNamespace: String,
  workspaceName: String,
  @(VertexProperty@field)
  id: String,
  @(VertexProperty@field)
  status: WorkflowStatuses.WorkflowStatus,
  @(VertexProperty@field)
  statusLastChangedDate: DateTime,
  @(VertexProperty@field)
  entityType: String,
  @(VertexProperty@field)
  entityName: String
)

// Encapsulating errors for workflows that failed to start
case class WorkflowFailure(
  workspaceNamespace: String,
  workspaceName: String,
  @(VertexProperty@field)
  entityName: String,
  @(VertexProperty@field)
  entityType: String,
  @(VertexProperty@field)
  errors: Seq[String]
)

// Status of a submission
case class Submission(
  @(VertexProperty@field)
  id: String,
  @(VertexProperty@field)
  submissionDate: DateTime,
  workspaceNamespace: String,
  workspaceName: String,
  @(VertexProperty@field)
  methodConfigurationNamespace: String,
  @(VertexProperty@field)
  methodConfigurationName: String,
  entityType: String,
  entityName: String,
  workflows: Seq[Workflow],
  notstarted: Seq[WorkflowFailure],
  @(VertexProperty@field)
  status: SubmissionStatuses.SubmissionStatus
)

object ExecutionJsonSupport extends JsonSupport {

  implicit val SubmissionRequestFormat = jsonFormat5(SubmissionRequest)

  implicit val ExecutionServiceStatusFormat = jsonFormat2(ExecutionServiceStatus)

  implicit val WorkflowFormat = jsonFormat7(Workflow)

  implicit val WorkflowFailureFormat = jsonFormat5(WorkflowFailure)

  implicit val SubmissionFormat = jsonFormat11(Submission)

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
