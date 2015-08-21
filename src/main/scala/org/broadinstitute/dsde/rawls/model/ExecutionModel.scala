package org.broadinstitute.dsde.rawls.model

import org.broadinstitute.dsde.rawls.model.SubmissionStatuses.SubmissionStatus
import org.broadinstitute.dsde.rawls.model.WorkflowStatuses.WorkflowStatus

import spray.json._
import org.joda.time.DateTime
import org.broadinstitute.dsde.rawls.RawlsException

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

case class ExecutionServiceOutputs(
  id: String,
  outputs: Map[String, Attribute]
)

case class ExecutionServiceLogs(
 id: String,
 logs: Map[String, Map[String,String]]
)

// Status of a successfully started workflow
case class Workflow(
  workflowId: String,
  status: WorkflowStatus,
  statusLastChangedDate: DateTime,
  workflowEntity: AttributeEntityReference,
  messages: Seq[AttributeString] = Seq.empty
)

// Encapsulating errors for workflows that failed to start
case class WorkflowFailure(
  entityName: String,
  entityType: String,
  errors: Seq[AttributeString]
)

case class TaskOutput(
  logs: Option[Map[String, String]],
  outputs: Option[Map[String, Attribute]]
)

case class WorkflowOutputs(
  workflowId: String,
  tasks: Map[String, TaskOutput]
)

// Status of a submission
case class Submission(
  submissionId: String,
  submissionDate: DateTime,
  submitter: String,
  methodConfigurationNamespace: String,
  methodConfigurationName: String,
  submissionEntity: AttributeEntityReference,
  workflows: Seq[Workflow],
  notstarted: Seq[WorkflowFailure],
  status: SubmissionStatus
)

object ExecutionJsonSupport extends JsonSupport {

  import WorkspaceJsonSupport.WorkspaceNameFormat

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

  implicit val SubmissionRequestFormat = jsonFormat5(SubmissionRequest)

  implicit val ExecutionServiceStatusFormat = jsonFormat2(ExecutionServiceStatus)

  implicit val ExecutionServiceOutputsFormat = jsonFormat2(ExecutionServiceOutputs)

  implicit val ExecutionServiceLogsFormat = jsonFormat2(ExecutionServiceLogs)

  implicit val TaskOutputFormat = jsonFormat2(TaskOutput)

  implicit val WorkflowOutputsFormat = jsonFormat2(WorkflowOutputs)

  implicit val WorkflowFormat = jsonFormat5(Workflow)

  implicit val WorkflowFailureFormat = jsonFormat3(WorkflowFailure)

  implicit val SubmissionFormat = jsonFormat9(Submission)
}

object WorkflowStatuses {
  val terminalStatuses: Seq[WorkflowStatus] = Seq(Failed, Succeeded, Aborted, Unknown)

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
      case "Aborted" => Aborted
      case "Unknown" => Unknown
      case _ => throw new RawlsException(s"invalid WorkflowStatus [${name}]")
    }
  }

  case object Submitted extends WorkflowStatus
  case object Running extends WorkflowStatus
  case object Failed extends WorkflowStatus
  case object Succeeded extends WorkflowStatus
  case object Aborted extends WorkflowStatus
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
