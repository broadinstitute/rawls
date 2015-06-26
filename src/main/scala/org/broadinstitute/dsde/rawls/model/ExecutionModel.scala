package org.broadinstitute.dsde.rawls.model

import scala.annotation.meta.field
import spray.json._
import com.wordnik.swagger.annotations.{ApiModel, ApiModelProperty}
import org.joda.time.DateTime
import org.broadinstitute.dsde.rawls.VertexProperty

/**
 * @author tsharpe
 */

// this is what a client sends to have us create a submission

// Request for a submission
@ApiModel(value = "Submission")
case class Submission(
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

// Cromwell's response to job submission (not decorated with annotations because it's not part of our API)
case class JobStatus(
  id: String,
  status: String
)

// Status of a workflow
@ApiModel(value = "WorkflowStatus")
case class WorkflowStatus(
  @(ApiModelProperty@field)("Workflow unique identifier") @(VertexProperty@field)
  id: String,
  @(ApiModelProperty@field)("Workflow status") @(VertexProperty@field)
  status: String,
  @(ApiModelProperty@field)("Status last-changed date") @(VertexProperty@field)
  statusLastChangedDate: DateTime,
  @(ApiModelProperty@field)("Entity name") @(VertexProperty@field)
  entityName: String
)

// Status of a submission
@ApiModel(value = "SubmissionStatus")
case class SubmissionStatus(
  @(ApiModelProperty@field)("Submission unique identifier") @(VertexProperty@field)
  id: String,
  @(ApiModelProperty@field)("Submission date") @(VertexProperty@field)
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
  workflowStatus: Seq[WorkflowStatus]
)

object ExecutionJsonSupport extends JsonSupport {

  implicit val SubmissionFormat = jsonFormat5(Submission)

  implicit val JobStatusFormat = jsonFormat2(JobStatus)

  implicit val WorkflowStatusFormat = jsonFormat4(WorkflowStatus)

  implicit val SubmissionStatusFormat = jsonFormat8(SubmissionStatus)
}
