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

// Status of a workflow
@ApiModel(value = "Workflow")
case class Workflow(
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
  workflow: Seq[Workflow]
)

object ExecutionJsonSupport extends JsonSupport {

  implicit val SubmissionRequestFormat = jsonFormat5(SubmissionRequest)

  implicit val ExecutionServiceStatusFormat = jsonFormat2(ExecutionServiceStatus)

  implicit val WorkflowFormat = jsonFormat4(Workflow)

  implicit val SubmissionFormat = jsonFormat8(Submission)
}
