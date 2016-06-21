package org.broadinstitute.dsde.rawls.dataaccess

import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkflowRecord
import org.broadinstitute.dsde.rawls.model._

import scala.concurrent.{Future}
import scala.util.Try

/**
  * Created by davidan on 6/14/16.
  */
trait ExecutionServiceCluster extends ErrorReportable {
  override val errorReportSource = "cromwell"

  // ====================
  // facade methods
  // ====================
  def submitWorkflow(wdl: String, inputs: String, options: Option[String], userInfo: UserInfo): Future[ExecutionServiceStatus]

  def submitWorkflows(workflowRecs: Seq[WorkflowRecord], wdl: String, inputs: Seq[String], options: Option[String], userInfo: UserInfo): Future[(ExecutionServiceId, Seq[Either[ExecutionServiceStatus, ExecutionServiceFailure]])]

  def status(wfe: WorkflowExecution, userInfo: UserInfo): Future[ExecutionServiceStatus]

  def callLevelMetadata(wfe: WorkflowExecution, userInfo: UserInfo): Future[ExecutionMetadata]

  def outputs(wfe: WorkflowExecution, userInfo: UserInfo): Future[ExecutionServiceOutputs]

  def logs(wfe: WorkflowExecution, userInfo: UserInfo): Future[ExecutionServiceLogs]

  def abort(wfe: WorkflowExecution, userInfo: UserInfo): Future[Try[ExecutionServiceStatus]]

}

case class WorkflowExecution(
  id: String,
  executionServiceId: String
)

object WorkflowExecution {
  def apply(wf: Workflow) = wf.workflowId match {
    case Some(id) => new WorkflowExecution(id, "0")
    case None => throw new RawlsException("can only process Workflow objects with an id")
  }
  def apply(wr: WorkflowRecord) = wr.externalId match {
    case Some(id) => new WorkflowExecution(id, "0")
    case None => throw new RawlsException("can only process WorkflowRecord objects with an id")
  }
}

case class ExecutionServiceId(id: String)