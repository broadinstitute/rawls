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
  executionServiceId: Option[String]
)

// we need the external id in order to query cromwell, and the execution service id in order to route to the right instance
object WorkflowExecution {
  def apply(wf: Workflow) = wf.workflowId match {
    case Some(id) => new WorkflowExecution(id, None)
    case None => throw new RawlsException("can only process Workflow objects with a workflow id")
  }
  def apply(wr: WorkflowRecord) = (wr.externalId, wr.executionServiceKey) match {
    case (Some(id), Some(execKey)) => new WorkflowExecution(id, Some(execKey))
    case _ => throw new RawlsException("can only process WorkflowRecord objects with an external id and execution service key")
  }
}

case class ExecutionServiceId(id: String)