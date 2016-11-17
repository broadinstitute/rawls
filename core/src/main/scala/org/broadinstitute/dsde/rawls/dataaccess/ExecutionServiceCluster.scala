package org.broadinstitute.dsde.rawls.dataaccess

import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkflowRecord
import org.broadinstitute.dsde.rawls.model._

import scala.concurrent.Future
import scala.util.Try

/**
  * Created by davidan on 6/14/16.
  */
trait ExecutionServiceCluster extends ErrorReportable {
  override val errorReportSource = "cromwell"

  // ====================
  // facade methods
  // ====================

  def submitWorkflows(workflowRecs: Seq[WorkflowRecord], wdl: String, inputs: Seq[String], options: Option[String], userInfo: UserInfo): Future[(ExecutionServiceId, Seq[Either[ExecutionServiceStatus, ExecutionServiceFailure]])]

  def status(workflowRec: WorkflowRecord, userInfo: UserInfo): Future[ExecutionServiceStatus]

  def callLevelMetadata(workflowRec: WorkflowRecord, userInfo: UserInfo): Future[ExecutionMetadata]

  def outputs(workflowRec: WorkflowRecord, userInfo: UserInfo): Future[ExecutionServiceOutputs]

  def logs(workflowRec: WorkflowRecord, userInfo: UserInfo): Future[ExecutionServiceLogs]

  def abort(workflowRec: WorkflowRecord, userInfo: UserInfo): Future[Try[ExecutionServiceStatus]]

}

class ExecutionServiceId(val id: String) {
  override def toString: String = id
  override def equals(obj: Any): Boolean =  obj match {
    case that:ExecutionServiceId => id.equals(that.id)
    case _ => false
  }
  override def hashCode: Int = id.hashCode
}

object ExecutionServiceId {
  def apply(id: String) = new ExecutionServiceId(id)
}