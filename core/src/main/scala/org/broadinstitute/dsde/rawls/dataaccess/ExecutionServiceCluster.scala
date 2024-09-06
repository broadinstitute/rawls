package org.broadinstitute.dsde.rawls.dataaccess

import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkflowRecord
import org.broadinstitute.dsde.rawls.model._
import spray.json.JsObject

import scala.concurrent.Future
import scala.util.Try

/**
  * Created by davidan on 6/14/16.
  */
trait ExecutionServiceCluster extends ErrorReportable {
  val errorReportSource = ErrorReportSource("cromwell")

  // ====================
  // facade methods
  // ====================

  def submitWorkflows(workflowRecs: Seq[WorkflowRecord],
                      wdl: WDL,
                      inputs: Seq[String],
                      options: Option[String],
                      labels: Option[Map[String, String]],
                      workflowCollection: Option[String],
                      userInfo: UserInfo
  ): Future[(ExecutionServiceId, Seq[Either[ExecutionServiceStatus, ExecutionServiceFailure]])]

  def status(workflowRec: WorkflowRecord, userInfo: UserInfo): Future[ExecutionServiceStatus]

  def outputs(workflowRec: WorkflowRecord, userInfo: UserInfo): Future[ExecutionServiceOutputs]

  def logs(workflowRec: WorkflowRecord, userInfo: UserInfo): Future[ExecutionServiceLogs]

  def abort(workflowRec: WorkflowRecord, userInfo: UserInfo): Future[Try[ExecutionServiceStatus]]

  def getCost(workflowRec: WorkflowRecord, workflowCostBreakdownParams: Option[WorkflowCostBreakdownParams], userInfo: UserInfo): Future[WorkflowCostBreakdown]

  def version: Future[ExecutionServiceVersion]

  // ====================

  // these work a little differently.  If the caller knows which execution service is handling the workflow
  // (because it was stored in the DB at submission time) then it passes its ID here.  If the caller doesn't know
  // (e.g. it's a subworkflow, which the DB doesn't track) then it queries all execution services until it finds a match.

  def findExecService(submissionId: String,
                      workflowId: String,
                      userInfo: UserInfo,
                      execId: Option[ExecutionServiceId] = None
  ): Future[ExecutionServiceId]

  def callLevelMetadata(submissionId: String,
                        workflowId: String,
                        metadataParams: MetadataParams,
                        execServiceId: Option[ExecutionServiceId],
                        userInfo: UserInfo
  ): Future[JsObject]

  def callLevelMetadataForCostCalculation(submissionId: String,
                                          workflowId: String,
                                          execId: Option[ExecutionServiceId],
                                          userInfo: UserInfo
  ): Future[JsObject]
}

class ExecutionServiceId(val id: String) {
  override def toString: String = id
  override def equals(obj: Any): Boolean = obj match {
    case that: ExecutionServiceId => id.equals(that.id)
    case _                        => false
  }
  override def hashCode: Int = id.hashCode
}

object ExecutionServiceId {
  def apply(id: String) = new ExecutionServiceId(id)
}
