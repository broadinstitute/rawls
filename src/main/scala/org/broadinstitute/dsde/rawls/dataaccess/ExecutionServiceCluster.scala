package org.broadinstitute.dsde.rawls.dataaccess

import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkflowRecord
import org.broadinstitute.dsde.rawls.model._

import scala.concurrent.Future
import scala.util.Try

/**
  * Created by davidan on 6/14/16.
  */
// TODO: keys for the map of members should probably be strings
trait ExecutionServiceCluster extends ErrorReportable {
  override val errorReportSource = "cromwell"

  // ====================
  // facade methods
  // ====================
  def submitWorkflow(wdl: String, inputs: String, options: Option[String], userInfo: UserInfo): Future[ExecutionServiceStatus]

  def submitWorkflows[T](wdl: String, inputs: Seq[String], options: Option[String], userInfo: UserInfo)(wf: T): Future[Seq[Either[ExecutionServiceStatus, ExecutionServiceFailure]]]

  def status[T](id: String, userInfo: UserInfo)(wf: T): Future[ExecutionServiceStatus]

  def callLevelMetadata[T](id: String, userInfo: UserInfo)(wf: T): Future[ExecutionMetadata]

  def outputs[T](id: String, userInfo: UserInfo)(wf: T): Future[ExecutionServiceOutputs]

  def logs[T](id: String, userInfo: UserInfo)(wf: T): Future[ExecutionServiceLogs]

  def abort[T](id: String, userInfo: UserInfo)(wf: T): Future[Try[ExecutionServiceStatus]]

}

