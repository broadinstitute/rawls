package org.broadinstitute.dsde.rawls.dataaccess

import org.broadinstitute.dsde.rawls.model._
import spray.json.JsObject

import scala.concurrent.Future
import scala.util.Try

/**
 * @author tsharpe
 */
trait ExecutionServiceDAO extends ErrorReportable {
  val errorReportSource = ErrorReportSource("cromwell")
  def submitWorkflows(wdl: String, inputs: Seq[String], options: Option[String], userInfo: UserInfo): Future[Seq[Either[ExecutionServiceStatus, ExecutionServiceFailure]]]
  def status(id: String, userInfo: UserInfo): Future[ExecutionServiceStatus]
  def callLevelMetadata(id: String, userInfo: UserInfo): Future[JsObject]
  def outputs(id: String, userInfo: UserInfo): Future[ExecutionServiceOutputs]
  def logs(id: String, userInfo: UserInfo): Future[ExecutionServiceLogs]
  def abort(id: String, userInfo: UserInfo): Future[Try[ExecutionServiceStatus]]
  def version: Future[ExecutionServiceVersion]

  // get the status of the execution service itself
  def getStatus(): Future[Map[String, SubsystemStatus]]
}
