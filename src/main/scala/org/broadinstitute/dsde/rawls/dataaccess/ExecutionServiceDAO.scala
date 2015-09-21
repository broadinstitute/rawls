package org.broadinstitute.dsde.rawls.dataaccess

import org.broadinstitute.dsde.rawls.model._
import spray.http.HttpCookie

/**
 * @author tsharpe
 */
trait ExecutionServiceDAO {
  def submitWorkflow(wdl: String, inputs: String, options: Option[String], userInfo: UserInfo): ExecutionServiceStatus
  def validateWorkflow(wdl: String, inputs: String, userInfo: UserInfo): ExecutionServiceValidation
  def status(id: String, userInfo: UserInfo): ExecutionServiceStatus
  def callLevelMetadata(id: String, userInfo: UserInfo): ExecutionMetadata
  def outputs(id: String, userInfo: UserInfo): ExecutionServiceOutputs
  def logs(id: String, userInfo: UserInfo): ExecutionServiceLogs
  def abort(id: String, userInfo: UserInfo): ExecutionServiceStatus
}
