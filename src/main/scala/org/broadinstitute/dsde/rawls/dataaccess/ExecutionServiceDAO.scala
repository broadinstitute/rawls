package org.broadinstitute.dsde.rawls.dataaccess

import org.broadinstitute.dsde.rawls.model._
import spray.http.HttpCookie

/**
 * @author tsharpe
 */
trait ExecutionServiceDAO {
  def submitWorkflow( wdl: String, inputs: String, authCookie: HttpCookie ): ExecutionServiceStatus
  def validateWorkflow( wdl: String, inputs: String, authCookie: HttpCookie ): ExecutionServiceValidation
  def status(id: String, authCookie: HttpCookie): ExecutionServiceStatus
  def outputs(id: String, authCookie: HttpCookie): ExecutionServiceOutputs
  def logs(id: String, authCookie: HttpCookie): ExecutionServiceLogs
  def abort(id: String, authCookie: HttpCookie): ExecutionServiceStatus
}
