package org.broadinstitute.dsde.rawls.dataaccess

import org.broadinstitute.dsde.rawls.model.{ExecutionServiceOutputs, ExecutionServiceStatus}
import spray.http.HttpCookie

/**
 * @author tsharpe
 */
trait ExecutionServiceDAO {
  def submitWorkflow( wdl: String, inputs: String, authCookie: HttpCookie ): ExecutionServiceStatus
  def status(id: String, authCookie: HttpCookie): ExecutionServiceStatus
  def outputs(id: String, authCookie: HttpCookie): ExecutionServiceOutputs
}
