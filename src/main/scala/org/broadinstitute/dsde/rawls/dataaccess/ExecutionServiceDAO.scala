package org.broadinstitute.dsde.rawls.dataaccess

import org.broadinstitute.dsde.rawls.model.JobStatus
import spray.http.HttpCookie

/**
 * @author tsharpe
 */
trait ExecutionServiceDAO {
  def submitJob( wdl: String, inputs: String, authCookie: HttpCookie ): JobStatus
}
