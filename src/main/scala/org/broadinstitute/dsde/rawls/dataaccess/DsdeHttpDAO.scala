package org.broadinstitute.dsde.rawls.dataaccess

import org.broadinstitute.dsde.rawls.model.UserInfo
import spray.client.pipelining
import spray.client.pipelining._
import spray.http.HttpHeaders.Authorization

/**
 * Created by dvoet on 9/2/15.
 */
trait DsdeHttpDAO {
  def addAuthHeader(userInfo: UserInfo): pipelining.RequestTransformer = {
    addHeader(Authorization(userInfo.accessToken))
  }
}
