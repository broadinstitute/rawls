package org.broadinstitute.dsde.rawls.dataaccess


import com.typesafe.scalalogging.LazyLogging
import cromwell.client.ApiClient
import cromwell.client.api.WomtoolApi
import cromwell.client.model.WorkflowDescription
import org.broadinstitute.dsde.rawls.model.UserInfo
import org.broadinstitute.dsde.rawls.util.Retry
import scala.util.Try

import scala.concurrent.duration._

class CromwellSwaggerClient(cromwellBasePath: String) extends LazyLogging {


  private def getCromwellWomtoolApi(accessToken: String): WomtoolApi = {
    val apiClient = new ApiClient()
    apiClient.setAccessToken(accessToken)
    apiClient.setBasePath(cromwellBasePath)
    new WomtoolApi(apiClient)
  }


  def describe(userInfo: UserInfo, wdl: String): Try[WorkflowDescription] = {
    Retry.retry(5.seconds, 30.seconds) { Try { getCromwellWomtoolApi(userInfo.accessToken.token).describe("v1", wdl, null, null, null, null) } }
  }

}