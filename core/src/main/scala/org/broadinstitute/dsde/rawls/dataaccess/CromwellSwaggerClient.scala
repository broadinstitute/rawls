package org.broadinstitute.dsde.rawls.dataaccess


import com.typesafe.scalalogging.LazyLogging
import cromwell.client.ApiClient
import cromwell.client.api.WomtoolApi
import cromwell.client.model.WorkflowDescription
import org.broadinstitute.dsde.rawls.model.{UserInfo, WDL}
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


  def describe(userInfo: UserInfo, wdl: WDL): Try[WorkflowDescription] =
    wdl.url match {
      case Some(url) =>
        logger.error(">>> wdl.url is ${wdl.url}")
        Retry.retry(5.seconds, 30.seconds) { Try { getCromwellWomtoolApi(userInfo.accessToken.token).describe("v1", null, url, null, null, null) } }
      case None =>
        Retry.retry(5.seconds, 30.seconds) { Try { getCromwellWomtoolApi(userInfo.accessToken.token).describe("v1", wdl.source, null, null, null, null) } }
    }


}