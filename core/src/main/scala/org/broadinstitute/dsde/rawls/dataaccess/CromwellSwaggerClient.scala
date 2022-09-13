package org.broadinstitute.dsde.rawls.dataaccess

import com.typesafe.scalalogging.LazyLogging
import cromwell.client.ApiClient
import cromwell.client.api.WomtoolApi
import cromwell.client.model.WorkflowDescription
import org.broadinstitute.dsde.rawls.model.{UserInfo, WDL, WdlSource, WdlUrl}
import org.broadinstitute.dsde.rawls.util.Retry

import scala.concurrent.duration._
import scala.util.Try

class CromwellSwaggerClient(cromwellBasePath: String) extends LazyLogging {

  private def getCromwellWomtoolApi(accessToken: String): WomtoolApi = {
    val apiClient = new ApiClient()
    apiClient.setAccessToken(accessToken)
    apiClient.setBasePath(cromwellBasePath)
    new WomtoolApi(apiClient)
  }

  def describe(userInfo: UserInfo, wdl: WDL): Try[WorkflowDescription] =
    Retry.retry(5.seconds, 30.seconds) {
      Try {
        wdl match {
          case wdl: WdlUrl =>
            getCromwellWomtoolApi(userInfo.accessToken.token).describe("v1", null, wdl.url, null, null, null)
          case wdl: WdlSource =>
            getCromwellWomtoolApi(userInfo.accessToken.token).describe("v1", wdl.source, null, null, null, null)
        }
      }
    }

}
