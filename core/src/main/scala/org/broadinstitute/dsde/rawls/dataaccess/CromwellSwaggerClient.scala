package org.broadinstitute.dsde.rawls.dataaccess


import java.io.File

import com.typesafe.scalalogging.LazyLogging
import cromwell.client.ApiClient
import cromwell.client.api.WomtoolApi
import cromwell.client.model.WorkflowDescription
import org.broadinstitute.dsde.rawls.model.UserInfo

import scala.util.Random

class CromwellSwaggerClient(cromwellBasePaths: List[String]) extends LazyLogging {


  private def getRandomCromwellWomtoolApi(accessToken: String): WomtoolApi = {
    logger.info("CROMWELL BASE PATH: " + Random.nextInt(cromwellBasePaths.length))
    logger.info("CROMWELL BASE PATHS: " + cromwellBasePaths.toString)
    val apiClient = new ApiClient()
    apiClient.setAccessToken(accessToken)
    apiClient.setBasePath(cromwellBasePaths(Random.nextInt(cromwellBasePaths.length)))
    new WomtoolApi(apiClient)
  }

  def validate(userInfo: UserInfo, wdl: String): WorkflowDescription = {
    //String version, String workflowSource, String workflowUrl, File workflowInputs, String workflowType, String workflowTypeVersion
    getRandomCromwellWomtoolApi(userInfo.accessToken.token).describe("v1", wdl, null, null, null, null)
  }

}