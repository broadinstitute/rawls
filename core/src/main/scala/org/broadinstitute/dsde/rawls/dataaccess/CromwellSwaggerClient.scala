package org.broadinstitute.dsde.rawls.dataaccess


import java.io.File

import cromwell.client.ApiClient
import cromwell.client.api.WomtoolApi
import cromwell.client.model.WorkflowDescription
import org.broadinstitute.dsde.rawls.model.UserInfo

class CromwellSwaggerClient(cromwellBasePath: String) {

  private def cromwellWomtoolApi(accessToken: String): WomtoolApi = {
    val apiClient = new ApiClient()
    apiClient.setAccessToken(accessToken)
    apiClient.setBasePath(cromwellBasePath)
    new WomtoolApi(apiClient)
  }


  def validate(userInfo: UserInfo, wdl: String): WorkflowDescription = {
    //String version, String workflowSource, String workflowUrl, File workflowInputs, String workflowType, String workflowTypeVersion
    cromwellWomtoolApi(userInfo.accessToken.token).describe("v1", wdl, null, null, null, null)
  }

}