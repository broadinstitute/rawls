package org.broadinstitute.dsde.rawls.workspace

import org.broadinstitute.dsde.workbench.client.leonardo.api.AppsV2Api;
import org.broadinstitute.dsde.workbench.client.leonardo.model.CreateAppRequest;
import org.broadinstitute.dsde.workbench.client.leonardo.model.AppType;

import scala.concurrent.Future
import org.broadinstitute.dsde.workbench.client.leonardo.ApiClient

class LeonardoClient(val leonardoBasePath: String) {

  def getAppsV2leonardoApi(accessToken: String): AppsV2Api = {
    val apiClient = new ApiClient()
    apiClient.setAccessToken(accessToken)
    apiClient.setBasePath(leonardoBasePath)
    new AppsV2Api(apiClient)
  }

  def createWDSInstance(token: String, workspaceId: String, appName: String): Unit = {
    val createAppRequest = new CreateAppRequest();
    createAppRequest.setAppType(AppType.CROMWELL);
    getAppsV2leonardoApi(token).createAppV2(workspaceId, appName, createAppRequest);
  }

}
