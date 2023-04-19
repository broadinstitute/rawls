package org.broadinstitute.dsde.rawls.dataaccess

import org.broadinstitute.dsde.rawls.config.LeonardoConfig
import org.broadinstitute.dsde.workbench.client.leonardo.api.AppsV2Api
import org.broadinstitute.dsde.workbench.client.leonardo.model.CreateAppRequest
import org.broadinstitute.dsde.workbench.client.leonardo.model.AppType

import java.util.UUID
import org.broadinstitute.dsde.workbench.client.leonardo.ApiClient

class HttpLeonardoDAO(leonardoConfig: LeonardoConfig) extends LeonardoDAO {

  private def getAppsV2leonardoApi(accessToken: String): AppsV2Api = {
    val apiClient = new ApiClient()
    apiClient.setAccessToken(accessToken)
    apiClient.setBasePath(leonardoConfig.baseUrl)
    new AppsV2Api(apiClient)
  }

  override def createWDSInstance(token: String, workspaceId: UUID): Unit =
    createApp(token, workspaceId, s"wds-$workspaceId", leonardoConfig.wdsType)

  override def createApp(token: String, workspaceId: UUID, appName: String, appType: String): Unit = {
    val createAppRequest = new CreateAppRequest()
    val appTypeEnum = AppType.fromValue(appType)
    createAppRequest.setAppType(appTypeEnum)
    getAppsV2leonardoApi(token).createAppV2(workspaceId.toString, appName, createAppRequest)
  }

}
