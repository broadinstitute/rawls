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

  override def createWDSInstance(token: String, workspaceId: UUID, sourceWorkspaceId: Option[UUID] = None): Unit =
    createApp(token, workspaceId, s"wds-$workspaceId", leonardoConfig.wdsType, sourceWorkspaceId)

  override def createApp(token: String,
                         workspaceId: UUID,
                         appName: String,
                         appType: String,
                         sourceWorkspaceId: Option[UUID]
  ): Unit = {
    val createAppRequest = new CreateAppRequest()
    val appTypeEnum = AppType.fromValue(appType)
    createAppRequest.setAppType(appTypeEnum)
    val customEnvironmentVariables: Map[String, UUID] = sourceWorkspaceId
      .map { id =>
        Map("sourceWorkspaceId" -> id)
      }
      .getOrElse(Map.empty)
    createAppRequest.setCustomEnvironmentVariables(customEnvironmentVariables)
    getAppsV2leonardoApi(token).createAppV2(workspaceId.toString, appName, createAppRequest)
  }

}
