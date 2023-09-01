package org.broadinstitute.dsde.rawls.dataaccess

import org.broadinstitute.dsde.rawls.config.LeonardoConfig
import org.broadinstitute.dsde.workbench.client.leonardo.api.{AppsApi, RuntimesApi}
import org.broadinstitute.dsde.workbench.client.leonardo.model.{
  AppAccessScope,
  AppType,
  CreateAppRequest,
  ListAppResponse,
  ListRuntimeResponse
}

import java.util.UUID
import org.broadinstitute.dsde.workbench.client.leonardo.ApiClient

import java.util
import scala.jdk.CollectionConverters._

class HttpLeonardoDAO(leonardoConfig: LeonardoConfig) extends LeonardoDAO {

  private def getAppsV2LeonardoApi(accessToken: String): AppsApi = {
    val apiClient = new ApiClient()
    apiClient.setAccessToken(accessToken)
    apiClient.setBasePath(leonardoConfig.baseUrl)
    new AppsApi(apiClient)
  }

  private def getRuntimesV2LeonardoApi(accessToken: String): RuntimesApi = {
    val apiClient = new ApiClient()
    apiClient.setAccessToken(accessToken)
    apiClient.setBasePath(leonardoConfig.baseUrl)
    new RuntimesApi(apiClient)
  }

  override def deleteApps(token: String, workspaceId: String, deleteDisk: Boolean) =
    getAppsV2LeonardoApi(token).deleteAllAppsV2(workspaceId, deleteDisk)

  override def listApps(token: String, workspaceId: String): Seq[ListAppResponse] =
    getAppsV2LeonardoApi(token).listAppsV2(workspaceId, null, false, null).asScala.toSeq

  override def listAzureRuntimes(token: String, workspaceId: String): Seq[ListRuntimeResponse] =
    getRuntimesV2LeonardoApi(token).listAzureRuntimesV2(workspaceId, null, false, null).asScala.toSeq

  override def deleteAzureRuntimes(token: String, workspaceId: String, deleteDisk: Boolean): Unit =
    getRuntimesV2LeonardoApi(token).deleteAllRuntimesV2(workspaceId, deleteDisk)

  override def createWDSInstance(token: String, workspaceId: UUID, sourceWorkspaceId: Option[UUID] = None): Unit =
    createApp(token, workspaceId, s"wds-$workspaceId", leonardoConfig.wdsType, sourceWorkspaceId)

  override def createApp(token: String,
                         workspaceId: UUID,
                         appName: String,
                         appType: String,
                         sourceWorkspaceId: Option[UUID]
  ): Unit = {
    val createAppRequest = buildAppRequest(appType, sourceWorkspaceId)
    getAppsV2LeonardoApi(token).createAppV2(workspaceId.toString, appName, createAppRequest)
  }

  protected[dataaccess] def buildAppRequest(appType: String, sourceWorkspaceId: Option[UUID]): CreateAppRequest = {
    val createAppRequest = new CreateAppRequest()
    sourceWorkspaceId.foreach { sourceId =>
      createAppRequest.setSourceWorkspaceId(sourceId.toString)
    }
    val appTypeEnum = AppType.fromValue(appType)
    createAppRequest.setAppType(appTypeEnum)
    createAppRequest.setAccessScope(AppAccessScope.WORKSPACE_SHARED)

    createAppRequest
  }

}
