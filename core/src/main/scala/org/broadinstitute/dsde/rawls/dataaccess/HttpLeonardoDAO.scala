package org.broadinstitute.dsde.rawls.dataaccess

import org.broadinstitute.dsde.rawls.config.LeonardoConfig
import org.broadinstitute.dsde.rawls.model.GoogleProjectId
import org.broadinstitute.dsde.workbench.client.leonardo.ApiClient
import org.broadinstitute.dsde.workbench.client.leonardo.api.{AppsApi, ResourcesApi, RuntimesApi}
import org.broadinstitute.dsde.workbench.client.leonardo.model._

import java.util.UUID
import scala.jdk.CollectionConverters._

class HttpLeonardoDAO(leonardoConfig: LeonardoConfig) extends LeonardoDAO {

  private def getAppsV2LeonardoApi(accessToken: String): AppsApi = {
    val apiClient = new ApiClient()
    apiClient.setAccessToken(accessToken)
    apiClient.setBasePath(leonardoConfig.baseUrl)
    new AppsApi(apiClient)
  }

  private def getResourcesLeonardoApi(accessToken: String) = {
    val apiClient = new ApiClient()
    apiClient.setAccessToken(accessToken)
    apiClient.setBasePath(leonardoConfig.baseUrl)
    new ResourcesApi(apiClient)
  }

  private def getRuntimesV2LeonardoApi(accessToken: String): RuntimesApi = {
    val apiClient = new ApiClient()
    apiClient.setAccessToken(accessToken)
    apiClient.setBasePath(leonardoConfig.baseUrl)
    new RuntimesApi(apiClient)
  }
  override def deleteApps(token: String, workspaceId: UUID, deleteDisk: Boolean) =
    getAppsV2LeonardoApi(token).deleteAllAppsV2(workspaceId.toString, deleteDisk)

  override def listApps(token: String, workspaceId: UUID): Seq[ListAppResponse] =
    getAppsV2LeonardoApi(token).listAppsV2(workspaceId.toString, null, false, null, null).asScala.toSeq

  override def listAzureRuntimes(token: String, workspaceId: UUID): Seq[ListRuntimeResponse] =
    getRuntimesV2LeonardoApi(token).listAzureRuntimesV2(workspaceId.toString, null, false, null).asScala.toSeq

  override def deleteAzureRuntimes(token: String, workspaceId: UUID, deleteDisk: Boolean): Unit =
    getRuntimesV2LeonardoApi(token).deleteAllRuntimesV2(workspaceId.toString, deleteDisk)

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

  override def cleanupAllResources(token: String, googleProjectId: GoogleProjectId): Unit =
    getResourcesLeonardoApi(token).cleanupAllResources(googleProjectId.value)

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
