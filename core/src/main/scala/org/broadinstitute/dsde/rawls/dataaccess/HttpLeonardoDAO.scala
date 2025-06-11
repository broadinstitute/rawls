package org.broadinstitute.dsde.rawls.dataaccess

import okhttp3.{Dispatcher, Protocol}
import org.broadinstitute.dsde.rawls.config.LeonardoConfig
import org.broadinstitute.dsde.rawls.model.GoogleProjectId
import org.broadinstitute.dsde.workbench.client.leonardo.ApiClient
import org.broadinstitute.dsde.workbench.client.leonardo.api.{AppsApi, DisksApi, ResourcesApi, RuntimesApi}
import org.broadinstitute.dsde.workbench.client.leonardo.model._

import java.util.UUID
import scala.jdk.CollectionConverters._

class HttpLeonardoDAO(leonardoConfig: LeonardoConfig) extends LeonardoDAO {

  private val okHttpClient = {
    val dispatcher = new Dispatcher()
    new ApiClient().getHttpClient.newBuilder
      .protocols(Seq(Protocol.HTTP_1_1).asJava)
      .dispatcher(dispatcher)
      .build()
  }

  protected def getApiClient(accessToken: String): ApiClient = {
    val leoApiClient = new ApiClient(okHttpClient)
    leoApiClient.setBasePath(leonardoConfig.baseUrl)
    leoApiClient.setAccessToken(accessToken)

    leoApiClient
  }

  private def getAppsV2LeonardoApi(accessToken: String): AppsApi = {
    val apiClient = getApiClient(accessToken)
    new AppsApi(apiClient)
  }

  private def getResourcesLeonardoApi(accessToken: String) = {
    val apiClient = getApiClient(accessToken)
    new ResourcesApi(apiClient)
  }

  private def getRuntimesV2LeonardoApi(accessToken: String): RuntimesApi = {
    val apiClient = getApiClient(accessToken)
    new RuntimesApi(apiClient)
  }

  private def getDisksV2LeonardoApi(accessToken: String): DisksApi = {
    val apiClient = getApiClient(accessToken)
    new DisksApi(apiClient)
  }

  override def deleteApps(token: String, workspaceId: UUID, deleteDisk: Boolean) =
    getAppsV2LeonardoApi(token).deleteAllAppsV2(workspaceId.toString, deleteDisk)

  override def listApps(token: String, workspaceId: UUID): Seq[ListAppResponse] =
    getAppsV2LeonardoApi(token).listAppsV2(workspaceId.toString, null, false, null, null).asScala.toSeq

  override def updateAppConfig(token: String,
                               googleProject: String,
                               name: String,
                               updateAppRequest: UpdateAppRequest
  ): Unit =
    getAppsV2LeonardoApi(token).updateApp(googleProject, name, updateAppRequest)

  override def listRuntimesByWorkspace(token: String, workspaceId: UUID): Seq[ListRuntimeResponse] =
    getRuntimesV2LeonardoApi(token).listRuntimesByWorkspaceV2(workspaceId.toString, null, null).asScala.toSeq

  override def updateRuntimeConfig(
    token: String,
    googleProject: String,
    name: String,
    updateRuntimeRequest: UpdateRuntimeRequest
  ): Unit =
    getRuntimesV2LeonardoApi(token).updateRuntime(googleProject, name, updateRuntimeRequest)

  override def listAzureRuntimes(token: String, workspaceId: UUID): Seq[ListRuntimeResponse] =
    getRuntimesV2LeonardoApi(token).listAzureRuntimesV2(workspaceId.toString, null, null).asScala.toSeq

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

  override def listDisksByWorkspaceNamespace(token: String,
                                             workspaceNamespace: String
  ): Seq[ListPersistentDiskResponse] = {
    val labels = "workspaceNamespace=" + workspaceNamespace
    getDisksV2LeonardoApi(token).listDisks(labels, null, null).asScala.toSeq
  }

  override def updateDiskConfig(
    token: String,
    googleProject: String,
    name: String,
    updateDiskRequest: UpdateDiskRequest
  ): Unit =
    getDisksV2LeonardoApi(token).updateDisk(googleProject, name, updateDiskRequest)

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
