package org.broadinstitute.dsde.rawls.dataaccess

import okhttp3.{Dispatcher, Protocol}
import org.broadinstitute.dsde.rawls.config.LeonardoConfig
import org.broadinstitute.dsde.rawls.model.GoogleProjectId
import org.broadinstitute.dsde.workbench.client.leonardo.ApiClient
import org.broadinstitute.dsde.workbench.client.leonardo.api.{AppsApi, ResourcesApi, RuntimesApi}
import org.broadinstitute.dsde.workbench.client.leonardo.model._

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

  override def listApps(token: String, googleProjectId: GoogleProjectId): Seq[ListAppResponse] =
    getAppsV2LeonardoApi(token).listAppByProject(googleProjectId.value, null, false, null, null).asScala.toSeq

  override def listRuntimes(token: String, googleProjectId: GoogleProjectId): Seq[ListRuntimeResponse] =
    getRuntimesV2LeonardoApi(token).listRuntimesByProject(googleProjectId.value, null).asScala.toSeq

  override def cleanupAllResources(token: String, googleProjectId: GoogleProjectId): Unit =
    getResourcesLeonardoApi(token).cleanupAllResources(googleProjectId.value)

}
