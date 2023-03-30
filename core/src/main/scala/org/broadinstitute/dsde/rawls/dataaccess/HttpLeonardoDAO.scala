package org.broadinstitute.dsde.rawls.dataaccess

import org.broadinstitute.dsde.workbench.client.leonardo.api.AppsV2Api
import org.broadinstitute.dsde.workbench.client.leonardo.model.CreateAppRequest
import org.broadinstitute.dsde.workbench.client.leonardo.model.AppType
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.stream.Materializer
import java.util.UUID

import scala.concurrent.ExecutionContext
import org.broadinstitute.dsde.rawls.util.HttpClientUtilsStandard
import org.broadinstitute.dsde.workbench.client.leonardo.ApiClient

class HttpLeonardoDAO(
  baseUrl: String,
  wdsType: String
) extends LeonardoDAO {

  def getAppsV2leonardoApi(accessToken: String): AppsV2Api = {
    val apiClient = new ApiClient()
    apiClient.setAccessToken(accessToken)
    apiClient.setBasePath(baseUrl)
    new AppsV2Api(apiClient)
  }

  def createWDSInstance(token: String, workspaceId: UUID, appName: String): Unit = {
    createApp(token, workspaceId, appName, wdsType);
  }

  def createApp(token: String, workspaceId: UUID, appName: String, appType: String): Unit = {
    val createAppRequest = new CreateAppRequest()
    val appTypeEnum = AppType.fromValue(appType)
    createAppRequest.setAppType(appTypeEnum)
    getAppsV2leonardoApi(token).createAppV2(workspaceId.toString, appName, createAppRequest);
  }

}
