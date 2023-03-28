package org.broadinstitute.dsde.rawls.dataaccess

import org.broadinstitute.dsde.workbench.client.leonardo.api.AppsV2Api;
import org.broadinstitute.dsde.workbench.client.leonardo.model.CreateAppRequest;
import org.broadinstitute.dsde.workbench.client.leonardo.model.AppType;
import org.broadinstitute.dsde.rawls.model.WorkspaceName
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.stream.Materializer
import scala.concurrent.ExecutionContext
import org.broadinstitute.dsde.rawls.util.{HttpClientUtilsStandard, Retry}

import scala.concurrent.{Future, ExecutionContext}
import org.broadinstitute.dsde.workbench.client.leonardo.ApiClient


class HttpLeonardoDAO(leonardoBasePath: String)(implicit
  val system: ActorSystem,
  val materializer: Materializer,
  val executionContext: ExecutionContext)
  extends DsdeHttpDAO with LeonardoDAO {
    val http = Http(system)
    val httpClientUtils = HttpClientUtilsStandard()

  def getAppsV2leonardoApi(accessToken: String): AppsV2Api = {
    val apiClient = new ApiClient()
    apiClient.setAccessToken(accessToken)
    apiClient.setBasePath(leonardoBasePath)
    new AppsV2Api(apiClient)
  }

  
  def createWDSInstance(token: String, workspaceId: String, appName: WorkspaceName): Unit = {
    val createAppRequest = new CreateAppRequest()
    createAppRequest.setAppType(AppType.CROMWELL)
    getAppsV2leonardoApi(token).createAppV2(workspaceId, appName.name, createAppRequest);
  }

}
