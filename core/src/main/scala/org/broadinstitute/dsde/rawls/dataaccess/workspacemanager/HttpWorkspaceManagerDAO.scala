package org.broadinstitute.dsde.rawls.dataaccess.workspacemanager

import java.util.UUID

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.client.RequestBuilding
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.stream.Materializer
import org.broadinstitute.dsde.rawls.dataaccess.DsdeHttpDAO
import org.broadinstitute.dsde.rawls.model.UserInfo
import org.broadinstitute.dsde.rawls.model.workspacemanager.WorkspaceManagerJsonSupport._
import org.broadinstitute.dsde.rawls.model.workspacemanager._
import org.broadinstitute.dsde.rawls.util.{HttpClientUtilsStandard, Retry}

import scala.concurrent.{ExecutionContext, Future}

class HttpWorkspaceManagerDAO(baseWorkspaceManagerUrl: String)(implicit val system: ActorSystem, val materializer: Materializer, val executionContext: ExecutionContext) extends WorkspaceManagerDAO with DsdeHttpDAO with Retry {

  override val http = Http(system)
  override val httpClientUtils = HttpClientUtilsStandard()

  private val workspaceManagerUrl = baseWorkspaceManagerUrl

  override def getWorkspace(workspaceId: UUID, userInfo: UserInfo): Future[WMGetWorkspaceResponse] = {
    def getWorkspaceUrl(workspaceId: UUID) = workspaceManagerUrl + s"/api/v1/workspaces/${workspaceId.toString}"
    val httpRequest = RequestBuilding.Get(getWorkspaceUrl(workspaceId))

    pipeline[WMGetWorkspaceResponse](userInfo) apply httpRequest
  }

  override def createWorkspace(workspaceId: UUID, userInfo: UserInfo): Future[WMCreateWorkspaceResponse] = {
    val createWorkspaceUrl = workspaceManagerUrl + "/api/v1/workspaces"
    val httpRequest = RequestBuilding.Post(createWorkspaceUrl, WMCreateWorkspaceRequest(workspaceId.toString, userInfo.accessToken.token, None, None))

    pipeline[WMCreateWorkspaceResponse](userInfo) apply httpRequest
  }

  override def createDataReference(workspaceId: UUID, createDataReferenceRequest: WMCreateDataReferenceRequest, userInfo: UserInfo): Future[WMDataReferenceResponse] = {
    def createDataReferenceUrl(workspaceId: UUID) = workspaceManagerUrl + s"/api/v1/workspaces/${workspaceId.toString}/datareferences"
    val httpRequest = RequestBuilding.Post(createDataReferenceUrl(workspaceId), createDataReferenceRequest)

    pipeline[WMDataReferenceResponse](userInfo) apply httpRequest
  }

  override def getDataReference(workspaceId: UUID, snapshotId: UUID, userInfo: UserInfo): Future[WMDataReferenceResponse] = {
    def getDataReferenceUrl(workspaceId: UUID, snapshotId: UUID) = workspaceManagerUrl + s"/api/v1/workspaces/${workspaceId.toString}/datareferences/${snapshotId.toString}"
    val httpRequest = RequestBuilding.Get(getDataReferenceUrl(workspaceId, snapshotId))

    pipeline[WMDataReferenceResponse](userInfo) apply httpRequest
  }

}
