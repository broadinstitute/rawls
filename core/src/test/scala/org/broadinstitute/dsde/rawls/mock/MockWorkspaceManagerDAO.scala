package org.broadinstitute.dsde.rawls.mock

import java.util.UUID

import akka.http.scaladsl.model.StatusCodes
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.WorkspaceManagerDAO
import org.broadinstitute.dsde.rawls.model.{ErrorReport, UserInfo}
import org.broadinstitute.dsde.rawls.model.workspacemanager.{WMCreateDataReferenceRequest, WMCreateWorkspaceResponse, WMDataReferenceResponse, WMGetWorkspaceResponse}
import spray.json.{JsObject, JsString}

import scala.collection.concurrent.TrieMap
import scala.concurrent.Future

class MockWorkspaceManagerDAO extends WorkspaceManagerDAO {

  val references: TrieMap[UUID, (Option[String], Option[JsObject])] = TrieMap()

  def mockGetWorkspaceResponse(workspaceId: UUID) = WMGetWorkspaceResponse(workspaceId.toString)
  def mockCreateWorkspaceResponse(workspaceId: UUID) = WMCreateWorkspaceResponse(workspaceId.toString)
  def mockReferenceResponse(referenceId: UUID, referenceType: Option[String], reference: Option[JsObject]) = WMDataReferenceResponse(referenceId.toString, "name", None, referenceType, reference, None, "COPY_NOTHING")

  override def getWorkspace(workspaceId: UUID, userInfo: UserInfo): Future[WMGetWorkspaceResponse] = Future.successful(mockGetWorkspaceResponse(workspaceId))

  override def createWorkspace(workspaceId: UUID, userInfo: UserInfo): Future[WMCreateWorkspaceResponse] = Future.successful(mockCreateWorkspaceResponse(workspaceId))

  override def createDataReference(workspaceId: UUID, createDataReferenceRequest: WMCreateDataReferenceRequest, userInfo: UserInfo): Future[WMDataReferenceResponse] = {
    if(createDataReferenceRequest.reference.get.getFields("snapshot").head.equals(JsString("fakesnapshot")))
      throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.NotFound, "Not found"))
    else {
      val newId = UUID.randomUUID()
      references.put(newId, (createDataReferenceRequest.referenceType, createDataReferenceRequest.reference))
      Future.successful(mockReferenceResponse(newId, createDataReferenceRequest.referenceType, createDataReferenceRequest.reference))
    }
  }

  override def getDataReference(workspaceId: UUID, referenceId: UUID, userInfo: UserInfo): Future[WMDataReferenceResponse] = {
    val thing = references.get(referenceId).getOrElse(throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.NotFound, "Not found")))

    Future.successful(WMDataReferenceResponse(referenceId.toString, "name", None, thing._1, thing._2, None, "COPY_NOTHING"))
  }
}
