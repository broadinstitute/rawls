package org.broadinstitute.dsde.rawls.mock

import java.util.UUID

import akka.http.scaladsl.model.StatusCodes
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.WorkspaceManagerDAO
import org.broadinstitute.dsde.rawls.model.{ErrorReport, UserInfo}
import bio.terra.workspace.model.{CreatedWorkspace, DataReferenceDescription, WorkspaceDescription}
import spray.json.{JsObject, JsString}

import scala.collection.concurrent.TrieMap
import scala.concurrent.Future

class MockWorkspaceManagerDAO extends WorkspaceManagerDAO {

  val references: TrieMap[UUID, (String, JsObject)] = TrieMap()

  def mockGetWorkspaceResponse(workspaceId: UUID) = new WorkspaceDescription().id(workspaceId)
  def mockCreateWorkspaceResponse(workspaceId: UUID) = new CreatedWorkspace().id(workspaceId.toString)
  def mockReferenceResponse(referenceId: UUID, referenceType: String, reference: JsObject) = new DataReferenceDescription().referenceId(referenceId).name("name").referenceType(DataReferenceDescription.ReferenceTypeEnum.fromValue(referenceType)).reference(reference.toString()).cloningInstructions(DataReferenceDescription.CloningInstructionsEnum.NOTHING)

  override def getWorkspace(workspaceId: UUID, userInfo: UserInfo): Future[WorkspaceDescription] = Future.successful(mockGetWorkspaceResponse(workspaceId))

  override def createWorkspace(workspaceId: UUID, userInfo: UserInfo): Future[CreatedWorkspace] = Future.successful(mockCreateWorkspaceResponse(workspaceId))

  override def createDataReference(workspaceId: UUID, name: String, referenceType: String, reference: JsObject, cloningInstructions: String, userInfo: UserInfo): Future[DataReferenceDescription] = {
    if(reference.getFields("snapshot").head.equals(JsString("fakesnapshot")))
      throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.NotFound, "Not found"))
    else {
      val newId = UUID.randomUUID()
      references.put(newId, (referenceType, reference))
      Future.successful(mockReferenceResponse(newId, referenceType, reference))
    }
  }

  override def getDataReference(workspaceId: UUID, referenceId: UUID, userInfo: UserInfo): Future[DataReferenceDescription] = {
    val thing = references.get(referenceId).getOrElse(throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.NotFound, "Not found")))

    Future.successful(mockReferenceResponse(referenceId, thing._1, thing._2))
  }
}
