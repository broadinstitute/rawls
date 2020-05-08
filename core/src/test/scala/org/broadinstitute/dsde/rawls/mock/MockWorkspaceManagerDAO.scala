package org.broadinstitute.dsde.rawls.mock

import java.util.UUID

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.WorkspaceManagerDAO
import org.broadinstitute.dsde.rawls.model.ErrorReport
import bio.terra.workspace.model.{CreatedWorkspace, DataReferenceDescription, WorkspaceDescription}
import spray.json.{JsObject, JsString}

import scala.collection.concurrent.TrieMap

class MockWorkspaceManagerDAO extends WorkspaceManagerDAO {

  val references: TrieMap[UUID, DataReferenceDescription] = TrieMap()

  def mockGetWorkspaceResponse(workspaceId: UUID) = new WorkspaceDescription().id(workspaceId)
  def mockCreateWorkspaceResponse(workspaceId: UUID) = new CreatedWorkspace().id(workspaceId.toString)
  def mockReferenceResponse(referenceId: UUID) = references.get(referenceId).getOrElse(throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.NotFound, "Not found")))

  override def getWorkspace(workspaceId: UUID, accessToken: OAuth2BearerToken): WorkspaceDescription = mockGetWorkspaceResponse(workspaceId)

  override def createWorkspace(workspaceId: UUID, folderManagerAccessToken: OAuth2BearerToken, bodyAccessToken: OAuth2BearerToken): CreatedWorkspace = mockCreateWorkspaceResponse(workspaceId)

  override def createDataReference(workspaceId: UUID, name: String, referenceType: String, reference: JsObject, cloningInstructions: String, accessToken: OAuth2BearerToken): DataReferenceDescription = {
    if(reference.getFields("snapshot").head.equals(JsString("fakesnapshot")))
      throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.NotFound, "Not found"))
    else {
      val newId = UUID.randomUUID()
      val ref = new DataReferenceDescription().referenceId(newId).name("name").workspaceId(workspaceId).referenceType(DataReferenceDescription.ReferenceTypeEnum.fromValue(referenceType)).reference(reference.toString()).cloningInstructions(DataReferenceDescription.CloningInstructionsEnum.NOTHING)
      references.put(newId, ref)
      mockReferenceResponse(newId)
    }
  }

  override def getDataReference(workspaceId: UUID, referenceId: UUID, accessToken: OAuth2BearerToken): DataReferenceDescription = {
    mockReferenceResponse(referenceId)
  }
}
