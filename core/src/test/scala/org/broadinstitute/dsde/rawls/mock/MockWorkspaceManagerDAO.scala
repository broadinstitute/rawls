package org.broadinstitute.dsde.rawls.mock

import java.util.UUID

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.WorkspaceManagerDAO
import org.broadinstitute.dsde.rawls.model.ErrorReport
import bio.terra.workspace.model.{CreatedWorkspace, DataReferenceDescription, DataReferenceList, WorkspaceDescription}
import scala.collection.JavaConverters._

import scala.collection.concurrent.TrieMap

class MockWorkspaceManagerDAO extends WorkspaceManagerDAO {

  val references: TrieMap[(UUID, UUID), DataReferenceDescription] = TrieMap()

  def mockGetWorkspaceResponse(workspaceId: UUID) = new WorkspaceDescription().id(workspaceId)
  def mockCreateWorkspaceResponse(workspaceId: UUID) = new CreatedWorkspace().id(workspaceId.toString)
  def mockReferenceResponse(workspaceId: UUID, referenceId: UUID) = references.get((workspaceId, referenceId)).getOrElse(throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.NotFound, "Not found")))
  def mockEnumerateReferenceResponse(workspaceId: UUID) = new DataReferenceList().resources(references.collect {
    case ((wsId, _), refDescription) if wsId == workspaceId => refDescription
  }.toList.asJava)

  override def getWorkspace(workspaceId: UUID, accessToken: OAuth2BearerToken): WorkspaceDescription = mockGetWorkspaceResponse(workspaceId)

  override def createWorkspace(workspaceId: UUID, folderManagerAccessToken: OAuth2BearerToken, bodyAccessToken: OAuth2BearerToken): CreatedWorkspace = mockCreateWorkspaceResponse(workspaceId)

  override def deleteWorkspace(workspaceId: UUID, folderManagerAccessToken: OAuth2BearerToken, bodyAccessToken: OAuth2BearerToken): Unit = ()

  override def createDataReference(workspaceId: UUID, name: String, referenceType: String, reference: String, cloningInstructions: String, accessToken: OAuth2BearerToken): DataReferenceDescription = {
    if(reference.contains("fakesnapshot"))
      throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.NotFound, "Not found"))
    else {
      val newId = UUID.randomUUID()
      val ref = new DataReferenceDescription().referenceId(newId).name(name).workspaceId(workspaceId).referenceType(DataReferenceDescription.ReferenceTypeEnum.fromValue(referenceType)).reference(reference.toString()).cloningInstructions(DataReferenceDescription.CloningInstructionsEnum.NOTHING)
      references.put((workspaceId, newId), ref)
      mockReferenceResponse(workspaceId, newId)
    }
  }

  override def getDataReference(workspaceId: UUID, referenceId: UUID, accessToken: OAuth2BearerToken): DataReferenceDescription = {
    mockReferenceResponse(workspaceId, referenceId)
  }

  override def getDataReferenceByName(workspaceId: UUID, refType: String, refName: String, accessToken: OAuth2BearerToken): DataReferenceDescription = ???

  override def enumerateDataReferences(workspaceId: UUID, offset: Int, limit: Int, accessToken: OAuth2BearerToken): DataReferenceList = {
    mockEnumerateReferenceResponse(workspaceId)
  }

  override def deleteDataReference(workspaceId: UUID, referenceId: UUID, accessToken: OAuth2BearerToken): Unit = {
    references.get((workspaceId, referenceId)).getOrElse(throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.NotFound, "Not found")))
    references -= ((workspaceId, referenceId))
  }
}
