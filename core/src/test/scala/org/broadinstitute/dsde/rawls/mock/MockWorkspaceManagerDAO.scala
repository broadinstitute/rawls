package org.broadinstitute.dsde.rawls.mock

import java.util.UUID

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import bio.terra.workspace.client.ApiException
import bio.terra.workspace.model._
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.WorkspaceManagerDAO
import org.broadinstitute.dsde.rawls.model.{DataReferenceName, ErrorReport}

import scala.collection.JavaConverters._
import scala.collection.concurrent.TrieMap

class MockWorkspaceManagerDAO extends WorkspaceManagerDAO {

  val references: TrieMap[(UUID, UUID), DataReferenceDescription] = TrieMap()

  def mockGetWorkspaceResponse(workspaceId: UUID) = new WorkspaceDescription().id(workspaceId)
  def mockCreateWorkspaceResponse(workspaceId: UUID) = new CreatedWorkspace().id(workspaceId)
  def mockReferenceResponse(workspaceId: UUID, referenceId: UUID) = references.getOrElse((workspaceId, referenceId), throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.NotFound, "Not found")))
  def mockEnumerateReferenceResponse(workspaceId: UUID) = new DataReferenceList().resources(references.collect {
    case ((wsId, _), refDescription) if wsId == workspaceId => refDescription
  }.toList.asJava)

  override def getWorkspace(workspaceId: UUID, accessToken: OAuth2BearerToken): WorkspaceDescription = mockGetWorkspaceResponse(workspaceId)

  override def createWorkspace(workspaceId: UUID, folderManagerAccessToken: OAuth2BearerToken, bodyAccessToken: OAuth2BearerToken): CreatedWorkspace = mockCreateWorkspaceResponse(workspaceId)

  override def deleteWorkspace(workspaceId: UUID, folderManagerAccessToken: OAuth2BearerToken, bodyAccessToken: OAuth2BearerToken): Unit = ()

  override def createDataReference(workspaceId: UUID, name: DataReferenceName, referenceType: ReferenceTypeEnum, reference: DataRepoSnapshot, cloningInstructions: CloningInstructionsEnum, accessToken: OAuth2BearerToken): DataReferenceDescription = {
    if(reference.toString.contains("fakesnapshot"))
      throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.NotFound, "Not found"))
    else {
      val newId = UUID.randomUUID()
      val ref = new DataReferenceDescription().referenceId(newId).name(name.value).workspaceId(workspaceId).referenceType(referenceType).reference(reference).cloningInstructions(CloningInstructionsEnum.NOTHING)
      references.put((workspaceId, newId), ref)
      mockReferenceResponse(workspaceId, newId)
    }
  }

  override def getDataReference(workspaceId: UUID, referenceId: UUID, accessToken: OAuth2BearerToken): DataReferenceDescription = {
    mockReferenceResponse(workspaceId, referenceId)
  }

  override def getDataReferenceByName(workspaceId: UUID, refType: ReferenceTypeEnum, refName: DataReferenceName, accessToken: OAuth2BearerToken): DataReferenceDescription = {
    this.references.find {
      case ((workspaceUUID, _), ref) => workspaceUUID == workspaceId && ref.getName == refName.value && ref.getReferenceType == refType
    }.getOrElse(throw new ApiException(StatusCodes.NotFound.intValue, s"$refType/$refName not found in workspace $workspaceId"))._2
  }

  override def enumerateDataReferences(workspaceId: UUID, offset: Int, limit: Int, accessToken: OAuth2BearerToken): DataReferenceList = {
    mockEnumerateReferenceResponse(workspaceId)
  }

  override def deleteDataReference(workspaceId: UUID, referenceId: UUID, accessToken: OAuth2BearerToken): Unit = {
    references.getOrElse((workspaceId, referenceId), throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.NotFound, "Not found")))
    references -= ((workspaceId, referenceId))
  }
}
