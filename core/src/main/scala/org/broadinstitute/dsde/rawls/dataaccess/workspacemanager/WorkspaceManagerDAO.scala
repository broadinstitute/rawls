package org.broadinstitute.dsde.rawls.dataaccess.workspacemanager

import java.util.UUID

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import bio.terra.workspace.model.{CreatedWorkspace, DataReferenceDescription, DataReferenceList, WorkspaceDescription}
import org.broadinstitute.dsde.workbench.model.ErrorReportSource

trait WorkspaceManagerDAO {
  val errorReportSource = ErrorReportSource("WorkspaceManager")

  def getWorkspace(workspaceId: UUID, accessToken: OAuth2BearerToken): WorkspaceDescription
  def createWorkspace(workspaceId: UUID, folderManagerAccessToken: OAuth2BearerToken, bodyAccessToken: OAuth2BearerToken): CreatedWorkspace
  def deleteWorkspace(workspaceId: UUID, folderManagerAccessToken: OAuth2BearerToken, bodyAccessToken: OAuth2BearerToken): Unit
  def createDataReference(workspaceId: UUID, name: String, referenceType: String, reference: String, cloningInstructions: String, accessToken: OAuth2BearerToken): DataReferenceDescription
  def deleteDataReference(workspaceId: UUID, referenceId: UUID, accessToken: OAuth2BearerToken): Unit
  def getDataReference(workspaceId: UUID, referenceId: UUID, accessToken: OAuth2BearerToken): DataReferenceDescription
  def getDataReferenceByName(workspaceId: UUID, refType: String, refName: String, accessToken: OAuth2BearerToken): DataReferenceDescription
  def enumerateDataReferences(workspaceId: UUID, offset: Int, limit: Int, accessToken: OAuth2BearerToken): DataReferenceList

}
