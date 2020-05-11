package org.broadinstitute.dsde.rawls.dataaccess.workspacemanager

import java.util.UUID

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import bio.terra.workspace.model.{CreatedWorkspace, DataReferenceDescription, DataReferenceList, WorkspaceDescription}
import org.broadinstitute.dsde.workbench.model.ErrorReportSource
import spray.json.JsObject

trait WorkspaceManagerDAO {
  val errorReportSource = ErrorReportSource("WorkspaceManager")

  def getWorkspace(workspaceId: UUID, accessToken: OAuth2BearerToken): WorkspaceDescription
  def createWorkspace(workspaceId: UUID, folderManagerAccessToken: OAuth2BearerToken, bodyAccessToken: OAuth2BearerToken): CreatedWorkspace
  def createDataReference(workspaceId: UUID, name: String, referenceType: String, reference: JsObject, cloningInstructions: String, accessToken: OAuth2BearerToken): DataReferenceDescription
  def getDataReference(workspaceId: UUID, referenceId: UUID, accessToken: OAuth2BearerToken): DataReferenceDescription
  def enumerateDataReferences(workspaceId: UUID, limit: int, offset: int, accessToken: OAuth2BearerToken): DataReferenceList

}
