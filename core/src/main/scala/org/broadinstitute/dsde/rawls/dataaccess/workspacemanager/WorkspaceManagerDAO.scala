package org.broadinstitute.dsde.rawls.dataaccess.workspacemanager

import java.util.UUID

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import bio.terra.workspace.model._
import org.broadinstitute.dsde.rawls.model.{DataReferenceDescriptionField, DataReferenceName}
import org.broadinstitute.dsde.workbench.model.ErrorReportSource

trait WorkspaceManagerDAO {
  val errorReportSource = ErrorReportSource("WorkspaceManager")

  def getWorkspace(workspaceId: UUID, accessToken: OAuth2BearerToken): WorkspaceDescription
  def createWorkspace(workspaceId: UUID, accessToken: OAuth2BearerToken): CreatedWorkspace
  def deleteWorkspace(workspaceId: UUID, accessToken: OAuth2BearerToken): Unit
  def createDataReference(workspaceId: UUID, name: DataReferenceName, description: Option[DataReferenceDescriptionField], referenceType: ReferenceTypeEnum, reference: DataRepoSnapshot, cloningInstructions: CloningInstructionsEnum, accessToken: OAuth2BearerToken): DataReferenceDescription
  def updateDataReference(workspaceId: UUID, referenceId: UUID, updateInfo: UpdateDataReferenceRequestBody, accessToken: OAuth2BearerToken): Unit
  def deleteDataReference(workspaceId: UUID, referenceId: UUID, accessToken: OAuth2BearerToken): Unit
  def getDataReference(workspaceId: UUID, referenceId: UUID, accessToken: OAuth2BearerToken): DataReferenceDescription
  def getDataReferenceByName(workspaceId: UUID, refType: ReferenceTypeEnum, refName: DataReferenceName, accessToken: OAuth2BearerToken): DataReferenceDescription
  def enumerateDataReferences(workspaceId: UUID, offset: Int, limit: Int, accessToken: OAuth2BearerToken): DataReferenceList
  def createBigQueryDatasetReference(workspaceId: UUID, metadata: DataReferenceRequestMetadata, dataset: GoogleBigQueryDatasetUid, accessToken: OAuth2BearerToken): BigQueryDatasetReference

}
