package org.broadinstitute.dsde.rawls.dataaccess.workspacemanager

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import bio.terra.workspace.model._
import org.broadinstitute.dsde.rawls.model.{DataReferenceDescriptionField, DataReferenceName}
import org.broadinstitute.dsde.workbench.model.ErrorReportSource

import java.util.UUID

trait WorkspaceManagerDAO {
  val errorReportSource = ErrorReportSource("WorkspaceManager")

  def getWorkspace(workspaceId: UUID, accessToken: OAuth2BearerToken): WorkspaceDescription
  def createWorkspace(workspaceId: UUID, accessToken: OAuth2BearerToken): CreatedWorkspace
  def createWorkspaceWithSpendProfile(workspaceId: UUID, displayName: String, spendProfileId: String, accessToken: OAuth2BearerToken): CreatedWorkspace
  def createAzureWorkspaceCloudContext(workspaceId: UUID, azureTenantId: String, azureResourceGroupId: String, azureSubscriptionId: String, accessToken: OAuth2BearerToken): CreateCloudContextResult
  def getWorkspaceCreateCloudContextResult(workspaceId: UUID, jobControlId: String, accessToken: OAuth2BearerToken): CreateCloudContextResult
  def deleteWorkspace(workspaceId: UUID, accessToken: OAuth2BearerToken): Unit
  def createDataRepoSnapshotReference(workspaceId: UUID, snapshotId: UUID, name: DataReferenceName, description: Option[DataReferenceDescriptionField], instanceName: String, cloningInstructions: CloningInstructionsEnum, accessToken: OAuth2BearerToken): DataRepoSnapshotResource
  def updateDataRepoSnapshotReference(workspaceId: UUID, referenceId: UUID, updateInfo: UpdateDataRepoSnapshotReferenceRequestBody, accessToken: OAuth2BearerToken): Unit
  def deleteDataRepoSnapshotReference(workspaceId: UUID, referenceId: UUID, accessToken: OAuth2BearerToken): Unit
  def getDataRepoSnapshotReference(workspaceId: UUID, referenceId: UUID, accessToken: OAuth2BearerToken): DataRepoSnapshotResource
  def getDataRepoSnapshotReferenceByName(workspaceId: UUID, refName: DataReferenceName, accessToken: OAuth2BearerToken): DataRepoSnapshotResource
  def enumerateDataRepoSnapshotReferences(workspaceId: UUID, offset: Int, limit: Int, accessToken: OAuth2BearerToken): ResourceList
  def enableApplication(workspaceId: UUID, applicationId: String, accessToken: OAuth2BearerToken): WorkspaceApplicationDescription
  def createAzureRelay(workspaceId: UUID, region: String, accessToken: OAuth2BearerToken): CreateControlledAzureRelayNamespaceResult
  def getCreateAzureRelayResult(workspaceId: UUID, jobControlId: String, accessToken: OAuth2BearerToken): CreateControlledAzureRelayNamespaceResult
}
