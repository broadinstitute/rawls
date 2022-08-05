package org.broadinstitute.dsde.rawls.dataaccess.workspacemanager

import akka.actor.ActorSystem
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.stream.Materializer
import bio.terra.workspace.api.{ReferencedGcpResourceApi, ResourceApi, WorkspaceApi}
import bio.terra.workspace.client.ApiClient
import bio.terra.workspace.model._
import org.broadinstitute.dsde.rawls.model.{DataReferenceDescriptionField, DataReferenceName}

import java.util.UUID
import scala.concurrent.ExecutionContext

class HttpWorkspaceManagerDAO(apiClientProvider: WorkspaceManagerApiClientProvider)(implicit val system: ActorSystem, val materializer: Materializer, val executionContext: ExecutionContext) extends WorkspaceManagerDAO {

  private def getApiClient(accessToken: String): ApiClient = {
    apiClientProvider.getApiClient(accessToken)
  }

  private def getWorkspaceApi(accessToken: OAuth2BearerToken): WorkspaceApi = {
    new WorkspaceApi(getApiClient(accessToken.token))
  }

  private def getReferencedGcpResourceApi(accessToken: OAuth2BearerToken): ReferencedGcpResourceApi = {
    new ReferencedGcpResourceApi(getApiClient(accessToken.token))
  }

  private def getResourceApi(accessToken: OAuth2BearerToken): ResourceApi = {
    new ResourceApi(getApiClient(accessToken.token))
  }

  private def getWorkspaceApplicationApi(accessToken: OAuth2BearerToken) = {
    apiClientProvider.getWorkspaceApplicationApi(accessToken.token)
  }

  private def getControlledAzureResourceApi(accessToken: OAuth2BearerToken) = {
    apiClientProvider.getControlledAzureResourceApi(accessToken.token)
  }

  private def createCommonFields(name: String) = {
      new ControlledResourceCommonFields().name(name).
        cloningInstructions(CloningInstructionsEnum.NOTHING).
        accessScope(AccessScope.SHARED_ACCESS).
        managedBy(ManagedBy.USER)
  }

  override def getWorkspace(workspaceId: UUID, accessToken: OAuth2BearerToken): WorkspaceDescription = {
    getWorkspaceApi(accessToken).getWorkspace(workspaceId)
  }

  override def createWorkspace(workspaceId: UUID, accessToken: OAuth2BearerToken): CreatedWorkspace = {
    getWorkspaceApi(accessToken).createWorkspace(new CreateWorkspaceRequestBody().id(workspaceId))
  }

  override def createWorkspaceWithSpendProfile(workspaceId: UUID,
                                               displayName: String,
                                               spendProfileId: String,
                                               accessToken: OAuth2BearerToken): CreatedWorkspace = {
    getWorkspaceApi(accessToken).createWorkspace(new CreateWorkspaceRequestBody()
      .id(workspaceId)
      .displayName(displayName)
      .spendProfile(spendProfileId)
      .stage(WorkspaceStageModel.MC_WORKSPACE))
  }

  override def createAzureWorkspaceCloudContext(workspaceId: UUID,
                                                azureTenantId: String,
                                                azureResourceGroupId: String,
                                                azureSubscriptionId: String,
                                                accessToken: OAuth2BearerToken): CreateCloudContextResult = {
    val jobControlId = UUID.randomUUID().toString
    val azureContext = new AzureContext().tenantId(azureTenantId).subscriptionId(azureSubscriptionId).resourceGroupId(azureResourceGroupId)
    getWorkspaceApi(accessToken).createCloudContext(
      new CreateCloudContextRequest()
        .cloudPlatform(CloudPlatform.AZURE)
        .jobControl(new JobControl().id(jobControlId))
        .azureContext(azureContext), workspaceId)
  }

  override def getWorkspaceCreateCloudContextResult(workspaceId: UUID, jobControlId: String, accessToken: OAuth2BearerToken): CreateCloudContextResult = {
    getWorkspaceApi(accessToken).getCreateCloudContextResult(workspaceId, jobControlId)
  }

  override def deleteWorkspace(workspaceId: UUID, accessToken: OAuth2BearerToken): Unit = {
    getWorkspaceApi(accessToken).deleteWorkspace(workspaceId)
  }

  override def createDataRepoSnapshotReference(workspaceId: UUID, snapshotId: UUID, name: DataReferenceName, description: Option[DataReferenceDescriptionField], instanceName: String, cloningInstructions: CloningInstructionsEnum, accessToken: OAuth2BearerToken): DataRepoSnapshotResource = {
    val snapshot = new DataRepoSnapshotAttributes().instanceName(instanceName).snapshot(snapshotId.toString)
    val commonFields = new ReferenceResourceCommonFields().name(name.value).cloningInstructions(CloningInstructionsEnum.NOTHING)
    description.map(d => commonFields.description(d.value))
    val request = new CreateDataRepoSnapshotReferenceRequestBody().snapshot(snapshot).metadata(commonFields)
    getReferencedGcpResourceApi(accessToken).createDataRepoSnapshotReference(request, workspaceId)
  }

  override def updateDataRepoSnapshotReference(workspaceId: UUID, referenceId: UUID, updateInfo: UpdateDataRepoSnapshotReferenceRequestBody, accessToken: OAuth2BearerToken): Unit = {
    getReferencedGcpResourceApi(accessToken).updateDataRepoSnapshotReferenceResource(updateInfo, workspaceId, referenceId)
  }

  override def deleteDataRepoSnapshotReference(workspaceId: UUID, referenceId: UUID, accessToken: OAuth2BearerToken): Unit = {
    getReferencedGcpResourceApi(accessToken).deleteDataRepoSnapshotReference(workspaceId, referenceId)
  }

  override def getDataRepoSnapshotReference(workspaceId: UUID, referenceId: UUID, accessToken: OAuth2BearerToken): DataRepoSnapshotResource = {
    getReferencedGcpResourceApi(accessToken).getDataRepoSnapshotReference(workspaceId, referenceId)
  }

  override def getDataRepoSnapshotReferenceByName(workspaceId: UUID, refName: DataReferenceName, accessToken: OAuth2BearerToken): DataRepoSnapshotResource = {
    getReferencedGcpResourceApi(accessToken).getDataRepoSnapshotReferenceByName(workspaceId, refName.value)
  }

  override def enumerateDataRepoSnapshotReferences(workspaceId: UUID, offset: Int, limit: Int, accessToken: OAuth2BearerToken): ResourceList = {
    getResourceApi(accessToken).enumerateResources(workspaceId, offset, limit, ResourceType.DATA_REPO_SNAPSHOT, StewardshipType.REFERENCED)
  }

  def enableApplication(workspaceId: UUID, applicationId: String, accessToken: OAuth2BearerToken): WorkspaceApplicationDescription = {
    getWorkspaceApplicationApi(accessToken).enableWorkspaceApplication(
      workspaceId, applicationId
    )
  }

  def createAzureRelay(workspaceId: UUID, region: String, accessToken: OAuth2BearerToken): CreateControlledAzureRelayNamespaceResult = {
    val jobControlId = UUID.randomUUID().toString
    getControlledAzureResourceApi(accessToken).createAzureRelayNamespace(
      new CreateControlledAzureRelayNamespaceRequestBody().common(
        createCommonFields(s"relay-${workspaceId}")
      ).azureRelayNamespace(
        new AzureRelayNamespaceCreationParameters().namespaceName(s"relay-ns-${workspaceId}").region(region)
      ).jobControl(new JobControl().id(jobControlId)),
      workspaceId
    )
  }

  def getCreateAzureRelayResult(workspaceId: UUID, jobControlId: String, accessToken: OAuth2BearerToken): CreateControlledAzureRelayNamespaceResult = {
    getControlledAzureResourceApi(accessToken).getCreateAzureRelayNamespaceResult(workspaceId, jobControlId)
  }

  def createAzureStorageAccount(workspaceId: UUID, region: String, accessToken: OAuth2BearerToken) = {
    // Storage account names must be unique and 3-24 characters in length, numbers and lowercase letters only.
    val prefix = workspaceId.toString.substring(0, workspaceId.toString.indexOf("-"))
    val suffix = workspaceId.toString.substring(workspaceId.toString.lastIndexOf("-") + 1)
    getControlledAzureResourceApi(accessToken).createAzureStorage(
      new CreateControlledAzureStorageRequestBody().common(
        createCommonFields(s"sa-${workspaceId}")
      ).azureStorage(
        new AzureStorageCreationParameters().storageAccountName(s"sa${prefix}${suffix}").region(region)
      ),
      workspaceId
    )
  }

  def createAzureStorageContainer(workspaceId: UUID, storageAccountId: UUID, accessToken: OAuth2BearerToken) = {
    getControlledAzureResourceApi(accessToken).createAzureStorageContainer(
      new CreateControlledAzureStorageContainerRequestBody().common(
        createCommonFields(s"sc-${workspaceId}")
      ).azureStorageContainer(
        new AzureStorageContainerCreationParameters().storageContainerName(s"sc-${workspaceId}").storageAccountId(storageAccountId)
      ),
      workspaceId
    )
  }
}
