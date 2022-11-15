package org.broadinstitute.dsde.rawls.dataaccess.workspacemanager

import akka.actor.ActorSystem
import akka.stream.Materializer
import bio.terra.workspace.api.{ReferencedGcpResourceApi, ResourceApi, WorkspaceApi}
import bio.terra.workspace.client.{ApiClient, ApiException}
import bio.terra.workspace.model._
import org.broadinstitute.dsde.rawls.model.{DataReferenceDescriptionField, DataReferenceName, RawlsRequestContext}
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail

import java.util.UUID
import scala.concurrent.ExecutionContext

class HttpWorkspaceManagerDAO(apiClientProvider: WorkspaceManagerApiClientProvider)(implicit
  val system: ActorSystem,
  val materializer: Materializer,
  val executionContext: ExecutionContext
) extends WorkspaceManagerDAO {

  private def getApiClient(ctx: RawlsRequestContext): ApiClient =
    apiClientProvider.getApiClient(ctx)

  private def getWorkspaceApi(ctx: RawlsRequestContext): WorkspaceApi =
    apiClientProvider.getWorkspaceApi(ctx)

  private def getReferencedGcpResourceApi(ctx: RawlsRequestContext): ReferencedGcpResourceApi =
    new ReferencedGcpResourceApi(getApiClient(ctx))

  private def getResourceApi(ctx: RawlsRequestContext): ResourceApi =
    new ResourceApi(getApiClient(ctx))

  private def getWorkspaceApplicationApi(ctx: RawlsRequestContext) =
    apiClientProvider.getWorkspaceApplicationApi(ctx)

  private def getControlledAzureResourceApi(ctx: RawlsRequestContext) =
    apiClientProvider.getControlledAzureResourceApi(ctx)

  private def getLandingZonesApi(ctx: RawlsRequestContext) =
    apiClientProvider.getLandingZonesApi(ctx)

  private def createCommonFields(name: String) =
    new ControlledResourceCommonFields()
      .name(name)
      .cloningInstructions(CloningInstructionsEnum.NOTHING)
      .accessScope(AccessScope.SHARED_ACCESS)
      .managedBy(ManagedBy.USER)

  override def getWorkspace(workspaceId: UUID, ctx: RawlsRequestContext): WorkspaceDescription =
    getWorkspaceApi(ctx).getWorkspace(workspaceId, null) // use default value for role

  override def createWorkspace(workspaceId: UUID, ctx: RawlsRequestContext): CreatedWorkspace =
    getWorkspaceApi(ctx).createWorkspace(new CreateWorkspaceRequestBody().id(workspaceId))

  override def createWorkspaceWithSpendProfile(workspaceId: UUID,
                                               displayName: String,
                                               spendProfileId: String,
                                               ctx: RawlsRequestContext
  ): CreatedWorkspace =
    getWorkspaceApi(ctx).createWorkspace(
      new CreateWorkspaceRequestBody()
        .id(workspaceId)
        .displayName(displayName)
        .spendProfile(spendProfileId)
        .stage(WorkspaceStageModel.MC_WORKSPACE)
    )

  override def createAzureWorkspaceCloudContext(workspaceId: UUID,
                                                azureTenantId: String,
                                                azureResourceGroupId: String,
                                                azureSubscriptionId: String,
                                                ctx: RawlsRequestContext
  ): CreateCloudContextResult = {
    val jobControlId = UUID.randomUUID().toString
    val azureContext = new AzureContext()
      .tenantId(azureTenantId)
      .subscriptionId(azureSubscriptionId)
      .resourceGroupId(azureResourceGroupId)
    getWorkspaceApi(ctx).createCloudContext(new CreateCloudContextRequest()
                                              .cloudPlatform(CloudPlatform.AZURE)
                                              .jobControl(new JobControl().id(jobControlId))
                                              .azureContext(azureContext),
                                            workspaceId
    )
  }

  override def getWorkspaceCreateCloudContextResult(workspaceId: UUID,
                                                    jobControlId: String,
                                                    ctx: RawlsRequestContext
  ): CreateCloudContextResult =
    getWorkspaceApi(ctx).getCreateCloudContextResult(workspaceId, jobControlId)

  override def deleteWorkspace(workspaceId: UUID, ctx: RawlsRequestContext): Unit =
    getWorkspaceApi(ctx).deleteWorkspace(workspaceId)

  override def createDataRepoSnapshotReference(workspaceId: UUID,
                                               snapshotId: UUID,
                                               name: DataReferenceName,
                                               description: Option[DataReferenceDescriptionField],
                                               instanceName: String,
                                               cloningInstructions: CloningInstructionsEnum,
                                               ctx: RawlsRequestContext
  ): DataRepoSnapshotResource = {
    val snapshot = new DataRepoSnapshotAttributes().instanceName(instanceName).snapshot(snapshotId.toString)
    val commonFields =
      new ReferenceResourceCommonFields().name(name.value).cloningInstructions(CloningInstructionsEnum.NOTHING)
    description.map(d => commonFields.description(d.value))
    val request = new CreateDataRepoSnapshotReferenceRequestBody().snapshot(snapshot).metadata(commonFields)
    getReferencedGcpResourceApi(ctx).createDataRepoSnapshotReference(request, workspaceId)
  }

  override def updateDataRepoSnapshotReference(workspaceId: UUID,
                                               referenceId: UUID,
                                               updateInfo: UpdateDataRepoSnapshotReferenceRequestBody,
                                               ctx: RawlsRequestContext
  ): Unit =
    getReferencedGcpResourceApi(ctx).updateDataRepoSnapshotReferenceResource(updateInfo, workspaceId, referenceId)

  override def deleteDataRepoSnapshotReference(workspaceId: UUID, referenceId: UUID, ctx: RawlsRequestContext): Unit =
    getReferencedGcpResourceApi(ctx).deleteDataRepoSnapshotReference(workspaceId, referenceId)

  override def getDataRepoSnapshotReference(workspaceId: UUID,
                                            referenceId: UUID,
                                            ctx: RawlsRequestContext
  ): DataRepoSnapshotResource =
    getReferencedGcpResourceApi(ctx).getDataRepoSnapshotReference(workspaceId, referenceId)

  override def getDataRepoSnapshotReferenceByName(workspaceId: UUID,
                                                  refName: DataReferenceName,
                                                  ctx: RawlsRequestContext
  ): DataRepoSnapshotResource =
    getReferencedGcpResourceApi(ctx).getDataRepoSnapshotReferenceByName(workspaceId, refName.value)

  override def enumerateDataRepoSnapshotReferences(workspaceId: UUID,
                                                   offset: Int,
                                                   limit: Int,
                                                   ctx: RawlsRequestContext
  ): ResourceList =
    getResourceApi(ctx).enumerateResources(workspaceId,
                                           offset,
                                           limit,
                                           ResourceType.DATA_REPO_SNAPSHOT,
                                           StewardshipType.REFERENCED
    )

  def enableApplication(workspaceId: UUID,
                        applicationId: String,
                        ctx: RawlsRequestContext
  ): WorkspaceApplicationDescription =
    getWorkspaceApplicationApi(ctx).enableWorkspaceApplication(
      workspaceId,
      applicationId
    )

  def createAzureRelay(workspaceId: UUID,
                       region: String,
                       ctx: RawlsRequestContext
  ): CreateControlledAzureRelayNamespaceResult = {
    val jobControlId = UUID.randomUUID().toString
    getControlledAzureResourceApi(ctx).createAzureRelayNamespace(
      new CreateControlledAzureRelayNamespaceRequestBody()
        .common(
          createCommonFields(s"relay-${workspaceId}")
        )
        .azureRelayNamespace(
          new AzureRelayNamespaceCreationParameters().namespaceName(s"relay-ns-${workspaceId}").region(region)
        )
        .jobControl(new JobControl().id(jobControlId)),
      workspaceId
    )
  }

  def getCreateAzureRelayResult(workspaceId: UUID,
                                jobControlId: String,
                                ctx: RawlsRequestContext
  ): CreateControlledAzureRelayNamespaceResult =
    getControlledAzureResourceApi(ctx).getCreateAzureRelayNamespaceResult(workspaceId, jobControlId)

  def createAzureStorageAccount(workspaceId: UUID, region: String, ctx: RawlsRequestContext) = {
    // Storage account names must be unique and 3-24 characters in length, numbers and lowercase letters only.
    val prefix = workspaceId.toString.substring(0, workspaceId.toString.indexOf("-"))
    val suffix = workspaceId.toString.substring(workspaceId.toString.lastIndexOf("-") + 1)
    getControlledAzureResourceApi(ctx).createAzureStorage(
      new CreateControlledAzureStorageRequestBody()
        .common(
          createCommonFields(s"sa-${workspaceId}")
        )
        .azureStorage(
          new AzureStorageCreationParameters().storageAccountName(s"sa${prefix}${suffix}").region(region)
        ),
      workspaceId
    )
  }

  def createAzureStorageContainer(workspaceId: UUID, storageAccountId: UUID, ctx: RawlsRequestContext) =
    getControlledAzureResourceApi(ctx).createAzureStorageContainer(
      new CreateControlledAzureStorageContainerRequestBody()
        .common(
          createCommonFields(s"sc-${workspaceId}").cloningInstructions(CloningInstructionsEnum.DEFINITION)
        )
        .azureStorageContainer(
          new AzureStorageContainerCreationParameters()
            .storageContainerName(s"sc-${workspaceId}")
            .storageAccountId(storageAccountId)
        ),
      workspaceId
    )

  override def getRoles(workspaceId: UUID, ctx: RawlsRequestContext) = getWorkspaceApi(ctx).getRoles(workspaceId)

  override def grantRole(workspaceId: UUID, email: WorkbenchEmail, role: IamRole, ctx: RawlsRequestContext): Unit =
    getWorkspaceApi(ctx).grantRole(
      new GrantRoleRequestBody().memberEmail(email.value),
      workspaceId,
      role
    )

  override def removeRole(workspaceId: UUID, email: WorkbenchEmail, role: IamRole, ctx: RawlsRequestContext): Unit =
    getWorkspaceApi(ctx).removeRole(workspaceId, role, email.value)

  override def createLandingZone(definition: String,
                                 version: String,
                                 billingProfileId: UUID,
                                 ctx: RawlsRequestContext
  ): CreateLandingZoneResult = {
    val jobControlId = UUID.randomUUID().toString
    getLandingZonesApi(ctx).createAzureLandingZone(
      new CreateAzureLandingZoneRequestBody()
        .definition(definition)
        .version(version)
        .billingProfileId(billingProfileId)
        .jobControl(new JobControl().id(jobControlId))
    )
  }

  override def getCreateAzureLandingZoneResult(jobId: String, ctx: RawlsRequestContext): AzureLandingZoneResult =
    getLandingZonesApi(ctx).getCreateAzureLandingZoneResult(jobId)

  override def deleteLandingZone(landingZoneId: UUID, ctx: RawlsRequestContext): DeleteAzureLandingZoneResult = {
    val jobControlId = UUID.randomUUID().toString
    getLandingZonesApi(ctx).deleteAzureLandingZone(
      new DeleteAzureLandingZoneRequestBody()
        .jobControl(new JobControl().id(jobControlId)),
      landingZoneId
    )
  }

  @throws(classOf[ApiException])
  override def getStatus(): Unit =
    apiClientProvider.getUnauthenticatedApi().serviceStatus()
}
