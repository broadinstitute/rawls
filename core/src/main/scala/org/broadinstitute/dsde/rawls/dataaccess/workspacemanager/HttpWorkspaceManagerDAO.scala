package org.broadinstitute.dsde.rawls.dataaccess.workspacemanager

import akka.actor.ActorSystem
import akka.stream.Materializer
import bio.terra.profile.model.ProfileModel
import bio.terra.workspace.api.{ReferencedGcpResourceApi, ResourceApi, WorkspaceApi}
import bio.terra.workspace.client.ApiClient
import bio.terra.workspace.model._
import org.broadinstitute.dsde.rawls.model.{DataReferenceDescriptionField, DataReferenceName, RawlsRequestContext}
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail

import java.util.UUID
import scala.concurrent.ExecutionContext
import scala.jdk.CollectionConverters._

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
    apiClientProvider.getResourceApi(ctx)

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

  override def cloneWorkspace(sourceWorkspaceId: UUID,
                              workspaceId: UUID,
                              displayName: String,
                              spendProfile: ProfileModel,
                              ctx: RawlsRequestContext,
                              location: Option[String]
  ): CloneWorkspaceResult =
    getWorkspaceApi(ctx).cloneWorkspace(
      new CloneWorkspaceRequest()
        .destinationWorkspaceId(workspaceId)
        .displayName(displayName)
        .spendProfile(spendProfile.getId.toString)
        .location(location.orNull),
      sourceWorkspaceId
    )

  override def getJob(jobControlId: String, ctx: RawlsRequestContext): JobReport =
    apiClientProvider.getJobsApi(ctx).retrieveJob(jobControlId)

  override def getCloneWorkspaceResult(workspaceId: UUID,
                                       jobControlId: String,
                                       ctx: RawlsRequestContext
  ): CloneWorkspaceResult =
    getWorkspaceApi(ctx).getCloneWorkspaceResult(workspaceId, jobControlId)

  override def createAzureWorkspaceCloudContext(workspaceId: UUID,
                                                ctx: RawlsRequestContext
  ): CreateCloudContextResult = {
    val jobControlId = UUID.randomUUID().toString
    getWorkspaceApi(ctx).createCloudContext(new CreateCloudContextRequest()
                                              .cloudPlatform(CloudPlatform.AZURE)
                                              .jobControl(new JobControl().id(jobControlId)),
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

  override def enableApplication(workspaceId: UUID,
                                 applicationId: String,
                                 ctx: RawlsRequestContext
  ): WorkspaceApplicationDescription =
    getWorkspaceApplicationApi(ctx).enableWorkspaceApplication(
      workspaceId,
      applicationId
    )

  override def disableApplication(workspaceId: UUID,
                                  applicationId: String,
                                  ctx: RawlsRequestContext
  ): WorkspaceApplicationDescription =
    getWorkspaceApplicationApi(ctx).disableWorkspaceApplication(workspaceId, applicationId)

  override def createAzureStorageAccount(workspaceId: UUID,
                                         region: String,
                                         ctx: RawlsRequestContext
  ): CreatedControlledAzureStorage = {
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

  override def createAzureStorageContainer(workspaceId: UUID,
                                           storageContainerName: String,
                                           storageAccountId: Option[UUID],
                                           ctx: RawlsRequestContext
  ) = {
    val creationParams =
      new AzureStorageContainerCreationParameters()
        .storageContainerName(storageContainerName)
        .storageAccountId(storageAccountId.orNull)

    val requestBody = new CreateControlledAzureStorageContainerRequestBody()
      .common(
        createCommonFields(storageContainerName).cloningInstructions(CloningInstructionsEnum.NOTHING)
      )
      .azureStorageContainer(creationParams)

    getControlledAzureResourceApi(ctx)
      .createAzureStorageContainer(requestBody, workspaceId)
  }

  override def cloneAzureStorageContainer(sourceWorkspaceId: UUID,
                                          destinationWorkspaceId: UUID,
                                          sourceContainerId: UUID,
                                          destinationContainerName: String,
                                          cloningInstructions: CloningInstructionsEnum,
                                          ctx: RawlsRequestContext
  ): CloneControlledAzureStorageContainerResult = {
    val jobControlId = UUID.randomUUID().toString
    getControlledAzureResourceApi(ctx).cloneAzureStorageContainer(
      new CloneControlledAzureStorageContainerRequest()
        .destinationWorkspaceId(destinationWorkspaceId)
        .name(destinationContainerName)
        .cloningInstructions(cloningInstructions)
        .jobControl(new JobControl().id(jobControlId)),
      sourceWorkspaceId,
      sourceContainerId
    )
  }

  override def getCloneAzureStorageContainerResult(workspaceId: UUID,
                                                   jobId: String,
                                                   ctx: RawlsRequestContext
  ): CloneControlledAzureStorageContainerResult =
    getControlledAzureResourceApi(ctx).getCloneAzureStorageContainerResult(workspaceId, jobId)

  override def enumerateStorageContainers(workspaceId: UUID,
                                          offset: Int,
                                          limit: Int,
                                          ctx: RawlsRequestContext
  ): ResourceList =
    getResourceApi(ctx).enumerateResources(workspaceId,
                                           offset,
                                           limit,
                                           ResourceType.AZURE_STORAGE_CONTAINER,
                                           StewardshipType.CONTROLLED
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
                                 landingZoneParameters: Map[String, String],
                                 billingProfileId: UUID,
                                 ctx: RawlsRequestContext,
                                 landingZoneId: Option[UUID] = None
  ): CreateLandingZoneResult = {
    val jobControlId = UUID.randomUUID().toString
    var foo = new CreateAzureLandingZoneRequestBody()
      .definition(definition)
      .version(version)
      .billingProfileId(billingProfileId)
      .parameters(
        landingZoneParameters
          .map { case (k, v) =>
            new AzureLandingZoneParameter().key(k).value(v)
          }
          .toList
          .asJava
      )
      .jobControl(new JobControl().id(jobControlId))
    if (landingZoneId.isDefined) {
      foo = foo.landingZoneId(landingZoneId.get)
    }
    getLandingZonesApi(ctx).createAzureLandingZone(foo)
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

  override def getLandingZone(landingZoneId: UUID, ctx: RawlsRequestContext): AzureLandingZone = {
    getLandingZonesApi(ctx).getAzureLandingZone(landingZoneId)
  }

  def getDeleteLandingZoneResult(jobId: String,
                                 landingZoneId: UUID,
                                 ctx: RawlsRequestContext
  ): DeleteAzureLandingZoneJobResult =
    getLandingZonesApi(ctx).getDeleteAzureLandingZoneResult(landingZoneId, jobId)

  override def throwWhenUnavailable(): Unit =
    apiClientProvider.getUnauthenticatedApi().serviceStatus()
}
