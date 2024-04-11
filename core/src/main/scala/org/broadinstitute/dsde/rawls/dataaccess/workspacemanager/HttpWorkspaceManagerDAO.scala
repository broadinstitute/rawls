package org.broadinstitute.dsde.rawls.dataaccess.workspacemanager

import akka.actor.ActorSystem
import akka.stream.Materializer
import bio.terra.profile.model.ProfileModel
import bio.terra.workspace.api.{ReferencedGcpResourceApi, ResourceApi, WorkspaceApi}
import bio.terra.workspace.client.{ApiClient, ApiException}
import bio.terra.workspace.model._
import com.typesafe.scalalogging.LazyLogging
import org.apache.http.HttpStatus
import org.broadinstitute.dsde.rawls.model.WorkspaceType.WorkspaceType
import org.broadinstitute.dsde.rawls.model.{
  DataReferenceDescriptionField,
  DataReferenceName,
  RawlsRequestContext,
  WorkspaceType
}
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail

import java.util.UUID
import scala.annotation.tailrec
import scala.concurrent.ExecutionContext
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

class HttpWorkspaceManagerDAO(apiClientProvider: WorkspaceManagerApiClientProvider)(implicit
  val system: ActorSystem,
  val materializer: Materializer,
  val executionContext: ExecutionContext
) extends WorkspaceManagerDAO
    with LazyLogging {

  private def getApiClient(ctx: RawlsRequestContext): ApiClient =
    apiClientProvider.getApiClient(ctx)

  private def getWorkspaceApi(ctx: RawlsRequestContext): WorkspaceApi =
    apiClientProvider.getWorkspaceApi(ctx)

  private def getReferencedGcpResourceApi(ctx: RawlsRequestContext): ReferencedGcpResourceApi =
    apiClientProvider.getReferencedGcpResourceApi(ctx)

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

  override def listWorkspaces(ctx: RawlsRequestContext, batchSize: Int = 100): List[WorkspaceDescription] = {
    @tailrec
    def listWorkspacesLoop(offset: Int = 0, acc: List[WorkspaceDescription] = List()): List[WorkspaceDescription] =
      getWorkspaceApi(ctx).listWorkspaces(offset, batchSize, null).getWorkspaces.asScala.toList match {
        case results if results.size < batchSize => results ::: acc
        case results                             => listWorkspacesLoop(offset + batchSize, results ::: acc)
      }

    listWorkspacesLoop()
  }

  override def createWorkspace(workspaceId: UUID,
                               workspaceType: WorkspaceType,
                               policyInputs: Option[WsmPolicyInputs],
                               ctx: RawlsRequestContext
  ): CreatedWorkspace = {
    val stage = workspaceType match {
      case WorkspaceType.RawlsWorkspace => WorkspaceStageModel.RAWLS_WORKSPACE
      case WorkspaceType.McWorkspace    => WorkspaceStageModel.MC_WORKSPACE
    }

    val request = new CreateWorkspaceRequestBody().id(workspaceId).stage(stage)
    policyInputs.map(request.policies)
    getWorkspaceApi(ctx).createWorkspace(request)
  }

  override def createWorkspaceWithSpendProfile(workspaceId: UUID,
                                               displayName: String,
                                               spendProfileId: String,
                                               billingProjectNamespace: String,
                                               applicationIds: Seq[String],
                                               cloudPlatform: CloudPlatform,
                                               policyInputs: Option[WsmPolicyInputs],
                                               ctx: RawlsRequestContext
  ): CreateWorkspaceV2Result = {
    val request = new CreateWorkspaceV2Request()
      .id(workspaceId)
      .displayName(displayName)
      .spendProfile(spendProfileId)
      .stage(WorkspaceStageModel.MC_WORKSPACE)
      .projectOwnerGroupId(billingProjectNamespace)
      .applicationIds(applicationIds.asJava)
      .cloudPlatform(cloudPlatform)
      .jobControl(new JobControl().id(UUID.randomUUID().toString))

    policyInputs.map(request.policies)

    getWorkspaceApi(ctx).createWorkspaceV2(request)
  }

  override def cloneWorkspace(sourceWorkspaceId: UUID,
                              workspaceId: UUID,
                              displayName: String,
                              spendProfile: Option[ProfileModel],
                              billingProjectNamespace: String,
                              ctx: RawlsRequestContext,
                              location: Option[String]
  ): CloneWorkspaceResult = {
    val request = new CloneWorkspaceRequest()
      .destinationWorkspaceId(workspaceId)
      .displayName(displayName)
      .location(location.orNull)
      .projectOwnerGroupId(billingProjectNamespace)

    spendProfile.map(_.getId.toString).map(request.spendProfile)

    getWorkspaceApi(ctx).cloneWorkspace(
      request,
      sourceWorkspaceId
    )
  }

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

  override def getCreateWorkspaceResult(jobControlId: String, ctx: RawlsRequestContext): CreateWorkspaceV2Result =
    getWorkspaceApi(ctx).getCreateWorkspaceV2Result(jobControlId)

  override def deleteWorkspace(workspaceId: UUID, ctx: RawlsRequestContext): Unit =
    getWorkspaceApi(ctx).deleteWorkspace(workspaceId)

  override def deleteWorkspaceV2(workspaceId: UUID, jobControlId: String, ctx: RawlsRequestContext): JobResult = {
    val deleteWorkspaceV2Request = new DeleteWorkspaceV2Request()
      .jobControl(new JobControl().id(jobControlId))

    getWorkspaceApi(ctx).deleteWorkspaceV2(deleteWorkspaceV2Request, workspaceId)
  }

  override def getDeleteWorkspaceV2Result(workspaceId: UUID,
                                          jobControlId: String,
                                          ctx: RawlsRequestContext
  ): JobResult =
    getWorkspaceApi(ctx).getDeleteWorkspaceV2Result(workspaceId, jobControlId)

  override def updateWorkspacePolicies(workspaceId: UUID,
                                       policyInputs: WsmPolicyInputs,
                                       ctx: RawlsRequestContext
  ): WsmPolicyUpdateResult = {
    val request =
      new WsmPolicyUpdateRequest().addAttributes(policyInputs).updateMode(WsmPolicyUpdateMode.FAIL_ON_CONFLICT)
    getWorkspaceApi(ctx).updatePolicies(request, workspaceId)
  }

  override def createDataRepoSnapshotReference(workspaceId: UUID,
                                               snapshotId: UUID,
                                               name: DataReferenceName,
                                               description: Option[DataReferenceDescriptionField],
                                               instanceName: String,
                                               cloningInstructions: CloningInstructionsEnum,
                                               properties: Option[Map[String, String]],
                                               ctx: RawlsRequestContext
  ): DataRepoSnapshotResource = {
    val snapshot = new DataRepoSnapshotAttributes().instanceName(instanceName).snapshot(snapshotId.toString)
    val commonFields =
      new ReferenceResourceCommonFields()
        .name(name.value)
        .cloningInstructions(cloningInstructions)
    description.map(d => commonFields.description(d.value))

    val requestProperties = new Properties
    properties.foreach(p => p.foreach(kv => requestProperties.add(new Property().key(kv._1).value(kv._2))))
    commonFields.properties(requestProperties)

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

  override def createAzureStorageContainer(workspaceId: UUID,
                                           storageContainerName: String,
                                           ctx: RawlsRequestContext
  ) = {
    val creationParams =
      new AzureStorageContainerCreationParameters()
        .storageContainerName(storageContainerName)

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
                                          prefixToClone: Option[String],
                                          ctx: RawlsRequestContext
  ): CloneControlledAzureStorageContainerResult = {
    val jobControlId = UUID.randomUUID().toString
    val prefixesToClone = prefixToClone match {
      case Some(prefix) => List(prefix)
      case _            => List()
    }
    getControlledAzureResourceApi(ctx).cloneAzureStorageContainer(
      new CloneControlledAzureStorageContainerRequest()
        .destinationWorkspaceId(destinationWorkspaceId)
        .name(destinationContainerName)
        .cloningInstructions(cloningInstructions)
        .prefixesToClone(prefixesToClone.asJava)
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
    var lzRequestBody = new CreateAzureLandingZoneRequestBody()
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
      lzRequestBody = lzRequestBody.landingZoneId(landingZoneId.get)
    }
    getLandingZonesApi(ctx).createAzureLandingZone(lzRequestBody)
  }

  override def getCreateAzureLandingZoneResult(jobId: String, ctx: RawlsRequestContext): AzureLandingZoneResult =
    getLandingZonesApi(ctx).getCreateAzureLandingZoneResult(jobId)

  override def getLandingZone(landingZoneId: UUID, ctx: RawlsRequestContext): AzureLandingZone =
    getLandingZonesApi(ctx).getAzureLandingZone(landingZoneId)

  /**
    *
    * @param landingZoneId
    * @param ctx
    * @return
    */
  override def deleteLandingZone(landingZoneId: UUID,
                                 ctx: RawlsRequestContext
  ): Option[DeleteAzureLandingZoneResult] = {
    val jobControlId = UUID.randomUUID().toString
    val body = new DeleteAzureLandingZoneRequestBody().jobControl(new JobControl().id(jobControlId))

    Try(getLandingZonesApi(ctx).deleteAzureLandingZone(body, landingZoneId)) match {
      // The service returns a 403 instead of a 404 when there's no landing zone present.
      // It's fine to move on here, because if this was a 403 for permission reasons, we won't be able to delete the billing profile anyway,
      // and we don't have a valid instance of a user having access to the billing project but not the underlying resources.
      case Failure(e: ApiException) if e.getCode == HttpStatus.SC_FORBIDDEN || e.getCode == HttpStatus.SC_NOT_FOUND =>
        logger.warn(s"No landing zone available with ID $landingZoneId for BPM-backed billing project.")
        None
      case Failure(t)      => throw t
      case Success(result) => Some(result)
    }
  }

  def getDeleteLandingZoneResult(jobId: String,
                                 landingZoneId: UUID,
                                 ctx: RawlsRequestContext
  ): DeleteAzureLandingZoneJobResult =
    getLandingZonesApi(ctx).getDeleteAzureLandingZoneResult(landingZoneId, jobId)

  override def throwWhenUnavailable(): Unit =
    apiClientProvider.getUnauthenticatedApi().serviceStatus()
}
