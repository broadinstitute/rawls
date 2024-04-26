package org.broadinstitute.dsde.rawls.monitor.workspace.runners.clone

import bio.terra.workspace.model.{
  AccessScope,
  CloneControlledAzureStorageContainerResult,
  CloningInstructionsEnum,
  ResourceDescription
}
import org.broadinstitute.dsde.rawls.dataaccess.WorkspaceManagerResourceMonitorRecordDao
import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkspaceManagerResourceMonitorRecord
import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkspaceManagerResourceMonitorRecord.{
  Complete,
  JobStatus,
  JobType
}
import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkspaceManagerResourceMonitorRecord.JobType.JobType
import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.WorkspaceManagerDAO
import org.broadinstitute.dsde.rawls.model.RawlsRequestContext
import org.broadinstitute.dsde.rawls.workspace.{MultiCloudWorkspaceService, WorkspaceRepository}

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters.CollectionHasAsScala
import scala.util.{Failure, Success, Try}

class CloneWorkspaceStorageContainerInitStep(
  val workspaceManagerDAO: WorkspaceManagerDAO,
  workspaceRepository: WorkspaceRepository,
  monitorRecordDao: WorkspaceManagerResourceMonitorRecordDao,
  workspaceId: UUID,
  job: WorkspaceManagerResourceMonitorRecord
)(implicit executionContext: ExecutionContext)
    extends WorkspaceCloningStep(workspaceRepository, monitorRecordDao, workspaceId, job) {

  override val jobType: JobType = JobType.CloneWorkspaceContainerInit

  override def runStep(userCtx: RawlsRequestContext): Future[JobStatus] = {
    val sourceWorkspaceId = WorkspaceCloningRunner.getSourceWorkspaceId(job.args) match {
      case Some(id) => id
      case None     => return fail("Clone Storage Container", "no source workspace specified").map(_ => Complete)
    }
    val prefixToClone = WorkspaceCloningRunner.getStorageContainerClonePrefix(job.args)
    val expectedContainerName = MultiCloudWorkspaceService.getStorageContainerName(sourceWorkspaceId)

    findSourceWorkspaceStorageContainer(sourceWorkspaceId, expectedContainerName, userCtx) match {
      case None =>
        fail(
          "Cloning Azure Storage Container",
          s"Workspace $sourceWorkspaceId does not have the expected storage container $expectedContainerName."
        )
        Future(Complete)
      case Some(container) =>
        cloneWorkspaceStorageContainer(sourceWorkspaceId, workspaceId, container, prefixToClone, userCtx) match {
          case None => Future(Complete)
          case Some(cloneResult) =>
            val cloneJobID = cloneResult.getJobReport.getId
            scheduleNextJob(UUID.fromString(cloneJobID)).map(_ => Complete)
        }
    }
  }

  def findSourceWorkspaceStorageContainer(
    sourceWorkspaceId: UUID,
    expectedContainerName: String,
    ctx: RawlsRequestContext
  ): Option[ResourceDescription] = {
    // Using limit of 200 to be safe, but we expect at most a handful of storage containers.
    val allContainers =
      workspaceManagerDAO.enumerateStorageContainers(sourceWorkspaceId, 0, 200, ctx).getResources.asScala
    val sharedAccessContainers = allContainers.filter(resource =>
      resource.getMetadata.getControlledResourceMetadata.getAccessScope == AccessScope.SHARED_ACCESS
    )
    if (sharedAccessContainers.size > 1) {
      logger.warn(
        s"Workspace being cloned has multiple shared access containers [ workspaceId='$sourceWorkspaceId' ]"
      )
    }
    sharedAccessContainers.find(resource => resource.getMetadata.getName == expectedContainerName)
  }

  // This is the async job we wait for in the original cloning implementation
  def cloneWorkspaceStorageContainer(
    sourceWorkspaceId: UUID,
    destinationWorkspaceId: UUID,
    container: ResourceDescription,
    prefixToClone: Option[String],
    ctx: RawlsRequestContext
  ): Option[CloneControlledAzureStorageContainerResult] = Try(
    Option(
      workspaceManagerDAO.cloneAzureStorageContainer(
        sourceWorkspaceId,
        destinationWorkspaceId,
        container.getMetadata.getResourceId,
        MultiCloudWorkspaceService.getStorageContainerName(destinationWorkspaceId),
        CloningInstructionsEnum.RESOURCE,
        prefixToClone,
        ctx
      )
    )
  ) match {
    case Failure(t) =>
      fail("Cloning Azure Storage Container", t.getMessage)
      None
    case Success(None) =>
      fail(
        "Cloning Azure Storage Container",
        s"No result returned from Workspace Manager"
      )
      None
    case Success(result) => result
  }

}
