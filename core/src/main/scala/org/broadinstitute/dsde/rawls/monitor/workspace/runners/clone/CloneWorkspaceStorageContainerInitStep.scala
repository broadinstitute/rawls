package org.broadinstitute.dsde.rawls.monitor.workspace.runners.clone

import bio.terra.workspace.model.{AccessScope, CloneControlledAzureStorageContainerResult, CloningInstructionsEnum}
import org.broadinstitute.dsde.rawls.dataaccess.WorkspaceManagerResourceMonitorRecordDao
import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkspaceManagerResourceMonitorRecord
import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkspaceManagerResourceMonitorRecord.{Complete, JobStatus, JobType}
import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkspaceManagerResourceMonitorRecord.JobType.JobType
import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.WorkspaceManagerDAO
import org.broadinstitute.dsde.rawls.model.RawlsRequestContext
import org.broadinstitute.dsde.rawls.workspace.{MultiCloudWorkspaceService, WorkspaceRepository}

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters.CollectionHasAsScala
import scala.util.Try

class CloneWorkspaceStorageContainerInitStep(
                                          val workspaceManagerDAO: WorkspaceManagerDAO,
                                          workspaceRepository: WorkspaceRepository,
                                          monitorRecordDao: WorkspaceManagerResourceMonitorRecordDao,
                                          workspaceId: UUID,
                                          job: WorkspaceManagerResourceMonitorRecord
                                        )
                                            (implicit executionContext: ExecutionContext)
  extends WorkspaceCloningStep(workspaceRepository, monitorRecordDao, workspaceId, job) {

  override val jobType: JobType = JobType.CloneWorkspaceContainerInit


  override def runStep(userCtx: RawlsRequestContext): Future[JobStatus] = {
    val sourceWorkspaceId = WorkspaceCloningRunner.getSourceWorkspaceId(job.args) match {
      case Some(id) => id
      case None => return fail("Clone Storage Container", "no source workspace specified").map(_ => Complete)
    }
    val prefixToClone = WorkspaceCloningRunner.getStorageContainerClonePrefix(job.args)

    cloneWorkspaceStorageContainer(sourceWorkspaceId, workspaceId, prefixToClone, userCtx) match {
      case Some(cloneResult) =>
        val cloneJobID = cloneResult.getJobReport.getId
        scheduleNextJob(UUID.fromString(cloneJobID)).map(_ => Complete)
      case None => Future(Complete)
    }

  }


  // This is the async job we wait for in the original cloning implementation
  def cloneWorkspaceStorageContainer(sourceWorkspaceId: UUID,
                                     destinationWorkspaceId: UUID,
                                     prefixToClone: Option[String],
                                     ctx: RawlsRequestContext
  ): Option[CloneControlledAzureStorageContainerResult] = {

    val expectedContainerName = MultiCloudWorkspaceService.getStorageContainerName(sourceWorkspaceId)
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
    sharedAccessContainers.find(resource => resource.getMetadata.getName == expectedContainerName) match {
      case Some(container) =>
          Try(
          Some(workspaceManagerDAO.cloneAzureStorageContainer(
            sourceWorkspaceId,
            destinationWorkspaceId,
            container.getMetadata.getResourceId,
            MultiCloudWorkspaceService.getStorageContainerName(destinationWorkspaceId),
            CloningInstructionsEnum.RESOURCE,
            prefixToClone,
            ctx
          ))
        ).recover {
            t: Throwable =>
              fail("Cloning Azure Storage Container", t.getMessage)
              None
          }.get
      case None =>
        fail(
          "Cloning Azure Storage Container",
          s"Workspace $sourceWorkspaceId does not have the expected storage container $expectedContainerName."
        )
        None
    }
  }
}
