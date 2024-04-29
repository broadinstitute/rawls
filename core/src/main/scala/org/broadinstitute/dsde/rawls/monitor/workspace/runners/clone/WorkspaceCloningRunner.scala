package org.broadinstitute.dsde.rawls.monitor.workspace.runners.clone

import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.dataaccess.{GoogleServicesDAO, LeonardoDAO, SamDAO, WorkspaceManagerResourceMonitorRecordDao}
import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkspaceManagerResourceMonitorRecord.{Complete, JobType}
import org.broadinstitute.dsde.rawls.dataaccess.slick.{WorkspaceManagerResourceJobRunner, WorkspaceManagerResourceMonitorRecord}
import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.WorkspaceManagerDAO
import org.broadinstitute.dsde.rawls.model.{AttributeBoolean, AttributeName, AttributeString, RawlsRequestContext, Workspace, WorkspaceRequest, WorkspaceState}
import org.broadinstitute.dsde.rawls.monitor.workspace.runners.UserCtxCreator
import org.broadinstitute.dsde.rawls.workspace.WorkspaceRepository

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

object WorkspaceCloningRunner {

  val SOURCE_WORKSPACE_KEY = "SOURCE_WORKSPACE"
  val STORAGE_CONTAINER_CLONE_PREFIX_KEY = "STORAGE_CONTAINER_CLONE_PREFIX"
  val DISABLE_AUTOMATIC_APP_CREATION_KEY = "DISABLE_AUTOMATIC_APP_CREATION"

  def buildCloningArgs(sourceWorkspace: Workspace, request: WorkspaceRequest): Map[String, String] = {
    val disableAutoAppCreate =
      request.attributes.get(AttributeName.withDefaultNS("disableAutomaticAppCreation")) match {
      case Some(AttributeString("true")) | Some(AttributeBoolean(true)) => "true"
      case _ => "false"
    }

    val args = Map(
      SOURCE_WORKSPACE_KEY -> sourceWorkspace.workspaceIdAsUUID.toString,
      DISABLE_AUTOMATIC_APP_CREATION_KEY -> disableAutoAppCreate
    )

    request.copyFilesWithPrefix match {
      case Some(prefix) => args + (STORAGE_CONTAINER_CLONE_PREFIX_KEY -> prefix)
      case None => args
    }
  }

  def isAutomaticAppCreationDisabled(args: Option[Map[String, String]]): Boolean =
    args.flatMap(argsMap => argsMap.get(DISABLE_AUTOMATIC_APP_CREATION_KEY)).getOrElse("false") == "true"

  def getSourceWorkspaceId(args: Option[Map[String, String]]): Option[UUID] =
    args.flatMap(argsMap => argsMap.get(SOURCE_WORKSPACE_KEY)).map(id => UUID.fromString(id))

  def getStorageContainerClonePrefix(args: Option[Map[String, String]]): Option[String] =
    args.flatMap(argsMap => argsMap.get(STORAGE_CONTAINER_CLONE_PREFIX_KEY))
}

class WorkspaceCloningRunner(
                              val samDAO: SamDAO,
                              val gcsDAO: GoogleServicesDAO,
                              leonardoDAO: LeonardoDAO,
                              workspaceManagerDAO: WorkspaceManagerDAO,
                              monitorRecordDao: WorkspaceManagerResourceMonitorRecordDao,
                              workspaceRepository: WorkspaceRepository,
                            ) extends WorkspaceManagerResourceJobRunner
  with LazyLogging
  with UserCtxCreator {


  def cloneFail(wsId: UUID, message: String)(implicit executionContext: ExecutionContext): Future[Int] =
    workspaceRepository.setFailedState(wsId, WorkspaceState.CreateFailed, message)


  override def apply(job: WorkspaceManagerResourceMonitorRecord)(implicit
                                                                 executionContext: ExecutionContext
  ): Future[WorkspaceManagerResourceMonitorRecord.JobStatus] = {
    def logFailure(msg: String, t: Option[Throwable] = None): Unit = {
      val logMessage = s"CloneWorkspace monitoring job with id ${job.jobControlId} failed: $msg"
      t match {
        case Some(t) => logger.error(logMessage, t)
        case None => logger.error(logMessage)
      }
    }

    if (!JobType.cloneJobTypes.contains(job.jobType)) {
      throw new IllegalArgumentException(s"${this.getClass.getSimpleName} called with invalid job type: ${job.jobType}")
    }

    val workspaceId = job.workspaceId match {
      case Some(id) => id
      case None =>
        logger.error(
          s"Job to monitor workspace deletion created with id ${job.jobControlId} but no workspace ID"
        )
        return Future.successful(Complete)
    }

    val userEmail = job.userEmail match {
      case Some(email) => email
      case None =>
        val msg =
          s"Unable to update clone status for workspace $workspaceId because no user email set on monitoring job"
        logFailure(msg)
        return cloneFail(workspaceId, msg).map(_ => Complete)
    }

    for {
      userCtx <- getUserCtx(userEmail)
      status <- runStep(job, workspaceId, userCtx)
    } yield status
  }

  def runStep(
               job: WorkspaceManagerResourceMonitorRecord,
               workspaceId: UUID,
               ctx: RawlsRequestContext
             )(implicit executionContext: ExecutionContext): Future[WorkspaceManagerResourceMonitorRecord.JobStatus] = {
    job.jobType match {
      case JobType.CloneWorkspaceInit =>
        new CloneWorkspaceInitStep(workspaceManagerDAO, workspaceRepository, monitorRecordDao, workspaceId, job)
          .runStep(ctx)
      case JobType.CreateWdsAppInClonedWorkspace =>
        new CloneWorkspaceCreateWDSAppStep(leonardoDAO, workspaceRepository, monitorRecordDao, workspaceId, job)
          .runStep(ctx)
      case JobType.CloneWorkspaceContainerInit =>
        new CloneWorkspaceStorageContainerInitStep(workspaceManagerDAO, workspaceRepository, monitorRecordDao, workspaceId, job)
          .runStep(ctx)
      case JobType.CloneWorkspaceContainerResult => Future(Complete)
      case _ => Future(Complete)
    }
  }


}











