package org.broadinstitute.dsde.rawls.monitor.workspace.runners

import bio.terra.workspace.model.{CloneControlledAzureStorageContainerResult, CloneWorkspaceResult, JobReport}
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.dataaccess.{GoogleServicesDAO, SamDAO, SlickDataSource}
import org.broadinstitute.dsde.rawls.dataaccess.slick.{
  WorkspaceManagerResourceJobRunner,
  WorkspaceManagerResourceMonitorRecord
}
import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkspaceManagerResourceMonitorRecord.{
  Complete,
  Incomplete,
  JobStatus
}
import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.WorkspaceManagerDAO
import org.broadinstitute.dsde.rawls.model.{RawlsRequestContext, Workspace}
import org.joda.time.DateTime

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

class CloneWorkspaceContainerRunner(
  samDAO: SamDAO,
  workspaceManagerDAO: WorkspaceManagerDAO,
  dataSource: SlickDataSource,
  gcsDAO: GoogleServicesDAO
) extends WorkspaceManagerResourceJobRunner
    with LazyLogging {

  override def apply(
    job: WorkspaceManagerResourceMonitorRecord
  )(implicit executionContext: ExecutionContext): Future[JobStatus] = {

    def logFailure(msg: String, t: Option[Throwable] = None): Unit = {
      val logMessage = s"CloneWorkspaceResult monitoring job with id ${job.jobControlId} failed: $msg"
      t match {
        case Some(t) => logger.error(logMessage, t)
        case None    => logger.error(logMessage)
      }
    }

    val workspaceId = job.workspaceId match {
      case Some(name) => name
      case None =>
        logFailure("no workspace id set")
        return Future.successful(Complete) // nothing more this runner can do with it
    }

    val userEmail = job.userEmail match {
      case Some(email) => email
      case None =>
        val msg =
          s"Unable to update clone status for workspace $workspaceId because no user email set on monitoring job"
        logFailure(msg)
        return cloneFail(workspaceId, msg).map(_ => Complete)
    }

    getUserCtx(userEmail).transformWith {
      case Failure(t) =>
        val msg =
          s"Unable to retrieve clone workspace results for workspace $workspaceId: unable to retrieve request context for $userEmail"
        logFailure(msg, Some(t))
        cloneFail(workspaceId, msg).map(_ => Incomplete)
      case Success(ctx) =>
        Try(workspaceManagerDAO.getJob(job.jobControlId.toString, ctx)) match {
          case Success(result) => handleCloneResult(workspaceId, result)
          case Failure(t) =>
            val msg = s"Api call to get clone result from workspace manager failed with: ${t.getMessage}"
            logFailure(msg, Some(t))
            cloneFail(workspaceId, msg).map(_ => Incomplete)
        }
    }

  }

  def handleCloneResult(workspaceId: UUID, result: JobReport)(implicit
    executionContext: ExecutionContext
  ): Future[JobStatus] = result.getStatus match {
    case JobReport.StatusEnum.RUNNING => Future.successful(Incomplete)
    case JobReport.StatusEnum.SUCCEEDED =>
      val completeTime = DateTime.parse(result.getCompleted)
      cloneSuccess(workspaceId, completeTime).map(_ => Complete)
    // set the error, and indicate this runner is finished with the job
    case JobReport.StatusEnum.FAILED =>
      cloneFail(workspaceId, "Cloning workspace resource container failed").map(_ => Complete)
  }

  def cloneSuccess(wsId: UUID, finishTime: DateTime)(implicit
    executionContext: ExecutionContext
  ): Future[Option[Workspace]] =
    getWorkspace(wsId).flatMap {
      case Some(workspace) =>
        val updatedWS = workspace.copy(completedCloneWorkspaceFileTransfer = Some(finishTime))
        dataSource.inTransaction(_.workspaceQuery.createOrUpdate(updatedWS)).map(Option(_))
      case None => Future.successful(None)
    }

  def cloneFail(wsId: UUID, message: String)(implicit executionContext: ExecutionContext): Future[Option[Workspace]] =
    getWorkspace(wsId).flatMap {
      case Some(workspace) =>
        dataSource
          .inTransaction(_.workspaceQuery.createOrUpdate(workspace.copy(errorMessage = Some(message))))
          .map(Option(_))
      case None => Future.successful(None)

    }

  def getWorkspace(wsId: UUID): Future[Option[Workspace]] = dataSource.inTransaction { dataAccess =>
    dataAccess.workspaceQuery.findById(wsId.toString)
  }

  def getUserCtx(userEmail: String)(implicit executionContext: ExecutionContext): Future[RawlsRequestContext] = for {
    petKey <- samDAO.getUserArbitraryPetServiceAccountKey(userEmail)
    userInfo <- gcsDAO.getUserInfoUsingJson(petKey)
  } yield RawlsRequestContext(userInfo)

}
