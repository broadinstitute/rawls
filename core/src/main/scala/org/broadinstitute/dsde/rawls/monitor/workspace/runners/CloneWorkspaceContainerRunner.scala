package org.broadinstitute.dsde.rawls.monitor.workspace.runners

import bio.terra.workspace.client.ApiException
import bio.terra.workspace.model.JobReport
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkspaceManagerResourceMonitorRecord.{
  Complete,
  Incomplete,
  JobStatus,
  JobType
}
import org.broadinstitute.dsde.rawls.dataaccess.slick.{
  WorkspaceManagerResourceJobRunner,
  WorkspaceManagerResourceMonitorRecord
}
import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.WorkspaceManagerDAO
import org.broadinstitute.dsde.rawls.dataaccess.{GoogleServicesDAO, SamDAO, SlickDataSource}
import org.broadinstitute.dsde.rawls.model.Workspace
import org.joda.time.DateTime

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

class CloneWorkspaceContainerRunner(
  val samDAO: SamDAO,
  workspaceManagerDAO: WorkspaceManagerDAO,
  dataSource: SlickDataSource,
  val gcsDAO: GoogleServicesDAO
) extends WorkspaceManagerResourceJobRunner
    with LazyLogging
    with UserCtxCreator {

  override def apply(
    job: WorkspaceManagerResourceMonitorRecord
  )(implicit executionContext: ExecutionContext): Future[JobStatus] = {

    def logFailure(msg: String, t: Option[Throwable] = None): Unit = {
      val logMessage = s"CloneWorkspaceContainerResult monitoring job with id ${job.jobControlId} failed: $msg"
      t match {
        case Some(t) => logger.error(logMessage, t)
        case None    => logger.error(logMessage)
      }
    }

    if (!job.jobType.equals(JobType.CloneWorkspaceContainerResult))
      throw new IllegalArgumentException(s"${this.getClass.getSimpleName} called with invalid job type: ${job.jobType}")

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
          case Failure(e: ApiException) =>
            e.getMessage
            e.getCode match {
              case 500 =>
                val msg = s"Clone Container operation with jobId ${job.jobControlId} failed: ${e.getMessage}"
                cloneFail(workspaceId, msg).map(_ => Complete)
              case 404 =>
                val msg = s"Unable to find jobId ${job.jobControlId} in WSM for clone container operation"
                cloneFail(workspaceId, msg).map(_ => Complete)
              case code =>
                logFailure(s"API call to get clone result failed with status code $code: ${e.getMessage}")
                Future.successful(Incomplete)
            }
          case Failure(t) =>
            val msg = s"API call to get clone result from workspace manager failed with: ${t.getMessage}"
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
  ): Future[Int] =
    getWorkspace(wsId).flatMap {
      case Some(_) =>
        dataSource.inTransaction(_.workspaceQuery.updateCompletedCloneWorkspaceFileTransfer(wsId, finishTime.toDate))
      case None => Future.successful(0)
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

}
