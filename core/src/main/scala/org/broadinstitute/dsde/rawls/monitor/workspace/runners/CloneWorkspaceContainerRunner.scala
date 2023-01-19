package org.broadinstitute.dsde.rawls.monitor.workspace.runners

import bio.terra.workspace.model.{CloneWorkspaceResult, JobReport}
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

    val workspace = getWorkspace(workspaceId).map(_.getOrElse {
      logFailure(s"no workspace found for workspace id: $workspaceId")
      return Future.successful(Complete)
    })

    val userEmail = job.userEmail match {
      case Some(email) => email
      case None =>
        val msg =
          s"Unable to update clone status for workspace $workspaceId because no user email set on monitoring job"
        logFailure(msg)
        return workspace.flatMap(ws => cloneFail(ws, msg).map(_ => Complete))
    }

    getUserCtx(userEmail).transformWith {
      case Failure(t) =>
        val msg =
          s"Unable to retrieve clone workspace results for workspace $workspaceId: unable to retrieve request context for $userEmail"
        logFailure(msg, Some(t))
        workspace.flatMap(ws => cloneFail(ws, msg).map(_ => Incomplete))
      case Success(ctx) =>
        Try(workspaceManagerDAO.getCloneWorkspaceResult(workspaceId, job.jobControlId.toString, ctx)) match {
          case Success(result) => workspace.flatMap(handleCloneResult(_, result))
          case Failure(t) =>
            val msg = s"Api call to get clone result from workspace manager failed with: ${t.getMessage}"
            logFailure(msg, Some(t))
            workspace.flatMap(ws => cloneFail(ws, msg).map(_ => Incomplete))
        }
    }

  }

  def handleCloneResult(ws: Workspace, result: CloneWorkspaceResult)(implicit
    executionContext: ExecutionContext
  ): Future[JobStatus] = Option(result.getJobReport).map(_.getStatus) match {
    case Some(JobReport.StatusEnum.RUNNING) => Future.successful(Incomplete)
    case Some(JobReport.StatusEnum.SUCCEEDED) =>
      val completeTime = DateTime.parse(result.getJobReport.getCompleted)
      cloneSuccess(ws, completeTime).map(_ => Complete)
    // set the error, and indicate this runner is finished with the job
    case Some(JobReport.StatusEnum.FAILED) =>
      val msg = Option(result.getErrorReport)
        .map(_.getMessage)
        .getOrElse("Cloning failure Reported, but no errors returned")
      cloneFail(ws, msg).map(_ => Complete)
    case None =>
      val msg = Option(result.getErrorReport)
        .flatMap(report => Option(report.getMessage))
        .map(errorMessage => s"Cloning Failed: $errorMessage")
        .getOrElse("No job status or errors returned from workspace cloning")
      cloneFail(ws, msg).map(_ => Complete)
  }

  def cloneSuccess(ws: Workspace, finishTime: DateTime): Future[Workspace] = {
    val updatedWS = ws.copy(completedCloneWorkspaceFileTransfer = Some(finishTime))
    dataSource.inTransaction(_.workspaceQuery.createOrUpdate(updatedWS))
  }

  def cloneFail(ws: Workspace, message: String): Future[Workspace] =
    dataSource.inTransaction(_.workspaceQuery.createOrUpdate(ws.copy(errorMessage = Some(message))))

  def getWorkspace(wsId: UUID): Future[Option[Workspace]] = dataSource.inTransaction { dataAccess =>
    dataAccess.workspaceQuery.findById(wsId.toString)
  }

  def getUserCtx(userEmail: String)(implicit executionContext: ExecutionContext): Future[RawlsRequestContext] = for {
    petKey <- samDAO.getUserArbitraryPetServiceAccountKey(userEmail)
    userInfo <- gcsDAO.getUserInfoUsingJson(petKey)
  } yield RawlsRequestContext(userInfo)

}
