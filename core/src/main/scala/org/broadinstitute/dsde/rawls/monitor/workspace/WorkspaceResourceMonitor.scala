package org.broadinstitute.dsde.rawls.monitor.workspace

import akka.actor.{Actor, Props}
import cats.Applicative
import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.config.WorkspaceManagerResourceMonitorConfig
import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkspaceManagerResourceMonitorRecord.JobType.JobType
import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkspaceManagerResourceMonitorRecord.{Incomplete, JobStatus}
import org.broadinstitute.dsde.rawls.dataaccess.slick.{
  WorkspaceManagerResourceJobRunner,
  WorkspaceManagerResourceMonitorRecord
}
import org.broadinstitute.dsde.rawls.dataaccess.{SlickDataSource, WorkspaceManagerResourceMonitorRecordDao}
import org.broadinstitute.dsde.rawls.monitor.workspace.WorkspaceResourceMonitor._

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.language.postfixOps
import scala.util.Failure

object WorkspaceResourceMonitor extends {

  def props(config: Config, dataSource: SlickDataSource, jobRunners: Map[JobType, WorkspaceManagerResourceJobRunner])(
    implicit executionContext: ExecutionContext
  ): Props = {
    val jobDao = WorkspaceManagerResourceMonitorRecordDao(dataSource)
    val monitor = new WorkspaceResourceMonitor(jobDao, jobRunners)
    Props(new WorkspaceMonitorRouter(WorkspaceManagerResourceMonitorConfig(config), monitor))
  }

  sealed trait WSMJobMonitorMessage

  case object CheckNow extends WSMJobMonitorMessage

  case class CheckDone(creatingCount: Int) extends WSMJobMonitorMessage

}

/**
  * A separate class for the actual akka behavior, to allow for easier isolation in testing
  * This class is only responsible for messaging and scheduling
  */
class WorkspaceMonitorRouter(val config: WorkspaceManagerResourceMonitorConfig, val monitor: WorkspaceResourceMonitor)(
  implicit val executionContext: ExecutionContext
) extends Actor
    with LazyLogging {

  self ! CheckDone(0)

  override def receive: Receive = {
    case CheckNow => monitor.checkJobs().andThen(res => self ! res.getOrElse(CheckDone(0)))

    // This monitor is always on and polling, and we want that default poll rate to be low, maybe once per minute.
    // if more jobs are active, we want to poll more frequently, say ~once per 5 seconds
    case CheckDone(0) =>
      context.system.scheduler.scheduleOnce(2 seconds, self, CheckNow)

    case CheckDone(_) =>
      context.system.scheduler.scheduleOnce(2 seconds, self, CheckNow)

    case Failure(t) =>
      logger.error(s"failure monitoring WSM Job", t)
      context.system.scheduler.scheduleOnce(2 seconds, self, CheckNow)

    case msg => logger.warn(s"WSMJobMonitor received unknown message: $msg")
  }

}

case class WorkspaceResourceMonitor(
  jobDao: WorkspaceManagerResourceMonitorRecordDao,
  jobRunners: Map[JobType, WorkspaceManagerResourceJobRunner]
)(implicit val executionContext: ExecutionContext)
    extends LazyLogging {

  /**
    * Run all jobs, and wait for them to complete
    *
    * @return the message containing the number of uncompleted jobs
    */
  def checkJobs(): Future[CheckDone] = for {
    jobs: Seq[WorkspaceManagerResourceMonitorRecord] <- jobDao.selectAll()
    jobResults <- Future.sequence(jobs.map(runJob))
  } yield CheckDone(jobResults.count(!_.isDone))

  /**
    * Runs the job in all job runners configured for the type of the job
    * Deletes the job if all runners have completed successfully
    *
    * @return true if all runners for the job completed successfully, false if any failed
    */
  def runJob(job: WorkspaceManagerResourceMonitorRecord): Future[JobStatus] =
    for {
      status <- jobRunners.getOrElse(job.jobType, AlwaysIncompleteJobRunner)(job).recover { case t: Throwable =>
        logger.warn(
          "Failure monitoring WSM job " +
            s"[ jobId = ${job.jobControlId}" +
            s", jobType = ${job.jobType}" +
            s"]",
          t
        )
        Incomplete
      }

      _ <- Applicative[Future].whenA(status.isDone) {
        jobDao.delete(job)
      }

    } yield status
}

case object AlwaysIncompleteJobRunner extends WorkspaceManagerResourceJobRunner {
  override def apply(job: WorkspaceManagerResourceMonitorRecord)(implicit
    executionContext: ExecutionContext
  ): Future[JobStatus] =
    Future.successful(Incomplete)
}
