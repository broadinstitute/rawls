package org.broadinstitute.dsde.rawls.monitor.workspace

import akka.actor.{Actor, Props}
import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.config.WorkspaceManagerResourceMonitorConfig
import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkspaceManagerResourceMonitorRecord.JobType.JobType
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

  def props(config: Config, dataSource: SlickDataSource, jobRunners: List[WorkspaceManagerResourceJobRunner])(implicit
    executionContext: ExecutionContext
  ): Props = {
    val jobDao = new WorkspaceManagerResourceMonitorRecordDao(dataSource)
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
    case CheckDone(count) if count == 0 =>
      context.system.scheduler.scheduleOnce(config.defaultRetrySeconds seconds, self, CheckNow)

    case CheckDone(_) =>
      context.system.scheduler.scheduleOnce(config.retryUncompletedJobsSeconds seconds, self, CheckNow)

    case Failure(t) =>
      logger.error(s"failure monitoring WSM Job", t)
      context.system.scheduler.scheduleOnce(config.defaultRetrySeconds seconds, self, CheckNow)

    case msg => logger.warn(s"WSMJobMonitor received unknown message: $msg")
  }

}

class WorkspaceResourceMonitor(
  jobDao: WorkspaceManagerResourceMonitorRecordDao,
  jobRunners: List[WorkspaceManagerResourceJobRunner]
)(implicit val executionContext: ExecutionContext)
    extends LazyLogging {

  val registeredRunners: Map[JobType, List[WorkspaceManagerResourceJobRunner]] = jobRunners.groupBy(_.jobType)

  /**
    * Run all jobs, and wait for them to complete
    * @return the message containing the number of uncompleted jobs
    */
  def checkJobs(): Future[CheckDone] = for {
    jobs: Seq[WorkspaceManagerResourceMonitorRecord] <- jobDao.selectAll()
    jobResults <- Future.sequence(jobs.map(runJob))
  } yield CheckDone(jobResults.count(_ == false))

  /**
    * Runs the job in all job runners configured for the type of the job
    * Deletes the job if all runners have completed successfully
    * @return true if all runners for the job completed successfully, false if any failed
    */
  def runJob(job: WorkspaceManagerResourceMonitorRecord): Future[Boolean] = for {
    jobResults <- Future.sequence(
      registeredRunners
        .getOrElse(job.jobType, List())
        .map(_.run(job).recover { case t =>
          logger.error(s"Exception monitoring WSM Job", t)
          false
        })
    )
  } yield
    if (jobResults.reduce((a, b) => a && b)) { // true iff all runners for the given job completed successfully
      jobDao.delete(job) // remove completed jobs
      true
    } else {
      false
    }

}
