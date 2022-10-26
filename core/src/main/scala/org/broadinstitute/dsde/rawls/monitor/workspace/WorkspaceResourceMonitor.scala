package org.broadinstitute.dsde.rawls.monitor.workspace

import akka.actor.{Actor, Props}
import com.typesafe.scalalogging.LazyLogging
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
  
  def props(dataSource: SlickDataSource, jobRunners: List[WorkspaceManagerResourceJobRunner])(implicit
    executionContext: ExecutionContext
  ): Props = Props(new WorkspaceMonitorRouter(new WorkspaceManagerResourceMonitorRecordDao(dataSource), jobRunners))

  sealed trait WSMJobMonitorMessage

  case object CheckNow extends WSMJobMonitorMessage

  case class CheckDone(creatingCount: Int) extends WSMJobMonitorMessage

}

class WorkspaceMonitorRouter(
  jobDao: WorkspaceManagerResourceMonitorRecordDao,
  jobRunners: List[WorkspaceManagerResourceJobRunner]
)(implicit val executionContext: ExecutionContext)
    extends Actor
    with LazyLogging {

  val monitor: WorkspaceResourceMonitor = new WorkspaceResourceMonitor(jobDao, jobRunners)

  override def receive: Receive = {
    case CheckNow => monitor.checkJobs().andThen(res => self ! res.getOrElse(CheckDone(0)))
    // This monitor is always on and polling, and we want that default poll rate to be low, maybe once per minute.
    // However, if projects are being created, we want to poll more frequently, say ~once per 5 seconds.
    case CheckDone(count) if count == 0 =>
      context.system.scheduler.scheduleOnce(1 minute, self, CheckNow)
    case CheckDone(_) => context.system.scheduler.scheduleOnce(5 seconds, self, CheckNow)

    case Failure(t) =>
      logger.error(s"failure monitoring WSM Job", t)
      context.system.scheduler.scheduleOnce(1 minute, self, CheckNow)
    case msg => logger.warn(s"WSMJobMonitor received unknown message: $msg")

  }

}

class WorkspaceResourceMonitor(
  jobDao: WorkspaceManagerResourceMonitorRecordDao,
  jobRunners: List[WorkspaceManagerResourceJobRunner],
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
    * Deletes
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
