package org.broadinstitute.dsde.rawls.monitor

import akka.actor.{Actor, Props}
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkspaceManagerResourceMonitorRecord.JobType.JobType
import org.broadinstitute.dsde.rawls.dataaccess.slick.{WorkspaceManagerResourceJobRunner, WorkspaceManagerResourceMonitorRecord}
import org.broadinstitute.dsde.rawls.dataaccess.{SlickDataSource, WorkspaceManagerResourceMonitorRecordDao}
import org.broadinstitute.dsde.rawls.monitor.WSMJobMonitor._

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.language.postfixOps
import scala.util.Failure



object WSMJobMonitor extends {

  def props(dataSource: SlickDataSource, jobRunners: List[WorkspaceManagerResourceJobRunner])(implicit
    executionContext: ExecutionContext
  ): Props = Props(WSMJobMonitor(dataSource, jobRunners))

  def apply(dataSource: SlickDataSource, jobRunners: List[WorkspaceManagerResourceJobRunner])(implicit
    executionContext: ExecutionContext
  ): WSMJobMonitor =
    new WSMJobMonitor(new WorkspaceManagerResourceMonitorRecordDao(dataSource), jobRunners)

  sealed trait WSMJobMonitorMessage

  case object CheckNow extends WSMJobMonitorMessage

  case class CheckDone(creatingCount: Int) extends WSMJobMonitorMessage

  // case class CheckJob(job: WorkspaceManagerResourceMonitorRecord) extends WSMJobMonitorMessage

}


class WSMJobMonitor(jobDao: WorkspaceManagerResourceMonitorRecordDao,
                    jobRunners: List[WorkspaceManagerResourceJobRunner]
)(implicit val executionContext: ExecutionContext)
    extends Actor
    with LazyLogging {


  val registeredRunners: Map[JobType, List[WorkspaceManagerResourceJobRunner]] = jobRunners.groupBy(_.jobType)

  self ! CheckDone(0) // initial poll 1 minute after init

  override def receive: Receive = {
    case CheckNow => checkJobs().andThen(res => self ! res.getOrElse(CheckDone(0)))
    // This monitor is always on and polling, and we want that default poll rate to be low, maybe once per minute.
    // However, if projects are being created, we want to poll more frequently, say ~once per 5 seconds.
    case CheckDone(count) if count == 0 =>
      context.system.scheduler.scheduleOnce(1 minute, self, CheckNow)
    case CheckDone(_) => context.system.scheduler.scheduleOnce(5 seconds, self, CheckNow)

    // case CheckJob(job) => registeredRunners.getOrElse(job.jobType, List()).foreach(_.run(job))
    case Failure(t) =>
      logger.error(s"failure monitoring WSM Job", t)
      context.system.scheduler.scheduleOnce(1 minute, self, CheckNow)
    case _ => logger.debug(s"WSMJobMonitor received unknown message")

  }


  def checkJobs(): Future[CheckDone] = for {
    jobs: Seq[WorkspaceManagerResourceMonitorRecord] <- jobDao.selectAll()
    jobResults <- Future.sequence(jobs.map(runJob))
  } yield CheckDone(jobResults.count(_ == true))

  def runJob(job: WorkspaceManagerResourceMonitorRecord): Future[Boolean] = for {
    jobResults <- Future.sequence(
      registeredRunners
        .getOrElse(job.jobType, List())
        .map(_.run(job).recover { case t =>
          self ! Failure(t)
          false
        })
    )
  } yield
    if (jobResults.reduce((a, b) => a && b)) {
      jobDao.delete(job)
      true
    } else {
      false
    }

}

