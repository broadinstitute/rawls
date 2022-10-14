package org.broadinstitute.dsde.rawls.monitor

import akka.actor.{Actor, Props}
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.billing.{BillingProfileManagerDAO, BillingRepository}
import org.broadinstitute.dsde.rawls.dataaccess.{SlickDataSource, WorkspaceManagerResourceMonitorRecordDao}
import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkspaceManagerResourceMonitorRecord
import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.WorkspaceManagerDAO
import org.broadinstitute.dsde.rawls.monitor.WSMJobMonitor._

import scala.concurrent.{ExecutionContext, Future}
import scala.language.postfixOps
import scala.util.Failure
import scala.concurrent.duration._

object WSMJobMonitor {

  def props(datasource: SlickDataSource, bpmDao: BillingProfileManagerDAO, wsmDao: WorkspaceManagerDAO)(implicit
    executionContext: ExecutionContext
  ): Props = Props(WSMJobMonitor(datasource, bpmDao, wsmDao))

  def apply(datasource: SlickDataSource, bpmDao: BillingProfileManagerDAO, wsmDao: WorkspaceManagerDAO)(implicit
    executionContext: ExecutionContext
  ): WSMJobMonitor =
    new WSMJobMonitor(
      new WorkspaceManagerResourceMonitorRecordDao(datasource),
      new BillingRepository(datasource),
      bpmDao,
      wsmDao
    )

  sealed trait WSMJobMonitorMessage

  case object CheckNow extends WSMJobMonitorMessage

  case class CheckDone(creatingCount: Int) extends WSMJobMonitorMessage

  case class CheckJob(job: WorkspaceManagerResourceMonitorRecord) extends WSMJobMonitorMessage

}

class WSMJobMonitor(
  jobDao: WorkspaceManagerResourceMonitorRecordDao,
  billingRepo: BillingRepository,
  bpmDao: BillingProfileManagerDAO,
  wsmDao: WorkspaceManagerDAO
)(implicit val exCtx: ExecutionContext)
    extends Actor
    with LazyLogging {

  self ! CheckDone(0) // initial poll 1 minute after init

  override def receive: Receive = {
    case CheckNow => checkJobs().andThen(res => self ! res.getOrElse(CheckDone(0))) // { self ! CheckDone(0) }
    // This monitor is always on and polling, and we want that default poll rate to be low, maybe once per minute.
    // However, if projects are being created, we want to poll more frequently, say ~once per 5 seconds.
    case CheckDone(creatingCount) if creatingCount == 0 =>
      context.system.scheduler.scheduleOnce(1 minute, self, CheckNow)
    case CheckDone(_) => context.system.scheduler.scheduleOnce(5 seconds, self, CheckNow)

    case CheckJob(job) =>
      runJob(
        job
      ) // in the future, we could do something like register different handlers for different jobs, and just dispatch them here
    case Failure(t) =>
      logger.error(s"failure monitoring WSM Job", t)
      context.system.scheduler.scheduleOnce(1 minute, self, CheckNow)
    case _ => logger.debug(s"WSMJobMonitor received unknown message")

  }

  def checkJobs(): Future[CheckDone] = for {
    jobs <- jobDao.selectAll()
    // TODO: find a way to wait for all of these to complete?
    //  prob not, if they're handled sequentially, but if there's no guarantee of in-order processing...
    _ = jobs.foreach(self ! CheckJob(_))
  } yield CheckDone(jobs.size)

  def runJob(job: WorkspaceManagerResourceMonitorRecord): Future[Unit] = job.jobType match {

    // case WSMJobType.LandingZoneCreating => Future.failed(new Exception()) // TODO: call impl

    case _ => Future.failed(new Exception())
  }

}
