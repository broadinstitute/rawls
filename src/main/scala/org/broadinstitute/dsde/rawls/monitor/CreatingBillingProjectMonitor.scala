package org.broadinstitute.dsde.rawls.monitor

import akka.actor.Status.Failure
import akka.actor.{Actor, Props}
import akka.pattern._
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.dataaccess.slick.RawlsBillingProjectRecord
import org.broadinstitute.dsde.rawls.dataaccess.{GoogleServicesDAO, SlickDataSource}
import org.broadinstitute.dsde.rawls.model.{RawlsBillingProjectName, ProjectStatuses}
import org.broadinstitute.dsde.rawls.monitor.CreatingBillingProjectMonitor.{CheckDone, CheckNow}
import scala.concurrent.duration._

import scala.concurrent.{Future, ExecutionContext}
import scala.util.Success

/**
 * Created by dvoet on 8/22/16.
 */
object CreatingBillingProjectMonitor {
  def props(datasource: SlickDataSource, gcsDAO: GoogleServicesDAO)(implicit executionContext: ExecutionContext): Props = {
    Props(new CreatingBillingProjectMonitorActor(datasource, gcsDAO))
  }

  sealed trait CreatingBillingProjectMonitorMessage
  case object CheckNow extends CreatingBillingProjectMonitorMessage
  case class CheckDone(creatingCount: Int) extends CreatingBillingProjectMonitorMessage
}

class CreatingBillingProjectMonitorActor(val datasource: SlickDataSource, val gcsDAO: GoogleServicesDAO)(implicit executionContext: ExecutionContext) extends Actor with CreatingBillingProjectMonitor with LazyLogging {
  self ! CheckNow

  override def receive = {
    case CheckNow => checkCreatingProjects pipeTo self

    case CheckDone(creatingCount) if creatingCount > 0 => context.system.scheduler.scheduleOnce(5 seconds, self, CheckNow)
    case CheckDone(creatingCount) => context.system.scheduler.scheduleOnce(1 minute, self, CheckNow)

    case Failure(t) =>
      logger.error(s"failure monitoring creating billing projects", t)
      context.system.scheduler.scheduleOnce(1 minute, self, CheckNow)
  }
}

trait CreatingBillingProjectMonitor {
  val datasource: SlickDataSource
  val gcsDAO: GoogleServicesDAO

  def checkCreatingProjects()(implicit executionContext: ExecutionContext): Future[CheckDone] = {
    for {
      creatingProjects <- datasource.inTransaction { _.rawlsBillingProjectQuery.listProjectsWithStatus(ProjectStatuses.Creating) }
      readyProjects <- setUsageExportBuckets(creatingProjects)
      updatedProjectCount <- datasource.inTransaction { _.rawlsBillingProjectQuery.updateStatus(readyProjects.map(project => RawlsBillingProjectName(project.projectName)), ProjectStatuses.Ready) }
    } yield {
      CheckDone(creatingProjects.size - updatedProjectCount)
    }

  }

  def setUsageExportBuckets(projects: Seq[RawlsBillingProjectRecord])(implicit executionContext: ExecutionContext): Future[Seq[RawlsBillingProjectRecord]] = {
    Future.traverse(projects) { project =>
      gcsDAO.setProjectUsageExportBucket(RawlsBillingProjectName(project.projectName)).map((project, _))
    } map {
      _.collect {
        case (project, Success(_)) => project
      }
    }
  }
}
