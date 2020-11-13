package org.broadinstitute.dsde.rawls.monitor

import akka.actor._
import cats.effect.{ContextShift, IO}
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.dataaccess.{GoogleServicesDAO, SlickDataSource}
import org.broadinstitute.dsde.rawls.monitor.WorkspaceBillingAccountMonitor.CheckAll

import scala.concurrent.{ExecutionContext}
import scala.concurrent.duration._

object WorkspaceBillingAccountMonitor {
  def props(datasource: SlickDataSource, gcsDAO: GoogleServicesDAO, initialDelay: FiniteDuration, pollInterval: FiniteDuration)(implicit executionContext: ExecutionContext, cs: ContextShift[IO]): Props = {
    Props(new WorkspaceBillingAccountMonitor(datasource, gcsDAO, initialDelay, pollInterval))
  }

  sealed trait WorkspaceBillingAccountsMessage
  case object CheckAll extends WorkspaceBillingAccountsMessage
}

class WorkspaceBillingAccountMonitor(datasource: SlickDataSource, gcsDAO: GoogleServicesDAO, initialDelay: FiniteDuration, pollInterval: FiniteDuration)(implicit executionContext: ExecutionContext, cs: ContextShift[IO]) extends Actor with LazyLogging {

  context.system.scheduler.scheduleWithFixedDelay(initialDelay, pollInterval, self, CheckAll)

  override def receive = {
    case CheckAll => checkAll()
  }

  private def checkAll() = {

  }
}
