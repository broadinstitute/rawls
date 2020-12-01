package org.broadinstitute.dsde.rawls.monitor

import akka.actor._
import akka.http.scaladsl.model.StatusCodes
import cats.effect.{ContextShift, IO}
import cats.implicits._
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.dataaccess.{GoogleServicesDAO, SlickDataSource}
import org.broadinstitute.dsde.rawls.model.{GoogleProjectId, RawlsBillingAccountName}
import org.broadinstitute.dsde.rawls.monitor.WorkspaceBillingAccountMonitor.CheckAll

import scala.concurrent.{ExecutionContext, Future}
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
    for {
      workspacesToUpdate <- datasource.inTransaction { dataAccess =>
        dataAccess.workspaceQuery.listWorkspaceGoogleProjectsWithIncorrectBillingAccounts()
      }
      _ <- workspacesToUpdate.toList.traverse {
        case (googleProjectId, billingAccount) => IO.fromFuture(IO(updateGoogleAndDatabase(googleProjectId, billingAccount)))
      }.unsafeToFuture()
    } yield()
  }

  private def updateGoogleAndDatabase(googleProjectId: GoogleProjectId, billingAccount: Option[RawlsBillingAccountName]): Future[Int] = {
    for {
      _ <- gcsDAO.updateGoogleProjectBillingAccount(googleProjectId, billingAccount).recoverWith {
        case e: RawlsExceptionWithErrorReport if e.errorReport.statusCode == Option(StatusCodes.Forbidden) && billingAccount.isDefined =>
          datasource.inTransaction( { dataAccess =>
            dataAccess.rawlsBillingProjectQuery.updateBillingAccountValidity(billingAccount.get, isInvalid = true)
          }).map(_ => throw e)
      }
      dbResult <- datasource.inTransaction( { dataAccess =>
        dataAccess.workspaceQuery.updateWorkspaceBillingAccount(googleProjectId, billingAccount)
      })
    } yield dbResult
  }
}
