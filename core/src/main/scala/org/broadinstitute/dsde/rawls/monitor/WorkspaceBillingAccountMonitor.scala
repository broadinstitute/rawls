package org.broadinstitute.dsde.rawls.monitor

import akka.actor._
import akka.http.scaladsl.model.StatusCodes
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.implicits._
import com.google.api.services.cloudbilling.model.ProjectBillingInfo
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.dataaccess.{GoogleServicesDAO, SlickDataSource}
import org.broadinstitute.dsde.rawls.model.{ErrorReport, GoogleProjectId, RawlsBillingAccountName, RawlsBillingProjectName}
import org.broadinstitute.dsde.rawls.monitor.WorkspaceBillingAccountMonitor.CheckAll

import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

object WorkspaceBillingAccountMonitor {
  def props(datasource: SlickDataSource, gcsDAO: GoogleServicesDAO, initialDelay: FiniteDuration, pollInterval: FiniteDuration)(implicit executionContext: ExecutionContext): Props = {
    Props(new WorkspaceBillingAccountMonitor(datasource, gcsDAO, initialDelay, pollInterval))
  }

  sealed trait WorkspaceBillingAccountsMessage
  case object CheckAll extends WorkspaceBillingAccountsMessage
  val BILLING_ACCOUNT_VALIDATION_ERROR_PREFIX = "Update Billing Account validation failed:"
}

class WorkspaceBillingAccountMonitor(dataSource: SlickDataSource, gcsDAO: GoogleServicesDAO, initialDelay: FiniteDuration, pollInterval: FiniteDuration)(implicit executionContext: ExecutionContext) extends Actor with LazyLogging {

  context.system.scheduler.scheduleWithFixedDelay(initialDelay, pollInterval, self, CheckAll)

  override def receive = {
    case CheckAll => checkAll()
  }

  private def checkAll() = {
    for {
      workspacesToUpdate <- dataSource.inTransaction { dataAccess =>
        dataAccess.workspaceQuery.listWorkspaceGoogleProjectsToUpdateWithNewBillingAccount()
      }
      _ = logger.info(s"Attempting to update workspaces: ${workspacesToUpdate.toList}")
      _ <- workspacesToUpdate.toList.traverse {
        case (googleProjectId, newBillingAccount, oldBillingAccount) =>
          IO.fromFuture(IO(updateBillingAccountInRawlsAndGoogle(googleProjectId, newBillingAccount, oldBillingAccount))).attempt.map {
            case Left(e) => {
              // We do not want to throw e here. traverse stops executing as soon as it encounters a Failure, but we
              // want to continue traversing the list to update the rest of the google project billing accounts even
              // if one of the update operations fails.
              logger.warn(s"Failed to update billing account from ${oldBillingAccount} to ${newBillingAccount} on project $googleProjectId", e)
              ()
            }
            case Right(res) => res
          }
      }.unsafeToFuture()
    } yield()
  }

  /**
    * Guiding Principle:  Always try to make Terra match what is on Google.  If they fall out of sync with each other,
    * Google wins.  If we don't know how to reconcile the difference, throw an error.
    * 1. DO NOT reenable billing if Google says Billing is disabled and Terra does not ALSO think billing is disabled
    * 2. DO NOT overwrite Billing Account if Terra and Google do not both agree on what the current Billing Account is
    *    2a. Except if Terra is trying to set the value to match whatever is currently set on Google
    * @param googleProjectId
    * @param newBillingAccount
    * @param oldBillingAccount
    * @param executionContext
    * @return
    */
  def updateBillingAccountInRawlsAndGoogle(googleProjectId: GoogleProjectId,
                                           newBillingAccount: Option[RawlsBillingAccountName],
                                           oldBillingAccount: Option[RawlsBillingAccountName])(implicit executionContext: ExecutionContext): Future[Unit] = {
    logger.info(s"Attempting to update Billing Account from ${oldBillingAccount} to ${newBillingAccount} on all Workspaces in Google Project ${googleProjectId}")
    for {
      _ <- updateBillingAccountOnGoogle(googleProjectId, newBillingAccount)
      _ <- setBillingAccountOnWorkspacesInProject(googleProjectId, newBillingAccount)
    } yield ()
  }

  private def updateBillingAccountOnGoogle(googleProjectId: GoogleProjectId, newBillingAccount: Option[RawlsBillingAccountName]): Future[Unit] = {
    val updateGoogleResult = gcsDAO.maybeUpdateBillingAccount(googleProjectId, newBillingAccount)

    updateGoogleResult.recoverWith {
      case e: RawlsExceptionWithErrorReport if e.errorReport.statusCode == Option(StatusCodes.Forbidden) && newBillingAccount.isDefined =>
        val message = s"Rawls does not have permission to set the Billing Account on ${googleProjectId} to ${newBillingAccount.get}"
        dataSource.inTransaction({ dataAccess =>
          dataAccess.rawlsBillingProjectQuery.updateBillingAccountValidity(newBillingAccount.get, isInvalid = true) >>
          dataAccess.workspaceQuery.updateWorkspaceBillingAccountErrorMessages(googleProjectId, s"${message} ${e.getMessage}")
        }).flatMap { _ =>
          logger.warn(message, e)
          Future.failed(e)
        }
      case e: Throwable =>
        dataSource.inTransaction { dataAccess =>
          dataAccess.workspaceQuery.updateWorkspaceBillingAccountErrorMessages(googleProjectId, e.getMessage)
        }.flatMap { _ =>
          Future.failed(e)
        }
    }
  }

  /**
    * Explicitly sets the Billing Account value for all Workspaces that are in the given Project.  Any logic or
    * conditionals controlling whether this update gets called should be written in the calling method(s).
    * @param googleProjectId
    * @param newBillingAccount
    * @return
    */
  private def setBillingAccountOnWorkspacesInProject(googleProjectId: GoogleProjectId,
                                                     newBillingAccount: Option[RawlsBillingAccountName]): Future[Int] =
    dataSource.inTransaction({ dataAccess =>
      dataAccess.workspaceQuery.updateWorkspaceBillingAccount(googleProjectId, newBillingAccount)
    })
}

final case class WorkspaceBillingAccountMonitorConfig(pollInterval: FiniteDuration, initialDelay: FiniteDuration)
