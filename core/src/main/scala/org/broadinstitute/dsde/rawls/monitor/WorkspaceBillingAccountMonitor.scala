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
import org.broadinstitute.dsde.rawls.model.{ErrorReport, GoogleProjectId, RawlsBillingAccountName}
import org.broadinstitute.dsde.rawls.monitor.WorkspaceBillingAccountMonitor.CheckAll

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

object WorkspaceBillingAccountMonitor {
  def props(datasource: SlickDataSource, gcsDAO: GoogleServicesDAO, initialDelay: FiniteDuration, pollInterval: FiniteDuration)(implicit executionContext: ExecutionContext): Props = {
    Props(new WorkspaceBillingAccountMonitor(datasource, gcsDAO, initialDelay, pollInterval))
  }

  sealed trait WorkspaceBillingAccountsMessage
  case object CheckAll extends WorkspaceBillingAccountsMessage
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
          IO.fromFuture(IO(updateGoogleAndDatabase(googleProjectId, newBillingAccount, oldBillingAccount))).attempt.map {
            case Left(e) => {
              // We do not want to throw e here. traverse stops executing as soon as it encounters a Failure, but we
              // want to continue traversing the list to update the rest of the google project billing accounts even
              // if one of the update operations fails.
              logger.warn(s"Failed to update billing account from ${oldBillingAccount}to ${newBillingAccount} on project $googleProjectId", e)
              ()
            }
            case Right(res) => res
          }
      }.unsafeToFuture()
    } yield()
  }

  def NEW_updateGoogleProjectBillingAccount(googleProjectId: GoogleProjectId,
                                            newBillingAccount: Option[RawlsBillingAccountName],
                                            oldBillingAccount: Option[RawlsBillingAccountName],
                                            force: Boolean = false)(implicit executionContext: ExecutionContext): Future[ProjectBillingInfo] = {

    // Guiding Principle:  Always try to make Terra match what is on Google.  If they fall out of sync with each other,
    // Google wins.  If we don't know how to reconcile the difference, throw an error.
    // 1. DO NOT reenable billing if Google says Billing is disabled and Terra does not ALSO think billing is disabled
    // 2. DO NOT overwrite Billing Account if Terra and Google do not both agree on what the current Billing Account is
    //    2a. Except if Terra is trying to set the value to match whatever is currently set on Google

    val shouldUpdate, projectBillingInfo = for {
      projectBillingInfo <- getBillingInfoForGoogleProject(googleProjectId)
      shouldUpdate = force || validateBillingAccountShouldBeUpdatedInGoogle(newBillingAccount, oldBillingAccount, projectBillingInfo)
    } yield (shouldUpdate, projectBillingInfo)


  }

  def shouldGoogleBeUpdated(oldBillingAccount: Option[RawlsBillingAccountName],
                            projectBillingInfo: ProjectBillingInfo): Boolean = {
    val currentBillingAccountOnGoogle = if (projectBillingInfo.getBillingAccountName == null || projectBillingInfo.getBillingAccountName.isBlank)
      None
    else
      Option(RawlsBillingAccountName(projectBillingInfo.getBillingAccountName))

    currentBillingAccountOnGoogle match {
      case oldBillingAccount => true // google and rawls
      case _ => false
    }
  }

  // maybe return a bool or just throw errors
  def validateBillingAccountShouldBeUpdatedInGoogle(newBillingAccount: Option[RawlsBillingAccountName],
                             oldBillingAccount: Option[RawlsBillingAccountName],
                             projectBillingInfo: ProjectBillingInfo): Boolean = {
    val currentBillingAccountOnGoogle = if (projectBillingInfo.getBillingAccountName == null || projectBillingInfo.getBillingAccountName.isBlank)
      None
    else
      Option(RawlsBillingAccountName(projectBillingInfo.getBillingAccountName))

    val isBillingEnableOnGoogle = projectBillingInfo.getBillingEnabled

    currentBillingAccountOnGoogle match {
      case oldBillingAccount => true // google and rawls
      case newBillingAccount => true // just rawls
      case _ => // neither
        throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.PreconditionFailed,
          s"Could not update Billing Account on Google Project ID ${projectBillingInfo.getProjectId} to Billing Account ${newBillingAccount} because Billing Account in Rawls ${oldBillingAccount} did not equal current Billing Account in Google ${projectBillingInfo.getBillingAccountName}"))
    }

    //    newBillingAccount match {
    //      case Some(billingAccount) if !isBillingEnableOnGoogle && oldBillingAccount.nonEmpty => // we don't think we need this...?
    //      case
    //    }
  }

  // validation should already be done
  // will determine which call to make to google to update the Billing Account or to disable billing
  def performUpdate(googleProjectId: GoogleProjectId,
                    newBillingAccount: Option[RawlsBillingAccountName],
                    billingAccountOnGoogle: Option[RawlsBillingAccountName]): Future[Unit] = {
    newBillingAccount match {
      case Some(billingAccountName) =>
        if (newBillingAccount != billingAccountOnGoogle) {
          setBillingAccountName(googleProjectId, billingAccountName)
        }
      case None =>
        disableBillingOnGoogleProject(googleProjectId)
    }
  }

  private def updateGoogleAndDatabase(googleProjectId: GoogleProjectId,
                                      newBillingAccount: Option[RawlsBillingAccountName],
                                      oldBillingAccount: Option[RawlsBillingAccountName]): Future[Int] = {
    for {
      _ <- gcsDAO.updateGoogleProjectBillingAccount(googleProjectId, newBillingAccount, oldBillingAccount).recoverWith {
        case e: RawlsExceptionWithErrorReport if e.errorReport.statusCode == Option(StatusCodes.Forbidden) && newBillingAccount.isDefined =>
          dataSource.inTransaction( { dataAccess =>
            dataAccess.rawlsBillingProjectQuery.updateBillingAccountValidity(newBillingAccount.get, isInvalid = true)
          }).map { _ =>
            logger.error(s"Rawls does not have permission to set the billing account on ${googleProjectId} to ${newBillingAccount.get}", e)
            throw e
          }
        case e =>
          dataSource.inTransaction { dataAccess =>
            dataAccess.workspaceQuery.updateWorkspaceBillingAccountErrorMessages(googleProjectId, e.getMessage)
          }.map { _ =>
            logger.error(s"Failure while trying to update Billing Account from ${oldBillingAccount} to ${newBillingAccount} on Google Project ${googleProjectId}", e)
            throw e
          }
      }
      dbResult <- dataSource.inTransaction( { dataAccess =>
        logger.info(s"Updating Billing Account from ${oldBillingAccount} to ${newBillingAccount} on all Workspaces in Google Project ${googleProjectId}")
        dataAccess.workspaceQuery.updateWorkspaceBillingAccount(googleProjectId, newBillingAccount)
      })
    } yield dbResult
  }
}

final case class WorkspaceBillingAccountMonitorConfig(pollInterval: FiniteDuration, initialDelay: FiniteDuration)
