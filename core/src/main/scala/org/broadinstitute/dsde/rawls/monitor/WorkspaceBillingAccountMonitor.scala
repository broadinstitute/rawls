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
    * @param force
    * @param executionContext
    * @return
    */
  def updateBillingAccountInRawlsAndGoogle(googleProjectId: GoogleProjectId,
                                           newBillingAccount: Option[RawlsBillingAccountName],
                                           oldBillingAccount: Option[RawlsBillingAccountName])(implicit executionContext: ExecutionContext): Future[ProjectBillingInfo] = {
    logger.info(s"Attempting to update Billing Account from ${oldBillingAccount} to ${newBillingAccount} on all Workspaces in Google Project ${googleProjectId}")
    for {
      projectBillingInfo <- gcsDAO.getBillingInfoForGoogleProject(googleProjectId)
      currentBillingAccountOnGoogle = getBillingAccountOption(projectBillingInfo)
      _ = validateBillingAccountValuesOrThrow(newBillingAccount, oldBillingAccount, currentBillingAccountOnGoogle)
      _ <- if (shouldGoogleBeUpdated(oldBillingAccount, currentBillingAccountOnGoogle)) {
        logger.warn("true google should be updated")
        setBillingAccountOnGoogleProject(googleProjectId, newBillingAccount)
      } else {
        logger.warn(s"Not updating google oldBillingAccount:${oldBillingAccount} currentBillingAccountOnGoogle:${currentBillingAccountOnGoogle}")
        Future.successful()
      }
      _ <- if (shouldRawlsBeUpdated(newBillingAccount, oldBillingAccount, currentBillingAccountOnGoogle)) {
        logger.warn("true rawls should be updated")
        setBillingAccountOnWorkspacesInProject(googleProjectId, oldBillingAccount, newBillingAccount)
      } else {
        logger.warn(s"Not updating rawls newBillingAccount:${newBillingAccount} oldBillingAccount:${oldBillingAccount} currentBillingAccountOnGoogle:${currentBillingAccountOnGoogle}")

        Future.successful()
      }
    } yield projectBillingInfo
  }

  /**
    * Explicitly sets the Billing Account value on the given Google Project.  Any logic or conditionals controlling
    * whether this update gets called should be written in the calling method(s).
    * @param googleProjectId
    * @param newBillingAccount
    */
  private def setBillingAccountOnGoogleProject(googleProjectId: GoogleProjectId,
                                               newBillingAccount: Option[RawlsBillingAccountName]): Future[ProjectBillingInfo] = {
    try {
      newBillingAccount match {
        case Some(billingAccount) => gcsDAO.setBillingAccountName(googleProjectId, billingAccount)
        case None => gcsDAO.disableBillingOnGoogleProject(googleProjectId)
      }
    } catch {
      case e: RawlsExceptionWithErrorReport if e.errorReport.statusCode == Option(StatusCodes.Forbidden) && newBillingAccount.isDefined =>
        val message = s"Rawls does not have permission to set the Billing Account on ${googleProjectId} to ${newBillingAccount.get}"
        dataSource.inTransaction({ dataAccess =>
          dataAccess.rawlsBillingProjectQuery.updateBillingAccountValidity(newBillingAccount.get, isInvalid = true)
          dataAccess.workspaceQuery.updateWorkspaceBillingAccountErrorMessages(googleProjectId, s"${message} ${e.getMessage}")
        }).map { _ =>
          logger.warn(message, e)
          throw e
        }
      case e: Throwable =>
        dataSource.inTransaction { dataAccess =>
          dataAccess.workspaceQuery.updateWorkspaceBillingAccountErrorMessages(googleProjectId, e.getMessage)
        }.map { _ =>
          throw e
        }
    }
  }

  /**
    * Explicitly sets the Billing Account value for all Workspaces that are in the given Project.  Any logic or
    * conditionals controlling whether this update gets called should be written in the calling method(s).
    * @param googleProjectId
    * @param oldBillingAccount
    * @param newBillingAccount
    * @return
    */
  private def setBillingAccountOnWorkspacesInProject(googleProjectId: GoogleProjectId,
                                                     oldBillingAccount: Option[RawlsBillingAccountName],
                                                     newBillingAccount: Option[RawlsBillingAccountName]): Future[Int] = {
    logger.warn("In setting billing account on workspaces")
    dataSource.inTransaction( { dataAccess =>
      dataAccess.workspaceQuery.updateWorkspaceBillingAccount(googleProjectId, newBillingAccount)
    })
  }

  /**
    * Gets the Billing Account name out of a ProjectBillingInfo object wrapped in an Option[RawlsBillingAccountName] and
    * appropriately converts `null` or `empty` String into a None.
    * @param projectBillingInfo
    * @return
    */
  private def getBillingAccountOption(projectBillingInfo: ProjectBillingInfo): Option[RawlsBillingAccountName] = {
    if (projectBillingInfo.getBillingAccountName == null || projectBillingInfo.getBillingAccountName.isBlank)
      None
    else
      Option(RawlsBillingAccountName(projectBillingInfo.getBillingAccountName))
  }

  /**
    * Google should only be updated if Terra and Google are in agreement about the value that it being overwritten.
    * @param oldBillingAccount
    * @param currentBillingAccountOnGoogle
    * @return
    */
  private def shouldGoogleBeUpdated(oldBillingAccount: Option[RawlsBillingAccountName],
                                    currentBillingAccountOnGoogle: Option[RawlsBillingAccountName]): Boolean = {
        currentBillingAccountOnGoogle == oldBillingAccount
  }

  /**
    * We want to keep Rawls in agreement with the Billing Account on Google, so if we are updating the Billing Account
    * on Google, then we also want to update the Billing Account recorded in Rawls.  If Rawls and Google do not agree
    * about what the old Billing Account was, if Rawls is being updated to be in sync with Google, then we should
    * allow the Rawls update to proceed.
    * @param newBillingAccount
    * @param oldBillingAccount
    * @param currentBillingAccountOnGoogle
    * @return
    */
  private def shouldRawlsBeUpdated(newBillingAccount: Option[RawlsBillingAccountName],
                                   oldBillingAccount: Option[RawlsBillingAccountName],
                                   currentBillingAccountOnGoogle: Option[RawlsBillingAccountName]): Boolean = {
    shouldGoogleBeUpdated(oldBillingAccount, currentBillingAccountOnGoogle) || (currentBillingAccountOnGoogle == newBillingAccount)
  }

  /**
    * Performs validations on the Billing Account changes being made.  We need to consider the new Billing Account
    * value we are trying to change to, the old Billing Account value that Rawls has on record, and the current Billing
    * Account value that Google is linked to.  Throws a `RawlsExceptionWithErrorReport` if there are any validation
    * failures.
    * @param newBillingAccount
    * @param oldBillingAccount
    * @param currentBillingAccountOnGoogle
    */
  private def validateBillingAccountValuesOrThrow(newBillingAccount: Option[RawlsBillingAccountName],
                                                  oldBillingAccount: Option[RawlsBillingAccountName],
                                                  currentBillingAccountOnGoogle: Option[RawlsBillingAccountName]): Unit = {
    val validationErrors = List[String]()

    // This uses `shouldRawlsBeUpdated` for efficiency, but nothing needs to fundamentally tie this method to shouldRawlsBeUpdated in the future
    if (!shouldRawlsBeUpdated(newBillingAccount, oldBillingAccount, currentBillingAccountOnGoogle)) {
      validationErrors + s"Billing Account on Google ${currentBillingAccountOnGoogle} does not match existing Billing Account in Rawls ${currentBillingAccountOnGoogle} or the desired new Billing Account ${newBillingAccount}"
    }

    if (validationErrors.nonEmpty) {
      val validationErrorMessage = s"Update Billing Account validation failed: ${validationErrors}"
      throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.PreconditionFailed, validationErrorMessage))
    }

    ()
  }
}

final case class WorkspaceBillingAccountMonitorConfig(pollInterval: FiniteDuration, initialDelay: FiniteDuration)
