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
import org.broadinstitute.dsde.rawls.model.{GoogleProjectId, RawlsBillingAccountName, RawlsBillingProjectName, RawlsUserSubjectId}
import org.broadinstitute.dsde.rawls.monitor.WorkspaceBillingAccountMonitor.CheckAll
import org.broadinstitute.dsde.rawls.monitor.migration.MigrationUtils.Outcome
import org.broadinstitute.dsde.rawls.monitor.migration.{MigrationUtils, WorkspaceMigration}

import java.sql.Timestamp
import java.time.Instant
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

final case class BillingAccountChange(id: Long,
                                      billingProjectName: RawlsBillingProjectName,
                                      userId: RawlsUserSubjectId,
                                      originalBillingAccount: Option[RawlsBillingAccountName],
                                      newBillingAccount: Option[RawlsBillingAccountName],
                                      created: Instant,
                                      googleSyncTime: Option[Instant],
                                      outcome: Option[Outcome]
                                     )

private[monitor]
object BillingAccountChanges {

  type RecordType = (
    Long, // id
      String, // billing project name
      String, // User Id
      Option[String], // Original billing account
      Option[String], // New billing account
      Timestamp, // Created
      Option[Timestamp], // Google sync time
      Option[String], // Outcome
      Option[String] // Message
    )

  def fromRecord(record: RecordType): Either[String, BillingAccountChange] = record match {
    case (id, billingProjectName, userId, originalBillingAccount, newBillingAccount, created, googleSyncTime, outcome, message) =>
      Outcome.fromFields(outcome, message).map { outcome =>
        BillingAccountChange(
          id,
          RawlsBillingProjectName(billingProjectName),
          RawlsUserSubjectId(userId),
          originalBillingAccount.map(RawlsBillingAccountName),
          newBillingAccount.map(RawlsBillingAccountName),
          created.toInstant,
          googleSyncTime.map(_.toInstant),
          outcome
        )
      }
  }

  def toRecord(billingAccountChange: BillingAccountChange): RecordType = {
    val (outcome, message) = billingAccountChange.outcome.map(Outcome.toTuple).getOrElse((None, None))
    (
      billingAccountChange.id,
      billingAccountChange.billingProjectName.value,
      billingAccountChange.userId.value,
      billingAccountChange.originalBillingAccount.map(_.value),
      billingAccountChange.newBillingAccount.map(_.value),
      Timestamp.from(billingAccountChange.created),
      billingAccountChange.googleSyncTime.map(Timestamp.from),
      outcome,
      message
    )
  }

  import slick.jdbc.MySQLProfile.api._

  final class BillingAccountChanges(tag: Tag)
    extends Table[BillingAccountChange](tag, "BILLING_ACCOUNT_CHANGES") {

    def id = column[Long]("ID", O.PrimaryKey, O.AutoInc)
    def billingProjectName = column[String]("BILLING_PROJECT_NAME")
    def userId = column[String]("USER_ID")
    def originalBillingAccount = column[Option[String]]("ORIGINAL_BILLING_ACCOUNT")
    def newBillingAccount = column[Option[String]]("NEW_BILLING_ACCOUNT")
    def created = column[Timestamp]("CREATED")
    def googleSyncTime = column[Option[Timestamp]]("GOOGLE_SYNC_TIME")
    def outcome = column[Option[String]]("OUTCOME")
    def message = column[Option[String]]("MESSAGE")

    override def * =
      (
        id, billingProjectName, userId, originalBillingAccount, newBillingAccount, created,
        googleSyncTime, outcome, message
      ) <> (
        r => MigrationUtils.unsafeFromEither(fromRecord(r)),
        toRecord(_: BillingAccountChange).some
      )
  }
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
    val updateGoogleResult = for {
      projectBillingInfo <- gcsDAO.getBillingInfoForGoogleProject(googleProjectId)
      currentBillingAccountOnGoogle = getBillingAccountOption(projectBillingInfo)
      _ <- if (newBillingAccount != currentBillingAccountOnGoogle) {
        setBillingAccountOnGoogleProject(googleProjectId, newBillingAccount)
      } else {
        logger.info(s"Not updating Billing Account on Google Project ${googleProjectId} because currentBillingAccountOnGoogle:${currentBillingAccountOnGoogle} is the same as the newBillingAccount:${newBillingAccount}")
        Future.successful()
      }
    } yield ()

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
    * Explicitly sets the Billing Account value on the given Google Project.  Any logic or conditionals controlling
    * whether this update gets called should be written in the calling method(s).
 *
    * @param googleProjectId
    * @param newBillingAccount
    */
  private def setBillingAccountOnGoogleProject(googleProjectId: GoogleProjectId,
                                               newBillingAccount: Option[RawlsBillingAccountName]): Future[ProjectBillingInfo] =
    newBillingAccount match {
      case Some(billingAccount) => gcsDAO.setBillingAccountName(googleProjectId, billingAccount)
      case None => gcsDAO.disableBillingOnGoogleProject(googleProjectId)
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
}

final case class WorkspaceBillingAccountMonitorConfig(pollInterval: FiniteDuration, initialDelay: FiniteDuration)
