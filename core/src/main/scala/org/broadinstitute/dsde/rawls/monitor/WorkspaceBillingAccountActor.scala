package org.broadinstitute.dsde.rawls.monitor

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import cats.Applicative
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.implicits._
import com.google.api.services.cloudbilling.model.ProjectBillingInfo
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.dataaccess.slick.{BillingAccountChange, WriteAction}
import org.broadinstitute.dsde.rawls.dataaccess.{GoogleServicesDAO, SlickDataSource}
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.monitor.migration.MigrationUtils.Implicits._
import org.broadinstitute.dsde.rawls.monitor.migration.MigrationUtils.Outcome
import org.broadinstitute.dsde.rawls.monitor.migration.MigrationUtils.Outcome.{Failure, Success}

import java.time.Instant
import scala.concurrent.duration._

object WorkspaceBillingAccountActor {
  sealed trait WorkspaceBillingAccountsMessage

  case object UpdateBillingAccounts extends WorkspaceBillingAccountsMessage

  def apply(dataSource: SlickDataSource,
            gcsDAO: GoogleServicesDAO,
            initialDelay: FiniteDuration,
            pollInterval: FiniteDuration): Behavior[WorkspaceBillingAccountsMessage] =
    Behaviors.setup { context =>
      val actor = WorkspaceBillingAccountActor(dataSource, gcsDAO)
      Behaviors.withTimers { scheduler =>
        scheduler.startTimerAtFixedRate(UpdateBillingAccounts, initialDelay, pollInterval)
        Behaviors.receiveMessage {
          case UpdateBillingAccounts =>
            try actor.updateBillingAccounts.unsafeRunSync()
            catch {
              case t: Throwable => context.executionContext.reportFailure(t)
            }
            Behaviors.same
        }
      }
    }
}

final case class WorkspaceBillingAccountActor(dataSource: SlickDataSource, gcsDAO: GoogleServicesDAO)
  extends LazyLogging {

  import dataSource.dataAccess._
  import dataSource.dataAccess.driver.api._

  /* Sync billing account changes to billing projects and their associated workspaces with Google
   * one at a time.
   *
   * Why not all at once?
   * To reduce the number of stale billing account changes we make.
   *
   * Let's imagine that there are lots of billing account changes to sync. If a user changes
   * the billing account on a billing project that's currently being sync'ed then we'll have to
   * sync the billing projects and all workspaces again immediately. If we sync one at a time
   * we'll have a better chance of doing this once.
   */
  def updateBillingAccounts: IO[Unit] =
    getABillingProjectChange.flatMap(_.traverse_ { billingAccountChange =>
      for {
        _ <- info("Updating Billing Account on Billing Project in Google",
          "changeId" -> billingAccountChange.id,
          "billingProject" -> billingAccountChange.billingProjectName.value,
          "newBillingAccount" -> billingAccountChange.newBillingAccount.map(_.value)
        )

        billingProject <- loadBillingProject(billingAccountChange.billingProjectName)
        billingProbeHasAccess <- billingProject.billingAccount.traverse(gcsDAO.testDMBillingAccountAccess(_).io)
        billingProjectSyncAttempt <- syncBillingProjectWithGoogle(billingProject).attempt
        syncOutcome = Outcome.fromEither(billingProjectSyncAttempt)
        _ <- recordBillingProjectSyncOutcome(billingAccountChange, billingProbeHasAccess, syncOutcome)
        _ <- updateWorkspacesInProject(billingAccountChange, billingProject, syncOutcome)
      } yield ()
    })


  def getABillingProjectChange: IO[Option[BillingAccountChange]] =
    dataSource
      .inTransaction(_ => BillingAccountChanges.latestChanges.unsynced.take(1).result).io
      .map(_.headOption)


  private def loadBillingProject(projectName: RawlsBillingProjectName): IO[RawlsBillingProject] =
    dataSource
      .inTransaction( _ => rawlsBillingProjectQuery.load(projectName))
      .io
      .map(_.getOrElse(throw new RawlsException(s"No such billing account $projectName")))


  private def syncBillingProjectWithGoogle(project: RawlsBillingProject): IO[Unit] =
    for {
      // only v1 billing projects are backed by google projects
      isV1BillingProject <- gcsDAO.rawlsCreatedGoogleProjectExists(project.googleProjectId).io
      _ <- Applicative[IO].whenA(isV1BillingProject)(
        updateBillingAccountOnGoogle(project.googleProjectId, project.billingAccount)
      )
    } yield ()


  private def recordBillingProjectSyncOutcome(change: BillingAccountChange,
                                              billingProbeCanAccessBillingAccount: Option[Boolean],
                                              outcome: Outcome): IO[Unit] =
    for {
      syncTime <- IO(Instant.now)

      baseInfo = Seq(
        "changeId" -> change.id,
        "billingProject" -> change.billingProjectName.value,
        "newBillingAccount" -> change.newBillingAccount.map(_.value)
      )

      (_, message) = Outcome.toTuple(outcome)
      _ <- if (outcome.isSuccess)
        info("Successfully updated Billing Account on Billing Project in Google", baseInfo :_*) else
        warn("Failed to update Billing Account on Billing Project in Google", ("details" -> message) +: baseInfo :_*)

      _ <- dataSource.inTransaction { _ =>
        val thisChange = BillingAccountChanges.withId(change.id)
        val thisBillingProject = rawlsBillingProjectQuery.withProjectName(change.billingProjectName)
        DBIO.seq(
          thisChange.setGoogleSyncTime(syncTime.some),
          thisChange.setOutcome(outcome.some),
          thisBillingProject.setInvalidBillingAccount(!billingProbeCanAccessBillingAccount.getOrElse(true)),
          thisBillingProject.setMessage(message)
        )
      }.io
    } yield ()


  def updateWorkspacesInProject(billingAccountChange: BillingAccountChange,
                                billingProject: RawlsBillingProject,
                                billingProjectSyncOutcome: Outcome): IO[Unit] =
    for {
      // v1 workspaces use the v1 billing project's google project and we've already attempted
      // to update which billing account it uses. We can update, therefore, all workspaces that
      // use that google project with the outcome of setting the billing account on the v1
      // billing project.
      _ <- setWorkspaceBillingAccountAndErrorMessage(
        workspaceQuery
          .withBillingProject(billingProject.projectName)
          .withGoogleProjectId(billingProject.googleProjectId),
        billingProject.billingAccount,
        Outcome.toTuple(billingProjectSyncOutcome)._2
      )

      // v2 workspaces have their own google project so we'll need to attempt to set the
      // billing account on each.
      v2Workspaces <- listV2WorkspacesInProject(billingProject)
      _ <- v2Workspaces.traverse_ { workspace =>
        for {
          _ <- info("Updating Billing Account on Workspace Google Project",
            "changeId" -> billingAccountChange.id,
            "workspace" -> workspace.toWorkspaceName,
            "newBillingAccount" -> billingAccountChange.newBillingAccount.map(_.value)
          )

          workspaceSyncAttempt <- updateBillingAccountOnGoogle(
            workspace.googleProjectId,
            billingProject.billingAccount
          ).attempt

          _ <- recordV2WorkspaceSyncOutcome(
            billingAccountChange,
            workspace,
            billingProject.billingAccount,
            Outcome.fromEither(workspaceSyncAttempt)
          )
        } yield ()
      }
    } yield ()


  private def listV2WorkspacesInProject(billingProject: RawlsBillingProject): IO[List[Workspace]] =
    dataSource.inTransaction { _ =>
      workspaceQuery
        .withBillingProject(billingProject.projectName)
        .withoutGoogleProjectId(billingProject.googleProjectId)
        .read
    }
      .io
      .map(_.toList)


  private def updateBillingAccountOnGoogle(googleProjectId: GoogleProjectId, newBillingAccount: Option[RawlsBillingAccountName]): IO[Unit] =
    for {
      projectBillingInfo <- gcsDAO.getBillingInfoForGoogleProject(googleProjectId).io
      currentBillingAccountOnGoogle = getBillingAccountOption(projectBillingInfo)
      _ <- Applicative[IO].whenA(newBillingAccount != currentBillingAccountOnGoogle) {
        setBillingAccountOnGoogleProject(googleProjectId, newBillingAccount)
      }
    } yield ()


  private def recordV2WorkspaceSyncOutcome(change: BillingAccountChange,
                                           workspace: Workspace,
                                           billingAccount: Option[RawlsBillingAccountName],
                                           outcome: Outcome): IO[Unit] =
    for {
      failureMessage <- outcome match {
        case Success =>
          IO.pure(None) <* info("Successfully updated Billing Account on Workspace Google Project",
            "changeId" -> change.id,
            "workspace" -> workspace.toWorkspaceName,
            "newBillingAccount" -> billingAccount.map(_.value)
          )
        case Failure(message) =>
          IO.pure(message.some) <* warn("Failed to update Billing Account on Workspace Google Project",
            "changeId" -> change.id,
            "details" -> message,
            "workspace" -> workspace.toWorkspaceName,
            "newBillingAccount" -> billingAccount.map(_.value)
          )
      }

      _ <- setWorkspaceBillingAccountAndErrorMessage(
        workspaceQuery.withWorkspaceId(workspace.workspaceIdAsUUID),
        billingAccount,
        failureMessage
      )
    } yield ()


  private def setWorkspaceBillingAccountAndErrorMessage(workspacesToUpdate: WorkspaceQueryType,
                                                        billingAccount: Option[RawlsBillingAccountName],
                                                        errorMessage: Option[String]): IO[Unit] =
    dataSource.inTransaction { _ =>
      workspacesToUpdate.setBillingAccountErrorMessage(errorMessage) *>
        Applicative[WriteAction].whenA(errorMessage.isEmpty) {
          workspacesToUpdate.setCurrentBillingAccountOnGoogleProject(billingAccount)
        }
    }.io


  /**
    * Explicitly sets the Billing Account value on the given Google Project.  Any logic or conditionals controlling
    * whether this update gets called should be written in the calling method(s).
    *
    * @param googleProjectId
    * @param newBillingAccount
    */
  private def setBillingAccountOnGoogleProject(googleProjectId: GoogleProjectId,
                                               newBillingAccount: Option[RawlsBillingAccountName]): IO[ProjectBillingInfo] =
    newBillingAccount match {
      case Some(billingAccount) => gcsDAO.setBillingAccountName(googleProjectId, billingAccount).io
      case None => gcsDAO.disableBillingOnGoogleProject(googleProjectId).io
    }


  /**
    * Gets the Billing Account name out of a ProjectBillingInfo object wrapped in an Option[RawlsBillingAccountName] and
    * appropriately converts `null` or `empty` String into a None.
    *
    * @param projectBillingInfo
    * @return
    */
  private def getBillingAccountOption(projectBillingInfo: ProjectBillingInfo): Option[RawlsBillingAccountName] =
    Option(projectBillingInfo.getBillingAccountName).filter(!_.isBlank).map(RawlsBillingAccountName)


  private def info(message: String, data: (String, Any)*) : IO[Unit] =
    IO(logger.info((("message" -> message) +: data).toJson.prettyPrint))


  private def warn(message: String, data: (String, Any)*) : IO[Unit] =
    IO(logger.warn((("message" -> message) +: data).toJson.prettyPrint))

}

final case class WorkspaceBillingAccountMonitorConfig(pollInterval: FiniteDuration, initialDelay: FiniteDuration)
