package org.broadinstitute.dsde.rawls.monitor

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import cats.data._
import cats.effect.unsafe.implicits.global
import cats.effect.{IO, LiftIO}
import cats.implicits.{catsSyntaxApplicativeError, catsSyntaxApply, catsSyntaxOptionId, catsSyntaxSemigroup, toFlatMapOps, toFoldableOps, toFunctorOps}
import cats.mtl.Ask
import cats.{Applicative, Functor, Monad, MonadThrow}
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.dataaccess.slick.{BillingAccountChange, ReadWriteAction, WriteAction}
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
    readABillingProjectChange.flatMap(_.traverse_ {
      syncBillingAccountChange[Kleisli[IO, BillingAccountChange, *]]
        .handleErrorWith(failChange[Kleisli[IO, BillingAccountChange, *]])
        .run
    })


  def readABillingProjectChange: IO[Option[BillingAccountChange]] =
    inTransaction {
      BillingAccountChanges
        .latestChanges
        .unsynced
        .take(1)
        .result
    }.map(_.headOption)


  private def syncBillingAccountChange[F[_]](implicit R: Ask[F, BillingAccountChange], M: MonadThrow[F], L: LiftIO[F])
  : F[Unit] =
    for {
      _ <- info("Updating Billing Account on Billing Project in Google")
      billingProject <- loadBillingProject
      // the billing probe can only access billing accounts that are defined
      billingProbeHasAccess <- billingProject.billingAccount match {
        case Some(accountName) => L.liftIO(gcsDAO.testDMBillingAccountAccess(accountName).io)
        case None => M.pure(false)
      }

      updateBillingProjectOutcome <- syncBillingProjectWithGoogle(billingProject)
        .attempt
        .map(Outcome.fromEither)

      _ <- writeUpdateBillingProjectOutcome(billingProbeHasAccess, updateBillingProjectOutcome)
      updateWorkspacesOutcome <- updateWorkspacesInProject(billingProject, updateBillingProjectOutcome)
      _ <- setBillingAccountChangeOutcome(updateBillingProjectOutcome |+| updateWorkspacesOutcome)
    } yield ()


  private def loadBillingProject[F[_]](implicit R: Ask[F, BillingAccountChange], M: Monad[F], L: LiftIO[F])
  : F[RawlsBillingProject] =
    for {
      projectName <- R.reader(_.billingProjectName)
      projectOpt <- inTransaction(rawlsBillingProjectQuery.load(projectName))
    } yield projectOpt.getOrElse(throw new IllegalStateException(s"No such billing account $projectName"))


  private def syncBillingProjectWithGoogle[F[_]](billingProject: RawlsBillingProject)
                                                (implicit R: Ask[F, BillingAccountChange], M: Monad[F], L: LiftIO[F])
  : F[Unit] =
    for {
      // only v1 billing projects are backed by google projects
      isV1BillingProject <- L.liftIO {
        gcsDAO.rawlsCreatedGoogleProjectExists(billingProject.googleProjectId).io
      }
      _ <- M.whenA(isV1BillingProject) {
        setGoogleProjectBillingProject(billingProject.googleProjectId)
      }
    } yield ()


  private def writeUpdateBillingProjectOutcome[F[_]](billingProbeCanAccessBillingAccount: Boolean, outcome: Outcome)
                                                    (implicit R: Ask[F, BillingAccountChange], M: Monad[F], L: LiftIO[F])
  : F[Unit] =
    for {
      change <- R.ask
      (_, message) = Outcome.toTuple(outcome)
      _ <- if (outcome.isSuccess)
        info("Successfully updated Billing Account on Billing Project in Google") else
        warn("Failed to update Billing Account on Billing Project in Google", "details" -> message)


      _ <- inTransaction {
        val thisBillingProject = rawlsBillingProjectQuery.withProjectName(change.billingProjectName)
        DBIO.seq(
          thisBillingProject.setInvalidBillingAccount(change.newBillingAccount.isDefined && !billingProbeCanAccessBillingAccount),
          thisBillingProject.setMessage(message)
        )
      }
    } yield ()


  def updateWorkspacesInProject[F[_]](billingProject: RawlsBillingProject, billingProjectSyncOutcome: Outcome)
                                     (implicit R: Ask[F, BillingAccountChange], M: MonadThrow[F], L: LiftIO[F])
  : F[Outcome] =
    for {
      // v1 workspaces use the v1 billing project's google project and we've already attempted
      // to update which billing account it uses. We can update, therefore, all workspaces that
      // use that google project with the outcome of setting the billing account on the v1
      // billing project.
      _ <- setWorkspaceBillingAccountAndErrorMessage(
        workspaceQuery
          .withBillingProject(billingProject.projectName)
          .withGoogleProjectId(billingProject.googleProjectId),
        Outcome.toTuple(billingProjectSyncOutcome)._2
      )

      // v2 workspaces have their own google project so we'll need to attempt to set the
      // billing account on each.
      v2Workspaces <- inTransaction {
        workspaceQuery
          .withBillingProject(billingProject.projectName)
          .withoutGoogleProjectId(billingProject.googleProjectId)
          .read
      }

      v2Outcome <- v2Workspaces.foldMapA { workspace =>
        for {
          _ <- info("Updating Billing Account on Workspace Google Project",
            "workspace" -> workspace.toWorkspaceName
          )

          workspaceSyncAttempt <- setGoogleProjectBillingProject(workspace.googleProjectId).attempt
          updateWorkspaceOutcome = Outcome.fromEither(workspaceSyncAttempt)
          _ <- writeUpdateV2WorkspaceOutcome(
            workspace,
            updateWorkspaceOutcome
          )
        } yield updateWorkspaceOutcome
      }
    } yield v2Outcome


  private def setGoogleProjectBillingProject[F[_]](googleProjectId: GoogleProjectId)
                                                  (implicit R: Ask[F, BillingAccountChange], M: Monad[F], L: LiftIO[F])
  : F[Unit] =
  for {
    projectBillingInfo <- L.liftIO {
      gcsDAO.getBillingInfoForGoogleProject(googleProjectId).io
    }

    // convert `null` or `empty` Strings to `None`
    currentBillingAccountOnGoogle = Option(projectBillingInfo.getBillingAccountName)
      .filter(!_.isBlank)
      .map(RawlsBillingAccountName)

    newBillingAccount <- R.reader(_.newBillingAccount)
    _ <- M.whenA(newBillingAccount != currentBillingAccountOnGoogle) {
      L.liftIO {
        gcsDAO.setBillingAccount(googleProjectId, newBillingAccount).io
      }
    }
  } yield ()


  private def writeUpdateV2WorkspaceOutcome[F[_]](workspace: Workspace, outcome: Outcome)
                                                 (implicit R: Ask[F, BillingAccountChange], M: Monad[F], L: LiftIO[F])
  : F[Unit] =
    for {
      failureMessage <- outcome match {
        case Success =>
          M.pure(None) <* info("Successfully updated Billing Account on Workspace Google Project",
            "workspace" -> workspace.toWorkspaceName,
          )
        case Failure(message) =>
          M.pure(message.some) <* warn("Failed to update Billing Account on Workspace Google Project",
            "details" -> message,
            "workspace" -> workspace.toWorkspaceName
          )
      }

      _ <- setWorkspaceBillingAccountAndErrorMessage(
        workspaceQuery.withWorkspaceId(workspace.workspaceIdAsUUID),
        failureMessage
      )
    } yield ()


  private def setWorkspaceBillingAccountAndErrorMessage[F[_]](workspacesToUpdate: WorkspaceQueryType, errorMessage: Option[String])
                                                             (implicit R: Ask[F, BillingAccountChange], L: LiftIO[F], M: Monad[F])
  : F[Unit] =
    for {
      billingAccount <- R.reader(_.newBillingAccount)
      _ <- inTransaction {
        workspacesToUpdate.setBillingAccountErrorMessage(errorMessage) *>
          Applicative[WriteAction].whenA(errorMessage.isEmpty) {
            workspacesToUpdate.setCurrentBillingAccountOnGoogleProject(billingAccount)
          }
      }
    } yield ()


  private def failChange[F[_]](throwable: Throwable)
                              (implicit R: Ask[F, BillingAccountChange], M: Monad[F], L: LiftIO[F])
  : F[Unit] =
    setBillingAccountChangeOutcome(Failure(throwable.getMessage))


  private def setBillingAccountChangeOutcome[F[_]](outcome: Outcome)
                                                  (implicit R: Ask[F, BillingAccountChange], M: Monad[F], L: LiftIO[F])
  : F[Unit] =
    for {
      changeId <- R.reader(_.id)
      record = BillingAccountChanges.withId(changeId)
      _ <- inTransaction {
        record.setGoogleSyncTime(Instant.now().some) *> record.setOutcome(outcome.some)
      }
    } yield ()


  private def inTransaction[A, F[_]](action: ReadWriteAction[A])
                                    (implicit F: LiftIO[F])
  : F[A] =
    F.liftIO {
      dataSource.inTransaction(_ => action).io
    }


  private def info[F[_]](message: String, data: (String, Any)*)
                        (implicit R: Ask[F, BillingAccountChange], F: Functor[F])
  : F[Unit] =
    logContext.map { context =>
      logger.info((("message" -> message) +: (data ++ context)).toJson.prettyPrint)
    }


  private def warn[F[_]](message: String, data: (String, Any)*)
                        (implicit R: Ask[F, BillingAccountChange], F: Functor[F])
  : F[Unit] =
    logContext.map { context =>
      logger.warn((("message" -> message) +: (data ++ context)).toJson.prettyPrint)
    }

  private def logContext[F[_]](implicit F: Ask[F, BillingAccountChange])
  : F[Map[String, Any]] =
    F.reader { change =>
      Map(
        "changeId" -> change.id,
        "billingProject" -> change.billingProjectName.value,
        "previousBillingAccount" -> change.previousBillingAccount.map(_.value),
        "newBillingAccount" -> change.newBillingAccount.map(_.value)
      )
    }
}

final case class WorkspaceBillingAccountMonitorConfig(pollInterval: FiniteDuration, initialDelay: FiniteDuration)
