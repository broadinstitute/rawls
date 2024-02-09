package org.broadinstitute.dsde.rawls.monitor

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import cats.data._
import cats.effect.unsafe.implicits.global
import cats.effect.{IO, LiftIO}
import cats.implicits.{catsSyntaxApplicativeError, catsSyntaxApplyOps, catsSyntaxOptionId, catsSyntaxSemigroup, toFlatMapOps, toFoldableOps, toFunctorOps}
import cats.mtl.Ask
import cats.{Applicative, Functor, Monad, MonadThrow}
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.dataaccess.slick.{BillingAccountChange, ReadWriteAction, WriteAction}
import org.broadinstitute.dsde.rawls.dataaccess.{GoogleServicesDAO, SamDAO, SlickDataSource}
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.monitor.migration.MigrationUtils.Implicits._
import org.broadinstitute.dsde.rawls.monitor.migration.MigrationUtils.Outcome
import org.broadinstitute.dsde.rawls.monitor.migration.MigrationUtils.Outcome.{Failure, Success}
import org.broadinstitute.dsde.rawls.util.TracingUtils

import java.time.Instant
import scala.concurrent.duration._

object BillingAccountChangeSynchronizer {
  sealed trait WorkspaceBillingAccountsMessage

  case object UpdateBillingAccounts extends WorkspaceBillingAccountsMessage

  def apply(dataSource: SlickDataSource,
            gcsDAO: GoogleServicesDAO,
            samDAO: SamDAO,
            initialDelay: FiniteDuration,
            pollInterval: FiniteDuration
  ): Behavior[WorkspaceBillingAccountsMessage] =
    Behaviors.setup { context =>
      val actor = BillingAccountChangeSynchronizer(dataSource, gcsDAO, samDAO)
      Behaviors.withTimers { scheduler =>
        scheduler.startTimerAtFixedRate(UpdateBillingAccounts, initialDelay, pollInterval)
        Behaviors.receiveMessage { case UpdateBillingAccounts =>
          try actor.updateBillingAccounts.unsafeRunSync()
          catch {
            case t: Throwable => context.executionContext.reportFailure(t)
          }
          Behaviors.same
        }
      }
    }
}

final case class BillingAccountChangeSynchronizer(dataSource: SlickDataSource,
                                                  gcsDAO: GoogleServicesDAO,
                                                  samDAO: SamDAO
) extends LazyLogging {

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
  def updateBillingAccounts: IO[Unit] = TracingUtils.traceIO("updateBillingAccounts") { tracingContext =>
    readABillingProjectChange.flatMap(_.traverse_ {
      syncBillingAccountChange[Kleisli[IO, BillingAccountChange, *]](tracingContext)
        .handleErrorWith(failChange[Kleisli[IO, BillingAccountChange, *]])
        .run
    })
  }

  def readABillingProjectChange: IO[Option[BillingAccountChange]] =
    inTransaction {
      BillingAccountChanges.latestChanges.unsynced
        .take(1)
        .result
    }.map(_.headOption)

  private def syncBillingAccountChange[F[_]](tracingContext: RawlsTracingContext)(implicit
    R: Ask[F, BillingAccountChange],
    M: MonadThrow[F],
    L: LiftIO[F]
  ): F[Unit] =
    for {
      billingProject <- loadBillingProject

      // v1 billing projects are backed by google projects and are used for v1 workspace billing
      updateBillingProjectOutcome <- M.ifM(isV1BillingProject(billingProject.projectName))(
        updateBillingProjectGoogleProject(billingProject, tracingContext),
        M.pure(Success.asInstanceOf[Outcome])
      )

      updateWorkspacesOutcome <- updateWorkspacesBillingAccountInGoogle(billingProject, tracingContext)
      _ <- writeBillingAccountChangeOutcome(updateBillingProjectOutcome |+| updateWorkspacesOutcome)
    } yield ()

  private def loadBillingProject[F[_]](implicit
    R: Ask[F, BillingAccountChange],
    M: Monad[F],
    L: LiftIO[F]
  ): F[RawlsBillingProject] =
    for {
      projectName <- R.reader(_.billingProjectName)
      projectOpt <- inTransaction(rawlsBillingProjectQuery.load(projectName))
    } yield projectOpt.getOrElse(throw new IllegalStateException(s"No such billing account $projectName"))

  private def updateBillingProjectGoogleProject[F[_]](
                                                       billingProject: RawlsBillingProject, tracingContext: RawlsTracingContext
  )(implicit R: Ask[F, BillingAccountChange], M: MonadThrow[F], L: LiftIO[F]): F[Outcome] =
    for {
      // the billing probe can only access billing accounts that are defined
      billingProbeHasAccess <- billingProject.billingAccount match {
        case Some(accountName) => L.liftIO(gcsDAO.testTerraBillingAccountAccess(accountName).io)
        case None              => M.pure(false)
      }

      outcome <- setGoogleProjectBillingAccount(billingProject.googleProjectId, tracingContext).attempt
        .map(Outcome.fromEither)

      _ <- writeUpdateBillingProjectOutcome(billingProbeHasAccess, outcome)
    } yield outcome

  // There's no quick way of telling if an arbitrary billing project is v1 or not. We don't want
  // to blindly set the billing account on any google project with an id equal to the billing
  // project name though. We can use Sam's resource Admin APIs to list the child resources of the
  // billing project. If a google project resource is listed then we can be sure that this is a v1
  // billing project.
  private def isV1BillingProject[F[_]](
    billingProjectName: RawlsBillingProjectName
  )(implicit R: Ask[F, BillingAccountChange], M: Monad[F], L: LiftIO[F]): F[Boolean] =
    L.liftIO {
      for {
        userInfo <- gcsDAO.getServiceAccountUserInfo().io
        childResources <- samDAO.asResourceAdmin(SamResourceTypeNames.billingProject,
                                                 billingProjectName.value,
                                                 SamBillingProjectPolicyNames.owner,
                                                 RawlsRequestContext(userInfo)
        ) {
          samDAO
            .listResourceChildren(SamResourceTypeNames.billingProject,
                                  billingProjectName.value,
                                  RawlsRequestContext(userInfo)
            )
            .io
        }

        googleProjectResource = SamFullyQualifiedResourceId(billingProjectName.value,
                                                            SamResourceTypeNames.googleProject.value
        )

      } yield childResources.contains(googleProjectResource)
    }

  private def writeUpdateBillingProjectOutcome[F[_]](billingProbeCanAccessBillingAccount: Boolean,
                                                     outcome: Outcome
  )(implicit R: Ask[F, BillingAccountChange], M: Monad[F], L: LiftIO[F]): F[Unit] =
    for {
      change <- R.ask
      (_, message) = Outcome.toTuple(outcome)
      _ <-
        if (outcome.isSuccess)
          info("Successfully updated Billing Account on Billing Project in Google")
        else
          warn("Failed to update Billing Account on Billing Project in Google", "details" -> message)

      _ <- inTransaction {
        val thisBillingProject = rawlsBillingProjectQuery.withProjectName(change.billingProjectName)
        DBIO.seq(
          thisBillingProject.setInvalidBillingAccount(
            change.newBillingAccount.isDefined && !billingProbeCanAccessBillingAccount
          ),
          thisBillingProject.setMessage(message)
        )
      }
    } yield ()

  def updateWorkspacesBillingAccountInGoogle[F[_]](
    billingProject: RawlsBillingProject,
    tracingContext: RawlsTracingContext
  )(implicit R: Ask[F, BillingAccountChange], M: MonadThrow[F], L: LiftIO[F]): F[Outcome] =
    for {
      // v2 workspaces have their own google project so we'll need to attempt to set the billing account on each.
      workspaces <- inTransaction {
        workspaceQuery
          .withBillingProject(billingProject.projectName)
          .withVersion(WorkspaceVersions.V2)
          .read
      }

      outcome <- workspaces.foldMapA { workspace =>
        for {
          attempt <- setGoogleProjectBillingAccount(workspace.googleProjectId, tracingContext).attempt
          updateWorkspaceOutcome = Outcome.fromEither(attempt)
          failureMessage <- updateWorkspaceOutcome match {
            case Success =>
              M.pure(None) <* info("Successfully updated Billing Account on Workspace Google Project",
                                   "workspace" -> workspace.toWorkspaceName
              )
            case Failure(message) =>
              M.pure(message.some) <* warn("Failed to update Billing Account on Workspace Google Project",
                                           "details" -> message,
                                           "workspace" -> workspace.toWorkspaceName
              )
          }

          _ <- writeWorkspaceBillingAccountAndErrorMessage(
            workspaceQuery.withWorkspaceId(workspace.workspaceIdAsUUID),
            failureMessage
          )
        } yield updateWorkspaceOutcome
      }
    } yield outcome

  private def setGoogleProjectBillingAccount[F[_]](
                                                    googleProjectId: GoogleProjectId, tracingContext: RawlsTracingContext
  )(implicit R: Ask[F, BillingAccountChange], M: Monad[F], L: LiftIO[F]): F[Unit] =
    for {
      projectBillingInfo <- L.liftIO {
        gcsDAO.getBillingInfoForGoogleProject(googleProjectId).io
      }

      // convert `null` or `empty` Strings to `None`
      currentBillingAccountOnGoogleProject =
        Option(projectBillingInfo.getBillingAccountName)
          .filter(!_.isBlank)
          .map(RawlsBillingAccountName)

      newBillingAccount <- R.reader(_.newBillingAccount)
      _ <- M.whenA(newBillingAccount != currentBillingAccountOnGoogleProject) {
        L.liftIO {
          gcsDAO.setBillingAccount(googleProjectId, newBillingAccount, tracingContext).io
        }
      }
    } yield ()

  private def writeWorkspaceBillingAccountAndErrorMessage[F[_]](workspacesToUpdate: WorkspaceQueryType,
                                                                errorMessage: Option[String]
  )(implicit R: Ask[F, BillingAccountChange], L: LiftIO[F], M: Monad[F]): F[Unit] =
    for {
      billingAccount <- R.reader(_.newBillingAccount)
      _ <- inTransaction {
        workspacesToUpdate.setErrorMessage(errorMessage) *>
          Applicative[WriteAction].whenA(errorMessage.isEmpty) {
            workspacesToUpdate.setCurrentBillingAccountOnGoogleProject(billingAccount)
          }
      }
    } yield ()

  private def failChange[F[_]](
    throwable: Throwable
  )(implicit R: Ask[F, BillingAccountChange], M: Monad[F], L: LiftIO[F]): F[Unit] =
    writeBillingAccountChangeOutcome(Failure(throwable.getMessage))

  private def writeBillingAccountChangeOutcome[F[_]](
    outcome: Outcome
  )(implicit R: Ask[F, BillingAccountChange], M: Monad[F], L: LiftIO[F]): F[Unit] =
    for {
      _ <- outcome match {
        case Success          => info("Successfully synchronized Billing Account change")
        case Failure(message) => warn("Failed to synchronize Billing Account change", "details" -> message)
      }

      changeId <- R.reader(_.id)
      record = BillingAccountChanges.withId(changeId)
      _ <- inTransaction {
        record.setGoogleSyncTime(Instant.now().some) *> record.setOutcome(outcome.some)
      }
    } yield ()

  private def inTransaction[A, F[_]](action: ReadWriteAction[A])(implicit F: LiftIO[F]): F[A] =
    F.liftIO {
      dataSource.inTransaction(_ => action).io
    }

  private def info[F[_]](message: String, data: (String, Any)*)(implicit
    R: Ask[F, BillingAccountChange],
    F: Functor[F]
  ): F[Unit] =
    logContext.map { context =>
      logger.info((("message" -> message) +: (data ++ context)).toJson.compactPrint)
    }

  private def warn[F[_]](message: String, data: (String, Any)*)(implicit
    R: Ask[F, BillingAccountChange],
    F: Functor[F]
  ): F[Unit] =
    logContext.map { context =>
      logger.warn((("message" -> message) +: (data ++ context)).toJson.compactPrint)
    }

  private def logContext[F[_]](implicit F: Ask[F, BillingAccountChange]): F[Map[String, Any]] =
    F.reader { change =>
      Map(
        "changeId" -> change.id,
        "billingProject" -> change.billingProjectName.value,
        "previousBillingAccount" -> change.previousBillingAccount.map(_.value),
        "newBillingAccount" -> change.newBillingAccount.map(_.value)
      )
    }
}

final case class BillingAccountSynchronizerConfig(pollInterval: FiniteDuration, initialDelay: FiniteDuration)
