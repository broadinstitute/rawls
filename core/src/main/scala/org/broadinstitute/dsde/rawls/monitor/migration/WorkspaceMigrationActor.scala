package org.broadinstitute.dsde.rawls.monitor.migration

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import cats.Applicative
import cats.Invariant.catsApplicativeForArrow
import cats.data.{NonEmptyList, OptionT, ReaderT}
import cats.effect.IO
import cats.effect.unsafe.IORuntime
import cats.effect.unsafe.implicits.global
import cats.implicits._
import com.google.cloud.Identity
import com.google.cloud.Identity.serviceAccount
import com.google.cloud.storage.Storage
import com.google.cloud.storage.Storage.{BucketGetOption, BucketSourceOption, BucketTargetOption}
import com.google.storagetransfer.v1.proto.TransferTypes.{TransferJob, TransferOperation}
import net.ceedubs.ficus.Ficus.{finiteDurationReader, toFicusConfig}
import net.ceedubs.ficus.readers.ValueReader
import org.broadinstitute.dsde.rawls.dataaccess.slick._
import org.broadinstitute.dsde.rawls.dataaccess.{GoogleServicesDAO, SamDAO, SlickDataSource}
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.monitor.migration.MigrationUtils.Implicits._
import org.broadinstitute.dsde.rawls.monitor.migration.MigrationUtils.Outcome.{toTuple, Failure, Success}
import org.broadinstitute.dsde.rawls.monitor.migration.MigrationUtils._
import org.broadinstitute.dsde.rawls.monitor.migration.WorkspaceMigrationActor.MigrateAction._
import org.broadinstitute.dsde.rawls.workspace.WorkspaceService
import org.broadinstitute.dsde.workbench.google.GoogleIamDAO
import org.broadinstitute.dsde.workbench.google2.GoogleStorageTransferService.ObjectDeletionOption.DeleteSourceObjectsAfterTransfer
import org.broadinstitute.dsde.workbench.google2.GoogleStorageTransferService.ObjectOverwriteOption.OverwriteObjectsAlreadyExistingInSink
import org.broadinstitute.dsde.workbench.google2.GoogleStorageTransferService.{
  JobTransferOptions,
  JobTransferSchedule,
  OperationName
}
import org.broadinstitute.dsde.workbench.google2.{GoogleStorageService, GoogleStorageTransferService, StorageRole}
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.{GcsBucketName, GoogleProject}
import org.typelevel.log4cats.slf4j.Slf4jLogger.getLogger
import spray.json.enrichAny

import java.sql.Timestamp
import java.time.{Instant, LocalDateTime, ZoneOffset}
import java.util.UUID
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._

/**
 * The `WorkspaceMigrationActor` is an Akka Typed Actor [1] that migrates a v1 Workspace to a
 * "rawls" v2 Workspace by moving the Workspace's Google Cloud resources into a dedicated
 * Google Project.
 *
 * The actor migrates workspaces by executing a series of operations (or pipeline) successively.
 * These operations are self-contained units defined by some initial and final state in which the
 * actor
 *  - reads a migration attempt from the database in some initial state
 *  - performs some action to advance its state
 *  - writes the new state back to the database.
 *
 * Thus, each operation can be executed independently; successive pipeline execution will eventually
 * migrate a Workspace successfully or report a failure. In any particular invocation, the actor
 * might do any and all of
 *  - update a Workspace record with a new Google Project and unlock the Workspace
 *  - start Storage Transfer jobs to "move" a Workspace's storage bucket to another Google Project
 *  - configure another Google Project for a Workspace
 *  - mark a migration attempt as a failure with an appropriate cause
 *  - start migrating a workspace, if one has been scheduled for migration
 *
 * Between migrating Workspaces, the actor maintains a table of Storage Transfer jobs that transfer
 * a Workspace's bucket to another bucket. When a job completes, the actor updates the associated
 * migration attempt with the outcome of the job. You should be aware of the Storage Transfer
 * Service quotas and limits [2] when configuring the actor.
 *
 * [1]: https://doc.akka.io/docs/akka/2.5.32/typed-actors.html
 * [2]: https://cloud.google.com/storage-transfer/quotas
 */
object WorkspaceMigrationActor {

  final case class Config(
    /** The interval between pipeline invocations. */
    pollingInterval: FiniteDuration,

    /** The interval between updating the status of ongoing storage transfer jobs. */
    transferJobRefreshInterval: FiniteDuration,

    /** The Google Project to bill all cloud operations to. */
    googleProjectToBill: GoogleProject,

    /** The parent folder to move re-purposed v1 Billing Project Google Projects into. */
    googleProjectParentFolder: GoogleFolderId,

    /** The maximum number of migration attempts that can be active at any one time. */
    maxConcurrentMigrationAttempts: Int,

    /** The interval to wait before restarting rate-limited migrations. */
    knownFailureRetryInterval: FiniteDuration,

    /** The maximum number of times a failed migration may be retried. */
    maxRetries: Int
  )

  implicit val configReader: ValueReader[Config] = ValueReader.relative { config =>
    Config(
      pollingInterval = config.as[FiniteDuration]("polling-interval"),
      transferJobRefreshInterval = config.as[FiniteDuration]("transfer-job-refresh-interval"),
      googleProjectToBill = GoogleProject(config.getString("google-project-id-to-bill")),
      googleProjectParentFolder = GoogleFolderId(config.getString("google-project-parent-folder-id")),
      maxConcurrentMigrationAttempts = config.getInt("max-concurrent-migrations"),
      knownFailureRetryInterval = config.as[FiniteDuration]("retry-interval"),
      maxRetries = config.getInt("max-retries")
    )
  }

  final case class MigrationDeps(dataSource: SlickDataSource,
                                 googleProjectToBill: GoogleProject,
                                 parentFolder: GoogleFolderId,
                                 maxConcurrentAttempts: Int,
                                 maxRetries: Int,
                                 workspaceService: WorkspaceService,
                                 storageService: GoogleStorageService[IO],
                                 storageTransferService: GoogleStorageTransferService[IO],
                                 userInfo: UserInfo,
                                 gcsDao: GoogleServicesDAO,
                                 googleIamDAO: GoogleIamDAO,
                                 samDao: SamDAO
  )

  type MigrateAction[A] = ReaderT[OptionT[IO, *], MigrationDeps, A]

  object MigrateAction {

    final def apply[A](f: MigrationDeps => OptionT[IO, A]): MigrateAction[A] =
      ReaderT(f)

    // lookup a value in the environment using `selector`
    final def asks[A](selector: MigrationDeps => A): MigrateAction[A] =
      ReaderT.ask[OptionT[IO, *], MigrationDeps].map(selector)

    final def fromFuture[A](future: => Future[A]): MigrateAction[A] =
      liftIO(future.io)

    // create a MigrateAction that ignores its input and returns the OptionT
    final def liftF[A](optionT: OptionT[IO, A]): MigrateAction[A] =
      ReaderT.liftF(optionT)

    // lift an IO action into the context of a MigrateAction
    final def liftIO[A](ioa: IO[A]): MigrateAction[A] =
      ReaderT.liftF(OptionT.liftF(ioa))

    // modify the environment that action is evaluated in
    final def local[A](f: MigrationDeps => MigrationDeps)(action: MigrateAction[A]): MigrateAction[A] =
      ReaderT.local(f)(action)

    final def raiseError(t: Throwable): MigrateAction[Unit] =
      liftIO(IO.raiseError(t))

    // Raises the error when the condition is true, otherwise returns unit
    final def raiseWhen(condition: Boolean)(t: => Throwable): MigrateAction[Unit] =
      liftIO(IO.raiseWhen(condition)(t))

    // empty action
    final def unit: MigrateAction[Unit] =
      pure()

    final def pure[A](a: A): MigrateAction[A] =
      ReaderT.pure(a)

    final def ifM[A](
      predicate: MigrateAction[Boolean]
    )(ifTrue: => MigrateAction[A], ifFalse: => MigrateAction[A]): MigrateAction[A] =
      predicate.ifM(ifTrue, ifFalse)

    // Stop executing this Migrate Action
    final def pass[A]: MigrateAction[A] =
      liftF(OptionT.none)
  }

  implicit class MigrateActionOps[A](fa: => MigrateAction[A]) {
    final def handleErrorWith(f: Throwable => MigrateAction[A]): MigrateAction[A] =
      MigrateAction { env =>
        OptionT(fa.run(env).value.handleErrorWith(f(_).run(env).value))
      }
    final def |(fb: => MigrateAction[A]): MigrateAction[A] =
      MigrateAction(env => fa(env).orElse(fb(env)))
  }

  // Read workspace migrations in various states, attempt to advance their state forward by one
  // step and write the outcome of each step to the database.
  final def migrate: MigrateAction[Unit] =
    List(
      restartMigration | startMigration,
      removeWorkspaceBucketIam,
      configureGoogleProject,
      createTempBucket,
      configureTransferIamToTmpBucket,
      issueTransferJobToTmpBucket,
      deleteWorkspaceBucket,
      createFinalWorkspaceBucket,
      configureTransferIamToFinalBucket,
      issueTransferJobToFinalWorkspaceBucket,
      deleteTemporaryBucket,
      restoreIamPoliciesAndUpdateWorkspaceRecord
    )
      .parTraverse_(runStep)

  // Sequence the action and return an empty MigrateAction if the action succeeded
  def runStep(action: MigrateAction[Unit]): MigrateAction[Unit] =
    action.mapF(optionT => OptionT(optionT.value.as(().some)))

  implicit val ec: ExecutionContext = implicitly[IORuntime].compute

  import slick.jdbc.MySQLProfile.api._

  val storageTransferJobs = PpwStorageTransferJobs.storageTransferJobs

  final def restartMigration: MigrateAction[Unit] =
    restartFailuresLike(FailureModes.noBucketPermissionsFailure, FailureModes.gcsUnavailableFailure) |
      reissueFailedStsJobs

  final def startMigration: MigrateAction[Unit] =
    for {
      (maxAttempts, maxReties) <- asks(d => (d.maxConcurrentAttempts, d.maxRetries))
      now <- nowTimestamp
      (id, workspaceName) <- inTransactionT { dataAccess =>
        import dataAccess.workspaceMigrationQuery._
        import dataAccess.{WorkspaceExtensions, workspaceQuery}
        for {
          // Use `OptionT` to guard starting more migrations when we're at capacity and
          // to encode non-determinism in picking a workspace to migrate
          activeFullMigrations <- OptionT.liftF(getNumActiveResourceLimitedMigrations)
          isBlocked <- OptionT.liftF(dataAccess.migrationRetryQuery.isPipelineBlocked(maxReties))

          // Only-child migrations are not subject to quotas as we don't need to create any
          // new resources for them
          (id, workspaceId, workspaceName) <-
            nextMigration(onlyChild = isBlocked || activeFullMigrations >= maxAttempts)

          _ <- OptionT.liftF[ReadWriteAction, Unit] {
            orM[ReadWriteAction](workspaceQuery.withWorkspaceId(workspaceId).lock,
                                 wasLockedByPreviousMigration(workspaceId)
            ).flatMap {
              update2(id, startedCol, Some(now), unlockOnCompletionCol, _).ignore
            }
          }
        } yield (id, workspaceName)
      }
      _ <- getLogger[MigrateAction].info(
        stringify(
          "migrationId" -> id,
          "workspace" -> workspaceName,
          "started" -> now
        )
      )
    } yield ()

  final def removeWorkspaceBucketIam: MigrateAction[Unit] =
    withMigration(_.workspaceMigrationQuery.removeWorkspaceBucketIamCondition) { (migration, workspace) =>
      for {
        (storageService, userInfo, googleProjectToBill) <- asks { d =>
          (d.storageService, d.userInfo, d.googleProjectToBill)
        }

        requesterPaysEnabled <- liftIO {
          for {
            bucketOpt <- storageService.getBucket(
              GoogleProject(workspace.googleProjectId.value),
              GcsBucketName(workspace.bucketName),
              bucketGetOptions = List(Storage.BucketGetOption.fields(Storage.BucketField.BILLING))
            )

            bucketInfo <- IO.fromOption(bucketOpt)(noWorkspaceBucketError(migration, workspace))
            requesterPaysEnabled = Option(bucketInfo.requesterPays()).exists(_.booleanValue())

            actorSaIdentity = serviceAccount(userInfo.userEmail.value)
            _ <- storageService
              .overrideIamPolicy(
                GcsBucketName(workspace.bucketName),
                Map(StorageRole.StorageAdmin -> NonEmptyList.one(actorSaIdentity)),
                bucketSourceOptions =
                  if (requesterPaysEnabled) List(BucketSourceOption.userProject(googleProjectToBill.value))
                  else List.empty
              )
              .compile
              .drain
          } yield requesterPaysEnabled
        }

        now <- nowTimestamp
        _ <- inTransaction { dataAccess =>
          dataAccess.workspaceMigrationQuery.update2(
            migration.id,
            dataAccess.workspaceMigrationQuery.requesterPaysEnabledCol,
            requesterPaysEnabled,
            dataAccess.workspaceMigrationQuery.workspaceBucketIamRemovedCol,
            now.some
          )
        }
        _ <- getLogger[MigrateAction].info(
          stringify(
            "migrationId" -> migration.id,
            "workspace" -> workspace.toWorkspaceName,
            "workspaceBucketIamRemoved" -> now
          )
        )
      } yield ()
    }

  final def configureGoogleProject: MigrateAction[Unit] =
    withMigration(_.workspaceMigrationQuery.configureGoogleProjectCondition) { (migration, workspace) =>
      val isSoleWorkspaceInBillingProjectGoogleProject: MigrateAction[Boolean] =
        pure(Seq(workspace.workspaceIdAsUUID) == _) ap inTransaction { dataAccess =>
          import dataAccess.{WorkspaceExtensions, workspaceQuery}
          workspaceQuery
            .withBillingProject(RawlsBillingProjectName(workspace.namespace))
            .map(_.id)
            .result
        }

      val makeError = (message: String, data: Map[String, Any]) =>
        WorkspaceMigrationException(
          message = s"The workspace migration failed while configuring a new Google Project: $message.",
          data = Map(
            "migrationId" -> migration.id,
            "workspace" -> workspace.toWorkspaceName
          ) ++ data
        )

      for {
        _ <- workspace.errorMessage.traverse_ { message =>
          raiseError {
            makeError("an error exists on workspace", Map("errorMessage" -> message))
          }
        }

        workspaceBillingAccount = workspace.currentBillingAccountOnGoogleProject.getOrElse(
          throw makeError("no billing account on workspace", Map.empty)
        )

        // Safe to assume that this exists if the workspace exists
        billingProject <- inTransactionT { dataAccess =>
          OptionT {
            dataAccess.rawlsBillingProjectQuery.load(RawlsBillingProjectName(workspace.namespace))
          }
        }

        billingProjectBillingAccount = billingProject.billingAccount.getOrElse(
          throw makeError("no billing account on billing project", Map("billingProject" -> billingProject.projectName))
        )

        _ <- raiseWhen(billingProject.invalidBillingAccount) {
          makeError(
            "invalid billing account on billing project",
            Map(
              "billingProject" -> billingProject.projectName,
              "billingProjectBillingAccount" -> billingProjectBillingAccount
            )
          )
        }

        _ <- raiseWhen(workspaceBillingAccount != billingProjectBillingAccount) {
          makeError(
            "billing account on workspace differs from billing account on billing project",
            Map(
              "workspaceBillingAccount" -> workspaceBillingAccount,
              "billingProject" -> billingProject.projectName,
              "billingProjectBillingAccount" -> billingProjectBillingAccount
            )
          )
        }

        (userInfo, workspaceService) <- asks(d => (d.userInfo, d.workspaceService))
        (googleProjectId, googleProjectNumber) <-
          ifM(isSoleWorkspaceInBillingProjectGoogleProject)(
            // when there's only one v1 workspace in a v1 billing project, we can re-use the
            // google project associated with the billing project and forgo the need to transfer
            // the workspace bucket to a new google project. Thus, the billing project will become
            // a v2 billing project as its association with a google project will be removed.
            for {
              samDao <- asks(_.samDao)
              // delete the google project resource in Sam while minimising length of time as admin
              _ <- samDao.asResourceAdmin(
                SamResourceTypeNames.billingProject,
                billingProject.googleProjectId.value,
                SamBillingProjectPolicyNames.owner,
                RawlsRequestContext(userInfo)
              ) {
                fromFuture {
                  samDao.deleteResource(
                    SamResourceTypeNames.googleProject,
                    billingProject.googleProjectId.value,
                    RawlsRequestContext(userInfo)
                  )
                }
              }

              (gcsDao, parentFolder) <- asks(d => (d.gcsDao, d.parentFolder))
              _ <- Applicative[MigrateAction].unlessA(billingProject.servicePerimeter.isDefined) {
                fromFuture {
                  gcsDao.addProjectToFolder(billingProject.googleProjectId, parentFolder.value)
                }
              }

              billingProjectPolicies <- fromFuture {
                samDao.admin.listPolicies(
                  SamResourceTypeNames.billingProject,
                  billingProject.projectName.value,
                  RawlsRequestContext(userInfo)
                )
              }

              policiesAddedByDeploymentManager = Set(
                SamBillingProjectPolicyNames.owner,
                SamBillingProjectPolicyNames.canComputeUser
              )

              _ <- removeIdentitiesFromGoogleProjectIam(
                GoogleProject(billingProject.googleProjectId.value),
                billingProjectPolicies
                  .filter(p => policiesAddedByDeploymentManager.contains(p.policyName))
                  .map(p => Identity.group(p.email.value))
              )

              now <- nowTimestamp.map(_.some)

              // short-circuit the bucket creation and transfer
              _ <- inTransaction { dataAccess =>
                import dataAccess.workspaceMigrationQuery._
                update10(
                  migration.id,
                  tmpBucketCreatedCol,
                  now,
                  workspaceBucketTransferIamConfiguredCol,
                  now,
                  workspaceBucketTransferJobIssuedCol,
                  now,
                  workspaceBucketTransferredCol,
                  now,
                  workspaceBucketDeletedCol,
                  now,
                  finalBucketCreatedCol,
                  now,
                  tmpBucketTransferIamConfiguredCol,
                  now,
                  tmpBucketTransferJobIssuedCol,
                  now,
                  tmpBucketTransferredCol,
                  now,
                  tmpBucketDeletedCol,
                  now
                )
              }

              googleProjectId = billingProject.googleProjectId
              googleProjectNumber <- billingProject.googleProjectNumber
                .map(pure)
                .getOrElse(
                  fromFuture {
                    gcsDao.getGoogleProject(googleProjectId).map(gcsDao.getGoogleProjectNumber)
                  }
                )
            } yield (googleProjectId, googleProjectNumber),
            fromFuture {
              for {
                createResult @ (googleProjectId, _) <- workspaceService.createGoogleProject(
                  billingProject,
                  rbsHandoutRequestId = workspace.workspaceId
                )
                _ <- workspaceService.setProjectBillingAccount(
                  googleProjectId,
                  billingProject,
                  billingProjectBillingAccount,
                  workspace.workspaceId
                )
              } yield createResult
            }
          )

        _ <- fromFuture {
          workspaceService.renameAndLabelProject(googleProjectId, workspace.workspaceId, workspace.toWorkspaceName)
        }

        now <- nowTimestamp
        _ <- inTransaction { dataAccess =>
          import dataAccess.setOptionValueObject
          import dataAccess.workspaceMigrationQuery._
          update3(migration.id,
                  newGoogleProjectIdCol,
                  googleProjectId.some,
                  newGoogleProjectNumberCol,
                  googleProjectNumber.some,
                  newGoogleProjectConfiguredCol,
                  Some(now)
          )
        }

        _ <- getLogger[MigrateAction].info(
          stringify(
            "migrationId" -> migration.id,
            "workspace" -> workspace.toWorkspaceName,
            "googleProjectConfigured" -> now
          )
        )
      } yield ()
    }

  final def createTempBucket: MigrateAction[Unit] =
    withMigration(_.workspaceMigrationQuery.createTempBucketConditionCondition) { (migration, workspace) =>
      for {
        tmpBucketName <-
          liftIO(randomSuffix("terra-workspace-migration-").map(GcsBucketName))

        googleProjectId = migration.newGoogleProjectId.getOrElse(
          throw noGoogleProjectError(migration, workspace)
        )
        _ <- createBucketInSameRegion(
          migration,
          workspace,
          sourceGoogleProject = GoogleProject(workspace.googleProjectId.value),
          sourceBucketName = GcsBucketName(workspace.bucketName),
          destGoogleProject = GoogleProject(googleProjectId.value),
          destBucketName = tmpBucketName
        )

        now <- nowTimestamp
        _ <- inTransaction { dataAccess =>
          import dataAccess.setOptionValueObject
          dataAccess.workspaceMigrationQuery.update2(migration.id,
                                                     dataAccess.workspaceMigrationQuery.tmpBucketCol,
                                                     Some(tmpBucketName),
                                                     dataAccess.workspaceMigrationQuery.tmpBucketCreatedCol,
                                                     Some(now)
          )
        }

        _ <- getLogger[MigrateAction].info(
          stringify(
            "migrationId" -> migration.id,
            "workspace" -> workspace.toWorkspaceName,
            "tmpBucketCreated" -> now
          )
        )
      } yield ()
    }

  final def configureTransferIamToTmpBucket: MigrateAction[Unit] =
    configureBucketTransferIam(
      _.workspaceMigrationQuery.configureWorkspaceBucketTransferIam,
      getSrcBucket = (workspaceBucket, _) => workspaceBucket,
      getDstBucket = (_, tmpBucket) => tmpBucket,
      _.workspaceMigrationQuery.workspaceBucketTransferIamConfiguredCol
    )

  final def issueTransferJobToTmpBucket: MigrateAction[Unit] =
    issueBucketTransferJob(
      _.workspaceMigrationQuery.issueTransferJobToTmpBucketCondition,
      getSrcBucket = (workspaceBucket, _) => workspaceBucket,
      getDstBucket = (_, tmpBucket) => tmpBucket,
      _.workspaceMigrationQuery.workspaceBucketTransferJobIssuedCol
    )

  final def deleteWorkspaceBucket: MigrateAction[Unit] =
    withMigration(_.workspaceMigrationQuery.deleteWorkspaceBucketCondition) { (migration, workspace) =>
      for {
        (storageService, googleProjectToBill) <- asks(s => (s.storageService, s.googleProjectToBill))

        _ <- liftIO {
          storageService
            .deleteBucket(
              GoogleProject(workspace.googleProjectId.value),
              GcsBucketName(workspace.bucketName),
              bucketSourceOptions =
                if (migration.requesterPaysEnabled) List(BucketSourceOption.userProject(googleProjectToBill.value))
                else List.empty
            )
            .compile
            .last
        }

        now <- nowTimestamp
        _ <- inTransaction { dataAccess =>
          dataAccess.workspaceMigrationQuery.update(
            migration.id,
            dataAccess.workspaceMigrationQuery.workspaceBucketDeletedCol,
            Some(now)
          )
        }

        _ <- getLogger[MigrateAction].info(
          stringify(
            "migrationId" -> migration.id,
            "workspace" -> workspace.toWorkspaceName,
            "workspaceBucketDeleted" -> now
          )
        )
      } yield ()
    }

  final def createFinalWorkspaceBucket: MigrateAction[Unit] =
    withMigration(_.workspaceMigrationQuery.createFinalWorkspaceBucketCondition) { (migration, workspace) =>
      for {
        (googleProjectId, tmpBucketName) <- getGoogleProjectAndTmpBucket(migration, workspace)
        destGoogleProject = GoogleProject(googleProjectId.value)
        _ <- createBucketInSameRegion(
          migration,
          workspace,
          sourceGoogleProject = destGoogleProject,
          sourceBucketName = tmpBucketName,
          destGoogleProject = destGoogleProject,
          destBucketName = GcsBucketName(workspace.bucketName)
        )

        now <- nowTimestamp
        _ <- inTransaction { dataAccess =>
          dataAccess.workspaceMigrationQuery.update(migration.id,
                                                    dataAccess.workspaceMigrationQuery.finalBucketCreatedCol,
                                                    Some(now)
          )
        }

        _ <- getLogger[MigrateAction].info(
          stringify(
            "migrationId" -> migration.id,
            "workspace" -> workspace.toWorkspaceName,
            "finalWorkspaceBucketCreated" -> now
          )
        )
      } yield ()
    }

  final def configureTransferIamToFinalBucket: MigrateAction[Unit] =
    configureBucketTransferIam(
      _.workspaceMigrationQuery.configureTmpBucketTransferIam,
      getSrcBucket = (_, tmpBucket) => tmpBucket,
      getDstBucket = (workspaceBucket, _) => workspaceBucket,
      _.workspaceMigrationQuery.tmpBucketTransferIamConfiguredCol
    )

  final def issueTransferJobToFinalWorkspaceBucket: MigrateAction[Unit] =
    issueBucketTransferJob(
      _.workspaceMigrationQuery.issueTransferJobToFinalWorkspaceBucketCondition,
      getSrcBucket = (_, tmpBucket) => tmpBucket,
      getDstBucket = (workspaceBucket, _) => workspaceBucket,
      _.workspaceMigrationQuery.tmpBucketTransferJobIssuedCol
    )

  final def deleteTemporaryBucket: MigrateAction[Unit] =
    withMigration(_.workspaceMigrationQuery.deleteTemporaryBucketCondition) { (migration, workspace) =>
      for {
        (googleProjectId, tmpBucketName) <- getGoogleProjectAndTmpBucket(migration, workspace)
        storageService <- asks(_.storageService)
        successOpt <- liftIO {
          // tmp bucket is never requester pays
          storageService
            .deleteBucket(GoogleProject(googleProjectId.value), tmpBucketName)
            .compile
            .last
        }

        _ <- raiseWhen(!successOpt.contains(true)) {
          noTmpBucketError(migration, workspace)
        }

        now <- nowTimestamp
        _ <- inTransaction { dataAccess =>
          dataAccess.workspaceMigrationQuery.update(migration.id,
                                                    dataAccess.workspaceMigrationQuery.tmpBucketDeletedCol,
                                                    Some(now)
          )
        }
        _ <- getLogger[MigrateAction].info(
          stringify(
            "migrationId" -> migration.id,
            "workspace" -> workspace.toWorkspaceName,
            "tmpBucketDeleted" -> now
          )
        )
      } yield ()
    }

  final def restoreIamPoliciesAndUpdateWorkspaceRecord: MigrateAction[Unit] =
    withMigration(_.workspaceMigrationQuery.restoreIamPoliciesAndUpdateWorkspaceRecordCondition) {
      (migration, workspace) =>
        for {
          deps <- asks(identity)

          googleProjectId = migration.newGoogleProjectId.getOrElse(
            throw noGoogleProjectError(migration, workspace)
          )

          (billingProjectOwnerPolicyGroup, workspacePolicies) <- fromFuture {
            import SamBillingProjectPolicyNames.owner
            for {
              billingProjectPolicies <- deps.samDao.admin.listPolicies(
                SamResourceTypeNames.billingProject,
                workspace.namespace,
                RawlsRequestContext(deps.userInfo)
              )

              billingProjectOwnerPolicyGroup = billingProjectPolicies
                .find(_.policyName == owner)
                .getOrElse(
                  throw WorkspaceMigrationException(
                    message = s"""Workspace migration failed: no "$owner" policy on billing project.""",
                    data = Map("migrationId" -> migration.id, "billingProject" -> workspace.namespace)
                  )
                )
                .email

              // The `can-compute` policy group is sync'ed for v2 workspaces. This
              // was done at the billing project level only for v1 workspaces.
              _ <- deps.samDao.syncPolicyToGoogle(
                SamResourceTypeNames.workspace,
                workspace.workspaceId,
                SamWorkspacePolicyNames.canCompute
              )

              workspacePolicies <- deps.samDao.admin.listPolicies(
                SamResourceTypeNames.workspace,
                workspace.workspaceId,
                RawlsRequestContext(deps.userInfo)
              )

              workspacePoliciesByName = workspacePolicies.map(p => p.policyName -> p.email).toMap
              _ <- deps.workspaceService.setupGoogleProjectIam(
                googleProjectId,
                workspacePoliciesByName,
                billingProjectOwnerPolicyGroup
              )
            } yield (billingProjectOwnerPolicyGroup, workspacePoliciesByName)
          }

          // Now we'll update the workspace record with the new google project id.
          // Why? Because the WorkspaceService does it in this order when creating the workspace.
          _ <- inTransaction {
            _.workspaceQuery
              .filter(_.id === workspace.workspaceIdAsUUID)
              .map(w => (w.googleProjectId, w.googleProjectNumber))
              .update((googleProjectId.value, migration.newGoogleProjectNumber.map(_.toString)))
          }

          authDomains <- liftIO {
            deps.samDao.asResourceAdmin(
              SamResourceTypeNames.workspace,
              workspace.workspaceId,
              SamWorkspacePolicyNames.owner,
              RawlsRequestContext(deps.userInfo)
            ) {
              deps.samDao
                .createResourceFull(
                  SamResourceTypeNames.googleProject,
                  googleProjectId.value,
                  Map.empty,
                  Set.empty,
                  RawlsRequestContext(deps.userInfo),
                  Some(SamFullyQualifiedResourceId(workspace.workspaceId, SamResourceTypeNames.workspace.value))
                )
                .io *>
                deps.samDao
                  .getResourceAuthDomain(
                    SamResourceTypeNames.workspace,
                    workspace.workspaceId,
                    RawlsRequestContext(deps.userInfo)
                  )
                  .io
            }
          }

          // when there isn't an auth domain, the billing project owners group is used in attempt
          // to reduce an individual's google group membership below the limit of 2000.
          _ <- liftIO {
            val bucketPolices =
              (if (authDomains.isEmpty)
                 workspacePolicies.updated(SamWorkspacePolicyNames.projectOwner, billingProjectOwnerPolicyGroup)
               else
                 workspacePolicies)
                .map { case (policyName, group) =>
                  WorkspaceAccessLevels.withPolicyName(policyName.value).map(_ -> group)
                }
                .flatten
                .toMap

            val bucket = GcsBucketName(workspace.bucketName)

            deps.gcsDao
              .updateBucketIam(
                bucket,
                bucketPolices,
                Option.when(migration.requesterPaysEnabled)(GoogleProjectId(deps.googleProjectToBill.value))
              )
              .io *>
              Applicative[IO].whenA(migration.requesterPaysEnabled) {
                deps.storageService
                  .setRequesterPays(
                    bucket,
                    migration.requesterPaysEnabled,
                    bucketTargetOptions = List(BucketTargetOption.userProject(deps.googleProjectToBill.value))
                  )
                  .compile
                  .drain
              }
          }

          _ <- inTransaction {
            _.workspaceQuery
              .filter(_.id === workspace.workspaceIdAsUUID)
              .map(w => (w.isLocked, w.workspaceVersion))
              .update((!migration.unlockOnCompletion, WorkspaceVersions.V2.value))
          }

          _ <- endMigration(migration.id, workspace.toWorkspaceName, Success)
        } yield ()
    }

  def restartFailuresLike(failureMessage: String, others: String*): MigrateAction[Unit] =
    retryFailuresLike(
      (dataAccess, migrationId) => {
        import dataAccess.workspaceMigrationQuery._
        update3(
          migrationId,
          finishedCol,
          Option.empty[Timestamp],
          outcomeCol,
          Option.empty[String],
          messageCol,
          Option.empty[String]
        )
      },
      failureMessage,
      others: _*
    )

  def reissueFailedStsJobs: MigrateAction[Unit] =
    retryFailuresLike(
      (dataAccess, migrationId) => {
        import dataAccess.workspaceMigrationQuery._
        (for {
          migration <- getAttempt(migrationId)
          _ <- OptionT.liftF {
            update5(
              migrationId,
              finishedCol,
              Option.empty[Timestamp],
              outcomeCol,
              Option.empty[String],
              messageCol,
              Option.empty[String],
              // reconfigure sts iam permissions for good measure
              if (migration.tmpBucketTransferIamConfigured.isDefined) tmpBucketTransferIamConfiguredCol
              else workspaceBucketTransferIamConfiguredCol,
              Option.empty[Timestamp],
              // re-issue the job
              if (migration.tmpBucketTransferIamConfigured.isDefined) tmpBucketTransferJobIssuedCol
              else workspaceBucketTransferJobIssuedCol,
              Option.empty[Timestamp]
            )
          }
        } yield ()).value
      },
      FailureModes.noObjectPermissionsFailure
    )

  def retryFailuresLike(update: (DataAccess, Long) => ReadWriteAction[Any],
                        failureMessage: String,
                        others: String*
  ): MigrateAction[Unit] =
    for {
      (maxAttempts, maxRetries) <- asks(d => (d.maxConcurrentAttempts, d.maxRetries))
      now <- nowTimestamp
      (migrationId, workspaceName, retryCount) <- inTransactionT { dataAccess =>
        import dataAccess.{migrationRetryQuery, workspaceMigrationQuery}
        for {
          (migrationId, workspaceName) <- migrationRetryQuery.nextFailureLike(
            maxRetries,
            failureMessage,
            others: _*
          )
          numAttempts <- OptionT.liftF(workspaceMigrationQuery.getNumActiveResourceLimitedMigrations)
          if numAttempts < maxAttempts
          retryCount <- OptionT.liftF[ReadWriteAction, Long] {
            for {
              MigrationRetry(id, _, numRetries) <- migrationRetryQuery.getOrCreate(migrationId)
              retryCount = numRetries + 1
              _ <- migrationRetryQuery.update(id, migrationRetryQuery.retriesCol, retryCount)
              _ <- update(dataAccess, migrationId)
            } yield retryCount
          }
        } yield (migrationId, workspaceName, retryCount)
      }
      _ <- getLogger[MigrateAction].info(
        stringify(
          "migrationId" -> migrationId,
          "workspace" -> workspaceName,
          "retry" -> retryCount,
          "retried" -> now
        )
      )
    } yield ()

  def removeIdentitiesFromGoogleProjectIam(googleProject: GoogleProject,
                                           identities: Set[Identity]
  ): MigrateAction[Unit] =
    asks(_.googleIamDAO).flatMap { googleIamDao =>
      fromFuture {
        for {
          googleProjectPolicy <- googleIamDao.getProjectPolicy(googleProject)

          rolesToRemove = googleProjectPolicy.getBindings.asScala
            .map { b =>
              b.getMembers.asScala
                .map(Identity.valueOf)
                .filter(identities.contains)
                .map(_ -> Set(b.getRole))
                .toMap
            }
            .reduce(_ |+| _)
            .toList

          _ <- rolesToRemove.traverse_ { case (identity, roles) =>
            val Array(memberType, email) = identity.toString.split(":")
            googleIamDao.removeIamRoles(googleProject,
                                        WorkbenchEmail(email),
                                        GoogleIamDAO.MemberType.stringToMemberType(memberType),
                                        roles
            )
          }
        } yield ()
      }
    }

  final def createBucketInSameRegion(migration: WorkspaceMigration,
                                     workspace: Workspace,
                                     sourceGoogleProject: GoogleProject,
                                     sourceBucketName: GcsBucketName,
                                     destGoogleProject: GoogleProject,
                                     destBucketName: GcsBucketName
  ): MigrateAction[Unit] =
    MigrateAction { env =>
      def pollForBucketToBeCreated(interval: FiniteDuration, deadline: Deadline): IO[Unit] =
        IO.sleep(interval).whileM_ {
          for {
            _ <- IO.raiseWhen(deadline.isOverdue()) {
              WorkspaceMigrationException(
                message = "Workspace migration failed: timed out waiting for bucket creation",
                data = Map(
                  "migrationId" -> migration.id,
                  "workspace" -> workspace.toWorkspaceName,
                  "googleProject" -> destGoogleProject,
                  "bucketName" -> destBucketName
                )
              )
            }

            // Don't need a requester pays project for the bucket in the new google project
            // as requester pays if enabled at the end of the migration, if at all.
            bucket <- env.storageService.getBucket(
              destGoogleProject,
              destBucketName
            )
          } yield bucket.isEmpty
        }

      OptionT.liftF {
        for {
          sourceBucketOpt <- env.storageService.getBucket(
            sourceGoogleProject,
            sourceBucketName,
            bucketGetOptions =
              if (migration.requesterPaysEnabled) List(BucketGetOption.userProject(env.googleProjectToBill.value))
              else List.empty
          )

          sourceBucket = sourceBucketOpt.getOrElse(
            throw WorkspaceMigrationException(
              message = "Workspace migration failed: cannot create bucket in same region as one that does not exist.",
              data = Map(
                "migrationId" -> migration.id,
                "workspace" -> workspace.toWorkspaceName,
                "sourceBucket" -> sourceBucketName
              )
            )
          )

          _ <- env.storageService
            .insertBucket(
              googleProject = destGoogleProject,
              bucketName = destBucketName,
              labels = Option(sourceBucket.getLabels).map(_.asScala.toMap).getOrElse(Map.empty),
              bucketPolicyOnlyEnabled = true,
              logBucket = GcsBucketName(
                GoogleServicesDAO.getStorageLogsBucketName(GoogleProjectId(destGoogleProject.value))
              ).some,
              location = Option(sourceBucket.getLocation)
            )
            .compile
            .drain

          _ <- pollForBucketToBeCreated(interval = 100.milliseconds, deadline = 10.seconds.fromNow)
        } yield ()
      }
    }

  final def configureBucketTransferIam(readMigrations: DataAccess => ReadAction[Vector[WorkspaceMigration]],
                                       getSrcBucket: (GcsBucketName, GcsBucketName) => GcsBucketName,
                                       getDstBucket: (GcsBucketName, GcsBucketName) => GcsBucketName,
                                       getColumn: DataAccess => ColumnName[Option[Timestamp]]
  ): MigrateAction[Unit] =
    withMigration(readMigrations) { (migration, workspace) =>
      for {
        (srcBucket, dstBucket) <- getSrcAndDstBuckets(migration, workspace, getSrcBucket, getDstBucket)

        (googleProject, storageService, transferService) <- asks { d =>
          (d.googleProjectToBill, d.storageService, d.storageTransferService)
        }

        _ <- liftIO {
          for {
            serviceAccount <- transferService.getStsServiceAccount(googleProject)
            serviceAccountList = NonEmptyList.one(Identity.serviceAccount(serviceAccount.email.value))
            // STS requires the following to read from the origin bucket and delete objects after
            // transfer
            _ <- storageService
              .setIamPolicy(
                srcBucket,
                Map(StorageRole.LegacyBucketWriter -> serviceAccountList,
                    StorageRole.ObjectViewer -> serviceAccountList
                ),
                bucketSourceOptions =
                  if (migration.requesterPaysEnabled) List(BucketSourceOption.userProject(googleProject.value))
                  else List.empty
              )
              .compile
              .drain

            // STS requires the following to write to the destination bucket.
            // Note destination bucket will not have requester pays enabled until end of migration.
            _ <- storageService
              .setIamPolicy(
                dstBucket,
                Map(StorageRole.LegacyBucketWriter -> serviceAccountList,
                    StorageRole.ObjectCreator -> serviceAccountList
                )
              )
              .compile
              .drain

          } yield ()
        }

        now <- nowTimestamp
        _ <- inTransaction { dataAccess =>
          dataAccess.workspaceMigrationQuery.update(migration.id, getColumn(dataAccess), now.some)
        }

        _ <- getLogger[MigrateAction].info(
          stringify(
            "migrationId" -> migration.id,
            "workspace" -> workspace.toWorkspaceName,
            "srcBucket" -> srcBucket,
            "dstBucket" -> dstBucket,
            "iamConfigured" -> now
          )
        )
      } yield ()
    }

  final def issueBucketTransferJob(readMigrations: DataAccess => ReadAction[Vector[WorkspaceMigration]],
                                   getSrcBucket: (GcsBucketName, GcsBucketName) => GcsBucketName,
                                   getDstBucket: (GcsBucketName, GcsBucketName) => GcsBucketName,
                                   getColumn: DataAccess => ColumnName[Option[Timestamp]]
  ): MigrateAction[Unit] =
    withMigration(readMigrations) { (migration, workspace) =>
      for {
        (srcBucket, dstBucket) <- getSrcAndDstBuckets(migration, workspace, getSrcBucket, getDstBucket)
        _ <- startBucketTransferJob(migration, workspace, srcBucket, dstBucket)
        now <- nowTimestamp
        _ <- inTransaction { dataAccess =>
          dataAccess.workspaceMigrationQuery.update(migration.id, getColumn(dataAccess), now.some)
        }
      } yield ()
    }

  final def getSrcAndDstBuckets(migration: WorkspaceMigration,
                                workspace: Workspace,
                                getSrcBucket: (GcsBucketName, GcsBucketName) => GcsBucketName,
                                getDstBucket: (GcsBucketName, GcsBucketName) => GcsBucketName
  ): MigrateAction[(GcsBucketName, GcsBucketName)] =
    liftIO {
      IO.fromOption(migration.tmpBucketName)(noTmpBucketError(migration, workspace)).map { tmpBucket =>
        val workspaceBucket = GcsBucketName(workspace.bucketName)
        (getSrcBucket(workspaceBucket, tmpBucket), getDstBucket(workspaceBucket, tmpBucket))
      }
    }

  final def startBucketTransferJob(migration: WorkspaceMigration,
                                   workspace: Workspace,
                                   srcBucket: GcsBucketName,
                                   dstBucket: GcsBucketName
  ): MigrateAction[TransferJob] =
    for {
      (storageTransferService, googleProject) <- asks { env =>
        (env.storageTransferService, env.googleProjectToBill)
      }

      transferJob <- liftIO {
        for {
          jobName <- randomSuffix("transferJobs/terra-workspace-migration-")
          transferJob <- storageTransferService.createTransferJob(
            jobName = GoogleStorageTransferService.JobName(jobName),
            jobDescription =
              s"""Terra workspace migration transferring workspace bucket contents from "${srcBucket}" to "${dstBucket}"
                 |(workspace: "${workspace.toWorkspaceName}", "migration: ${migration.id}")"""".stripMargin,
            projectToBill = googleProject,
            srcBucket,
            dstBucket,
            JobTransferSchedule.Immediately,
            options = JobTransferOptions(
              // operations sometimes fail with missing storage.objects.delete on source objects.
              // If we don't clobber the destination objects when we re-issue the transfer job,
              // the job will succeed but some source objects will not be deleted and we'll fail
              // to delete the source bucket.
              whenToOverwrite = OverwriteObjectsAlreadyExistingInSink,
              // bucket must be empty after job completes so we can delete it
              whenToDelete = DeleteSourceObjectsAfterTransfer
            ).some
          )
        } yield transferJob
      }

      _ <- getLogger[MigrateAction].info(
        stringify(
          "migrationId" -> migration.id,
          "workspace" -> workspace.toWorkspaceName,
          "transferJob" -> transferJob.getName,
          "srcBucket" -> srcBucket,
          "dstBucket" -> dstBucket,
          "issued" -> transferJob.getCreationTime
        )
      )

      _ <- inTransaction { _ =>
        storageTransferJobs
          .map(job => (job.jobName, job.migrationId, job.destBucket, job.originBucket))
          .insert((transferJob.getName, migration.id, dstBucket.value, srcBucket.value))
      }
    } yield transferJob

  final def refreshTransferJobs: MigrateAction[PpwStorageTransferJob] =
    for {
      transferJob <- peekTransferJob
      (storageTransferService, googleProject) <- asks { env =>
        (env.storageTransferService, env.googleProjectToBill)
      }

      // Transfer operations are listed after they've been started. For bucket-to-bucket transfers
      // we expect at most one operation. Polling the latest operation will allow for jobs to be
      // restarted in the cloud console.
      outcome <- liftF {
        import OptionT.{fromOption, liftF}
        for {
          job <- liftF(storageTransferService.getTransferJob(transferJob.jobName, googleProject))
          operationName <- fromOption[IO](Option(job.getLatestOperationName))
          if !operationName.isBlank
          operation <- liftF(storageTransferService.getTransferOperation(OperationName(operationName)))
          outcome <- getOperationOutcome(operation)
        } yield outcome
      }

      (status, message) = toTuple(outcome)
      now <- nowTimestamp
      _ <- inTransaction { _ =>
        storageTransferJobs
          .filter(_.id === transferJob.id)
          .map(row => (row.finished, row.outcome, row.message))
          .update(now.some, status.some, message)
      }

      _ <- getLogger[MigrateAction].info(
        stringify(
          "migrationId" -> transferJob.migrationId,
          "transferJob" -> transferJob.jobName.value,
          "finished" -> now,
          "outcome" -> outcome.toJson.compactPrint
        )
      )

    } yield transferJob.copy(finished = now.some, outcome = outcome.some)

  final def getOperationOutcome(operation: TransferOperation): OptionT[IO, Outcome] =
    OptionT {
      Some(operation)
        .filter(_.hasEndTime)
        .traverse(_.getStatus match {
          case TransferOperation.Status.SUCCESS => IO.pure(Success)
          case TransferOperation.Status.ABORTED =>
            IO.pure {
              Failure(
                s"""Transfer job "${operation.getTransferJobName}" failed: """ +
                  s"""operation "${operation.getName}" was aborted."""
              )
            }
          case TransferOperation.Status.FAILED =>
            IO.pure {
              Failure(
                s"""Transfer job "${operation.getTransferJobName}" failed: """ +
                  s"""operation "${operation.getName}" failed with errors: """ +
                  operation.getErrorBreakdownsList
              )
            }
          case other =>
            // shouldn't really happen, but something could have gone wrong in the wire.
            // report the error and try again later. todo: don't try indefinitely
            IO.raiseError(
              WorkspaceMigrationException(
                "Illegal transfer operation status",
                Map(
                  "transferJob" -> operation.getTransferJobName,
                  "operationName" -> operation.getName,
                  "status" -> other,
                  "operation" -> operation
                )
              )
            )
        })
    }

  final def updateMigrationTransferJobStatus(transferJob: PpwStorageTransferJob): MigrateAction[Unit] =
    transferJob.outcome.traverse_ {
      case Success =>
        transferJobSucceeded(transferJob)
      case failure =>
        inTransactionT(_.workspaceMigrationQuery.getWorkspaceName(transferJob.migrationId)).flatMap {
          endMigration(transferJob.migrationId, _, failure)
        }
    }

  final def transferJobSucceeded(transferJob: PpwStorageTransferJob): MigrateAction[Unit] =
    withMigration(_.workspaceMigrationQuery.withMigrationId(transferJob.migrationId)) { (migration, workspace) =>
      for {
        (storageTransferService, storageService, googleProject) <- asks { env =>
          (env.storageTransferService, env.storageService, env.googleProjectToBill)
        }
        _ <- liftIO {
          for {
            serviceAccount <- storageTransferService.getStsServiceAccount(googleProject)
            serviceAccountList = NonEmptyList.one(Identity.serviceAccount(serviceAccount.email.value))

            _ <- storageService
              .removeIamPolicy(
                transferJob.originBucket,
                Map(StorageRole.LegacyBucketReader -> serviceAccountList,
                    StorageRole.ObjectViewer -> serviceAccountList
                ),
                bucketSourceOptions =
                  if (migration.requesterPaysEnabled) List(BucketSourceOption.userProject(googleProject.value))
                  else List.empty
              )
              .compile
              .drain

            _ <- storageService
              .removeIamPolicy(
                transferJob.destBucket,
                Map(StorageRole.LegacyBucketWriter -> serviceAccountList,
                    StorageRole.ObjectCreator -> serviceAccountList
                )
              )
              .compile
              .drain
          } yield ()
        }

        transferred <- nowTimestamp.map(_.some)
        _ <- inTransaction { dataAccess =>
          import dataAccess.workspaceMigrationQuery._
          update(
            migration.id,
            if (migration.workspaceBucketTransferred.isEmpty) workspaceBucketTransferredCol
            else tmpBucketTransferredCol,
            transferred
          )
        }
      } yield ()
    }

  final def endMigration(migrationId: Long, workspaceName: WorkspaceName, outcome: Outcome): MigrateAction[Unit] =
    for {
      now <- nowTimestamp
      _ <- inTransaction {
        _.workspaceMigrationQuery.setMigrationFinished(migrationId, now, outcome)
      }
      _ <- getLogger[MigrateAction].info(
        stringify(
          "migrationId" -> migrationId,
          "workspace" -> workspaceName,
          "finished" -> now,
          "outcome" -> outcome.toJson.compactPrint
        )
      )
    } yield ()

  final def withMigration(
    selectMigrations: DataAccess => ReadAction[Seq[WorkspaceMigration]]
  )(attempt: (WorkspaceMigration, Workspace) => MigrateAction[Unit]): MigrateAction[Unit] =
    for {
      (migration, workspace) <- inTransactionT { dataAccess =>
        OptionT[ReadWriteAction, (WorkspaceMigration, Workspace)] {
          for {
            migrations <- selectMigrations(dataAccess)

            workspaces <- dataAccess.workspaceQuery
              .listByIds(migrations.map(_.workspaceId))

          } yield migrations.zip(workspaces).headOption
        }
      }

      _ <- attempt(migration, workspace).handleErrorWith { t =>
        endMigration(migration.id, workspace.toWorkspaceName, Failure(t.getMessage))
      }
    } yield ()

  final def peekTransferJob: MigrateAction[PpwStorageTransferJob] =
    nowTimestamp.flatMap { now =>
      inTransactionT { dataAccess =>
        import dataAccess.driver.api._
        for {
          job <- OptionT[ReadWriteAction, PpwStorageTransferJob] {
            storageTransferJobs
              .filter(_.finished.isEmpty)
              .sortBy(_.updated.asc)
              .take(1)
              .result
              .headOption
          }

          // touch the job so that next call to peek returns another
          _ <- OptionT.liftF[ReadWriteAction, Unit] {
            storageTransferJobs.filter(_.id === job.id).map(_.updated).update(now).ignore
          }

        } yield job.copy(updated = now)
      }
    }

  final def getGoogleProjectAndTmpBucket(migration: WorkspaceMigration,
                                         workspace: Workspace
  ): MigrateAction[(GoogleProjectId, GcsBucketName)] =
    liftIO {
      for {
        googleProjectId <- IO.fromOption(migration.newGoogleProjectId)(noGoogleProjectError(migration, workspace))
        bucketName <- IO.fromOption(migration.tmpBucketName)(noTmpBucketError(migration, workspace))
      } yield (googleProjectId, bucketName)
    }

  final def inTransactionT[A](action: DataAccess => OptionT[ReadWriteAction, A]): MigrateAction[A] =
    for {
      dataSource <- asks(_.dataSource)
      result <- liftF(OptionT(dataSource.inTransaction(action(_).value).io))
    } yield result

  final def inTransaction[A](action: DataAccess => ReadWriteAction[A]): MigrateAction[A] =
    for {
      dataSource <- asks(_.dataSource)
      result <- liftIO(dataSource.inTransaction(action).io)
    } yield result

  final def getWorkspace(workspaceId: UUID): MigrateAction[Workspace] =
    inTransaction {
      _.workspaceQuery.findByIdOrFail(workspaceId.toString)
    }

  final def nowTimestamp: MigrateAction[Timestamp] =
    liftIO(IO {
      Timestamp.valueOf(LocalDateTime.ofInstant(Instant.now, ZoneOffset.UTC))
    })

  final def randomSuffix(str: String): IO[String] = IO {
    str ++ UUID.randomUUID.toString.replace("-", "")
  }

  final def noGoogleProjectError(migration: WorkspaceMigration, workspace: Workspace): Throwable =
    WorkspaceMigrationException(
      message = "Workspace migration failed: Google Project not found.",
      data = Map(
        "migrationId" -> migration.id,
        "workspace" -> workspace.toWorkspaceName,
        "googleProjectId" -> migration.newGoogleProjectId
      )
    )

  final def noWorkspaceBucketError(migration: WorkspaceMigration, workspace: Workspace): Throwable =
    WorkspaceMigrationException(
      message = "Workspace migration failed: Workspace cloud bucket not found.",
      data = Map(
        "migrationId" -> migration.id,
        "workspace" -> workspace.toWorkspaceName,
        "workspaceBucket" -> workspace.bucketName
      )
    )

  final def noTmpBucketError[A](migration: WorkspaceMigration, workspace: Workspace): Throwable =
    WorkspaceMigrationException(
      message = "Workspace migration failed: Temporary cloud storage bucket not found.",
      data = Map(
        "migrationId" -> migration.id,
        "workspace" -> workspace.toWorkspaceName,
        "tmpBucket" -> migration.tmpBucketName
      )
    )

  sealed trait Message
  case object RunMigration extends Message
  case object RefreshTransferJobs extends Message
  case object RetryKnownFailures extends Message

  def apply(actorConfig: Config,
            dataSource: SlickDataSource,
            workspaceService: RawlsRequestContext => WorkspaceService,
            storageService: GoogleStorageService[IO],
            storageTransferService: GoogleStorageTransferService[IO],
            gcsDao: GoogleServicesDAO,
            iamDao: GoogleIamDAO,
            samDao: SamDAO
  ): Behavior[Message] =
    Behaviors.setup { context =>
      def unsafeRunMigrateAction[A](action: MigrateAction[A]): Behavior[Message] = {
        try
          gcsDao
            .getServiceAccountUserInfo()
            .io
            .flatMap { userInfo =>
              action
                .run(
                  MigrationDeps(
                    dataSource,
                    actorConfig.googleProjectToBill,
                    actorConfig.googleProjectParentFolder,
                    actorConfig.maxConcurrentMigrationAttempts,
                    actorConfig.maxRetries,
                    workspaceService(RawlsRequestContext(userInfo)),
                    storageService,
                    storageTransferService,
                    userInfo,
                    gcsDao,
                    iamDao,
                    samDao
                  )
                )
                .value
            }
            .void
            .unsafeRunSync
        catch {
          case failure: Throwable => context.executionContext.reportFailure(failure)
        }
        Behaviors.same
      }

      Behaviors.withTimers { scheduler =>
        scheduler.startTimerAtFixedRate(RunMigration, actorConfig.pollingInterval)
        scheduler.startTimerAtFixedRate(RefreshTransferJobs, actorConfig.transferJobRefreshInterval)
        scheduler.startTimerAtFixedRate(RetryKnownFailures, actorConfig.knownFailureRetryInterval)

        Behaviors.receiveMessage { message =>
          unsafeRunMigrateAction {
            message match {
              case RunMigration =>
                migrate

              case RefreshTransferJobs =>
                refreshTransferJobs >>= updateMigrationTransferJobStatus

              case RetryKnownFailures =>
                List(
                  restartFailuresLike(FailureModes.stsRateLimitedFailure, FailureModes.gcsUnavailableFailure),
                  reissueFailedStsJobs
                )
                  .traverse_(r => runStep(r.foreverM)) // Greedily retry
            }
          }
        }
      }
    }

}
