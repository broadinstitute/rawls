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
import org.broadinstitute.dsde.rawls.monitor.migration.MultiregionalBucketMigrationActor.MigrateAction._
import org.broadinstitute.dsde.rawls.workspace.WorkspaceService
import org.broadinstitute.dsde.workbench.google.GoogleIamDAO
import org.broadinstitute.dsde.workbench.google2.GoogleStorageTransferService.ObjectDeletionOption.DeleteSourceObjectsAfterTransfer
import org.broadinstitute.dsde.workbench.google2.GoogleStorageTransferService.ObjectOverwriteOption.OverwriteObjectsAlreadyExistingInSink
import org.broadinstitute.dsde.workbench.google2.GoogleStorageTransferService.{
  JobTransferOptions,
  JobTransferSchedule,
  OperationName
}
import org.broadinstitute.dsde.workbench.google2.util.RetryPredicates
import org.broadinstitute.dsde.workbench.google2.{GoogleStorageService, GoogleStorageTransferService, StorageRole}
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
  * The `MultiregionalBucketMigrationActor` is an Akka Typed Actor [1] that migrates a v2 workspace's
  * multiregional GCS bucket to a single GCP region.
  *
  * The actor migrates workspace buckets by executing a series of operations (or pipeline) successively.
  * These operations are self-contained units defined by some initial and final state in which the
  * actor
  *  - reads a migration attempt from the database in some initial state
  *  - performs some action to advance its state
  *  - writes the new state back to the database.
  *
  * Thus, each operation can be executed independently; successive pipeline execution will eventually
  * migrate a Workspace's bucket successfully or report a failure. In any particular invocation, the
  * actor might do any and all of
  *  - start Storage Transfer jobs to "move" a Workspace's storage bucket to a new GCP region
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
object MultiregionalBucketMigrationActor {

  final case class Config(
    /** The interval between pipeline invocations. */
    pollingInterval: FiniteDuration,

    /** The interval between updating the status of ongoing storage transfer jobs. */
    transferJobRefreshInterval: FiniteDuration,

    /** The Google Project to bill all cloud operations to. */
    googleProjectToBill: GoogleProject,

    /** The maximum number of migration attempts that can be active at any one time. */
    maxConcurrentMigrationAttempts: Int,

    /** The interval to wait before restarting rate-limited migrations. */
    knownFailureRetryInterval: FiniteDuration,

    /** The maximum number of times a failed migration may be retried. */
    maxRetries: Int,

    /** Default GCP location buckets are created in */
    defaultBucketLocation: String
  )

  implicit val configReader: ValueReader[Config] = ValueReader.relative { config =>
    Config(
      pollingInterval = config.as[FiniteDuration]("polling-interval"),
      transferJobRefreshInterval = config.as[FiniteDuration]("transfer-job-refresh-interval"),
      googleProjectToBill = GoogleProject(config.getString("google-project-id-to-bill")),
      maxConcurrentMigrationAttempts = config.getInt("max-concurrent-migrations"),
      knownFailureRetryInterval = config.as[FiniteDuration]("retry-interval"),
      maxRetries = config.getInt("max-retries"),
      defaultBucketLocation = config.getString("default-bucket-location")
    )
  }

  final case class MigrationDeps(dataSource: SlickDataSource,
                                 googleProjectToBill: GoogleProject,
                                 maxConcurrentAttempts: Int,
                                 maxRetries: Int,
                                 defaultBucketLocation: String,
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

  // Read bucket migrations in various states, attempt to advance their state forward by one
  // step and write the outcome of each step to the database.
  final def migrate: MigrateAction[Unit] =
    List(
      restartMigration | startMigration,
      removeWorkspaceBucketIam,
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

  val storageTransferJobs = MultiregionalStorageTransferJobs.storageTransferJobs

  final def restartMigration: MigrateAction[Unit] =
    restartFailuresLike(FailureModes.noBucketPermissionsFailure,
                        FailureModes.gcsUnavailableFailure,
                        FailureModes.bucketNotFoundFailure
    ) |
      reissueFailedStsJobs

  final def startMigration: MigrateAction[Unit] =
    for {
      (maxAttempts, maxRetries) <- asks(d => (d.maxConcurrentAttempts, d.maxRetries))
      now <- nowTimestamp
      (id, workspaceName) <- inTransactionT { dataAccess =>
        import dataAccess.multiregionalBucketMigrationQuery._
        import dataAccess.{WorkspaceExtensions, workspaceQuery}

        for {
          // Use `OptionT` to guard starting more migrations when we're at capacity and
          // to encode non-determinism in picking a workspace bucket to migrate
          activeFullMigrations <- OptionT.liftF(getNumActiveResourceLimitedMigrations)
          isBlocked <- OptionT.liftF(dataAccess.multiregionalBucketMigrationRetryQuery.isPipelineBlocked(maxRetries))

          (id, workspaceId, workspaceName) <-
            if (!isBlocked && activeFullMigrations < maxAttempts) {
              nextMigration()
            } else OptionT.none[ReadWriteAction, (Long, UUID, String)]

          _ <- OptionT.liftF[ReadWriteAction, Unit] {
            workspaceQuery.withWorkspaceId(workspaceId).lock.ignore
          }
          _ <- OptionT.liftF(dataAccess.multiregionalBucketMigrationQuery.update(id, startedCol, Some(now)))
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
    withMigration(_.multiregionalBucketMigrationQuery.removeWorkspaceBucketIamCondition) { (migration, workspace) =>
      val makeError = (message: String, data: Map[String, Any]) =>
        WorkspaceMigrationException(
          message = s"The bucket migration failed while removing workspace bucket IAM: $message.",
          data = Map(
            "migrationId" -> migration.id,
            "workspace" -> workspace.toWorkspaceName
          ) ++ data
        )

      for {
        (storageService, userInfo, googleProjectToBill) <- asks { d =>
          (d.storageService, d.userInfo, d.googleProjectToBill)
        }

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
                  else List.empty,
                version = 3
              )
              .compile
              .drain
          } yield requesterPaysEnabled
        }

        now <- nowTimestamp
        _ <- inTransaction { dataAccess =>
          dataAccess.multiregionalBucketMigrationQuery.update2(
            migration.id,
            dataAccess.multiregionalBucketMigrationQuery.requesterPaysEnabledCol,
            requesterPaysEnabled,
            dataAccess.multiregionalBucketMigrationQuery.workspaceBucketIamRemovedCol,
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

  final def createTempBucket: MigrateAction[Unit] =
    withMigration(_.multiregionalBucketMigrationQuery.createTempBucketConditionCondition) { (migration, workspace) =>
      for {
        tmpBucketName <-
          liftIO(randomSuffix("terra-multiregional-migration-").map(GcsBucketName))

        _ <- createBucketInTargetRegion(
          migration,
          workspace,
          sourceBucketName = GcsBucketName(workspace.bucketName),
          destBucketName = tmpBucketName
        )

        now <- nowTimestamp
        _ <- inTransaction { dataAccess =>
          import dataAccess.setOptionValueObject
          dataAccess.multiregionalBucketMigrationQuery.update2(
            migration.id,
            dataAccess.multiregionalBucketMigrationQuery.tmpBucketCol,
            Some(tmpBucketName),
            dataAccess.multiregionalBucketMigrationQuery.tmpBucketCreatedCol,
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
      _.multiregionalBucketMigrationQuery.configureWorkspaceBucketTransferIam,
      getSrcBucket = (workspaceBucket, _) => workspaceBucket,
      getDstBucket = (_, tmpBucket) => tmpBucket,
      _.multiregionalBucketMigrationQuery.workspaceBucketTransferIamConfiguredCol
    )

  final def issueTransferJobToTmpBucket: MigrateAction[Unit] =
    issueBucketTransferJob(
      _.multiregionalBucketMigrationQuery.issueTransferJobToTmpBucketCondition,
      getSrcBucket = (workspaceBucket, _) => workspaceBucket,
      getDstBucket = (_, tmpBucket) => tmpBucket,
      _.multiregionalBucketMigrationQuery.workspaceBucketTransferJobIssuedCol
    )

  final def deleteWorkspaceBucket: MigrateAction[Unit] =
    withMigration(_.multiregionalBucketMigrationQuery.deleteWorkspaceBucketCondition) { (migration, workspace) =>
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
          dataAccess.multiregionalBucketMigrationQuery.update(
            migration.id,
            dataAccess.multiregionalBucketMigrationQuery.workspaceBucketDeletedCol,
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
    withMigration(_.multiregionalBucketMigrationQuery.createFinalWorkspaceBucketCondition) { (migration, workspace) =>
      for {
        (googleProjectId, tmpBucketName) <- getGoogleProjectAndTmpBucket(migration, workspace)

        _ <- createBucketInTargetRegion(
          migration,
          workspace,
          sourceBucketName = tmpBucketName,
          destBucketName = GcsBucketName(workspace.bucketName)
        )

        now <- nowTimestamp
        _ <- inTransaction { dataAccess =>
          dataAccess.multiregionalBucketMigrationQuery.update(
            migration.id,
            dataAccess.multiregionalBucketMigrationQuery.finalBucketCreatedCol,
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
      _.multiregionalBucketMigrationQuery.configureTmpBucketTransferIam,
      getSrcBucket = (_, tmpBucket) => tmpBucket,
      getDstBucket = (workspaceBucket, _) => workspaceBucket,
      _.multiregionalBucketMigrationQuery.tmpBucketTransferIamConfiguredCol
    )

  final def issueTransferJobToFinalWorkspaceBucket: MigrateAction[Unit] =
    issueBucketTransferJob(
      _.multiregionalBucketMigrationQuery.issueTransferJobToFinalWorkspaceBucketCondition,
      getSrcBucket = (_, tmpBucket) => tmpBucket,
      getDstBucket = (workspaceBucket, _) => workspaceBucket,
      _.multiregionalBucketMigrationQuery.tmpBucketTransferJobIssuedCol
    )

  final def deleteTemporaryBucket: MigrateAction[Unit] =
    withMigration(_.multiregionalBucketMigrationQuery.deleteTemporaryBucketCondition) { (migration, workspace) =>
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
          dataAccess.multiregionalBucketMigrationQuery.update(
            migration.id,
            dataAccess.multiregionalBucketMigrationQuery.tmpBucketDeletedCol,
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
    withMigration(_.multiregionalBucketMigrationQuery.restoreIamPoliciesCondition) { (migration, workspace) =>
      for {
        deps <- asks(identity)

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
                  message = s"""Bucket migration failed: no "$owner" policy on billing project.""",
                  data = Map("migrationId" -> migration.id, "billingProject" -> workspace.namespace)
                )
              )
              .email

            workspacePolicies <- deps.samDao.admin.listPolicies(
              SamResourceTypeNames.workspace,
              workspace.workspaceId,
              RawlsRequestContext(deps.userInfo)
            )

            workspacePoliciesByName = workspacePolicies.map(p => p.policyName -> p.email).toMap
          } yield (billingProjectOwnerPolicyGroup, workspacePoliciesByName)
        }

        authDomains <- liftIO {
          deps.samDao.asResourceAdmin(
            SamResourceTypeNames.workspace,
            workspace.workspaceId,
            SamWorkspacePolicyNames.owner,
            RawlsRequestContext(deps.userInfo)
          ) {
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
          val bucketPolicies =
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
              bucketPolicies,
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
            .map(_.isLocked)
            .update(false)
        }

        _ <- endMigration(migration.id, workspace.toWorkspaceName, Success)
      } yield ()
    }

  def restartFailuresLike(failureMessage: String, others: String*): MigrateAction[Unit] =
    retryFailuresLike(
      (dataAccess, migrationId) => {
        import dataAccess.multiregionalBucketMigrationQuery._
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
        import dataAccess.multiregionalBucketMigrationQuery._
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
        import dataAccess.{multiregionalBucketMigrationQuery, multiregionalBucketMigrationRetryQuery}
        for {
          (migrationId, workspaceName) <- multiregionalBucketMigrationRetryQuery.nextFailureLike(
            maxRetries,
            failureMessage,
            others: _*
          )
          numAttempts <- OptionT.liftF(multiregionalBucketMigrationQuery.getNumActiveResourceLimitedMigrations)
          if numAttempts < maxAttempts
          retryCount <- OptionT.liftF[ReadWriteAction, Long] {
            for {
              MultiregionalBucketMigrationRetry(id, _, numRetries) <- multiregionalBucketMigrationRetryQuery
                .getOrCreate(migrationId)
              retryCount = numRetries + 1
              _ <- multiregionalBucketMigrationRetryQuery.update(id,
                                                                 multiregionalBucketMigrationRetryQuery.retriesCol,
                                                                 retryCount
              )
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

  // Create bucket in us-central1
  final def createBucketInTargetRegion(migration: MultiregionalBucketMigration,
                                       workspace: Workspace,
                                       sourceBucketName: GcsBucketName,
                                       destBucketName: GcsBucketName
  ): MigrateAction[Unit] =
    MigrateAction { env =>
      def pollForBucketToBeCreated(interval: FiniteDuration, deadline: Deadline): IO[Unit] =
        IO.sleep(interval).whileM_ {
          for {
            _ <- IO.raiseWhen(deadline.isOverdue()) {
              WorkspaceMigrationException(
                message = "Bucket migration failed: timed out waiting for bucket creation",
                data = Map(
                  "migrationId" -> migration.id,
                  "workspace" -> workspace.toWorkspaceName,
                  "googleProject" -> workspace.googleProjectId,
                  "bucketName" -> destBucketName
                )
              )
            }

            // Don't need a requester pays project for the bucket in the new region
            // as requester pays if enabled at the end of the migration, if at all.
            bucket <- env.storageService.getBucket(
              GoogleProject(workspace.googleProjectId.value),
              destBucketName
            )
          } yield bucket.isEmpty
        }

      OptionT.liftF {
        for {
          sourceBucketOpt <- env.storageService.getBucket(
            GoogleProject(workspace.googleProjectId.value),
            sourceBucketName,
            bucketGetOptions =
              if (migration.requesterPaysEnabled) List(BucketGetOption.userProject(env.googleProjectToBill.value))
              else List.empty
          )

          sourceBucket = sourceBucketOpt.getOrElse(
            throw WorkspaceMigrationException(
              message = "Bucket migration failed: source bucket not found.",
              data = Map(
                "migrationId" -> migration.id,
                "workspace" -> workspace.toWorkspaceName,
                "sourceBucket" -> sourceBucketName
              )
            )
          )

          _ <- env.storageService
            .insertBucket(
              googleProject = GoogleProject(workspace.googleProjectId.value),
              bucketName = destBucketName,
              labels = Option(sourceBucket.getLabels).map(_.asScala.toMap).getOrElse(Map.empty),
              bucketPolicyOnlyEnabled = true,
              logBucket = GcsBucketName(
                GoogleServicesDAO.getStorageLogsBucketName(workspace.googleProjectId)
              ).some,
              location = env.defaultBucketLocation.some,
              autoclassEnabled = true
            )
            .compile
            .drain

          _ <- pollForBucketToBeCreated(interval = 100.milliseconds, deadline = 30.seconds.fromNow)
        } yield ()
      }
    }

  final def configureBucketTransferIam(readMigrations: DataAccess => ReadAction[Vector[MultiregionalBucketMigration]],
                                       getSrcBucket: (GcsBucketName, GcsBucketName) => GcsBucketName,
                                       getDstBucket: (GcsBucketName, GcsBucketName) => GcsBucketName,
                                       getTimestampColumnToUpdate: DataAccess => ColumnName[Option[Timestamp]]
  ): MigrateAction[Unit] =
    withMigration(readMigrations) { (migration, workspace) =>
      for {
        (srcBucket, dstBucket) <- getSrcAndDstBuckets(migration, workspace, getSrcBucket, getDstBucket)

        (googleProject, storageService, transferService) <- asks { d =>
          (d.googleProjectToBill, d.storageService, d.storageTransferService)
        }

        _ <- liftIO {
          for {
            serviceAccount <- transferService.getStsServiceAccount(GoogleProject(workspace.googleProjectId.value))
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
          dataAccess.multiregionalBucketMigrationQuery.update(migration.id,
                                                              getTimestampColumnToUpdate(dataAccess),
                                                              now.some
          )
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

  final def issueBucketTransferJob(readMigrations: DataAccess => ReadAction[Vector[MultiregionalBucketMigration]],
                                   getSrcBucket: (GcsBucketName, GcsBucketName) => GcsBucketName,
                                   getDstBucket: (GcsBucketName, GcsBucketName) => GcsBucketName,
                                   getTimestampColumnToUpdate: DataAccess => ColumnName[Option[Timestamp]]
  ): MigrateAction[Unit] =
    withMigration(readMigrations) { (migration, workspace) =>
      for {
        (srcBucket, dstBucket) <- getSrcAndDstBuckets(migration, workspace, getSrcBucket, getDstBucket)
        _ <- startBucketTransferJob(migration, workspace, srcBucket, dstBucket)
        now <- nowTimestamp
        _ <- inTransaction { dataAccess =>
          dataAccess.multiregionalBucketMigrationQuery.update(migration.id,
                                                              getTimestampColumnToUpdate(dataAccess),
                                                              now.some
          )
        }
      } yield ()
    }

  final def getSrcAndDstBuckets(migration: MultiregionalBucketMigration,
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

  final def startBucketTransferJob(migration: MultiregionalBucketMigration,
                                   workspace: Workspace,
                                   srcBucket: GcsBucketName,
                                   dstBucket: GcsBucketName
  ): MigrateAction[TransferJob] =
    for {
      storageTransferService <- asks(_.storageTransferService)

      transferJob <- liftIO {
        for {
          jobName <- randomSuffix("transferJobs/terra-multiregional-migration-")
          transferJob <- storageTransferService.createTransferJob(
            jobName = GoogleStorageTransferService.JobName(jobName),
            jobDescription =
              s"""Terra multiregional bucket migration transferring workspace bucket contents from "${srcBucket}" to "${dstBucket}"
                 |(workspace: "${workspace.toWorkspaceName}", "migration: ${migration.id}")"""".stripMargin,
            projectToBill = GoogleProject(workspace.googleProjectId.value),
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
          .map(job => (job.jobName, job.migrationId, job.destBucket, job.sourceBucket, job.googleProject))
          .insert(
            (transferJob.getName, migration.id, dstBucket.value, srcBucket.value, workspace.googleProjectId.value.some)
          )
      }
    } yield transferJob

  final def refreshTransferJobs: MigrateAction[MultiregionalStorageTransferJob] =
    for {
      transferJob <- peekTransferJob
      (storageTransferService, googleProject) <- asks { env =>
        (env.storageTransferService, env.googleProjectToBill)
      }

      // Transfer operations are listed after they've been started. For bucket-to-bucket transfers
      // we expect at most one operation. Polling the latest operation will allow for jobs to be
      // restarted in the cloud console.
      operation <- liftF {
        import OptionT.{fromOption, liftF}
        for {
          job <- liftF(
            storageTransferService.getTransferJob(transferJob.jobName,
                                                  transferJob.googleProject.getOrElse(googleProject)
            )
          )
          operationName <- fromOption[IO](Option(job.getLatestOperationName))
          if !operationName.isBlank
          operation <- liftF(storageTransferService.getTransferOperation(OperationName(operationName)))
        } yield operation
      }

      progressOpt = STSJobProgress.fromTransferOperation(operation)
      outcomeOpt = getOperationOutcome(operation)
      (statusOpt, messageOpt) = outcomeOpt.map(toTuple).unzip

      now <- nowTimestamp
      maybeFinishedTimestamp = outcomeOpt.map(_ => now)
      _ <- inTransaction { _ =>
        storageTransferJobs
          .filter(_.id === transferJob.id)
          .map(row =>
            (row.finished,
             row.outcome,
             row.message,
             row.totalBytesToTransfer,
             row.bytesTransferred,
             row.totalObjectsToTransfer,
             row.objectsTransferred
            )
          )
          .update(
            (maybeFinishedTimestamp,
             statusOpt,
             messageOpt.flatten,
             progressOpt.map(_.totalBytesToTransfer),
             progressOpt.map(_.bytesTransferred),
             progressOpt.map(_.totalObjectsToTransfer),
             progressOpt.map(_.objectsTransferred)
            )
          )
      }

      _ <- getLogger[MigrateAction].info(
        stringify(
          "migrationId" -> transferJob.migrationId,
          "transferJob" -> transferJob.jobName.value,
          "finished" -> now,
          "outcome" -> outcomeOpt.map(_.toJson.compactPrint)
        )
      )

    } yield transferJob.copy(
      finished = maybeFinishedTimestamp,
      outcome = outcomeOpt,
      totalBytesToTransfer = progressOpt.map(_.totalBytesToTransfer),
      bytesTransferred = progressOpt.map(_.bytesTransferred),
      totalObjectsToTransfer = progressOpt.map(_.totalObjectsToTransfer),
      objectsTransferred = progressOpt.map(_.objectsTransferred)
    )

  final def getOperationOutcome(operation: TransferOperation): Option[Outcome] =
    Option(operation).filter(_.hasEndTime).map { finishedOperation =>
      finishedOperation.getStatus match {
        case TransferOperation.Status.SUCCESS => Success
        case TransferOperation.Status.ABORTED =>
          Failure(
            s"""Transfer job "${operation.getTransferJobName}" failed: """ +
              s"""operation "${operation.getName}" was aborted."""
          )
        case TransferOperation.Status.FAILED =>
          Failure(
            s"""Transfer job "${operation.getTransferJobName}" failed: """ +
              s"""operation "${operation.getName}" failed with errors: """ +
              operation.getErrorBreakdownsList
          )
        case other =>
          // shouldn't really happen, but something could have gone wrong in the wire.
          // report the error and try again later. todo: don't try indefinitely
          throw WorkspaceMigrationException(
            "Illegal transfer operation status",
            Map(
              "transferJob" -> operation.getTransferJobName,
              "operationName" -> operation.getName,
              "status" -> other,
              "operation" -> operation
            )
          )
      }
    }

  final def updateMigrationTransferJobStatus(transferJob: MultiregionalStorageTransferJob): MigrateAction[Unit] =
    transferJob.outcome.traverse_ {
      case Success =>
        transferJobSucceeded(transferJob)
      case failure =>
        inTransactionT(_.multiregionalBucketMigrationQuery.getWorkspaceName(transferJob.migrationId)).flatMap {
          endMigration(transferJob.migrationId, _, failure)
        }
    }

  final def transferJobSucceeded(transferJob: MultiregionalStorageTransferJob): MigrateAction[Unit] =
    withMigration(_.multiregionalBucketMigrationQuery.withMigrationId(transferJob.migrationId)) {
      (migration, workspace) =>
        for {
          (storageTransferService, storageService, googleProject) <- asks { env =>
            (env.storageTransferService, env.storageService, env.googleProjectToBill)
          }
          _ <- liftIO {
            for {
              serviceAccount <- storageTransferService.getStsServiceAccount(
                transferJob.googleProject.getOrElse(googleProject)
              )
              serviceAccountList = NonEmptyList.one(Identity.serviceAccount(serviceAccount.email.value))

              retryConfig = RetryPredicates.retryAllConfig.copy(retryable =
                RetryPredicates.combine(
                  Seq(RetryPredicates.standardGoogleRetryPredicate, RetryPredicates.whenStatusCode(404))
                )
              )

              _ <- storageService
                .removeIamPolicy(
                  transferJob.sourceBucket,
                  Map(StorageRole.LegacyBucketReader -> serviceAccountList,
                      StorageRole.ObjectViewer -> serviceAccountList
                  ),
                  bucketSourceOptions =
                    if (migration.requesterPaysEnabled) List(BucketSourceOption.userProject(googleProject.value))
                    else List.empty,
                  retryConfig = retryConfig
                )
                .compile
                .drain

              _ <- storageService
                .removeIamPolicy(
                  transferJob.destBucket,
                  Map(StorageRole.LegacyBucketWriter -> serviceAccountList,
                      StorageRole.ObjectCreator -> serviceAccountList
                  ),
                  retryConfig = retryConfig
                )
                .compile
                .drain
            } yield ()
          }

          transferred <- nowTimestamp.map(_.some)
          _ <- inTransaction { dataAccess =>
            import dataAccess.multiregionalBucketMigrationQuery._
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
        _.multiregionalBucketMigrationQuery.setMigrationFinished(migrationId, now, outcome)
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
    selectMigrations: DataAccess => ReadAction[Seq[MultiregionalBucketMigration]]
  )(attempt: (MultiregionalBucketMigration, Workspace) => MigrateAction[Unit]): MigrateAction[Unit] =
    for {
      (migration, workspace) <- inTransactionT { dataAccess =>
        OptionT[ReadWriteAction, (MultiregionalBucketMigration, Workspace)] {
          for {
            migrations <- selectMigrations(dataAccess)

            workspaces <- dataAccess.workspaceQuery
              .listV2WorkspacesByIds(migrations.map(_.workspaceId))

          } yield migrations.zip(workspaces).headOption
        }
      }

      _ <- attempt(migration, workspace).handleErrorWith { t =>
        endMigration(migration.id, workspace.toWorkspaceName, Failure(t.getMessage))
      }
    } yield ()

  final def peekTransferJob: MigrateAction[MultiregionalStorageTransferJob] =
    nowTimestamp.flatMap { now =>
      inTransactionT { dataAccess =>
        import dataAccess.driver.api._
        for {
          job <- OptionT[ReadWriteAction, MultiregionalStorageTransferJob] {
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

  final def getGoogleProjectAndTmpBucket(migration: MultiregionalBucketMigration,
                                         workspace: Workspace
  ): MigrateAction[(GoogleProjectId, GcsBucketName)] =
    liftIO {
      for {
        googleProjectId <- IO.fromOption(Option(workspace.googleProjectId))(noGoogleProjectError(migration, workspace))
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

  final def noGoogleProjectError(migration: MultiregionalBucketMigration, workspace: Workspace): Throwable =
    WorkspaceMigrationException(
      message = "Bucket migration failed: Google Project not found.",
      data = Map(
        "migrationId" -> migration.id,
        "workspace" -> workspace.toWorkspaceName,
        "googleProjectId" -> workspace.googleProjectId
      )
    )

  final def noWorkspaceBucketError(migration: MultiregionalBucketMigration, workspace: Workspace): Throwable =
    WorkspaceMigrationException(
      message = "Bucket migration failed: Workspace cloud bucket not found.",
      data = Map(
        "migrationId" -> migration.id,
        "workspace" -> workspace.toWorkspaceName,
        "workspaceBucket" -> workspace.bucketName
      )
    )

  final def noTmpBucketError[A](migration: MultiregionalBucketMigration, workspace: Workspace): Throwable =
    WorkspaceMigrationException(
      message = "Bucket migration failed: Temporary cloud storage bucket not found.",
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
                    actorConfig.maxConcurrentMigrationAttempts,
                    actorConfig.maxRetries,
                    actorConfig.defaultBucketLocation,
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
                  restartFailuresLike(FailureModes.stsRateLimitedFailure,
                                      FailureModes.gcsUnavailableFailure,
                                      FailureModes.bucketNotFoundFailure
                  ),
                  reissueFailedStsJobs
                )
                  .traverse_(r => runStep(r.foreverM)) // Greedily retry
            }
          }
        }
      }
    }

}
