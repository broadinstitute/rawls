package org.broadinstitute.dsde.rawls.monitor.migration

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import cats.Invariant.catsApplicativeForArrow
import cats.data.{NonEmptyList, OptionT, ReaderT}
import cats.effect.IO
import cats.effect.unsafe.IORuntime
import cats.effect.unsafe.implicits.global
import cats.implicits._
import com.google.cloud.Identity
import com.google.cloud.Identity.serviceAccount
import com.google.cloud.storage.Storage
import com.google.cloud.storage.Storage.{BucketGetOption, BucketSourceOption}
import com.google.storagetransfer.v1.proto.TransferTypes.TransferJob
import net.ceedubs.ficus.Ficus.{finiteDurationReader, toFicusConfig}
import net.ceedubs.ficus.readers.ValueReader
import org.broadinstitute.dsde.rawls.dataaccess.slick._
import org.broadinstitute.dsde.rawls.dataaccess.{GoogleServicesDAO, SamDAO, SlickDataSource}
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.monitor.migration.MigrationUtils.Implicits._
import org.broadinstitute.dsde.rawls.monitor.migration.MigrationUtils.Outcome.Failure
import org.broadinstitute.dsde.rawls.monitor.migration.MigrationUtils._
import org.broadinstitute.dsde.rawls.monitor.migration.MultiregionalBucketMigrationActor.MigrateAction._
import org.broadinstitute.dsde.rawls.workspace.WorkspaceService
import org.broadinstitute.dsde.workbench.google.GoogleIamDAO
import org.broadinstitute.dsde.workbench.google2.GoogleStorageTransferService.ObjectDeletionOption.DeleteSourceObjectsAfterTransfer
import org.broadinstitute.dsde.workbench.google2.GoogleStorageTransferService.ObjectOverwriteOption.OverwriteObjectsAlreadyExistingInSink
import org.broadinstitute.dsde.workbench.google2.GoogleStorageTransferService.{JobTransferOptions, JobTransferSchedule}
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
    maxRetries: Int
  )

  implicit val configReader: ValueReader[Config] = ValueReader.relative { config =>
    Config(
      pollingInterval = config.as[FiniteDuration]("polling-interval"),
      transferJobRefreshInterval = config.as[FiniteDuration]("transfer-job-refresh-interval"),
      googleProjectToBill = GoogleProject(config.getString("google-project-id-to-bill")),
      maxConcurrentMigrationAttempts = config.getInt("max-concurrent-migrations"),
      knownFailureRetryInterval = config.as[FiniteDuration]("retry-interval"),
      maxRetries = config.getInt("max-retries")
    )
  }

  final case class MigrationDeps(dataSource: SlickDataSource,
                                 googleProjectToBill: GoogleProject,
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
      createTempBucket,
      configureTransferIamToTmpBucket,
      issueTransferJobToTmpBucket
    )
      .parTraverse_(runStep)

  // Sequence the action and return an empty MigrateAction if the action succeeded
  def runStep(action: MigrateAction[Unit]): MigrateAction[Unit] =
    action.mapF(optionT => OptionT(optionT.value.as(().some)))

  implicit val ec: ExecutionContext = implicitly[IORuntime].compute

  import slick.jdbc.MySQLProfile.api._

  val storageTransferJobs = MultiregionalStorageTransferJobs.storageTransferJobs

  final def restartMigration: MigrateAction[Unit] =
    restartFailuresLike(FailureModes.noBucketPermissionsFailure, FailureModes.gcsUnavailableFailure) |
      reissueFailedStsJobs

  final def startMigration: MigrateAction[Unit] =
    for {
      (maxAttempts, maxReties) <- asks(d => (d.maxConcurrentAttempts, d.maxRetries))
      now <- nowTimestamp
      (id, workspaceName) <- inTransactionT { dataAccess =>
        import dataAccess.multiregionalBucketMigrationQuery._
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
          message = s"The workspace migration failed while removing workspace bucket IAM: $message.",
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
        import dataAccess.{migrationRetryQuery, multiregionalBucketMigrationQuery}
        for {
          (migrationId, workspaceName) <- migrationRetryQuery.nextFailureLike(
            maxRetries,
            failureMessage,
            others: _*
          )
          numAttempts <- OptionT.liftF(multiregionalBucketMigrationQuery.getNumActiveResourceLimitedMigrations)
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

  // Create bucket in us-central1-a
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
                message = "Workspace migration failed: timed out waiting for bucket creation",
                data = Map(
                  "migrationId" -> migration.id,
                  "workspace" -> workspace.toWorkspaceName,
                  "googleProject" -> workspace.googleProjectId,
                  "bucketName" -> destBucketName
                )
              )
            }

            // Don't need a requester pays project for the bucket in the new google project
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
              message = "Workspace migration failed: source bucket not found.",
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
              location = Option("us-central1") // todo: parameterize this
            )
            .compile
            .drain

          _ <- pollForBucketToBeCreated(interval = 100.milliseconds, deadline = 10.seconds.fromNow)
        } yield ()
      }
    }

  final def configureBucketTransferIam(readMigrations: DataAccess => ReadAction[Vector[MultiregionalBucketMigration]],
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
          dataAccess.multiregionalBucketMigrationQuery.update(migration.id, getColumn(dataAccess), now.some)
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
                                   getColumn: DataAccess => ColumnName[Option[Timestamp]]
  ): MigrateAction[Unit] =
    withMigration(readMigrations) { (migration, workspace) =>
      for {
        (srcBucket, dstBucket) <- getSrcAndDstBuckets(migration, workspace, getSrcBucket, getDstBucket)
        _ <- startBucketTransferJob(migration, workspace, srcBucket, dstBucket)
        now <- nowTimestamp
        _ <- inTransaction { dataAccess =>
          dataAccess.multiregionalBucketMigrationQuery.update(migration.id, getColumn(dataAccess), now.some)
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
      (storageTransferService, googleProject) <- asks { env =>
        (env.storageTransferService, env.googleProjectToBill)
      }

      transferJob <- liftIO {
        for {
          jobName <- randomSuffix("transferJobs/terra-multiregional-migration-")
          transferJob <- storageTransferService.createTransferJob(
            jobName = GoogleStorageTransferService.JobName(jobName),
            jobDescription =
              s"""Terra multiregional bucket migration transferring workspace bucket contents from "${srcBucket}" to "${dstBucket}"
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
          .map(job => (job.jobName, job.migrationId, job.destBucket, job.sourceBucket))
          .insert((transferJob.getName, migration.id, dstBucket.value, srcBucket.value))
      }
    } yield transferJob

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
      message = "Workspace migration failed: Google Project not found.",
      data = Map(
        "migrationId" -> migration.id,
        "workspace" -> workspace.toWorkspaceName,
        "googleProjectId" -> workspace.googleProjectId
      )
    )

  final def noWorkspaceBucketError(migration: MultiregionalBucketMigration, workspace: Workspace): Throwable =
    WorkspaceMigrationException(
      message = "Workspace migration failed: Workspace cloud bucket not found.",
      data = Map(
        "migrationId" -> migration.id,
        "workspace" -> workspace.toWorkspaceName,
        "workspaceBucket" -> workspace.bucketName
      )
    )

  final def noTmpBucketError[A](migration: MultiregionalBucketMigration, workspace: Workspace): Throwable =
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
        scheduler.startTimerAtFixedRate(RetryKnownFailures, actorConfig.knownFailureRetryInterval)

        Behaviors.receiveMessage { message =>
          unsafeRunMigrateAction {
            message match {
              case RunMigration =>
                migrate

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
