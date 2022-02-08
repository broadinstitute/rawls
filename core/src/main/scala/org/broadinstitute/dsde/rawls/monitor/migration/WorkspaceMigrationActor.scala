package org.broadinstitute.dsde.rawls.monitor.migration

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import cats.data.{NonEmptyList, OptionT, ReaderT}
import cats.effect.unsafe.implicits.{global => ioruntime}
import cats.effect.IO
import cats.implicits._
import com.google.cloud.Identity
import com.google.cloud.storage.Storage.BucketGetOption
import com.google.longrunning.Operation
import com.google.storagetransfer.v1.proto.TransferTypes.TransferJob
import org.broadinstitute.dsde.rawls.dataaccess.slick._
import org.broadinstitute.dsde.rawls.dataaccess.{GoogleServicesDAO, SlickDataSource}
import org.broadinstitute.dsde.rawls.model.{GoogleProjectId, RawlsBillingProjectName, Workspace}
import org.broadinstitute.dsde.rawls.monitor.migration.MigrationUtils.Implicits._
import org.broadinstitute.dsde.rawls.monitor.migration.MigrationUtils.Outcome.{Failure, Success, toTuple}
import org.broadinstitute.dsde.rawls.monitor.migration.MigrationUtils._
import org.broadinstitute.dsde.rawls.monitor.migration.WorkspaceMigrationHistory._
import org.broadinstitute.dsde.rawls.workspace.WorkspaceService
import org.broadinstitute.dsde.workbench.google2.GoogleStorageTransferService.JobTransferSchedule
import org.broadinstitute.dsde.workbench.google2.{GoogleStorageService, GoogleStorageTransferService, StorageRole}
import org.broadinstitute.dsde.workbench.model.google.{GcsBucketName, GoogleProject}

import java.sql.Timestamp
import java.time.{Instant, LocalDateTime, ZoneOffset}
import java.util.UUID
import scala.collection.JavaConversions._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.FiniteDuration
import scala.language.higherKinds


object WorkspaceMigrationActor {

  import slick.jdbc.MySQLProfile.api._

  val workspaceMigrations = WorkspaceMigrationHistory.workspaceMigrations
  val storageTransferJobs = PpwStorageTransferJobs.storageTransferJobs


  final def truncate: WriteAction[Unit] =
    storageTransferJobs.delete >> workspaceMigrations.delete >> DBIO.successful()


  final def isInQueueToMigrate(workspace: Workspace): ReadAction[Boolean] =
    workspaceMigrations
      .filter { m => m.workspaceId === workspace.workspaceIdAsUUID && !m.started.isDefined }
      .length
      .result
      .map(_ > 0)


  final def isMigrating(workspace: Workspace): ReadAction[Boolean] =
    workspaceMigrations
      .filter { m =>
        m.workspaceId === workspace.workspaceIdAsUUID &&
          m.started.isDefined &&
          m.finished.isEmpty
      }
      .length
      .result
      .map(_ > 0)


  final def schedule(workspace: Workspace): ReadWriteAction[Unit] =
    workspaceMigrations
      .map(_.workspaceId)
      .insert(workspace.workspaceIdAsUUID)
      .ignore


  final case class MigrationDeps(dataSource: SlickDataSource,
                                 googleProjectToBill: GoogleProject,
                                 workspaceService: WorkspaceService,
                                 storageService: GoogleStorageService[IO],
                                 storageTransferService: GoogleStorageTransferService[IO])


  type MigrateAction[A] = ReaderT[OptionT[IO, *], MigrationDeps, A]

  object MigrateAction {

    // lookup a value in the environment using `selector`
    final def asks[A](selector: MigrationDeps => A): MigrateAction[A] =
      ReaderT.ask[OptionT[IO, *], MigrationDeps].map(selector)

    // create a MigrateAction that ignores its input and returns the OptionT
    final def liftF[A](optionT: OptionT[IO, A]): MigrateAction[A] =
      ReaderT.liftF(optionT)

    // lift an IO action into the context of a MigrateAction
    final def liftIO[A](ioa: IO[A]): MigrateAction[A] =
      ReaderT.liftF(OptionT.liftF(ioa))

    // modify the environment that action is evaluated in
    final def local[A](f: MigrationDeps => MigrationDeps)(action: MigrateAction[A]): MigrateAction[A] =
      ReaderT.local(f)(action)

    // Raises the error when the condition is true, otherwise returns unit
    final def raiseWhen(condition: Boolean)(t: => Throwable): MigrateAction[Unit] =
      MigrateAction.liftIO(IO.raiseWhen(condition)(t))

    // empty action
    final def unit: MigrateAction[Unit] =
      ReaderT.pure()
  }


  implicit class MigrateActionOps[A](action: MigrateAction[A]) {
    final def handleErrorWith(f: Throwable => MigrateAction[A]): MigrateAction[A] =
      ReaderT { env =>
        OptionT(action.run(env).value.handleErrorWith(f(_).run(env).value))
      }
  }


  // Read workspace migrations in various states, attempt to advance their state forward by one
  // step and write the outcome of each step to the database.
  // These operations are combined backwards for testing so only one step is run at a time. This
  // has the effect of eagerly finishing migrations rather than starting new ones.
  final def migrate: MigrateAction[Unit] =
    List(
      startMigration,
      claimAndConfigureGoogleProject,
      createTempBucket,
      issueWorkspaceBucketTransferJob,
      deleteWorkspaceBucket,
      createFinalWorkspaceBucket,
      issueTmpBucketTransferJob,
      deleteTemporaryBucket,
      updateWorkspaceRecord
    )
      .reverse
      .traverse_(runStep)


  // Sequence the action and return an empty MigrateAction if the action succeeded
  def runStep(action: MigrateAction[Unit]): MigrateAction[Unit] =
    action.mapF(optionT => OptionT(optionT.value.as(().some)))


  final def startMigration: MigrateAction[Unit] =
    withMigration(_.started.isEmpty) { (migration, _) =>
      for {
        now <- nowTimestamp
        _ <- inTransaction { _ =>
          workspaceMigrations
            .filter(_.id === migration.id)
            .map(_.started)
            .update(now.some)
        }
      } yield ()
    }


  final def claimAndConfigureGoogleProject: MigrateAction[Unit] =
    withMigration(m => m.started.isDefined && m.newGoogleProjectConfigured.isEmpty) {
      (migration, workspace) =>
        val makeError = (message: String, data: Map[String, Any]) => WorkspaceMigrationException(
          message = s"The workspace migration failed while configuring a new Google Project: $message.",
          data = Map(
            ("migrationId" -> migration.id),
            ("workspace" -> workspace.toWorkspaceName)
          ) ++ data
        )

        for {
          _ <- MigrateAction.raiseWhen(workspace.billingAccountErrorMessage.isDefined) {
            makeError("a billing account error exists on workspace", Map(
              "billingAccountErrorMessage" -> workspace.billingAccountErrorMessage.get
            ))
          }

          workspaceBillingAccount = workspace.currentBillingAccountOnGoogleProject.getOrElse(
            throw makeError("no billing account on workspace", Map.empty)
          )

          // Safe to assume that this exists if the workspace exists
          billingProject <- inTransactionT {
            _.rawlsBillingProjectQuery.load(RawlsBillingProjectName(workspace.namespace))
          }

          billingProjectBillingAccount = billingProject.billingAccount.getOrElse(
            throw makeError("no billing account on billing project", Map(
              "billingProject" -> billingProject.projectName
            ))
          )

          _ <- MigrateAction.raiseWhen(billingProject.invalidBillingAccount) {
            makeError("invalid billing account on billing project", Map(
              ("billingProject" -> billingProject.projectName),
              ("billingProjectBillingAccount" -> billingProjectBillingAccount)
            ))
          }

          _ <- MigrateAction.raiseWhen(workspaceBillingAccount != billingProjectBillingAccount) {
            makeError("billing account on workspace differs from billing account on billing project", Map(
              ("workspaceBillingAccount" -> workspaceBillingAccount),
              ("billingProject" -> billingProject.projectName),
              ("billingProjectBillingAccount" -> billingProjectBillingAccount)
            ))
          }

          workspaceService <- MigrateAction.asks(_.workspaceService)
          (googleProjectId, googleProjectNumber) <- MigrateAction.liftIO {
            workspaceService.setupGoogleProject(
              billingProject,
              workspaceBillingAccount,
              workspace.workspaceId,
              workspace.toWorkspaceName
            ).io
          }

          configured <- nowTimestamp
          _ <- inTransaction { _ =>
            workspaceMigrations
              .filter(_.id === migration.id)
              .map(r => (r.newGoogleProjectId, r.newGoogleProjectNumber, r.newGoogleProjectConfigured))
              .update((googleProjectId.value.some, googleProjectNumber.value.some, configured.some))
          }
        } yield ()
    }


  final def createTempBucket: MigrateAction[Unit] =
    withMigration(m => m.newGoogleProjectConfigured.isDefined && m.tmpBucketCreated.isEmpty) {
      (migration, workspace) =>
        for {
          tmpBucketName <- MigrateAction.liftIO(randomSuffix("terra-workspace-migration-"))
          googleProjectId = migration.newGoogleProjectId.getOrElse(
            throw noGoogleProjectError(migration, workspace)
          )
          _ <- createBucketInSameRegion(
            migration,
            workspace,
            GcsBucketName(workspace.bucketName),
            googleProjectId,
            GcsBucketName(tmpBucketName)
          )

          created <- nowTimestamp
          _ <- inTransaction { _ =>
            workspaceMigrations
              .filter(_.id === migration.id)
              .map(r => (r.tmpBucket, r.tmpBucketCreated))
              .update((tmpBucketName.some, created.some))
          }
        } yield ()
    }


  final def issueWorkspaceBucketTransferJob: MigrateAction[Unit] =
    withMigration(m => m.tmpBucketCreated.isDefined && m.workspaceBucketTransferJobIssued.isEmpty) {
      (migration, workspace) =>
        for {
          tmpBucketName <- MigrateAction.liftIO(IO {
            migration.tmpBucketName.getOrElse(throw noGoogleProjectError(migration, workspace))
          })

          _ <- startBucketTransferJob(
            migration,
            workspace,
            GcsBucketName(workspace.bucketName),
            tmpBucketName
          )

          issued <- nowTimestamp
          _ <- inTransaction { _ =>
            workspaceMigrations
              .filter(_.id === migration.id)
              .map(_.workspaceBucketTransferJobIssued)
              .update(issued.some)
          }
        } yield ()
    }


  final def deleteWorkspaceBucket: MigrateAction[Unit] =
    withMigration(m => m.workspaceBucketTransferred.isDefined && m.workspaceBucketDeleted.isEmpty) {
      (migration, workspace) =>
        for {
          storageService <- MigrateAction.asks(_.storageService)
          successOpt <- MigrateAction.liftIO {
            storageService.deleteBucket(
              GoogleProject(workspace.googleProjectId.value),
              GcsBucketName(workspace.bucketName),
              isRecursive = true
            ).compile.last
          }

          _ <- MigrateAction.raiseWhen(!successOpt.contains(true)) {
            noWorkspaceBucketError(migration, workspace)
          }

          deleted <- nowTimestamp
          _ <- inTransaction { _ =>
            workspaceMigrations
              .filter(_.id === migration.id)
              .map(_.workspaceBucketDeleted)
              .update(deleted.some)
          }
        } yield ()
    }


  final def createFinalWorkspaceBucket: MigrateAction[Unit] =
    withMigration(m => m.workspaceBucketDeleted.isDefined && m.finalBucketCreated.isEmpty) {
      (migration, workspace) =>
        for {
          (googleProjectId, tmpBucketName) <- getGoogleProjectAndTmpBucket(migration, workspace)
          _ <- createBucketInSameRegion(
            migration,
            workspace,
            tmpBucketName,
            googleProjectId,
            GcsBucketName(workspace.bucketName)
          )

          created <- nowTimestamp
          _ <- inTransaction { _ =>
            workspaceMigrations
              .filter(_.id === migration.id)
              .map(r => r.finalBucketCreated)
              .update(created.some)
          }
        } yield ()
    }


  final def issueTmpBucketTransferJob: MigrateAction[Unit] =
    withMigration(m => m.finalBucketCreated.isDefined && m.tmpBucketTransferJobIssued.isEmpty) {
      (migration, workspace) =>
        for {
          tmpBucketName <- MigrateAction.liftIO(IO {
            migration.tmpBucketName.getOrElse(throw noGoogleProjectError(migration, workspace))
          })

          _ <- startBucketTransferJob(
            migration,
            workspace,
            tmpBucketName,
            GcsBucketName(workspace.bucketName)
          )

          issued <- nowTimestamp
          _ <- inTransaction { _ =>
            workspaceMigrations
              .filter(_.id === migration.id)
              .map(_.tmpBucketTransferJobIssued)
              .update(issued.some)
          }
        } yield ()
    }


  final def deleteTemporaryBucket: MigrateAction[Unit] =
    withMigration(m => m.tmpBucketTransferred.isDefined && m.tmpBucketDeleted.isEmpty) {
      (migration, workspace) =>
        for {
          (googleProjectId, tmpBucketName) <- getGoogleProjectAndTmpBucket(migration, workspace)
          storageService <- MigrateAction.asks(_.storageService)
          successOpt <- MigrateAction.liftIO {
            storageService.deleteBucket(
              GoogleProject(googleProjectId.value),
              tmpBucketName,
              isRecursive = true
            ).compile.last
          }

          _ <- MigrateAction.raiseWhen(!successOpt.contains(true)) {
            noTmpBucketError(migration, workspace)
          }

          deleted <- nowTimestamp
          _ <- inTransaction { _ =>
            workspaceMigrations
              .filter(_.id === migration.id)
              .map(_.tmpBucketDeleted)
              .update(deleted.some)
          }
        } yield ()
    }


  final def updateWorkspaceRecord: MigrateAction[Unit] =
    withMigration(_.tmpBucketDeleted.isDefined) { (migration, workspace) =>
      for {
        // note that the google project number may not exist so we'll only parse out the project id
        googleProjectId <- MigrateAction.liftIO(IO {
          migration.newGoogleProjectId.getOrElse(throw noGoogleProjectError(migration, workspace))
        })

        _ <- inTransaction(dataAccess =>
          dataAccess
            .workspaceQuery
            .filter(_.id === workspace.workspaceIdAsUUID)
            .map(w => (w.googleProjectId, w.googleProjectNumber))
            .update((googleProjectId.value, migration.newGoogleProjectNumber.map(_.toString)))
        )

        _ <- migrationFinished(migration.id, Success)
      } yield ()
    }


  final def createBucketInSameRegion(migration: WorkspaceMigration,
                                     workspace: Workspace,
                                     sourceBucketName: GcsBucketName,
                                     destGoogleProject: GoogleProjectId,
                                     destBucketName: GcsBucketName)
  : MigrateAction[Unit] =
    for {
      (storageService, googleProjectToBill) <- MigrateAction.asks { env =>
        (env.storageService, env.googleProjectToBill)
      }
      _ <- MigrateAction.liftIO {
        for {
          sourceBucketOpt <- storageService.getBucket(
            googleProjectToBill, // bill "requester-pays" requests to the migration project
            sourceBucketName,
            List(BucketGetOption.userProject(destGoogleProject.value))
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

          // todo: CA-1637 do we need to transfer the storage logs for this workspace? the logs are prefixed
          // with the ws bucket name, so we COULD do it, but do we HAVE to? it's a csv with the bucket
          // and the storage_byte_hours in it that is kept for 180 days
          _ <- storageService.insertBucket(
            googleProject = GoogleProject(destGoogleProject.value),
            bucketName = destBucketName,
            labels = Option(sourceBucket.getLabels).map(_.toMap).getOrElse(Map.empty),
            bucketPolicyOnlyEnabled = true,
            logBucket = GcsBucketName(GoogleServicesDAO.getStorageLogsBucketName(GoogleProjectId(destGoogleProject.value))).some,
            location = Option(sourceBucket.getLocation)
          ).compile.drain
        } yield ()
      }
    } yield ()


  final def startBucketTransferJob(migration: WorkspaceMigration,
                                   workspace: Workspace,
                                   originBucket: GcsBucketName,
                                   destBucket: GcsBucketName)
  : MigrateAction[TransferJob] =
    for {
      (storageTransferService, storageService, googleProject) <- MigrateAction.asks { env =>
        (env.storageTransferService, env.storageService, env.googleProjectToBill)
      }

      transferJob <- MigrateAction.liftIO {
        for {
          serviceAccount <- storageTransferService.getStsServiceAccount(googleProject)
          serviceAccountList = NonEmptyList.one(Identity.serviceAccount(serviceAccount.email.value))

          // STS requires the following to read from the origin bucket
          _ <- storageService.setIamPolicy(originBucket, Map(
            (StorageRole.LegacyBucketReader -> serviceAccountList),
            (StorageRole.ObjectViewer -> serviceAccountList)
          )).compile.drain

          // STS requires the following to write to the destination bucket
          _ <- storageService.setIamPolicy(destBucket, Map(
            (StorageRole.LegacyBucketWriter -> serviceAccountList),
            (StorageRole.ObjectCreator -> serviceAccountList)
          )).compile.drain

          jobName <- randomSuffix("transferJobs/terra-workspace-migration-")

          transferJob <- storageTransferService.createTransferJob(
            jobName = GoogleStorageTransferService.JobName(jobName),
            jobDescription =
              s"""Terra workspace migration transferring workspace bucket contents from "${originBucket}" to "${destBucket}"
                 |(workspace: "${workspace.toWorkspaceName}", "migration: ${migration.id}")"""".stripMargin,
            projectToBill = googleProject,
            originBucket,
            destBucket,
            JobTransferSchedule.Immediately
          )
        } yield transferJob
      }

      _ <- inTransaction { _ =>
        storageTransferJobs
          .map(job => (job.jobName, job.migrationId, job.destBucket, job.originBucket))
          .insert((transferJob.getName, migration.id, destBucket.value, originBucket.value))
      }

    } yield transferJob


  final def refreshTransferJobs: MigrateAction[PpwStorageTransferJob] =
    for {
      transferJob <- peekTransferJob
      (storageTransferService, googleProject) <- MigrateAction.asks { env =>
        (env.storageTransferService, env.googleProjectToBill)
      }

      outcome <- MigrateAction.liftF {
        OptionT {
          storageTransferService
            .listTransferOperations(transferJob.jobName, googleProject)
            .map(_.toList.foldMapK(getOperationOutcome))
        }
      }

      (status, message) = toTuple(outcome)
      _ <- inTransaction { _ =>
        storageTransferJobs
          .filter(_.id === transferJob.id)
          .map(row => (row.outcome, row.message))
          .update(status, message)
      }
    } yield transferJob.copy(outcome = outcome.some)


  final def getOperationOutcome(operation: Operation): Option[Outcome] =
    operation.getDone.some.collect { case true =>
      if (!operation.hasError)
        Success.asInstanceOf[Outcome]
      else {
        val status = operation.getError
        if (status.getDetailsCount == 0)
          Failure(status.getMessage) else
          Failure(status.getMessage ++ " : " ++ status.getDetailsList.toString)
      }
    }


  final def updateMigrationTransferJobStatus(transferJob: PpwStorageTransferJob): MigrateAction[Unit] =
    transferJob.outcome match {
      case Some(Success) => transferJobSucceeded(transferJob)
      case Some(failure) => migrationFinished(transferJob.migrationId, failure)
      case _ => MigrateAction.unit
    }


  final def transferJobSucceeded(transferJob: PpwStorageTransferJob): MigrateAction[Unit] =
    withMigration(_.id === transferJob.migrationId) { (migration, _) =>
      for {
        (storageTransferService, storageService, googleProject) <- MigrateAction.asks { env =>
          (env.storageTransferService, env.storageService, env.googleProjectToBill)
        }

        _ <- MigrateAction.liftIO {
          for {
            serviceAccount <- storageTransferService.getStsServiceAccount(googleProject)
            serviceAccountList = NonEmptyList.one(Identity.serviceAccount(serviceAccount.email.value))

            _ <- storageService.removeIamPolicy(transferJob.originBucket, Map(
              (StorageRole.LegacyBucketReader -> serviceAccountList),
              (StorageRole.ObjectViewer -> serviceAccountList)
            )).compile.drain

            _ <- storageService.removeIamPolicy(transferJob.destBucket, Map(
              (StorageRole.LegacyBucketWriter -> serviceAccountList),
              (StorageRole.ObjectCreator -> serviceAccountList)
            )).compile.drain

          } yield ()
        }

        transferred <- nowTimestamp.map(_.some)
        _ <- inTransaction { _ =>
          val migrationQuery = workspaceMigrations.filter(_.id === transferJob.migrationId)
          if (migration.workspaceBucketTransferred.isEmpty)
            migrationQuery.map(_.workspaceBucketTransferred).update(transferred) else
            migrationQuery.map(_.tmpBucketTransferred).update(transferred)

        }
      } yield ()
    }


  final def migrationFinished(migrationId: Long, outcome: Outcome): MigrateAction[Unit] =
    nowTimestamp.flatMap { finished =>
      inTransaction { _ =>
        val (status, message) = toTuple(outcome)
        workspaceMigrations
          .filter(_.id === migrationId)
          .map(m => (m.finished, m.outcome, m.message))
          .update((finished.some, status, message))
          .ignore
      }
    }


  final def withMigration(filter: WorkspaceMigrationHistory => Rep[Boolean])
                         (attempt: (WorkspaceMigration, Workspace) => MigrateAction[Unit])
  : MigrateAction[Unit] =
    for {
      (migration, workspace) <- inTransactionT { dataAccess =>
        (for {
          migrations <- workspaceMigrations
            .filter(row => row.finished.isEmpty && filter(row))
            .sortBy(_.updated.asc)
            .take(1)
            .result

          workspaces <- dataAccess
            .workspaceQuery
            .listByIds(migrations.map(_.workspaceId))

        } yield migrations.zip(workspaces)).map(_.headOption)
      }

      _ <- attempt(migration, workspace).handleErrorWith { t =>
        migrationFinished(migration.id, Failure(t.getMessage))
      }
    } yield ()


  final def peekTransferJob: MigrateAction[PpwStorageTransferJob] =
    nowTimestamp.flatMap { now =>
      inTransactionT { _ =>
        for {
          job <- storageTransferJobs
            .filter(_.finished.isEmpty)
            .sortBy(_.updated.asc)
            .take(1)
            .result
            .headOption

          // touch the job so that next call to peek returns another
          _ <- if (job.isDefined)
            storageTransferJobs.filter(_.id === job.get.id).map(_.updated).update(now) else
            DBIO.successful()

        } yield job.map(_.copy(updated = now))
      }
    }


  final def getGoogleProjectAndTmpBucket(migration: WorkspaceMigration, workspace: Workspace)
  : MigrateAction[(GoogleProjectId, GcsBucketName)] =
    MigrateAction.liftIO(IO {
      (
        migration.newGoogleProjectId.getOrElse(throw noGoogleProjectError(migration, workspace)),
        migration.tmpBucketName.getOrElse(throw noTmpBucketError(migration, workspace))
      )
    })


  final def inTransactionT[A](action: DataAccess => ReadWriteAction[Option[A]]): MigrateAction[A] =
    inTransaction(action).mapF(optT => OptionT(optT.value.map(_.flatten)))


  final def inTransaction[A](action: DataAccess => ReadWriteAction[A]): MigrateAction[A] =
    for {
      dataSource <- MigrateAction.asks(_.dataSource)
      result <- MigrateAction.liftIO(dataSource.inTransaction(action).io)
    } yield result


  final def getWorkspace(workspaceId: UUID): MigrateAction[Workspace] =
    inTransaction {
      _.workspaceQuery.findByIdOrFail(workspaceId.toString)
    }


  final def nowTimestamp: MigrateAction[Timestamp] =
    MigrateAction.liftIO(IO {
      Timestamp.valueOf(LocalDateTime.ofInstant(Instant.now, ZoneOffset.UTC))
    })


  final def randomSuffix(str: String): IO[String] = IO {
    str ++ UUID.randomUUID.toString.replace("-", "")
  }


  final def noGoogleProjectError(migration: WorkspaceMigration, workspace: Workspace): Throwable =
    WorkspaceMigrationException(
      message = "Workspace migration failed: Google Project not found.",
      data = Map(
        ("migrationId" -> migration.id),
        ("workspace" -> workspace.toWorkspaceName),
        ("googleProjectId" -> migration.newGoogleProjectId)
      )
    )


  final def noWorkspaceBucketError(migration: WorkspaceMigration, workspace: Workspace): Throwable =
    WorkspaceMigrationException(
      message = "Workspace migration failed: Workspace cloud bucket not found.",
      data = Map(
        ("migrationId" -> migration.id),
        ("workspace" -> workspace.toWorkspaceName),
        ("workspaceBucket" -> workspace.bucketName)
      )
    )


  final def noTmpBucketError[A](migration: WorkspaceMigration, workspace: Workspace): Throwable =
    WorkspaceMigrationException(
      message = "Workspace migration failed: Temporary cloud storage bucket not found.",
      data = Map(
        ("migrationId" -> migration.id),
        ("workspace" -> workspace.toWorkspaceName),
        ("tmpBucket" -> migration.tmpBucketName)
      )
    )


  sealed trait Message
  final case class Schedule(workspace: Workspace) extends Message
  case object RunMigration extends Message
  case object RefreshTransferJobs extends Message


  def apply(pollingInterval: FiniteDuration,
            dataSource: SlickDataSource,
            googleProjectToBill: GoogleProject,
            workspaceService: WorkspaceService,
            storageService: GoogleStorageService[IO],
            storageTransferService: GoogleStorageTransferService[IO])
  : Behavior[Message] =
    Behaviors.setup { context =>

      def unsafeRunMigrateAction[A](action: MigrateAction[A]): Behavior[Message] = {
        try {
          action
            .run(
              MigrationDeps(
                dataSource,
                googleProjectToBill,
                workspaceService,
                storageService,
                storageTransferService
              )
            )
            .value
            .void
            .unsafeRunSync
        } catch {
          case failure: Throwable => context.executionContext.reportFailure(failure)
        }
        Behaviors.same
      }

      Behaviors.withTimers { scheduler =>
        scheduler.startTimerAtFixedRate(RunMigration, pollingInterval)
        // two sts jobs are created per migration so run at twice frequency
        scheduler.startTimerAtFixedRate(RefreshTransferJobs, pollingInterval / 2)

        Behaviors.receiveMessage { message =>
          unsafeRunMigrateAction {
            message match {
              case Schedule(workspace) =>
                inTransaction(_ => schedule(workspace))

              case RunMigration =>
                migrate

              case RefreshTransferJobs =>
                refreshTransferJobs >>= updateMigrationTransferJobStatus
            }
          }
        }
      }
    }

}

