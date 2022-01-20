package org.broadinstitute.dsde.rawls.monitor.migration

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import cats.data.{OptionT, ReaderT}
import cats.effect.{Clock, IO}
import cats.implicits._
import com.google.cloud.storage.Storage.BucketGetOption
import com.google.longrunning.Operation
import com.google.storagetransfer.v1.proto.TransferTypes.TransferJob
import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.dataaccess.slick._
import org.broadinstitute.dsde.rawls.dataaccess.{GoogleServicesDAO, SlickDataSource}
import org.broadinstitute.dsde.rawls.model.{GoogleProjectId, RawlsBillingProject, Workspace}
import org.broadinstitute.dsde.rawls.monitor.migration.MigrationUtils.Implicits.semigroupOutcome
import org.broadinstitute.dsde.rawls.monitor.migration.MigrationUtils.Outcome.{Failure, Success, toTuple}
import org.broadinstitute.dsde.rawls.monitor.migration.MigrationUtils._
import org.broadinstitute.dsde.rawls.monitor.migration.WorkspaceMigrationHistory._
import org.broadinstitute.dsde.rawls.workspace.WorkspaceService
import org.broadinstitute.dsde.workbench.google2.GoogleStorageTransferService.JobTransferSchedule
import org.broadinstitute.dsde.workbench.google2.{GoogleStorageService, GoogleStorageTransferService}
import org.broadinstitute.dsde.workbench.model.google.{GcsBucketName, GoogleProject}

import java.sql.Timestamp
import java.util.UUID
import scala.collection.JavaConversions._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.DurationInt
import scala.language.higherKinds


object WorkspaceMigrationActor {

  import slick.jdbc.MySQLProfile.api._

  val workspaceMigrations = WorkspaceMigrationHistory.workspaceMigrations
  val storageTransferJobs = PpwStorageTransferJobs.storageTransferJobs


  final def truncate: WriteAction[Unit] =
    storageTransferJobs.delete >> workspaceMigrations.delete >> DBIO.successful()


  final def isInQueueToMigrate(workspace: Workspace): ReadWriteAction[Boolean] =
    workspaceMigrations
      .filter { m => m.workspaceId === workspace.workspaceIdAsUUID && !m.started.isDefined }
      .length
      .result
      .map(_ > 0)


  final def isMigrating(workspace: Workspace): ReadWriteAction[Boolean] =
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
                                 billingProject: RawlsBillingProject,
                                 workspaceService: WorkspaceService,
                                 storageService: GoogleStorageService[IO],
                                 storageTransferService: GoogleStorageTransferService[IO])


  type MigrateAction[A] = ReaderT[OptionT[IO, *], MigrationDeps, A]

  object MigrateAction {

    // lookup a value in the environment using `selector`
    final def asks[T](selector: MigrationDeps => T): MigrateAction[T] =
      ReaderT.ask[OptionT[IO, *], MigrationDeps].map(selector)

    // create a MigrateAction that ignores its input and returns the OptionT
    final def liftF[A](optionT: OptionT[IO, A]): MigrateAction[A] =
      ReaderT.liftF(optionT)

    // lift an IO action into the context of a MigrateAction
    final def liftIO[A](ioa: IO[A]): MigrateAction[A] =
      ReaderT.liftF(OptionT.liftF(ioa))

    // empty action
    final def unit: MigrateAction[Unit] =
      ReaderT.pure()
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
      deleteTemporaryBucket
    )
      .reverse
      .traverse_(runStep)


  // Sequence the action and return an empty MigrateAction if the action succeeded
  def runStep(action: MigrateAction[Unit]): MigrateAction[Unit] =
    action.mapF(optionT => OptionT(optionT.value.as(().some)))


  final def startMigration: MigrateAction[Unit] =
    for {
      migration <- findMigration(_.started.isEmpty)
      now <- nowTimestamp
      _ <- inTransaction { _ =>
        workspaceMigrations
          .filter(_.id === migration.id)
          .map(_.started)
          .update(now.some)
      }
    } yield ()


  final def claimAndConfigureGoogleProject: MigrateAction[Unit] =
    for {
      migration <- findMigration { m =>
        m.started.isDefined && m.newGoogleProjectConfigured.isEmpty
      }

      workspace <- getWorkspace(migration.workspaceId)
      currentBillingAccountOnGoogleProject <- MigrateAction.liftIO(IO {
        workspace.currentBillingAccountOnGoogleProject.getOrElse(
          throw new RawlsException(s"""No billing account for workspace '${workspace.workspaceId}'""")
        )
      })

      (workspaceService, billingProject) <- MigrateAction.asks { env =>
        (env.workspaceService, env.billingProject)
      }

      (googleProjectId, googleProjectNumber) <- MigrateAction.liftIO {
        workspaceService.setupGoogleProject(
          billingProject,
          currentBillingAccountOnGoogleProject,
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


  final def createTempBucket: MigrateAction[Unit] =
    for {
      migration <- findMigration { m =>
        m.newGoogleProjectConfigured.isDefined && m.tmpBucketCreated.isEmpty
      }

      googleProjectId <- MigrateAction.liftIO(IO {
        migration.newGoogleProjectId.getOrElse(throw noGoogleProjectError(migration))
      })

      workspace <- getWorkspace(migration.workspaceId)
      tmpBucketName <- MigrateAction.liftIO(randomSuffix("terra-workspace-migration-"))
      _ <- createBucketInSameRegion(
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
          .ignore
      }
    } yield ()


  final def issueWorkspaceBucketTransferJob: MigrateAction[Unit] =
    for {
      migration <- findMigration { m =>
        m.tmpBucketCreated.isDefined && m.workspaceBucketTransferJobIssued.isEmpty
      }

      tmpBucketName <- MigrateAction.liftIO(IO {
        migration.tmpBucketName.getOrElse(throw noGoogleProjectError(migration))
      })

      workspace <- getWorkspace(migration.workspaceId)
      _ <- startBucketTransferJob(
        migration.id,
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


  final def deleteWorkspaceBucket: MigrateAction[Unit] =
    for {
      migration <- findMigration { m =>
        m.workspaceBucketTransferred.isDefined && m.workspaceBucketDeleted.isEmpty
      }

      workspace <- getWorkspace(migration.workspaceId)
      storageService <- MigrateAction.asks(_.storageService)
      _ <- MigrateAction.liftIO {
        for {
          successOpt <- storageService.deleteBucket(
            GoogleProject(workspace.googleProjectId.value),
            GcsBucketName(workspace.bucketName),
            isRecursive = true
          ).compile.last

          _ <- IO.raiseUnless(successOpt.contains(true)) {
            noWorkspaceBucketError(GcsBucketName(workspace.bucketName))
          }
        } yield ()
      }

      deleted <- nowTimestamp
      _ <- inTransaction { _ =>
        workspaceMigrations
          .filter(_.id === migration.id)
          .map(_.workspaceBucketDeleted)
          .update(deleted.some)
          .ignore
      }
    } yield ()


  final def createFinalWorkspaceBucket: MigrateAction[Unit] =
    for {
      migration <- findMigration { m =>
        m.workspaceBucketDeleted.isDefined && m.finalBucketCreated.isEmpty
      }

      (googleProjectId, tmpBucketName) <- MigrateAction.liftIO(IO {
        (
          migration.newGoogleProjectId.getOrElse(throw noGoogleProjectError(migration)),
          migration.tmpBucketName.getOrElse(throw noTmpBucketError(migration))
        )
      })

      workspace <- getWorkspace(migration.workspaceId)
      _ <- createBucketInSameRegion(tmpBucketName, googleProjectId, GcsBucketName(workspace.bucketName))
      created <- nowTimestamp
      _ <- inTransaction { _ =>
        workspaceMigrations
          .filter(_.id === migration.id)
          .map(r => r.finalBucketCreated)
          .update(created.some)
      }
    } yield ()


  final def issueTmpBucketTransferJob: MigrateAction[Unit] =
    for {
      migration <- findMigration { m =>
        m.finalBucketCreated.isDefined && m.tmpBucketTransferJobIssued.isEmpty
      }

      tmpBucketName <- MigrateAction.liftIO(IO {
        migration.tmpBucketName.getOrElse(throw noGoogleProjectError(migration))
      })

      workspace <- getWorkspace(migration.workspaceId)
      _ <- startBucketTransferJob(
        migration.id,
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


  final def deleteTemporaryBucket: MigrateAction[Unit] =
    for {
      migration <- findMigration { m =>
        m.tmpBucketTransferred.isDefined && m.tmpBucketDeleted.isEmpty
      }

      (googleProjectId, tmpBucketName) <- MigrateAction.liftIO(IO {
        (
          migration.newGoogleProjectId.getOrElse(throw noGoogleProjectError(migration)),
          migration.tmpBucketName.getOrElse(throw noTmpBucketError(migration))
        )
      })

      storageService <- MigrateAction.asks(_.storageService)
      _ <- MigrateAction.liftIO {
        for {
          successOpt <- storageService.deleteBucket(
            GoogleProject(googleProjectId.value),
            tmpBucketName,
            isRecursive = true
          ).compile.last

          _ <- IO.raiseUnless(successOpt.contains(true)) {
            noTmpBucketError(migration)
          }
        } yield ()
      }

      deleted <- nowTimestamp
      _ <- inTransaction { _ =>
        workspaceMigrations
          .filter(_.id === migration.id)
          .map(_.tmpBucketDeleted)
          .update(deleted.some)
          .ignore
      }
    } yield ()


  final def createBucketInSameRegion(sourceBucketName: GcsBucketName,
                                     destGoogleProject: GoogleProjectId,
                                     destBucketName: GcsBucketName)
  : MigrateAction[Unit] =
    for {
      (storageService, billingProject) <- MigrateAction.asks { env =>
        (env.storageService, env.billingProject)
      }
      _ <- MigrateAction.liftIO {
        for {
          sourceBucketOpt <- storageService.getBucket(
            // todo: figure out who pays for this
            GoogleProject(billingProject.googleProjectId.value),
            sourceBucketName,
            List(BucketGetOption.userProject(destGoogleProject.value))
          )

          sourceBucket <- IO(sourceBucketOpt.getOrElse(throw noWorkspaceBucketError(sourceBucketName)))

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


  final def startBucketTransferJob(migrationId: Long,
                                   originBucket: GcsBucketName,
                                   destBucket: GcsBucketName)
  : MigrateAction[TransferJob] =
    for {
      (storageTransferService, billingProject) <- MigrateAction.asks { env =>
        (env.storageTransferService, GoogleProject(env.billingProject.googleProjectId.value))
      }
      transferJob <- MigrateAction.liftIO {
        for {
          jobName <- randomSuffix("transferJobs/terra-workspace-migration-")
          transferJob <- storageTransferService.createTransferJob(
            jobName = GoogleStorageTransferService.JobName(jobName),
            jobDescription = s"""Bucket transfer job from "${originBucket}" to "${destBucket}".""",
            projectToBill = billingProject,
            originBucket,
            destBucket,
            JobTransferSchedule.Immediately
          )
        } yield transferJob
      }

      _ <- inTransaction { _ =>
        storageTransferJobs
          .map(job => (job.jobName, job.migrationId, job.destBucket, job.originBucket))
          .insert((transferJob.getName, migrationId, destBucket.value, originBucket.value))
          .ignore
      }

    } yield transferJob


  final def refreshTransferJobs: MigrateAction[(Long, Outcome)] =
    for {
      record <- peekTransferJob
      (storageTransferService, billingProject) <- MigrateAction.asks { env =>
        (env.storageTransferService, GoogleProject(env.billingProject.googleProjectId.value))
      }

      outcome <- MigrateAction.liftF {
        OptionT {
          storageTransferService
            .listTransferOperations(record.jobName, billingProject)
            .map(_.foldLeft(Option.empty[Outcome]) {
              (outcome, operation) => outcome |+| getOperationOutcome(operation)
            })
        }
      }

      (status, message) = toTuple(outcome)
      _ <- inTransaction { _ =>
        storageTransferJobs
          .filter(_.id === record.id)
          .take(1)
            .map(row => (row.outcome, row.message))
            .update(status, message)
      }
    } yield (record.migrationId, outcome)


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


  final def transferJobSucceeded(migrationId: Long): MigrateAction[Unit] =
    nowTimestamp.flatMap { transferred =>
      inTransaction { dataAccess =>
        val migrations = workspaceMigrations.filter(_.id === migrationId)
        // we can encode which bucket was transferred by filtering instead of explicitly looking
        // it up and branching. If the workspace bucket was already transferred then the job
        // relates to the tmp bucket, otherwise the job relates to the workspace bucket.
        DBIO.seq(
          migrations
            .filter(m => m.workspaceBucketTransferred.isDefined && m.tmpBucketTransferred.isDefined)
            .take(1)
            .result
            .headOption
            .flatMap {
              case None => DBIO.successful()
              // If this triggers then a job has been issued more than once. This is likely caused
              // by running the migration pipeline in parallel.
              case Some(m) => dataAccess
                .workspaceQuery
                .findByIdOrFail(m.workspaceId.toString)
                .flatMap { workspace =>
                  DBIO.failed(new RawlsException(
                    s"""Workspace migration "$migrationId" has already transferred
                       |workspace "${m.workspaceId}" bucket "${workspace.bucketName}"
                       |to Google Project "${m.newGoogleProjectId}".""".stripMargin
                  ))
                }
            },
          migrations
            .filter(_.workspaceBucketTransferred.isDefined)
            .map(_.tmpBucketTransferred)
            .update(transferred.some),
          migrations
            .filter(_.workspaceBucketTransferred.isEmpty)
            .map(_.workspaceBucketTransferred)
            .update(transferred.some)
        )
      }
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


  def getMigrations(workspaceUuid: UUID): MigrateAction[Seq[WorkspaceMigration]] =
    inTransaction { _ =>
      workspaceMigrations
        .filter(_.workspaceId === workspaceUuid)
        .sortBy(_.id)
        .result
    }


  final def findMigration(predicate: WorkspaceMigrationHistory => Rep[Boolean])
  : MigrateAction[WorkspaceMigration] =
    inTransactionT { _ =>
      workspaceMigrations
        .filter(row => row.finished.isEmpty && predicate(row))
        .sortBy(_.updated)
        .take(1)
        .result
        .map(_.headOption)
    }


  final def peekTransferJob
  : MigrateAction[PpwStorageTransferJob] =
    nowTimestamp.flatMap { now =>
      inTransactionT { _ =>
        val job = storageTransferJobs
          .filter(_.finished.isEmpty)
          .sortBy(_.updated)
          .take(1)

        // touch the job so the next peek considers another row to update
        job.map(_.updated).update(now) >> job.result.headOption
    }
  }


  final def inTransactionT[T](action: DataAccess => ReadWriteAction[Option[T]]): MigrateAction[T] =
    inTransaction(action).mapF(optT => OptionT(optT.value.map(_.flatten)))


  final def inTransaction[T](action: DataAccess => ReadWriteAction[T]): MigrateAction[T] =
    for {
      dataSource <- MigrateAction.asks(_.dataSource)
      result <- MigrateAction.liftIO(dataSource.inTransaction(action).io)
    } yield result


  final def getWorkspace(workspaceId: UUID): MigrateAction[Workspace] =
    inTransaction {
      _.workspaceQuery.findByIdOrFail(workspaceId.toString)
    }


  final def nowTimestamp: MigrateAction[Timestamp] =
    MigrateAction.liftIO {
      Clock[IO].realTimeInstant.map(Timestamp.from)
    }


  final def randomSuffix(str: String): IO[String] = IO {
    str ++ UUID.randomUUID.toString.replace("-", "")
  }

  final def noGoogleProjectError[A](migration: WorkspaceMigration): Throwable =
    new IllegalStateException(s"""Google Project "${migration.newGoogleProjectId}" was not found for migration "${migration.id}".""")


  final def noWorkspaceBucketError[A](name: GcsBucketName): Throwable =
    new IllegalStateException(s"""Failed to retrieve workspace bucket "${name}".""")


  final def noTmpBucketError[A](migration: WorkspaceMigration): Throwable =
    new IllegalStateException(s"""Temporary storage bucket "${migration.tmpBucketName}" was not found for migration "${migration.id}".""")


  sealed trait Message
  final case class Schedule(workspace: Workspace) extends Message
  final case object RunMigration extends Message
  final case object RefreshTransferJobs extends Message


  def apply(dataSource: SlickDataSource,
            billingProject: RawlsBillingProject,
            workspaceService: WorkspaceService,
            storageService: GoogleStorageService[IO],
            storageTransferService: GoogleStorageTransferService[IO])
  : Behavior[Message] =
    Behaviors.setup { context =>

      def unsafeStartMigrateAction[T](action: MigrateAction[T]): Behavior[Message] = {
        context.executionContext.execute { () =>
          try {
            action
              .run(
                MigrationDeps(
                  dataSource,
                  billingProject,
                  workspaceService,
                  storageService,
                  storageTransferService
                )
              )
              .value
              .void
              .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
          } catch {
            case failure: Throwable => context.executionContext.reportFailure(failure)
          }
        }
        Behaviors.same
      }

      Behaviors.withTimers { scheduler =>
        // run migration pipeline every 10s
        val period = 10.second
        scheduler.startTimerAtFixedRate(RunMigration, period)

        // two sts jobs are created per migration so run at twice frequency
        scheduler.startTimerAtFixedRate(RefreshTransferJobs, period / 2)

        Behaviors.receiveMessage { message =>
          unsafeStartMigrateAction {
            message match {
              case Schedule(workspace) =>
                inTransaction(_ => schedule(workspace))

              case RunMigration =>
                migrate

              case RefreshTransferJobs =>
                refreshTransferJobs.flatMap { case (migrationId, outcome) =>
                  outcome match {
                    case Success => transferJobSucceeded(migrationId)
                    case Failure(_) => migrationFinished(migrationId, outcome)
                  }
                }
            }
          }
        }
      }
    }

}

