package org.broadinstitute.dsde.rawls.monitor.migration

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import cats.data.{OptionT, ReaderT}
import cats.effect.IO
import cats.implicits._
import com.google.cloud.storage.Storage.BucketGetOption
import com.google.storagetransfer.v1.proto.TransferTypes.TransferJob
import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.dataaccess.slick._
import org.broadinstitute.dsde.rawls.dataaccess.{GoogleServicesDAO, SlickDataSource}
import org.broadinstitute.dsde.rawls.model.{GoogleProjectId, RawlsBillingProject, Workspace}
import org.broadinstitute.dsde.rawls.monitor.migration.MigrationUtils._
import org.broadinstitute.dsde.rawls.monitor.migration.WorkspaceMigrationHistory._
import org.broadinstitute.dsde.rawls.workspace.WorkspaceService
import org.broadinstitute.dsde.workbench.google2.GoogleStorageTransferService.JobTransferSchedule
import org.broadinstitute.dsde.workbench.google2.{GoogleStorageService, GoogleStorageTransferService}
import org.broadinstitute.dsde.workbench.model.google.{GcsBucketName, GoogleProject}

import java.sql.Timestamp
import java.time.LocalDateTime
import java.util.UUID
import scala.collection.JavaConversions._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.language.higherKinds


object WorkspaceMigrationMonitor {

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


  type MigrateAction[T] = ReaderT[OptionT[IO, *], MigrationDeps, T]

  object MigrateAction {
    final def asks[T](f: MigrationDeps => T): MigrateAction[T] =
      ReaderT.ask[OptionT[IO, *], MigrationDeps].map(f)


    final def liftIO[A](ioa: IO[A]): MigrateAction[A] =
      ReaderT.liftF(OptionT.liftF(ioa))


    final def unit: MigrateAction[Unit] =
      ReaderT.pure()
  }


  def getMigrations(workspaceUuid: UUID): MigrateAction[Seq[WorkspaceMigration]] =
    inTransaction { _ =>
      workspaceMigrations
        .filter(_.workspaceId === workspaceUuid)
        .sortBy(_.id)
        .result
    }


  final def migrate: MigrateAction[Unit] =
    startMigration >>
      claimAndConfigureGoogleProject >>
      createTempBucket >>
      issueWorkspaceBucketTransferJob >>
      deleteWorkspaceBucket >>
      createFinalWorkspaceBucket >>
      issueTmpBucketTransferJob >>
      deleteTemporaryBucket >>
      MigrateAction.unit


  final def startMigration: MigrateAction[Unit] =
    for {
      migration <- findMigration(_.started.isEmpty)
      now <- timestampNow
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

      workspaceService <- MigrateAction.asks(_.workspaceService)
      billingProject <- MigrateAction.asks(_.billingProject)
      result <- MigrateAction.liftIO {
        workspaceService.setupGoogleProject(
          billingProject,
          currentBillingAccountOnGoogleProject,
          workspace.workspaceId,
          workspace.toWorkspaceName
        ).io
      }

      (googleProjectId, googleProjectNumber) = result
      configured <- timestampNow
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

      created <- timestampNow
      _ <- inTransaction { _ =>
        workspaceMigrations
          .filter(_.id === migration.id)
          .map(r => (r.tmpBucket, r.tmpBucketCreated))
          .update((tmpBucketName.some, created.some))
          .ignore
      }
    } yield ()


  final def issueWorkspaceBucketTransferJob: MigrateAction[TransferJob] =
    for {
      migration <- findMigration { m =>
        m.tmpBucketCreated.isDefined && m.workspaceBucketTransferJobIssued.isEmpty
      }

      tmpBucketName <- MigrateAction.liftIO(IO {
        migration.tmpBucketName.getOrElse(throw noGoogleProjectError(migration))
      })

      workspace <- getWorkspace(migration.workspaceId)
      transferJob <- startBucketTransferJob(migration, GcsBucketName(workspace.bucketName), tmpBucketName)
      issued <- timestampNow
      _ <- inTransaction { _ =>
        workspaceMigrations
          .filter(_.id === migration.id)
          .map(_.workspaceBucketTransferJobIssued)
          .update(issued.some)
      }
    } yield transferJob


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

      deleted <- timestampNow
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

      tmpBucketName <- MigrateAction.liftIO(IO {
        migration.tmpBucketName.getOrElse(throw noTmpBucketError(migration))
      })

      googleProjectId <- MigrateAction.liftIO(IO {
        migration.newGoogleProjectId.getOrElse(throw noGoogleProjectError(migration))
      })

      workspace <- getWorkspace(migration.workspaceId)
      _ <- createBucketInSameRegion(tmpBucketName, googleProjectId, GcsBucketName(workspace.bucketName))
      created <- timestampNow
      _ <- inTransaction { _ =>
        workspaceMigrations
          .filter(_.id === migration.id)
          .map(r => r.finalBucketCreated)
          .update(created.some)
      }
    } yield ()


  final def issueTmpBucketTransferJob: MigrateAction[TransferJob] =
    for {
      migration <- findMigration { m =>
        m.finalBucketCreated.isDefined && m.tmpBucketTransferJobIssued.isEmpty
      }

      tmpBucketName <- MigrateAction.liftIO(IO {
        migration.tmpBucketName.getOrElse(throw noGoogleProjectError(migration))
      })

      workspace <- getWorkspace(migration.workspaceId)
      transferJob <- startBucketTransferJob(migration, tmpBucketName, GcsBucketName(workspace.bucketName))
      issued <- timestampNow
      _ <- inTransaction { _ =>
        workspaceMigrations
          .filter(_.id === migration.id)
          .map(_.tmpBucketTransferJobIssued)
          .update(issued.some)
      }
    } yield transferJob


  final def deleteTemporaryBucket: MigrateAction[Unit] =
    for {
      migration <- findMigration { m =>
        m.tmpBucketTransferred.isDefined && m.tmpBucketDeleted.isEmpty
      }

      googleProjectId <- MigrateAction.liftIO(IO {
        migration.newGoogleProjectId.getOrElse(throw noGoogleProjectError(migration))
      })

      tmpBucketName <- MigrateAction.liftIO(IO {
        migration.tmpBucketName.getOrElse(throw noTmpBucketError(migration))
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

      deleted <- timestampNow
      _ <- inTransaction { _ =>
        workspaceMigrations
          .filter(_.id === migration.id)
          .map(_.tmpBucketDeleted)
          .update(deleted.some)
          .ignore
      }
    } yield ()


  final def findMigration(predicate: WorkspaceMigrationHistory => Rep[Boolean])
  : MigrateAction[WorkspaceMigration] =
    inTransaction { _ =>
      workspaceMigrations
        .filter(row => row.finished.isEmpty && predicate(row))
        .sortBy(_.updated)
        .take(1)
        .result
        .map(_.headOption)
    }
      .mapF(optT => OptionT(optT.value.map(_.flatten)))


  final def createBucketInSameRegion(sourceBucketName: GcsBucketName,
                                     destGoogleProject: GoogleProjectId,
                                     destBucketName: GcsBucketName)
  : MigrateAction[Unit] =
    for {
      storageService <- MigrateAction.asks(_.storageService)
      billingProject <- MigrateAction.asks(_.billingProject)
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


  final def startBucketTransferJob(migration: WorkspaceMigration,
                                   originBucket: GcsBucketName,
                                   destBucket: GcsBucketName)
  : MigrateAction[TransferJob] =
    for {
      storageTransferService <- MigrateAction.asks(_.storageTransferService)
      billingProject <- MigrateAction.asks(_.billingProject)
      transferJob <- MigrateAction.liftIO {
        for {
          jobName <- randomSuffix("transferJobs/terra-workspace-migration-")
          transferJob <- storageTransferService.createTransferJob(
            jobName = GoogleStorageTransferService.JobName(jobName),
            jobDescription = s"""Bucket transfer job from "${originBucket}" to "${destBucket}".""",
            projectToBill = GoogleProject(billingProject.googleProjectId.value),
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
          .ignore
      }

    } yield transferJob


  final def inTransaction[T](action: DataAccess => ReadWriteAction[T]): MigrateAction[T] =
    for {
      dataSource <- MigrateAction.asks(_.dataSource)
      result <- MigrateAction.liftIO(dataSource.inTransaction(action).io)
    } yield result


  final def getWorkspace(workspaceId: UUID): MigrateAction[Workspace] =
    inTransaction {
      _.workspaceQuery.findByIdOrFail(workspaceId.toString)
    }


  final def timestampNow: MigrateAction[Timestamp] =
    MigrateAction.liftIO(IO {
      Timestamp.valueOf(LocalDateTime.now)
    })


  final def randomSuffix(str: String): IO[String] = IO {
    str ++ UUID.randomUUID.toString.replace("-", "")
  }

  final def noGoogleProjectError[A](migration: WorkspaceMigration): Throwable =
    new IllegalStateException(s"""Google Project "${migration.newGoogleProjectId}" was not found for migration "${migration.id}".""")


  final def noWorkspaceBucketError[A](name: GcsBucketName): Throwable =
    new IllegalStateException(s"""Failed to retrieve workspace bucket "${name}".""")


  final def noTmpBucketError[A](migration: WorkspaceMigration): Throwable =
    new IllegalStateException(s"""Temporary storage bucket "${migration.tmpBucketName}" was not found for migration "${migration.id}".""")

}


object WorkspaceMigrationActor {
  final case class Schedule(workspace: Workspace)

  def apply(dataSource: SlickDataSource): Behavior[Schedule] = Behaviors.receive { (_, message) =>
    dataSource.inTransaction(_ => WorkspaceMigrationMonitor.schedule(message.workspace))
    Behaviors.same
  }
}
