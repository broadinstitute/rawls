package org.broadinstitute.dsde.rawls.monitor.migration

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import cats.data.ReaderT
import cats.effect.IO
import cats.implicits._
import com.google.cloud.storage.Storage.BucketGetOption
import com.google.storagetransfer.v1.proto.TransferTypes.TransferJob
import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.dataaccess.slick._
import org.broadinstitute.dsde.rawls.dataaccess.{GoogleServicesDAO, SlickDataSource}
import org.broadinstitute.dsde.rawls.model.{GoogleProjectId, GoogleProjectNumber, RawlsBillingProject, Workspace}
import org.broadinstitute.dsde.rawls.monitor.migration.MigrationStatus._
import org.broadinstitute.dsde.rawls.monitor.migration.MigrationUtils._
import org.broadinstitute.dsde.rawls.workspace.WorkspaceService
import org.broadinstitute.dsde.workbench.google2.GoogleStorageTransferService.JobTransferSchedule
import org.broadinstitute.dsde.workbench.google2.{GoogleStorageService, GoogleStorageTransferService}
import org.broadinstitute.dsde.workbench.model.google.{GcsBucketName, GoogleProject}
import slick.dbio.DBIOAction

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
    storageTransferJobs.delete >> workspaceMigrations.delete >> DBIOAction.successful()


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


  final def schedule(workspace: Workspace): WriteAction[Unit] = workspaceMigrations
    .map(_.workspaceId)
    .insert(workspace.workspaceIdAsUUID)
    .ignore


  final def timestampNow: IO[Timestamp] = IO(Timestamp.valueOf(LocalDateTime.now))


  final case class MigrationDeps(dataSource: SlickDataSource,
                                 billingProject: RawlsBillingProject,
                                 workspaceService: WorkspaceService,
                                 storageService: GoogleStorageService[IO],
                                 storageTransferService: GoogleStorageTransferService[IO])


  final def claimAndConfigureNewGoogleProject[R](migration: WorkspaceMigration, workspace: Workspace)
  : ReaderT[IO, MigrationDeps, (GoogleProjectId, GoogleProjectNumber)] =
    for {
      res <- ReaderT { environment: MigrationDeps =>
        environment.workspaceService.setupGoogleProject(
          environment.billingProject,
          workspace.currentBillingAccountOnGoogleProject.getOrElse(
            throw new RawlsException(s"""No billing account for workspace '${workspace.workspaceId}'""")
          ),
          workspace.workspaceId,
          workspace.toWorkspaceName
        ).io
      }

      (googleProjectId, googleProjectNumber) = res
      configured <- ReaderT.liftK(timestampNow)

      _ <- inTransaction { _ =>
        workspaceMigrations
          .filter(_.id === migration.id)
          .map(r => (r.newGoogleProjectId, r.newGoogleProjectNumber, r.newGoogleProjectConfigured))
          .update((googleProjectId.value.some, googleProjectNumber.value.some, configured.some))
          .ignore
      }

    } yield (googleProjectId, googleProjectNumber)


  final def createTempBucket(attempt: WorkspaceMigration,
                             workspace: Workspace,
                             googleProjectId: GoogleProjectId)
  : ReaderT[IO, MigrationDeps, GcsBucketName] =
    for {
      tmpBucketName <- ReaderT.liftF(randomSuffix("terra-workspace-migration-"))

      _ <- createBucketInSameRegion(
        GcsBucketName(workspace.bucketName),
        googleProjectId,
        GcsBucketName(tmpBucketName)
      )

      created <- ReaderT.liftF(timestampNow)

      _ <- inTransaction { _ =>
        workspaceMigrations
          .filter(_.id === attempt.id)
          .map(r => (r.tmpBucket, r.tmpBucketCreated))
          .update((tmpBucketName.value.some, created.some))
          .ignore
      }

    } yield GcsBucketName(tmpBucketName)


  final def deleteWorkspaceBucket(migration: WorkspaceMigration, workspace: Workspace)
  : ReaderT[IO, MigrationDeps, Unit] =
    for {
      deleted <- ReaderT { environment: MigrationDeps =>
        for {
          successOpt <- environment.storageService.deleteBucket(
            GoogleProject(workspace.googleProjectId.value),
            GcsBucketName(workspace.bucketName),
            isRecursive = true
          ).compile.last

          deleted <- timestampNow

          _ <- IO.raiseUnless(successOpt.contains(true)) {
            noWorkspaceBucketError(GcsBucketName(workspace.bucketName))
          }
        } yield deleted
      }

      _ <- inTransaction { _ =>
        workspaceMigrations
          .filter(_.id === migration.id)
          .map(_.workspaceBucketDeleted)
          .update(deleted.some)
          .ignore
      }
    } yield ()


  final def deleteTemporaryBucket(migration: WorkspaceMigration): ReaderT[IO, MigrationDeps, Unit] =
    for {
      deleted <- ReaderT { environment: MigrationDeps =>
        for {
          googleProjectId <- IO(migration.newGoogleProjectId.getOrElse(throw noGoogleProjectError(migration)))
          tmpBucketName <- IO(migration.tmpBucketName.getOrElse(throw noTmpBucketError(migration)))

          successOpt <- environment.storageService.deleteBucket(
            GoogleProject(googleProjectId.value),
            tmpBucketName,
            isRecursive = true
          ).compile.last

          deleted <- timestampNow

          _ <- IO.raiseUnless(successOpt.contains(true)) {
            noTmpBucketError(migration)
          }

        } yield deleted
      }

      _ <- inTransaction { _ =>
        workspaceMigrations
          .filter(_.id === migration.id)
          .map(_.tmpBucketDeleted)
          .update(deleted.some)
          .ignore
      }
    } yield ()


  final def createFinalBucket(migration: WorkspaceMigration, workspace: Workspace)
  : ReaderT[IO, MigrationDeps, GcsBucketName] =
    for {
      res <- ReaderT.liftF(IO {
        (
          migration.newGoogleProjectId.getOrElse(throw noGoogleProjectError(migration)),
          migration.tmpBucketName.getOrElse(throw noTmpBucketError(migration))
        )
      })

      (googleProjectId, tmpBucket) = res

      _ <- createBucketInSameRegion(tmpBucket, googleProjectId, GcsBucketName(workspace.bucketName))

      created <- ReaderT.liftF(timestampNow)

      _ <- inTransaction { _ =>
        workspaceMigrations
          .filter(_.id === migration.id)
          .map(_.finalBucketCreated)
          .update(created.some)
          .ignore
      }

    } yield GcsBucketName(workspace.bucketName)


  final def createBucketInSameRegion(sourceBucketName: GcsBucketName,
                                     destGoogleProject: GoogleProjectId,
                                     destBucketName: GcsBucketName)
  : ReaderT[IO, MigrationDeps, Unit] =
    ReaderT { environment =>
      for {
        sourceBucketOpt <- environment.storageService.getBucket(
          // todo: figure out who pays for this
          GoogleProject(environment.billingProject.googleProjectId.value),
          sourceBucketName,
          List(BucketGetOption.userProject(destGoogleProject.value))
        )

        sourceBucket <- IO(sourceBucketOpt.getOrElse(throw noWorkspaceBucketError(sourceBucketName)))

        // todo: CA-1637 do we need to transfer the storage logs for this workspace? the logs are prefixed
        // with the ws bucket name, so we COULD do it, but do we HAVE to? it's a csv with the bucket
        // and the storage_byte_hours in it that is kept for 180 days
        _ <- environment.storageService.insertBucket(
          googleProject = GoogleProject(destGoogleProject.value),
          bucketName = destBucketName,
          labels = Option(sourceBucket.getLabels).map(_.toMap).getOrElse(Map.empty),
          bucketPolicyOnlyEnabled = true,
          logBucket = Option(GcsBucketName(GoogleServicesDAO.getStorageLogsBucketName(GoogleProjectId(destGoogleProject.value)))),
          location = Option(sourceBucket.getLocation)
        ).compile.drain

      } yield ()
    }


  final def startStorageTransferJobToFinalBucket(migration: WorkspaceMigration, workspace: Workspace)
  : ReaderT[IO, MigrationDeps, TransferJob] =
    for {
      tmpBucketName <- ReaderT.liftF(IO(migration.tmpBucketName.getOrElse(throw noTmpBucketError(migration))))
      result <- startBucketStorageTransferJob(migration, tmpBucketName, GcsBucketName(workspace.bucketName))
    } yield result


  final def noGoogleProjectError[A](migration: WorkspaceMigration): Throwable =
    new IllegalStateException(s"""Google Project "${migration.newGoogleProjectId}" was not found for migration "${migration.id}."""")


  final def noWorkspaceBucketError[A](name: GcsBucketName): Throwable =
    new IllegalStateException(s"""Failed to retrieve workspace bucket "${name}".""")


  final def noTmpBucketError[A](migration: WorkspaceMigration): Throwable =
    new IllegalStateException(s"""Temporary storage bucket "${migration.tmpBucketName}" was not found for migration "${migration.id}."""")


  final def startBucketStorageTransferJob(migration: WorkspaceMigration,
                                          originBucket: GcsBucketName,
                                          destBucket: GcsBucketName)
  : ReaderT[IO, MigrationDeps, TransferJob] =
    for {
      transferJob <- ReaderT { environment: MigrationDeps =>
        for {
          jobName <- randomSuffix("transferJobs/terra-workspace-migration-")
          transferJob <- environment.storageTransferService.createTransferJob(
            jobName = GoogleStorageTransferService.JobName(jobName),
            jobDescription = s"""Bucket transfer job from "${originBucket}" to "${destBucket}".""",
            projectToBill = GoogleProject(environment.billingProject.googleProjectId.value),
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


  final def inTransaction[T](action: DataAccess => ReadWriteAction[T]): ReaderT[IO, MigrationDeps, T] =
    ReaderT(_.dataSource.inTransaction(action).io)


  final def randomSuffix(str: String): IO[String] = IO {
    str ++ UUID.randomUUID.toString.replace("-", "")
  }


  def getWorkspace(workspaceId: UUID): ReaderT[IO, MigrationDeps, Workspace] =
    inTransaction(_.workspaceQuery.findByIdOrFail(workspaceId.toString))


  def getMigrations(workspaceUuid: UUID): ReaderT[IO, MigrationDeps, Seq[WorkspaceMigration]] =
    inTransaction { _ =>
      workspaceMigrations
        .filter(_.workspaceId === workspaceUuid)
        .sortBy(_.id)
        .result
    }


  def step(m: WorkspaceMigration): ReaderT[IO, MigrationDeps, Unit] =
    getWorkspace(m.workspaceId).flatMap { workspace =>
      val ignore: ReaderT[IO, MigrationDeps, Unit] = ReaderT.pure()
      m.getStatus match {
        case Created(_) => for {
          time <- ReaderT.liftF(timestampNow)
          _ <- inTransaction(_ => workspaceMigrations.update(m.copy(started = time.some)))
        } yield ()

        case Started(_) =>
          claimAndConfigureNewGoogleProject(m, workspace) *> ignore

        case GoogleProjectConfigured(_, googleProjectId) =>
          createTempBucket(m, workspace, googleProjectId) *> ignore

        case TmpBucketCreated(_, tmpBucket) =>
          startBucketStorageTransferJob(m, GcsBucketName(workspace.bucketName), tmpBucket) *> ignore

        // todo yield while sts job is in progress

        case WorkspaceBucketTransferred(_) =>
          deleteWorkspaceBucket(m, workspace)

        case WorkspaceBucketDeleted(_) =>
          createFinalBucket(m, workspace) *> ignore

        case FinalWorkspaceBucketCreated(_) =>
          startStorageTransferJobToFinalBucket(m, workspace) *> ignore

        // todo yield while sts job is in progress

        case TmpBucketTransferred(_) =>
          startStorageTransferJobToFinalBucket(m, workspace) *> ignore

        case _ => ignore
      }
    }
}


object WorkspaceMigrationActor {
  final case class Schedule(workspace: Workspace)

  def apply(dataSource: SlickDataSource): Behavior[Schedule] = Behaviors.receive { (_, message) =>
    dataSource.inTransaction(_ => WorkspaceMigrationMonitor.schedule(message.workspace))
    Behaviors.same
  }
}
