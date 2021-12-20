package org.broadinstitute.dsde.rawls.monitor.migration

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import cats.effect.IO
import cats.implicits.catsSyntaxOptionId
import com.google.cloud.storage.Storage.BucketGetOption
import com.google.storagetransfer.v1.proto.TransferTypes.TransferJob
import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.dataaccess.slick.{ReadAction, WriteAction}
import org.broadinstitute.dsde.rawls.dataaccess.{GoogleServicesDAO, SlickDataSource}
import org.broadinstitute.dsde.rawls.model.{GoogleProjectId, GoogleProjectNumber, RawlsBillingProject, Workspace}
import org.broadinstitute.dsde.rawls.monitor.migration.MigrationUtils.{IgnoreResultExtensionMethod, InsertExtensionMethod}
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


  final def timestampNow: IO[Timestamp] = IO.delay(Timestamp.valueOf(LocalDateTime.now))


  final def claimAndConfigureNewGoogleProject(migration: WorkspaceMigration,
                                              workspaceService: WorkspaceService,
                                              workspace: Workspace,
                                              billingProject: RawlsBillingProject
                                             ): IO[(GoogleProjectId, GoogleProjectNumber, WriteAction[Unit])] =
    for {
      res <- IO.fromFuture(IO {
        workspaceService.setupGoogleProject(
          billingProject,
          workspace.currentBillingAccountOnGoogleProject.getOrElse(
            throw new RawlsException(s"""No billing account for workspace '${workspace.workspaceId}'""")
          ),
          workspace.workspaceId,
          workspace.toWorkspaceName
        )
      })

      (googleProjectId, googleProjectNumber) = res
      configured <- timestampNow

    } yield (googleProjectId, googleProjectNumber, workspaceMigrations
      .filter(_.id === migration.id)
      .map(r => (r.newGoogleProjectId, r.newGoogleProjectNumber, r.newGoogleProjectConfigured))
      .update((googleProjectId.value.some, googleProjectNumber.value.some, configured.some))
      .ignore
    )

  final def createTempBucket(attempt: WorkspaceMigration,
                             workspace: Workspace,
                             googleStorageService: GoogleStorageService[IO]
                            ): IO[(GcsBucketName, WriteAction[Unit])] = {
    val tmpBucketName = GcsBucketName(
      "terra-workspace-migration-" + UUID.randomUUID.toString.replace("-", "")
    )

    for {
      googleProjectId <- IO(attempt.newGoogleProjectId.getOrElse(throw noGoogleProjectError(attempt)))

      _ <- createBucketInSameRegion(
        GcsBucketName(workspace.bucketName),
        GoogleProject(googleProjectId.value),
        tmpBucketName,
        googleStorageService
      )

      created <- timestampNow

    } yield (tmpBucketName, workspaceMigrations
      .filter(_.id === attempt.id)
      .map(r => (r.tmpBucket, r.tmpBucketCreated))
      .update((tmpBucketName.value.some, created.some))
      .ignore
    )
  }

  final def deleteWorkspaceBucket(migration: WorkspaceMigration,
                                  workspace: Workspace,
                                  googleStorageService: GoogleStorageService[IO]
                                 ): IO[WriteAction[Unit]] =
    for {
      msuccess <- googleStorageService.deleteBucket(
        GoogleProject(workspace.googleProjectId.value),
        GcsBucketName(workspace.bucketName),
        isRecursive = true
      ).compile.last

      deleted <- timestampNow

      _<- IO.raiseUnless(msuccess.contains(true)) {
        noWorkspaceBucketError(GcsBucketName(workspace.bucketName))
      }

    } yield workspaceMigrations
      .filter(_.id === migration.id)
      .map(_.workspaceBucketDeleted)
      .update(deleted.some)
      .ignore


  final def deleteTemporaryBucket(migration: WorkspaceMigration,
                                  googleStorageService: GoogleStorageService[IO]
                                 ): IO[WriteAction[Unit]] =
    for {
      googleProjectId <- IO(migration.newGoogleProjectId.getOrElse(throw noGoogleProjectError(migration)))
      tmpBucketName <- IO(migration.tmpBucketName.getOrElse(throw noTmpBucketError(migration)))

      msuccess <- googleStorageService.deleteBucket(
        GoogleProject(googleProjectId.value),
        tmpBucketName,
        isRecursive = true
      ).compile.last

      deleted <- timestampNow

      _ <- IO.raiseUnless(msuccess.contains(true))(noTmpBucketError(migration))

    } yield workspaceMigrations
      .filter(_.id === migration.id)
      .map(_.tmpBucketDeleted)
      .update(deleted.some)
      .ignore


  final def createFinalBucket(migration: WorkspaceMigration,
                              workspace: Workspace,
                              googleStorageService: GoogleStorageService[IO]
                            ): IO[(GcsBucketName, WriteAction[Unit])] =
    for {
      googleProjectId <- IO(migration.newGoogleProjectId.getOrElse(throw noGoogleProjectError(migration)))
      tmpBucket <- IO(migration.tmpBucketName.getOrElse(throw noTmpBucketError(migration)))

      _ <- createBucketInSameRegion(
        tmpBucket,
        GoogleProject(googleProjectId.value),
        GcsBucketName(workspace.bucketName),
        googleStorageService
      )

      created <- timestampNow
    } yield (GcsBucketName(workspace.bucketName), workspaceMigrations
      .filter(_.id === migration.id)
      .map(_.finalBucketCreated)
      .update(created.some)
      .ignore
    )


  final def createBucketInSameRegion(sourceBucketName: GcsBucketName,
                                     destGoogleProject: GoogleProject,
                                     destBucketName: GcsBucketName,
                                     googleStorageService: GoogleStorageService[IO]
                                    ): IO[Unit] =
    for {
      // todo: figure out who pays for this
      sourceBucketOpt <- googleStorageService.getBucket(
        GoogleProject(null),
        sourceBucketName,
        List(BucketGetOption.userProject(destGoogleProject.value))
      )

      sourceBucket <- IO(sourceBucketOpt.getOrElse(throw noWorkspaceBucketError(sourceBucketName)))

      // todo: CA-1637 do we need to transfer the storage logs for this workspace? the logs are prefixed
      // with the ws bucket name, so we COULD do it, but do we HAVE to? it's a csv with the bucket
      // and the storage_byte_hours in it that is kept for 180 days
      _ <- googleStorageService.insertBucket(
        googleProject = destGoogleProject,
        bucketName = destBucketName,
        labels = Option(sourceBucket.getLabels).map(_.toMap).getOrElse(Map.empty),
        bucketPolicyOnlyEnabled = true,
        logBucket = Option(GcsBucketName(GoogleServicesDAO.getStorageLogsBucketName(GoogleProjectId(destGoogleProject.value)))),
        location = Option(sourceBucket.getLocation)
      ).compile.drain

    } yield ()


  final def startStorageTransferJobToTmpBucket(migration: WorkspaceMigration,
                                               workspace: Workspace,
                                               projectToBill: GoogleProject,
                                               sts: GoogleStorageTransferService[IO]
                                              ): IO[(TransferJob, WriteAction[Unit])] =
    IO.raiseWhen(migration.tmpBucketName.isEmpty)(noTmpBucketError(migration)) *>
      startBucketStorageTransferJob(
        migration,
        GcsBucketName(workspace.bucketName),
        migration.tmpBucketName.get,
        projectToBill,
        sts
      )


  final def startStorageTransferJobToFinalBucket(migration: WorkspaceMigration,
                                                 workspace: Workspace,
                                                 projectToBill: GoogleProject,
                                                 sts: GoogleStorageTransferService[IO]
                                                ): IO[(TransferJob, WriteAction[Unit])] =
    IO.raiseWhen(migration.tmpBucketName.isEmpty)(noTmpBucketError(migration)) *>
      startBucketStorageTransferJob(
        migration,
        migration.tmpBucketName.get,
        GcsBucketName(workspace.bucketName),
        projectToBill,
        sts
      )


  final def noGoogleProjectError[A](migration: WorkspaceMigration): Throwable =
    new IllegalStateException(s"""Google Project "${migration.newGoogleProjectId}" was not found for migration "${migration.id}."""")


  final def noWorkspaceBucketError[A](name: GcsBucketName): Throwable =
    new IllegalStateException(s"""Failed to retrieve workspace bucket "${name}".""")


  final def noTmpBucketError[A](migration: WorkspaceMigration): Throwable =
    new IllegalStateException(s"""Temporary storage bucket "${migration.tmpBucketName}" was not found for migration "${migration.id}."""")


  final def startBucketStorageTransferJob(migration: WorkspaceMigration,
                                          originBucket: GcsBucketName,
                                          destBucket: GcsBucketName,
                                          projectToBill: GoogleProject,
                                          sts: GoogleStorageTransferService[IO]
                                          ): IO[(TransferJob, WriteAction[Unit])] =
    for {
      transferJob <- sts.createTransferJob(
        jobName = GoogleStorageTransferService.JobName(
          "transferJobs/terra-workspace-migration-" + UUID.randomUUID.toString.replace("-", "")
        ),
        jobDescription = s"""Bucket transfer job from "${originBucket}" to "${destBucket}".""",
        projectToBill = projectToBill,
        originBucket,
        destBucket,
        JobTransferSchedule.Immediately
      )
    } yield (transferJob, storageTransferJobs
      .map(job => (job.jobName, job.migrationId, job.destBucket, job.originBucket))
      .insert((transferJob.getName, migration.id, destBucket.value, originBucket.value))
      .ignore
    )
}


object WorkspaceMigrationActor {
  final case class Schedule(workspace: Workspace)

  def apply(dataSource: SlickDataSource): Behavior[Schedule] = Behaviors.receive { (_, message) =>
    dataSource.inTransaction(_ => WorkspaceMigrationMonitor.schedule(message.workspace))
    Behaviors.same
  }
}
