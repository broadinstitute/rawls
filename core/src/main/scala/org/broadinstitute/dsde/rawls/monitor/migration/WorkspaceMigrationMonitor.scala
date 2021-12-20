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


  final def claimAndConfigureNewGoogleProject(migrationAttempt: WorkspaceMigration,
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
      .filter(_.id === migrationAttempt.id)
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
      googleProject <- attempt.newGoogleProjectId match {
        case Some(projectId) => IO.pure(GoogleProject(projectId.value))
        case None            => failNoGoogleProject(attempt)
      }

      _ <- createBucketInSameRegion(
        GcsBucketName(workspace.bucketName),
        googleProject,
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

      _ <- IO.raiseUnless(msuccess.contains(true))(new IllegalStateException(
        s"""Failed to delete workspace bucket: "${workspace.bucketName}" was not found."""
      ))

    } yield workspaceMigrations
      .filter(_.id === migration.id)
      .map(_.workspaceBucketDeleted)
      .update(deleted.some)
      .ignore


  final def deleteTemporaryBucket(migration: WorkspaceMigration,
                                  googleStorageService: GoogleStorageService[IO]
                                 ): IO[WriteAction[Unit]] =
    for {
      _ <- IO.whenA(migration.newGoogleProjectId.isEmpty)(failNoGoogleProject(migration))
      _ <- IO.whenA(migration.tmpBucketName.isEmpty)(failNoTmpBucket(migration))

      tmpBucketName = migration.tmpBucketName.get
      msuccess <- googleStorageService.deleteBucket(
        GoogleProject(migration.newGoogleProjectId.get.value),
        tmpBucketName,
        isRecursive = true
      ).compile.last

      deleted <- timestampNow

      _ <- IO.raiseUnless(msuccess.contains(true))(new IllegalStateException(
        s"""Failed to delete temporary bucket: "${tmpBucketName}" was not found."""
      ))

    } yield workspaceMigrations
      .filter(_.id === migration.id)
      .map(_.tmpBucketDeleted)
      .update(deleted.some)
      .ignore


  final def createFinalBucket(attempt: WorkspaceMigration,
                              workspace: Workspace,
                              googleStorageService: GoogleStorageService[IO]
                            ): IO[(GcsBucketName, WriteAction[Unit])] =
    for {
      googleProject <- attempt.newGoogleProjectId match {
        case Some(projectId) => IO.pure(GoogleProject(projectId.value))
        case None            => failNoGoogleProject(attempt)
      }

      tmpBucket <- attempt.tmpBucketName match {
        case Some(bucketName) => IO.pure(bucketName)
        case None             => failNoTmpBucket(attempt)
      }

      _ <- createBucketInSameRegion(
        tmpBucket,
        googleProject,
        GcsBucketName(workspace.bucketName),
        googleStorageService
      )

      created <- timestampNow
    } yield (GcsBucketName(workspace.bucketName), workspaceMigrations
      .filter(_.id === attempt.id)
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

      sourceBucket <- sourceBucketOpt match {
        case Some(bucket) => IO.pure(bucket)
        case None         => failNoWorkspaceBucket(sourceBucketName)
      }

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


  final def failNoWorkspaceBucket[A](name: GcsBucketName): IO[A] =
    IO.raiseError(new IllegalStateException(s"""Failed to retrieve workspace bucket "${name}"."""))


  final def failNoGoogleProject[A](attempt: WorkspaceMigration): IO[A] =
    IO.raiseError(new IllegalStateException(s"""Google Project was not created for migration "${attempt.id}.""""))


  final def failNoTmpBucket[A](attempt: WorkspaceMigration): IO[A] =
    IO.raiseError(new IllegalStateException(s"""Temporary storage bucket was not created for migration "${attempt.id}.""""))


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
        jobDescription = s"""Bucket transfer job from ${originBucket} to ${destBucket}""",
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
