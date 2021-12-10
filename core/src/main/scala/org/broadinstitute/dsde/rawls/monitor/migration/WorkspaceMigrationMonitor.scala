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
    storageTransferJobs.delete >> workspaceMigrations.delete >> DBIOAction.successful(())

  final def isInQueueToMigrate(workspace: Workspace): ReadAction[Boolean] =
    workspaceMigrations
      .filter { m => m.workspaceId === workspace.workspaceIdAsUUID && !m.started.isDefined }
      .length
      .result
      .map(_ > 0)


  final def isMigrating(workspace: Workspace): ReadAction[Boolean] =
    workspaceMigrations
      .filter { m => m.workspaceId === workspace.workspaceIdAsUUID && m.started.isDefined && m.finished.isEmpty }
      .length
      .result
      .map(_ > 0)


  final def schedule(workspace: Workspace): WriteAction[Unit] =
    DBIO.seq(workspaceMigrations.map(_.workspaceId) += workspace.workspaceIdAsUUID)


  final def claimAndConfigureNewGoogleProject(migrationAttempt: WorkspaceMigration,
                                              workspaceService: WorkspaceService,
                                              workspace: Workspace,
                                              billingProject: RawlsBillingProject
                                             ): IO[(GoogleProjectId, GoogleProjectNumber, WriteAction[Unit])] =
    IO.fromFuture {
      IO {
        for {
          (googleProjectId, googleProjectNumber) <- workspaceService.setupGoogleProject(
            billingProject,
            workspace.currentBillingAccountOnGoogleProject.getOrElse(
              throw new RawlsException(s"""No billing account for workspace '${workspace.workspaceId}'""")
            ),
            workspace.workspaceId,
            workspace.toWorkspaceName
          )
        } yield (googleProjectId, googleProjectNumber, DBIO.seq(
          workspaceMigrations.filter(_.id === migrationAttempt.id)
            .map(r => (r.newGoogleProjectId, r.newGoogleProjectNumber, r.newGoogleProjectConfigured))
            .update((googleProjectId.value.some, googleProjectNumber.value.some, Timestamp.valueOf(LocalDateTime.now).some))
        ))
      }
    }


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

    } yield (tmpBucketName, DBIO.seq(
      workspaceMigrations
        .filter(_.id === attempt.id)
        .map(r => (r.tmpBucket, r.tmpBucketCreated))
        .update((tmpBucketName.value.some, Timestamp.valueOf(LocalDateTime.now).some))
    ))
  }


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

    } yield (GcsBucketName(workspace.bucketName), DBIO.seq(
      workspaceMigrations
        .filter(_.id === attempt.id)
        .map(_.finalBucketCreated)
        .update(Timestamp.valueOf(LocalDateTime.now).some)
    ))


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
    } yield (transferJob, DBIO.seq(
      storageTransferJobs.map(job => (job.jobName, job.migrationId, job.destBucket, job.originBucket)) +=
        (transferJob.getName, migration.id, destBucket.value, originBucket.value)
      )
    )
}


object WorkspaceMigrationActor {
  final case class Schedule(workspace: Workspace)

  def apply(dataSource: SlickDataSource): Behavior[Schedule] = Behaviors.receive { (_, message) =>
    dataSource.inTransaction(_ => WorkspaceMigrationMonitor.schedule(message.workspace))
    Behaviors.same
  }
}
