package org.broadinstitute.dsde.rawls.monitor

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import cats.effect.IO
import cats.implicits.{catsSyntaxOptionId, toTraverseOps}
import com.google.cloud.storage.Storage.BucketGetOption
import org.apache.commons.lang3.SerializationException
import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.dataaccess.slick.{ReadAction, WriteAction}
import org.broadinstitute.dsde.rawls.dataaccess.{GoogleServicesDAO, SlickDataSource}
import org.broadinstitute.dsde.rawls.model.{GoogleProjectId, GoogleProjectNumber, RawlsBillingProject, Workspace}
import org.broadinstitute.dsde.rawls.monitor.MigrationOutcome.{Failure, Success}
import org.broadinstitute.dsde.rawls.workspace.WorkspaceService
import org.broadinstitute.dsde.workbench.google2.GoogleStorageService
import org.broadinstitute.dsde.rawls.monitor.StorageTransferServiceOutcome.{FailureStorageTransfer, SuccessStorageTransfer}
import org.broadinstitute.dsde.workbench.google2.GoogleStorageTransferService.JobTransferSchedule
import org.broadinstitute.dsde.workbench.google2.{GoogleStorageService, GoogleStorageTransferService}
import org.broadinstitute.dsde.workbench.model.google.{GcsBucketName, GoogleProject}

import java.sql.Timestamp
import java.time.LocalDateTime
import java.util.UUID
import scala.collection.JavaConversions._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future


sealed trait MigrationOutcome

object MigrationOutcome {
  case object Success extends MigrationOutcome

  final case class Failure(message: String) extends MigrationOutcome
}

final case class V1WorkspaceMigrationAttempt(id: Long,
                                             workspaceId: UUID,
                                             created: Timestamp,
                                             started: Option[Timestamp],
                                             finished: Option[Timestamp],
                                             outcome: Option[MigrationOutcome],
                                             newGoogleProjectId: Option[GoogleProjectId],
                                             newGoogleProjectNumber: Option[GoogleProjectNumber],
                                             newGoogleProjectConfigured: Option[Timestamp],
                                             tmpBucketName: Option[GcsBucketName],
                                             tmpBucketCreated: Option[Timestamp],
                                             finalBucketCreated: Option[Timestamp]
                                            )

object V1WorkspaceMigrationAttempt {
  type RecordType = (
      Long, UUID, Timestamp, Option[Timestamp], Option[Timestamp], Option[String], Option[String],
        Option[String], Option[String], Option[Timestamp],
        Option[String], Option[Timestamp],
        Option[Timestamp]
      )

  def fromRecord(record: RecordType): Either[String, V1WorkspaceMigrationAttempt] = {
    type EitherStringT[T] = Either[String, T]
    record match {
      case (id, workspaceId, created, started, finished, outcome, message,
      newGoogleProjectId, newGoogleProjectNumber, newGoogleProjectConfigured,
      tmpBucketName, tmpBucketCreated,
      finalBucketCreated) =>
        outcome
          .traverse[EitherStringT, MigrationOutcome] {
            case "Success" => Right(Success)
            case "Failure" => Right(Failure(message.getOrElse("")))
            case other => Left(
              s"""Failed to read V1WorkspaceMigrationAttempt from record.
                 | Unknown migration outcome -- "$other"""".stripMargin)
          }
          .map { outcome =>
            V1WorkspaceMigrationAttempt(
              id,
              workspaceId,
              created,
              started,
              finished,
              outcome,
              newGoogleProjectId.map(GoogleProjectId),
              newGoogleProjectNumber.map(GoogleProjectNumber),
              newGoogleProjectConfigured,
              tmpBucketName.map(GcsBucketName),
              tmpBucketCreated,
              finalBucketCreated
            )
          }
    }
  }

  def unsafeFromRecord(record: RecordType): V1WorkspaceMigrationAttempt =
    fromRecord(record) match {
      case Right(r) => r
      case Left(msg) => throw new SerializationException(msg)
    }

  def toRecord(attempt: V1WorkspaceMigrationAttempt): RecordType = {
    val (outcome: Option[String], message: Option[String]) = attempt.outcome match {
      case Some(Success) => ("Success".some, None)
      case Some(Failure(message)) => ("Failure".some, message.some)
      case None => (None, None)
    }
    (
      attempt.id,
      attempt.workspaceId,
      attempt.created,
      attempt.started,
      attempt.finished,
      outcome,
      message,
      attempt.newGoogleProjectId.map(_.value),
      attempt.newGoogleProjectNumber.map(_.value),
      attempt.newGoogleProjectConfigured,
      attempt.tmpBucketName.map(_.value),
      attempt.tmpBucketCreated,
      attempt.finalBucketCreated
    )
  }
}

trait V1WorkspaceMigrationComponent {

  import slick.jdbc.MySQLProfile.api._

  private val v1WorkspaceMigrationHistory: String = "V1_WORKSPACE_MIGRATION_HISTORY"

  final class V1WorkspaceMigrationHistory(tag: Tag)
    extends Table[V1WorkspaceMigrationAttempt](tag, v1WorkspaceMigrationHistory) {

    def id = column[Long]("id", O.PrimaryKey, O.AutoInc)
    def workspaceId = column[UUID]("WORKSPACE_ID", O.SqlType("BINARY(16)"))
    def created = column[Timestamp]("CREATED")
    def started = column[Option[Timestamp]]("STARTED")
    def finished = column[Option[Timestamp]]("FINISHED")
    def outcome = column[Option[String]]("OUTCOME")
    def message = column[Option[String]]("MESSAGE")
    def newGoogleProjectId = column[Option[String]]("NEW_GOOGLE_PROJECT_ID")
    def newGoogleProjectNumber = column[Option[String]]("NEW_GOOGLE_PROJECT_NUMBER")
    def newGoogleProjectConfigured = column[Option[Timestamp]]("NEW_GOOGLE_PROJECT_CONFIGURED")
    def tmpBucket = column[Option[String]]("TMP_BUCKET")
    def tmpBucketCreated = column[Option[Timestamp]]("TMP_BUCKET_CREATED")
    def finalBucketCreated = column[Option[Timestamp]]("FINAL_BUCKET_CREATED")

    override def * =
      (id, workspaceId, created, started, finished, outcome, message,
        newGoogleProjectId, newGoogleProjectNumber, newGoogleProjectConfigured,
        tmpBucket, tmpBucketCreated,
        finalBucketCreated
      ) <> (V1WorkspaceMigrationAttempt.unsafeFromRecord, V1WorkspaceMigrationAttempt.toRecord(_: V1WorkspaceMigrationAttempt).some)
  }

  val migrations = TableQuery[V1WorkspaceMigrationHistory]
}

sealed trait StorageTransferServiceOutcome

object StorageTransferServiceOutcome {
  case object SuccessStorageTransfer extends StorageTransferServiceOutcome

  final case class FailureStorageTransfer(message: String) extends StorageTransferServiceOutcome
}

final case class PpwStorageTransferJobAttempt(id: Long,
                                              jobName: GoogleStorageTransferService.JobName,
                                              migrationId: Long,
                                              created: Timestamp,
                                              destBucket: GcsBucketName,
                                              originBucket: GcsBucketName,
                                              finished: Option[Timestamp],
                                              outcome: Option[StorageTransferServiceOutcome],
                                            )

object PpwStorageTransferJobAttempt {
  type RecordType = (Long, String, Long, Timestamp, String, String, Option[Timestamp], Option[String], Option[String])

  def fromRecord(record: RecordType): Either[String, PpwStorageTransferJobAttempt] = {
    type EitherStringT[T] = Either[String, T]
    record match {
      case (id, jobName, migrationId, created, destBucket, originBucket, finished, outcome, message) =>
        outcome
          .traverse[EitherStringT, StorageTransferServiceOutcome] {
            case "Success" => Right(SuccessStorageTransfer)
            case "Failure" => Right(FailureStorageTransfer(message.getOrElse("")))
            case other => Left(s"""Failed to read PpwStorageTransferJobRecord from record.
                                  | Unknown outcome -- "$other"""".stripMargin)
          }
          .map { outcome =>
            PpwStorageTransferJobAttempt(
              id,
              GoogleStorageTransferService.JobName(jobName),
              migrationId,
              created,
              GcsBucketName(destBucket),
              GcsBucketName(originBucket),
              finished,
              outcome
            )
          }
    }
  }

  def unsafeFromRecord(record: RecordType): PpwStorageTransferJobAttempt =
    fromRecord(record) match {
      case Right(r) => r
      case Left(msg) => throw new SerializationException(msg)
    }

  def toRecord(attempt: PpwStorageTransferJobAttempt): RecordType = {
    val (outcome: Option[String], message: Option[String]) = attempt.outcome match {
      case Some(SuccessStorageTransfer) => ("Success".some, None)
      case Some(FailureStorageTransfer(message)) => ("Failure".some, message.some)
      case None => (None, None)
    }
    (
      attempt.id,
      attempt.jobName.value,
      attempt.migrationId,
      attempt.created,
      attempt.destBucket.value,
      attempt.originBucket.value,
      attempt.finished,
      outcome,
      message
    )
  }
}

trait PpwStorageTransferServiceComponent {
  import slick.jdbc.MySQLProfile.api._

  private val ppwStorageTransferServiceJob: String = "PPW_STORAGE_TRANSFER_SERVICE_JOB"

  final class PpwStorageTransferServiceJob(tag: Tag)
    extends Table[PpwStorageTransferJobAttempt](tag, ppwStorageTransferServiceJob) {

    def id = column[Long]("id", O.PrimaryKey, O.AutoInc)
    def jobName = column[String]("JOB_NAME")
    def migrationId = column[Long]("MIGRATION_ID")
    def created = column[Timestamp]("CREATED")
    def destBucket = column[String]("DEST_BUCKET")
    def originBucket = column[String]("ORIGIN_BUCKET")
    def finished = column[Option[Timestamp]]("FINISHED")
    def outcome = column[Option[String]]("OUTCOME")
    def message = column[Option[String]]("MESSAGE")

    override def * =
      (id, jobName, migrationId, created, destBucket, originBucket, finished, outcome, message) <>
        (PpwStorageTransferJobAttempt.unsafeFromRecord, PpwStorageTransferJobAttempt.toRecord(_: PpwStorageTransferJobAttempt).some)
  }

  val storageTransferJobs = TableQuery[PpwStorageTransferServiceJob]
}

object V1WorkspaceMigrationMonitor
  extends V1WorkspaceMigrationComponent with PpwStorageTransferServiceComponent {

  import slick.jdbc.MySQLProfile.api._


  final def isInQueueToMigrate(workspace: Workspace): ReadAction[Boolean] =
    migrations
      .filter { m => m.workspaceId === workspace.workspaceIdAsUUID && !m.started.isDefined }
      .length
      .result
      .map(_ > 0)


  final def isMigrating(workspace: Workspace): ReadAction[Boolean] =
    migrations
      .filter { m => m.workspaceId === workspace.workspaceIdAsUUID && m.started.isDefined && m.finished.isEmpty }
      .length
      .result
      .map(_ > 0)


  final def schedule(workspace: Workspace): WriteAction[Unit] =
    DBIO.seq(migrations.map(_.workspaceId) += workspace.workspaceIdAsUUID)


  final def claimAndConfigureNewGoogleProject(migrationAttempt: V1WorkspaceMigrationAttempt,
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
          migrations.filter(_.id === migrationAttempt.id)
            .map(r => (r.newGoogleProjectId, r.newGoogleProjectNumber, r.newGoogleProjectConfigured))
            .update((googleProjectId.value.some, googleProjectNumber.value.some, Timestamp.valueOf(LocalDateTime.now).some))
        ))
      }
    }


  final def createTempBucket(attempt: V1WorkspaceMigrationAttempt,
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
      migrations
        .filter(_.id === attempt.id)
        .map(r => (r.tmpBucket, r.tmpBucketCreated))
        .update((tmpBucketName.value.some, Timestamp.valueOf(LocalDateTime.now).some))
    ))
  }


  final def createFinalBucket(attempt: V1WorkspaceMigrationAttempt,
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
      migrations
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


  final def failNoGoogleProject[A](attempt: V1WorkspaceMigrationAttempt): IO[A] =
    IO.raiseError(new IllegalStateException(s"""Google Project was not created for migration "${attempt.id}.""""))


  final def failNoTmpBucket[A](attempt: V1WorkspaceMigrationAttempt): IO[A] =
    IO.raiseError(new IllegalStateException(s"""Temporary storage bucket was not created for migration "${attempt.id}.""""))


  final def transferDataFromOriginBucketToDestBucket(originBucket: GcsBucketName, destBucket: GcsBucketName, workspace: Workspace, projectToBill: GoogleProject, dataSource: SlickDataSource, migrationAttempt: V1WorkspaceMigrationAttempt, googleStorageTransferService: GoogleStorageTransferService[Future]) = {

    val storageTransferServiceJobName = GoogleStorageTransferService.JobName(
      "terra-workspace-migration-" + UUID.randomUUID.toString.replace("-", "")
    )
    for {
      transferJob <- googleStorageTransferService.createTransferJob(
        jobName = storageTransferServiceJobName,
        jobDescription = s"""A transfer of data from ${originBucket} to ${destBucket}""",
        projectToBill = projectToBill,
        originBucket,
        destBucket,
        JobTransferSchedule.Immediately
      )
    } yield(transferJob, DBIO.seq(
      storageTransferJobs.map(
        job => (job.jobName, job.migrationId, job.destBucket, job.originBucket)) += (transferJob.getName, migrationAttempt.id, destBucket.value, originBucket.value)
      )
    )
  }
}


object V1WorkspaceMigrationActor {
  final case class Schedule(workspace: Workspace)

  def apply(dataSource: SlickDataSource): Behavior[Schedule] = Behaviors.receive { (_, message) =>
    dataSource.inTransaction(_ => V1WorkspaceMigrationMonitor.schedule(message.workspace))
    Behaviors.same
  }
}
