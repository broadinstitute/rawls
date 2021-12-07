package org.broadinstitute.dsde.rawls.monitor

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import cats.effect.IO
import cats.implicits.{catsSyntaxOptionId, toTraverseOps}
import com.google.cloud.storage.Storage.BucketGetOption
import org.apache.commons.lang3.SerializationException
import org.broadinstitute.dsde.rawls.dataaccess.slick.{ReadAction, ReadWriteAction, WriteAction}
import org.broadinstitute.dsde.rawls.dataaccess.{GoogleServicesDAO, SlickDataSource}
import org.broadinstitute.dsde.rawls.model.{GoogleProjectId, Workspace}
import org.broadinstitute.dsde.rawls.monitor.MigrationOutcome.{Failure, Success}
import org.broadinstitute.dsde.workbench.google2.GoogleStorageService
import org.broadinstitute.dsde.workbench.model.google.{GcsBucketName, GoogleProject}

import java.sql.Timestamp
import java.time.LocalDateTime
import java.util.UUID
import scala.collection.JavaConversions._
import scala.concurrent.ExecutionContext.Implicits.global


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
                                             tmpBucketName: Option[GcsBucketName],
                                             tmpBucketCreated: Option[Timestamp]
                                            )

object V1WorkspaceMigrationAttempt {
  type RecordType = (Long, UUID, Timestamp, Option[Timestamp], Option[Timestamp], Option[String], Option[String], Option[String], Option[Timestamp])

  def fromRecord(record: RecordType): Either[String, V1WorkspaceMigrationAttempt] = {
    type EitherStringT[T] = Either[String, T]
    record match {
      case (id, workspaceId, created, started, finished, outcome, message, tmpBucketName, tmpBucketCreated) =>
        outcome
          .traverse[EitherStringT, MigrationOutcome] {
            case "Success" => Right(Success)
            case "Failure" => Right(Failure(message.getOrElse("")))
            case other => Left(s"""Failed to read V1WorkspaceMigrationAttempt from record.
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
              tmpBucketName.map(GcsBucketName),
              tmpBucketCreated
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
      attempt.tmpBucketName.map(_.value),
      attempt.tmpBucketCreated
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
    def tmpBucket = column[Option[String]]("TMP_BUCKET")
    def tmpBucketCreated = column[Option[Timestamp]]("TMP_BUCKET_CREATED")

    override def * =
      (id, workspaceId, created, started, finished, outcome, message, tmpBucket, tmpBucketCreated) <>
        (V1WorkspaceMigrationAttempt.unsafeFromRecord, V1WorkspaceMigrationAttempt.toRecord(_: V1WorkspaceMigrationAttempt).some)
  }

  val migrations = TableQuery[V1WorkspaceMigrationHistory]
}

object V1WorkspaceMigrationMonitor
  extends V1WorkspaceMigrationComponent {

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

  final def createBucketInSameRegion(sourceBucketName: GcsBucketName,
                                     destGoogleProject: GoogleProject,
                                     destBucketName: GcsBucketName,
                                     googleStorageService: GoogleStorageService[IO]
                                    ): IO[Unit] = {
    for {
      // todo: figure out who pays for this
      sourceBucketOpt <- googleStorageService.getBucket(
        GoogleProject(null),
        sourceBucketName,
        List(BucketGetOption.userProject(destGoogleProject.value))
      )

      sourceBucket = sourceBucketOpt.getOrElse(
        throw new IllegalStateException(s"Source bucket ${sourceBucketName} could not be found")
      )

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
  }

  final def createTempBucket(migrationAttempt: V1WorkspaceMigrationAttempt,
                             workspace: Workspace,
                             destGoogleProject: GoogleProject,
                             googleStorageService: GoogleStorageService[IO]
                            ): IO[(GcsBucketName, ReadWriteAction[Unit])] = {
    val tmpBucketName = GcsBucketName(
      "terra-workspace-migration-" + UUID.randomUUID.toString.replace("-", "")
    )

    for {
      _ <- createBucketInSameRegion(
        GcsBucketName(workspace.bucketName),
        destGoogleProject,
        tmpBucketName,
        googleStorageService
      )
    } yield (tmpBucketName, DBIO.seq(
      migrations
        .filter(_.id === migrationAttempt.id)
        .map(r => (r.tmpBucket, r.tmpBucketCreated))
        .update((tmpBucketName.value.some, Timestamp.valueOf(LocalDateTime.now).some))
    ))
  }
}

object V1WorkspaceMigrationActor {
  final case class Schedule(workspace: Workspace)

  def apply(dataSource: SlickDataSource): Behavior[Schedule] = Behaviors.receive { (_, message) =>
    dataSource.inTransaction(_ => V1WorkspaceMigrationMonitor.schedule(message.workspace))
    Behaviors.same
  }

}
