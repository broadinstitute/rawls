package org.broadinstitute.dsde.rawls.monitor

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import cats.data.NonEmptyList
import cats.effect.IO
import cats.implicits.{catsSyntaxOptionId, toTraverseOps}
import com.google.api.services.storage.model.Bucket
import com.google.cloud.storage.Storage.BucketGetOption
import org.apache.commons.lang3.SerializationException
import org.broadinstitute.dsde.rawls.dataaccess.{GoogleServicesDAO, SlickDataSource}
import org.broadinstitute.dsde.rawls.dataaccess.slick.{ReadAction, WriteAction}
import org.broadinstitute.dsde.rawls.model.{GoogleProjectId, Workspace}
import org.broadinstitute.dsde.rawls.monitor.MigrationOutcome.{Failure, Success}
import org.broadinstitute.dsde.workbench.google2.GoogleStorageService
import org.broadinstitute.dsde.workbench.model.google.{GcsBucketName, GoogleProject}

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
                                             created: LocalDateTime,
                                             started: Option[LocalDateTime],
                                             finished: Option[LocalDateTime],
                                             outcome: Option[MigrationOutcome])

object V1WorkspaceMigrationAttempt {
  type RecordType = (Long, UUID, LocalDateTime, Option[LocalDateTime], Option[LocalDateTime], Option[String], Option[String])

  def fromRecord(record: RecordType): Either[String, V1WorkspaceMigrationAttempt] = {
    type EitherStringT[T] = Either[String, T]
    record match {
      case (id, workspaceId, created, started, finished, outcome, message) =>
        outcome
          .traverse[EitherStringT, MigrationOutcome] {
            case "Success" => Right(Success)
            case "Failure" => Right(Failure(message.getOrElse("")))
            case other => Left(s"""Failed to read V1WorkspaceMigrationAttempt from record.
                                   | Unknown migration outcome -- "$other"""".stripMargin)
          }
          .map {
            V1WorkspaceMigrationAttempt(id, workspaceId, created, started, finished, _)
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
    (attempt.id, attempt.workspaceId, attempt.created, attempt.started, attempt.finished, outcome, message)
  }
}

trait V1WorkspaceMigrationComponent {

  import slick.jdbc.MySQLProfile.api._

  private val v1WorkspaceMigrationHistory: String = "V1_WORKSPACE_MIGRATION_HISTORY"

  final class V1WorkspaceMigrationHistory(tag: Tag)
    extends Table[V1WorkspaceMigrationAttempt](tag, v1WorkspaceMigrationHistory) {

    def id = column[Long]("id", O.PrimaryKey, O.AutoInc)
    def workspaceId = column[UUID]("WORKSPACE_ID", O.SqlType("BINARY(16)"))
    def created = column[LocalDateTime]("CREATED")
    def started = column[Option[LocalDateTime]]("STARTED")
    def finished = column[Option[LocalDateTime]]("FINISHED")
    def outcome = column[Option[String]]("OUTCOME")
    def message = column[Option[String]]("MESSAGE")

    override def * =
      (id, workspaceId, created, started, finished, outcome, message) <>
        (V1WorkspaceMigrationAttempt.unsafeFromRecord, V1WorkspaceMigrationAttempt.toRecord(_: V1WorkspaceMigrationAttempt).some)
  }

  val migrations = TableQuery[V1WorkspaceMigrationHistory]
}

object V1WorkspaceMigrationMonitor
  extends V1WorkspaceMigrationComponent {

  import slick.jdbc.MySQLProfile.api._

  final def isMigrating(workspace: Workspace): ReadAction[Boolean] =
    migrations
      .filter { m => m.workspaceId === workspace.workspaceIdAsUUID && m.started.isDefined && m.finished.isEmpty }
      .length
      .result
      .map(_ > 0)

  final def schedule(workspace: Workspace): WriteAction[Unit] =
    DBIO.seq(migrations.map(_.workspaceId) += workspace.workspaceIdAsUUID)

  final def createBucketInSameRegion(destGoogleProject: GoogleProject, sourceGoogleProject: GoogleProject, sourceBucketName: GcsBucketName, bucketName: GcsBucketName, googleStorageService: GoogleStorageService[IO]) = {
    for {
      sourceBucketOpt <- googleStorageService.getBucket(sourceGoogleProject, sourceBucketName, List(BucketGetOption.userProject(destGoogleProject.value))) // todo: figure out who pays for this
      Some(sourceBucket) = sourceBucketOpt
      // todo: figure out what the labels on bucket are, and whether we need to keep them when we create new buckets
      newBucket <- googleStorageService.insertBucket(destGoogleProject, bucketName, sourceBucket.getAcl().toList match {
        case Nil => None
        case x:: xs => NonEmptyList(x, xs).some
      }).compile.drain
    } yield()
  }

}

object V1WorkspaceMigrationActor {
  final case class Schedule(workspace: Workspace)

  def apply(dataSource: SlickDataSource, gcsDAO: GoogleServicesDAO): Behavior[Schedule] = Behaviors.receive { (_, message) =>
    dataSource.inTransaction(_ => V1WorkspaceMigrationMonitor.schedule(message.workspace))
    Behaviors.same
  }

}
