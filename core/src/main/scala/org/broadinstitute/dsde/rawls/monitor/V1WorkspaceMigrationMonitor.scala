package org.broadinstitute.dsde.rawls.monitor

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import cats.implicits.{catsSyntaxOptionId, toTraverseOps}
import org.apache.commons.lang3.SerializationException
import org.broadinstitute.dsde.rawls.dataaccess.SlickDataSource
import org.broadinstitute.dsde.rawls.dataaccess.slick.{ReadAction, WriteAction}
import org.broadinstitute.dsde.rawls.model.Workspace
import org.broadinstitute.dsde.rawls.monitor.MigrationOutcome.{Failure, Success}

import java.time.LocalDateTime
import java.util.UUID
import scala.concurrent.ExecutionContext.Implicits.global


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

}

object V1WorkspaceMigrationActor {
  final case class Schedule(workspace: Workspace)

  def apply(dataSource: SlickDataSource): Behavior[Schedule] = Behaviors.receive { (_, message) =>
    dataSource.inTransaction(_ => V1WorkspaceMigrationMonitor.schedule(message.workspace))
    Behaviors.same
  }

}
