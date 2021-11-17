package org.broadinstitute.dsde.rawls.monitor

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import org.broadinstitute.dsde.rawls.dataaccess.SlickDataSource
import org.broadinstitute.dsde.rawls.dataaccess.slick.{DriverComponent, ReadAction, WriteAction}
import org.broadinstitute.dsde.rawls.model.Workspace

import java.time.LocalDateTime
import java.util.UUID
import scala.concurrent.ExecutionContext.Implicits.global


final case class V1WorkspaceMigrationAttempt(workspaceId: UUID,
                                             created: LocalDateTime,
                                             started: Option[LocalDateTime],
                                             finished: Option[LocalDateTime],
                                             outcome: Option[String],
                                             message: Option[String])

trait V1WorkspaceMigrationComponent {
  this: DriverComponent =>

  import driver.api._

  private val v1WorkspaceMigrationHistory: String = "V1_WORKSPACE_MIGRATION_HISTORY"

  final class V1WorkspaceMigrationHistory(tag: Tag)
    extends Table[V1WorkspaceMigrationAttempt](tag, v1WorkspaceMigrationHistory) {

    def workspaceId = column[UUID]("WORKSPACE_ID")
    def created = column[LocalDateTime]("CREATED")
    def started = column[Option[LocalDateTime]]("STARTED")
    def finished = column[Option[LocalDateTime]]("FINISHED")
    def outcome = column[Option[String]]("OUTCOME")
    def message = column[Option[String]]("MESSAGE")

    override def * =
      (workspaceId, created, started, finished, outcome, message) <>
        (V1WorkspaceMigrationAttempt.tupled, V1WorkspaceMigrationAttempt.unapply)
  }
}

object V1WorkspaceMigrationMonitor {

  import slick.jdbc.H2Profile.api._

  private val v1WorkspaceMigrationHistory: String = "V1_WORKSPACE_MIGRATION_HISTORY"

  final def isMigrating(workspace: Workspace): ReadAction[Boolean] =
    sql"""SELECT COUNT(*) FROM #${v1WorkspaceMigrationHistory} h
          WHERE h.WORKSPACE_ID = ${workspace.workspaceId}
            AND h.STARTED IS NOT NULL
            AND h.FINISHED IS NULL""".as[Int]
      .headOption
      .map(_.getOrElse(0) > 0)

  final def schedule(workspace: Workspace): WriteAction[Unit] =
    sqlu"""INSERT INTO #${v1WorkspaceMigrationHistory} (WORKSPACE_ID)
           VALUES (${workspace.workspaceId})""".map(_ => ())
}

object V1WorkspaceMigrationActor {
  final case class Schedule(workspace: Workspace)

  def apply(dataSource: SlickDataSource): Behavior[Schedule] = Behaviors.receive { (_, message) =>
    dataSource.inTransaction(_ => V1WorkspaceMigrationMonitor.schedule(message.workspace))
    Behaviors.same
  }

}
