package org.broadinstitute.dsde.rawls.monitor

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import org.broadinstitute.dsde.rawls.dataaccess.SlickDataSource
import org.broadinstitute.dsde.rawls.dataaccess.slick.{ReadAction, WriteAction}
import org.broadinstitute.dsde.rawls.model.Workspace

import scala.concurrent.ExecutionContext.Implicits.global

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
