package org.broadinstitute.dsde.rawls.monitor

import akka.actor.TypedActor.dispatcher
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.dataaccess.SlickDataSource
import org.broadinstitute.dsde.rawls.dataaccess.slick.{ReadAction, WriteAction}
import org.broadinstitute.dsde.rawls.model.Workspace
import org.broadinstitute.dsde.rawls.monitor.V1WorkspaceMigrationActor.Schedule

object V1WorkspaceMigrationMonitor {

  import slick.jdbc.H2Profile.api._

  private val v1WorkspaceMigrationHistory: String = "V1_WORKSPACE_MIGRATION_HISTORY"

  final def isMigrating(workspace: Workspace): ReadAction[Boolean] =
    sqlu"""SELECT COUNT(*) FROM ${v1WorkspaceMigrationHistory} h
           WHERE h.WORKSPACE_ID = '${workspace.workspaceId}'
             AND h.STARTED IS NOT NULL
             AND h.FINISHED IS NULL""".map(_ > 0)

  final def schedule(workspace: Workspace): WriteAction[Unit] =
    sqlu"""INSERT INTO ${v1WorkspaceMigrationHistory} (workspaceId)
           VALUES (${workspace.workspaceId})""".map(_ => ())
}

object V1WorkspaceMigrationActor {
  final case class Schedule(workspace: Workspace)
}

final class V1WorkspaceMigrationActor(dataSource: SlickDataSource) extends LazyLogging {

  def apply(): Behavior[Schedule] = Behaviors.receive { (_, message) =>
    dataSource.inTransaction(_ => V1WorkspaceMigrationMonitor.schedule(message.workspace))
    Behaviors.same
  }

}

