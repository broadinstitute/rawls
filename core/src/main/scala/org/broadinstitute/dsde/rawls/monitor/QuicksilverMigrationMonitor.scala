package org.broadinstitute.dsde.rawls.monitor

import akka.actor._
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.dataaccess.SlickDataSource
import org.broadinstitute.dsde.rawls.monitor.QuicksilverMigrationMonitor.{MigrateWorkspace, NextWorkspace, StartAll}

import java.util.UUID
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

object QuicksilverMigrationMonitor {
  def props(datasource: SlickDataSource, initialDelay: FiniteDuration)(implicit
    executionContext: ExecutionContext
  ): Props =
    Props(new QuicksilverMigrationMonitor(datasource, initialDelay))

  sealed trait QuicksilverMigrationMessage
  case object StartAll extends QuicksilverMigrationMessage
  case class MigrateWorkspace(workspaceId: UUID) extends QuicksilverMigrationMessage
  case object NextWorkspace extends QuicksilverMigrationMessage

}

class QuicksilverMigrationMonitor(datasource: SlickDataSource, initialDelay: FiniteDuration)(implicit
  executionContext: ExecutionContext
) extends Actor
    with LazyLogging {

  context.system.scheduler.scheduleOnce(initialDelay, self, StartAll)

  override def receive: Receive = {
    case StartAll                           => startAll()
    case NextWorkspace                      => nextWorkspace()
    case currentWorkspace: MigrateWorkspace => migrateWorkspace(currentWorkspace.workspaceId)
  }

  private def startAll(): Unit = {
    // TODO: retrieve the current workspaceId from the MIGRATION_PROCESS table
    // TODO: if current workspaceId is null, retrieve the first workspaceId, ordering by workspaceId asc
    // TODO: send a MigrateWorkspace message for the current workspace id
  }

  private def nextWorkspace(): Unit = {
    // TODO: retrieve the current workspaceId from the MIGRATION_PROCESS table
    // TODO: if current workspaceId is null, throw error
    // TODO: retrieve the next workspaceId which is greater than the current workspaceId, ordering by workspaceId asc
    // TODO: send a MigrateWorkspace message for the next workspace id
  }

  private def migrateWorkspace(workspaceId: UUID): Unit = {
    // TODO: persist current workspaceId to MIGRATION_PROCESS table
    // TODO: get a LocalEntityProvider for this workspace
    // TODO: retrieve all entity types for this workspace, using LocalEntityProvider
    // TODO: loop over all entity types
    val entityTypes = Seq()
    entityTypes.foreach { entityType =>
      // TODO: retrieve a chunk of entities, using LocalEntityProvider
      // TODO: loop over these entities
      val entities = Seq()
      entities.foreach { entity =>
        // TODO: does this entity have any AttributeValueList or AttributeEntityReferenceList attributes?
        val hasListAttributes = true
        if (hasListAttributes) {
          // TODO: skip to next entity; nothing to do
        } else {
          // TODO: serialize this entity via CompactEntitySerialization.toSql()
          // TODO: persist this entity to the ENTITY_CORRECTIONS table (id, attributes; anything else?)
        }
      }
      // TODO: loop back and retrieve the next chunk of entities of this type, until we've processed them all
    }
  }
}
