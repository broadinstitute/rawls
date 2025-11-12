package org.broadinstitute.dsde.rawls.monitor

import akka.actor._
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.dataaccess.SlickDataSource
import org.broadinstitute.dsde.rawls.dataaccess.slick.{DataAccess, ReadWriteAction}
import org.broadinstitute.dsde.rawls.entities.compact.CompactEntitySerialization
import org.broadinstitute.dsde.rawls.entities.{EntityManager, EntityRequestArguments}
import org.broadinstitute.dsde.rawls.model.{
  AttributeEntityReferenceList,
  AttributeName,
  AttributeValueList,
  Entity,
  RawlsRequestContext,
  RawlsUserEmail,
  RawlsUserSubjectId,
  UserInfo,
  Workspace
}
import org.broadinstitute.dsde.rawls.monitor.QuicksilverMigrationMonitor.{MigrateWorkspace, NextWorkspace, StartAll}
import slick.dbio.DBIO

import java.util.UUID
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

object QuicksilverMigrationMonitor {
  def props(datasource: SlickDataSource, entityManager: EntityManager, initialDelay: FiniteDuration)(implicit
    executionContext: ExecutionContext
  ): Props =
    Props(new QuicksilverMigrationMonitor(datasource, entityManager, initialDelay))

  sealed trait QuicksilverMigrationMessage
  case object StartAll extends QuicksilverMigrationMessage
  case class MigrateWorkspace(workspaceId: UUID) extends QuicksilverMigrationMessage
  case object NextWorkspace extends QuicksilverMigrationMessage

}

/**
 * This monitor requires manual intervention before it can be run:
 *  1. Create a table CURRENT_MIGRATION with one column `workspace_id` of binary(16)
 *  2. Insert one row into CURRENT_MIGRATION which is the first workspace in the database, ordered by workspace_id asc
 *  3. Create a table ENTITY_CORRECTIONS with columns workspace_id, entity_type, name, and attributes with the
 *      same datatypes as their corresponding columns in ENTITY
 */
class QuicksilverMigrationMonitor(datasource: SlickDataSource,
                                  entityManager: EntityManager,
                                  initialDelay: FiniteDuration
)(implicit
  executionContext: ExecutionContext
) extends Actor
    with LazyLogging {

  context.system.scheduler.scheduleOnce(initialDelay, self, StartAll)

  val fakeUserInfo =
    UserInfo(RawlsUserEmail("QuicksilverMigrationMonitor"), OAuth2BearerToken(""), 3600, RawlsUserSubjectId("0"))

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

  private def migrateWorkspace(workspaceId: UUID): Unit =
    datasource.inTransaction { dataAccess =>
      for {
        // retrieve the workspace
        workspaceOption <- dataAccess.workspaceQuery.loadWorkspace(dataAccess.workspaceQuery.findByIdQuery(workspaceId))
        workspace = workspaceOption.get
        // persist current workspaceId to MIGRATION_PROCESS table
        _ <- dataAccess.compactEntityQuery.updateCurrentMigration(workspaceId)
        // migrate this workspace
        workspaceMigrationResult <- migrateWorkspace(workspace, dataAccess)

      } yield workspaceMigrationResult
    }

  private def migrateWorkspace(workspace: Workspace, dataAccess: DataAccess): ReadWriteAction[Int] = {
    val entityRequestArguments = EntityRequestArguments(workspace, RawlsRequestContext(fakeUserInfo, None))
    for {
      // get a LocalEntityProvider for this workspace
      localProvider <- DBIO.from(entityManager.getLocalProvider(entityRequestArguments))
      // retrieve all entity types for this workspace, using LocalEntityProvider
      // retrieve entity types for workspace
      allTypes <- dataAccess.entityQuery.getEntityTypesWithCounts(workspace.workspaceIdAsUUID)
      // loop over all entity types and migrate each one
      results <- DBIO.sequence(allTypes.keySet.map { entityType =>
        migrateEntityType(workspace, entityType, dataAccess)
      })
    } yield results.sum
  }

  private def migrateEntityType(workspace: Workspace,
                                entityType: String,
                                dataAccess: DataAccess
  ): ReadWriteAction[Int] = {
    // TODO: retrieve a chunk of entities, using LocalEntityProvider
    val entities = Seq()
    migrateEntityBatch(workspace, entityType, entities, dataAccess)
    // TODO: loop back and retrieve the next chunk of entities of this type, until we've processed them all
    DBIO.successful(3)
  }

  private def migrateEntityBatch(workspace: Workspace,
                                 entityType: String,
                                 entities: Seq[Entity],
                                 dataAccess: DataAccess
  ): ReadWriteAction[Int] = {
    // loop over these entities
    val entityOperations = entities.map { entity =>
      // does this entity have any AttributeValueList or AttributeEntityReferenceList attributes?
      val listAttributesOnly = entity.attributes.filter {
        case (attributeName @ AttributeName(_, _), attribute @ AttributeValueList(_))           => true
        case (attributeName @ AttributeName(_, _), attribute @ AttributeEntityReferenceList(_)) => true
        case _                                                                                  => false
      }
      val hasListAttributes = listAttributesOnly.nonEmpty

      if (hasListAttributes) {
        // serialize this entity into Quicksilver
        val quickSilverSerialized = CompactEntitySerialization.toSql(listAttributesOnly).compactPrint
        // persist this entity to the ENTITY_CORRECTIONS table
        dataAccess.compactEntityQuery.saveMigratedEntity(workspace.workspaceIdAsUUID,
                                                         entity.entityType,
                                                         entity.name,
                                                         quickSilverSerialized
        )
      } else {
        DBIO.successful(0)
      }
    }
    DBIO.sequence(entityOperations) map { results => results.sum }
  }

}
