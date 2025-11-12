package org.broadinstitute.dsde.rawls.monitor

import akka.actor._
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.dataaccess.SlickDataSource
import org.broadinstitute.dsde.rawls.dataaccess.slick.{DataAccess, ReadWriteAction}
import org.broadinstitute.dsde.rawls.entities.base.EntityProvider
import org.broadinstitute.dsde.rawls.entities.compact.CompactEntitySerialization
import org.broadinstitute.dsde.rawls.entities.{EntityManager, EntityRequestArguments}
import org.broadinstitute.dsde.rawls.model.SortDirections.Ascending
import org.broadinstitute.dsde.rawls.model.{
  AttributeEntityReferenceList,
  AttributeName,
  AttributeValueList,
  Entity,
  EntityQuery,
  RawlsRequestContext,
  RawlsUserEmail,
  RawlsUserSubjectId,
  UserInfo,
  Workspace
}
import org.broadinstitute.dsde.rawls.monitor.QuicksilverMigrationMonitor.{
  AllDone,
  MigrateWorkspace,
  NextWorkspace,
  StartAll
}
import slick.dbio.DBIO

import java.util.UUID
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.language.postfixOps

object QuicksilverMigrationMonitor {
  def props(datasource: SlickDataSource, entityManager: EntityManager, initialDelay: FiniteDuration)(implicit
    executionContext: ExecutionContext
  ): Props =
    Props(new QuicksilverMigrationMonitor(datasource, entityManager, initialDelay))

  sealed trait QuicksilverMigrationMessage
  case object StartAll extends QuicksilverMigrationMessage
  case class MigrateWorkspace(workspaceId: UUID) extends QuicksilverMigrationMessage
  case object NextWorkspace extends QuicksilverMigrationMessage
  case object AllDone extends QuicksilverMigrationMessage

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

  // so the actor doesn't die if it receives no messages for a while; workspaces can be big
  context.setReceiveTimeout(8 hours)
  // start it all going
  context.system.scheduler.scheduleOnce(initialDelay, self, StartAll)

  // hardcoded configs
  private val fakeUserInfo =
    UserInfo(RawlsUserEmail("QuicksilverMigrationMonitor"), OAuth2BearerToken(""), 3600, RawlsUserSubjectId("0"))
  private val ctx = RawlsRequestContext(fakeUserInfo, None)
  private val chunkSize = 500

  override def receive: Receive = {
    case StartAll                           => startAll()
    case NextWorkspace                      => nextWorkspace()
    case currentWorkspace: MigrateWorkspace => startWorkspace(currentWorkspace.workspaceId)
    case AllDone                            => self ! PoisonPill
  }

  private def startAll(): Unit =
    datasource.inTransaction { dataAccess =>
      for {
        // retrieve the current workspaceId from the MIGRATION_PROCESS table
        currentMigrationWorkspaceId <- dataAccess.compactEntityQuery.getCurrentMigration
        // if current workspaceId is null, insert the first workspaceId, else use the one we just looked up
        maybeBootstrap: UUID <-
          if (currentMigrationWorkspaceId.isEmpty) {
            dataAccess.compactEntityQuery.bootstrapCurrentMigration map { _ =>
              dataAccess.compactEntityQuery.getCurrentMigration
            }
          } else {
            DBIO.successful(currentMigrationWorkspaceId.get)
          }
      } yield self ! MigrateWorkspace(maybeBootstrap)
    }

  private def nextWorkspace(): Unit =
    datasource.inTransaction { dataAccess =>
      for {
        // retrieve the current workspaceId from the MIGRATION_PROCESS table
        currentMigrationWorkspaceId <- dataAccess.compactEntityQuery.getCurrentMigration
        lastMigrationId = currentMigrationWorkspaceId.get
        // retrieve the next workspaceId which is greater than the current workspaceId, ordering by workspaceId asc
        nextMigrationWorkspaceId <- dataAccess.compactEntityQuery.nextMigration(lastMigrationId)
      } yield
        if (nextMigrationWorkspaceId.isDefined) {
          // send a MigrateWorkspace message for the next workspace id
          self ! MigrateWorkspace(nextMigrationWorkspaceId.get)
        } else {
          self ! AllDone
        }
    }

  private def startWorkspace(workspaceId: UUID): Unit =
    datasource.inTransaction { dataAccess =>
      for {
        // retrieve the workspace
        workspaceOption <- dataAccess.workspaceQuery.loadWorkspace(dataAccess.workspaceQuery.findByIdQuery(workspaceId))
        workspace = workspaceOption.get
        // persist current workspaceId to MIGRATION_PROCESS table
        _ <- dataAccess.compactEntityQuery.updateCurrentMigration(workspaceId)
        // migrate this workspace
        workspaceMigrationResult <- migrateWorkspace(workspace, dataAccess)

      } yield self ! NextWorkspace
    }

  private def migrateWorkspace(workspace: Workspace, dataAccess: DataAccess): ReadWriteAction[Int] = {
    logger.info(s"migrating ${workspace.workspaceId} ${workspace.toWorkspaceName} ...")
    val entityRequestArguments = EntityRequestArguments(workspace, ctx)
    for {
      // clear any previously-migrated entities from this workspace, in case we are restarting migrations
      // after a Rawls reboot
      _ <- dataAccess.compactEntityQuery.restartWorkspace(workspace.workspaceIdAsUUID)
      // get a LocalEntityProvider for this workspace
      localProvider <- DBIO.from(entityManager.getLocalProvider(entityRequestArguments))
      // retrieve all entity types for this workspace, using LocalEntityProvider
      // retrieve entity types for workspace
      allTypes <- dataAccess.entityQuery.getEntityTypesWithCounts(workspace.workspaceIdAsUUID)
      _ = logger.info(s"  ... ${allTypes.size} entity types in this workspace (${allTypes.keySet.mkString})")
      // loop over all entity types and migrate each one
      results <- DBIO.sequence(allTypes.map { case (entityType, count) =>
        logger.info(s"    ... $entityType: $count entities to consider ...")
        migrateEntityType(workspace, entityType, localProvider, dataAccess)
      })
    } yield results.sum
  }

  private def migrateEntityType(workspace: Workspace,
                                entityType: String,
                                localProvider: EntityProvider,
                                dataAccess: DataAccess
  ): ReadWriteAction[Int] = {
    // inner method for recursion
    def processNextChunk(page: Int, rowsUpdated: Int): ReadWriteAction[Int] =
      for {
        queryConfig <- DBIO.successful(EntityQuery(page, chunkSize, "name", Ascending, None))
        // retrieve a chunk of entities, using LocalEntityProvider
        queryResults <- DBIO.from(localProvider.queryEntities(entityType, queryConfig, ctx))
        migrationResults <-
          if (queryResults.results.nonEmpty) {
            migrateEntityChunk(workspace, queryResults.results, dataAccess) flatMap { count =>
              // loop back and retrieve the next chunk of entities of this type, until we've processed them all
              processNextChunk(page + 1, rowsUpdated + count)
            }
          } else {
            DBIO.successful(0)
          }
      } yield migrationResults

    processNextChunk(1, 0) map { totalRowsUpdated =>
      logger.info(s"        ... $entityType: $totalRowsUpdated rows actually updated")
      totalRowsUpdated
    }

  }

  private def migrateEntityChunk(workspace: Workspace,
                                 entities: Seq[Entity],
                                 dataAccess: DataAccess
  ): ReadWriteAction[Int] = {
    // loop over these entities
    val entityOperations = entities.map { entity =>
      // does this entity have any AttributeValueList or AttributeEntityReferenceList attributes?
      val listAttributesOnly = entity.attributes.filter {
        case (_ @AttributeName(_, _), _ @AttributeValueList(_))           => true
        case (_ @AttributeName(_, _), _ @AttributeEntityReferenceList(_)) => true
        case _                                                            => false
      }
      if (listAttributesOnly.nonEmpty) {
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
