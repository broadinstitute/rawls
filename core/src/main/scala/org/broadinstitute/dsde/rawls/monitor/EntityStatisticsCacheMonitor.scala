package org.broadinstitute.dsde.rawls.monitor

import akka.actor._
import akka.pattern._
import cats.effect.{ContextShift, IO}
import cats.implicits._
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.dataaccess.SlickDataSource
import org.broadinstitute.dsde.rawls.monitor.EntityStatisticsCacheMonitor.{EntityStatisticsCacheMessage, HandleBacklog, Sweep}
import slick.dbio.DBIO

import java.sql.Timestamp
import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import scala.language.postfixOps

object EntityStatisticsCacheMonitor {
  def props(datasource: SlickDataSource, limit: Int, timeoutPerWorkspace: Duration)(implicit executionContext: ExecutionContext, cs: ContextShift[IO]): Props = {
    Props(new EntityStatisticsCacheMonitorActor(datasource, limit, timeoutPerWorkspace))
  }

  sealed trait EntityStatisticsCacheMessage
  case object Sweep extends EntityStatisticsCacheMessage
  case object HandleBacklog extends EntityStatisticsCacheMessage
}

class EntityStatisticsCacheMonitorActor(val dataSource: SlickDataSource, val limit: Int, val timeoutPerWorkspace: Duration)(implicit val executionContext: ExecutionContext, val cs: ContextShift[IO]) extends Actor with EntityStatisticsCacheMonitor with LazyLogging {
  import context._

  setReceiveTimeout(limit * timeoutPerWorkspace)

  override def preStart(): Unit = {
    super.preStart()
    self ! Sweep
  }

  override def receive = {
    case Sweep => sweep() pipeTo self
    case HandleBacklog => handleBacklog() pipeTo self
  }

}

trait EntityStatisticsCacheMonitor extends LazyLogging {

  implicit val executionContext: ExecutionContext
  implicit val cs: ContextShift[IO]
  val dataSource: SlickDataSource
  val limit: Int

  def sweep() = {
    val checkFuture = for {
      recordsToUpdate <- dataSource.inTransaction { dataAccess =>
        dataAccess.workspaceQuery.listOutdatedEntityCaches(limit)
      }

      updateResults <- recordsToUpdate.toList.traverse { case (workspaceId, lastModified) =>
        IO.fromFuture(IO(updateStatisticsCache(workspaceId, lastModified))).map { _ =>
          (workspaceId, lastModified.getTime, System.currentTimeMillis())
        }
      }.unsafeToFuture()
    } yield {
      updateResults.foreach { case (workspaceId, lastModified, cacheUpdated) =>
        logger.info(s"Updated workspace $workspaceId. Cache was ${cacheUpdated - lastModified} millis out of date")
      }
      logger.info(s"Sweep complete. Updated entity cache for ${recordsToUpdate.length} workspace(s), with the limit set to $limit")
      if(recordsToUpdate.length < limit) HandleBacklog
      else Sweep
    }

    checkFuture.recover {
      case t: Throwable => {
        logger.error(s"Error updating statistics cache", t)
        //There was a failure, so we'll just go back to sweeping
        Sweep
      }
    }
  }

  def handleBacklog(): Future[EntityStatisticsCacheMessage] = {
    dataSource.inTransaction { dataAccess =>
      for {
        backloggedWorkspaces <- dataAccess.workspaceQuery.listBackloggedEntityCaches(limit)
        numMarked <- DBIO.sequence(backloggedWorkspaces map { case (workspaceId, lastModified) =>
          dataAccess.workspaceQuery.updateCacheLastUpdated(workspaceId, new Timestamp(lastModified.getTime - 1))
        })
      } yield {
        logger.info(s"Marked ${numMarked.length} workspaces in the backlog to be picked up by entity statistics cache monitor")
        Sweep
      }
    }
  }

  def updateStatisticsCache(workspaceId: UUID, timestamp: Timestamp): Future[Unit] = {
    val deleteFuture = dataSource.inTransaction { dataAccess =>
      for {
        //update entity statistics
        entityTypesWithCounts <- dataAccess.entityQuery.getEntityTypesWithCounts(workspaceId)
        _ <- dataAccess.entityTypeStatisticsQuery.deleteAllForWorkspace(workspaceId)
        _ <- dataAccess.entityTypeStatisticsQuery.batchInsert(workspaceId, entityTypesWithCounts)
        //update entity attribute statistics
        entityTypesWithAttrNames <- dataAccess.entityQuery.getAttrNamesAndEntityTypes(workspaceId)
        _ <- dataAccess.entityAttributeStatisticsQuery.deleteAllForWorkspace(workspaceId)
        _ <- dataAccess.entityAttributeStatisticsQuery.batchInsert(workspaceId, entityTypesWithAttrNames)
        //update cache update date
        _ <- dataAccess.workspaceQuery.updateCacheLastUpdated(workspaceId, timestamp)
      } yield ()
    }

    deleteFuture.recover {
      case t: Throwable =>
        logger.error(s"Error updating statistics cache", t)
    }
  }
}