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

object EntityStatisticsCacheMonitor {
  def props(datasource: SlickDataSource, initialDelay: FiniteDuration, pollInterval: FiniteDuration, limit: Int)(implicit executionContext: ExecutionContext, cs: ContextShift[IO]): Props = {
    Props(new EntityStatisticsCacheMonitor(datasource, initialDelay, pollInterval, limit))
  }

  sealed trait EntityStatisticsCacheMessage
  case object Sweep extends EntityStatisticsCacheMessage
  case object HandleBacklog extends EntityStatisticsCacheMessage
}

class EntityStatisticsCacheMonitor(dataSource: SlickDataSource, initialDelay: FiniteDuration, pollInterval: FiniteDuration, limit: Int)(implicit executionContext: ExecutionContext, cs: ContextShift[IO]) extends Actor with LazyLogging {

  override def preStart(): Unit = {
    super.preStart()
    sweep() pipeTo self
  }

  override def receive = {
    case Sweep => sweep() pipeTo self
    case HandleBacklog => handleBacklog() pipeTo self
  }

  private def sweep() = {
    val checkFuture = for {
      recordsToUpdate <- dataSource.inTransaction { dataAccess =>
        dataAccess.workspaceQuery.listOutdatedEntityCaches(limit)
      }

      _ <- recordsToUpdate.toList.traverse { case (workspaceId, lastModified) =>
        IO.fromFuture(IO(updateStatisticsCache(workspaceId, lastModified)))
      }.unsafeToFuture()
    } yield {
      logger.info(s"Updated entity cache for ${recordsToUpdate.length} workspace(s), with the limit set to $limit")
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

  private def handleBacklog(): Future[EntityStatisticsCacheMessage] = {
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

  private def markBackloggedWorkspace(workspaceId: UUID, timestamp: Timestamp): Future[Int] = {
    dataSource.inTransaction { dataAccess =>
      dataAccess.workspaceQuery.updateCacheLastUpdated(workspaceId, new Timestamp(timestamp.getTime - 1))
    }
  }

  private def updateStatisticsCache(workspaceId: UUID, timestamp: Timestamp): Future[Unit] = {
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