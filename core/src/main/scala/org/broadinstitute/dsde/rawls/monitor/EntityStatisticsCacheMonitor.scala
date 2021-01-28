package org.broadinstitute.dsde.rawls.monitor

import akka.actor._
import cats.effect.{ContextShift, IO}
import cats.implicits._
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.dataaccess.SlickDataSource
import org.broadinstitute.dsde.rawls.monitor.EntityStatisticsCacheMonitor.Sweep

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
}

class EntityStatisticsCacheMonitor(datasource: SlickDataSource, initialDelay: FiniteDuration, pollInterval: FiniteDuration, limit: Int)(implicit executionContext: ExecutionContext, cs: ContextShift[IO]) extends Actor with LazyLogging {

  context.system.scheduler.schedule(initialDelay, pollInterval, self, Sweep)

  override def receive = {
    case Sweep => sweep()
  }

  private def sweep() = {
    val checkFuture = for {
      recordsToUpdate <- datasource.inTransaction { dataAccess =>
        dataAccess.workspaceQuery.listOutdatedEntityCaches(limit)
      }

      _ <- recordsToUpdate.toList.traverse { workspaceToUpdate =>
        IO.fromFuture(IO(updateStatisticsCache(workspaceToUpdate._1, workspaceToUpdate._2)))
      }.unsafeToFuture()
    } yield ()

    checkFuture.failed.foreach {
      // there was a failure, log it and it will retry later
      t: Throwable => logger.error("Error updating statistics cache", t)
    }
  }

  private def updateStatisticsCache(workspaceId: UUID, timestamp: Timestamp): Future[Unit] = {
    val deleteFuture = datasource.inTransaction { dataAccess =>
      for {
        entityTypesWithCounts <- dataAccess.entityQuery.getEntityTypesWithCounts(workspaceId)
        _ <- dataAccess.entityTypeStatisticsQuery.batchInsert(workspaceId, entityTypesWithCounts)
        entityTypesWithAttrNames <- dataAccess.entityQuery.getAttrNamesAndEntityTypes(workspaceId)
        _ <- dataAccess.entityAttributeStatisticsQuery.batchInsert(workspaceId, entityTypesWithAttrNames)
        _ <- dataAccess.workspaceQuery.updateCacheLastUpdated(workspaceId, timestamp)
      } yield ()
    }

    deleteFuture.recover {
      case t: Throwable =>
        logger.error(s"Error updating statistics cache", t)
    }
  }
}