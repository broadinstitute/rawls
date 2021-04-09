package org.broadinstitute.dsde.rawls.monitor

import akka.actor._
import akka.pattern._
import cats.effect.{ContextShift, IO}
import com.typesafe.scalalogging.LazyLogging
import io.opencensus.scala.Tracing.trace
import io.opencensus.trace.{AttributeValue => OpenCensusAttributeValue}
import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.dataaccess.SlickDataSource
import org.broadinstitute.dsde.rawls.monitor.EntityStatisticsCacheMonitor._
import org.broadinstitute.dsde.rawls.util.OpenCensusDBIOUtils.traceDBIOWithParent
import slick.dbio.DBIO

import java.sql.Timestamp
import java.util.{Calendar, UUID}
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import scala.language.postfixOps

object EntityStatisticsCacheMonitor {
  def props(datasource: SlickDataSource, timeoutPerWorkspace: Duration, standardPollInterval: FiniteDuration, workspaceCooldown: FiniteDuration)(implicit executionContext: ExecutionContext): Props = {
    Props(new EntityStatisticsCacheMonitorActor(datasource, timeoutPerWorkspace, standardPollInterval, workspaceCooldown))
  }

  sealed trait EntityStatisticsCacheMessage
  case object Sweep extends EntityStatisticsCacheMessage
  case object ScheduleDelayedSweep extends EntityStatisticsCacheMessage
}

class EntityStatisticsCacheMonitorActor(val dataSource: SlickDataSource, val timeoutPerWorkspace: Duration, val standardPollInterval: FiniteDuration, val workspaceCooldown: FiniteDuration)(implicit val executionContext: ExecutionContext) extends Actor with EntityStatisticsCacheMonitor with LazyLogging {
  import context._

  setReceiveTimeout(timeoutPerWorkspace)

  override def preStart(): Unit = {
    super.preStart()
    self ! Sweep
  }

  override def receive = {
    case Sweep => sweep() pipeTo self
    case ScheduleDelayedSweep => context.system.scheduler.scheduleOnce(standardPollInterval, self, Sweep)
  }

}

trait EntityStatisticsCacheMonitor extends LazyLogging {

  implicit val executionContext: ExecutionContext
  val dataSource: SlickDataSource
  val standardPollInterval: FiniteDuration
  val workspaceCooldown: FiniteDuration

  def sweep() = {
    trace("EntityStatisticsCacheMonitor.sweep") { rootSpan =>
      dataSource.inTransaction { dataAccess =>
        // calculate now - workspaceCooldown as the upper bound for workspace last_modified
        val maxModifiedTime = nowMinus(workspaceCooldown)
        //Note: Ignored workspaces have a cacheLastUpdated timestamp of 1000ms after epoch
        val minCacheTime = new Timestamp(1000)

        dataAccess.workspaceQuery.findMostOutdatedEntityCacheAfter(minCacheTime, maxModifiedTime).flatMap {
          case Some((workspaceId, lastModified)) =>
            rootSpan.putAttribute("workspaceId", OpenCensusAttributeValue.stringAttributeValue(workspaceId.toString))
            traceDBIOWithParent("updateStatisticsCache", rootSpan) { _ =>
              DBIO.from(updateStatisticsCache(workspaceId, lastModified).map { _ =>
                logger.info(s"Updated entity cache for workspace $workspaceId. Cache was ${System.currentTimeMillis() - lastModified.getTime}ms out of date.")
                Sweep
              })
            }
          case None =>
            traceDBIOWithParent("nothing-to-update", rootSpan) { _ =>
              logger.info(s"All workspace entity caches are up to date. Sleeping for ${standardPollInterval}")
              DBIO.successful(ScheduleDelayedSweep)
            }
        }
      }
    }
  }

  def updateStatisticsCache(workspaceId: UUID, timestamp: Timestamp): Future[Unit] = {
    val updateFuture = dataSource.inTransaction { dataAccess =>
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

    updateFuture.recover {
      case t: Throwable =>
        logger.error(s"Error updating statistics cache for workspaceId ${workspaceId}", t)
        dataSource.inTransaction { dataAccess =>
          logger.error(s"Workspace ${workspaceId} will be ignored by the entity cache monitor.")
          //We will set the cacheLastUpdated timestamp to the lowest possible value in MySQL.
          // This is a "magic" value that allows the monitor to skip this problematic workspace
          // so it does not get caught in a loop. These workspaces will require manual intervention.
          dataAccess.workspaceQuery.updateCacheLastUpdated(workspaceId, new Timestamp(1000))
        }
    }
  }

  def nowMinus(duration: FiniteDuration): Timestamp = {
    val durationSeconds = duration.toSeconds.toInt
    val nowTime = Calendar.getInstance
    nowTime.add(Calendar.SECOND, durationSeconds * -1)
    new Timestamp(nowTime.getTime.toInstant.toEpochMilli)
  }

}
