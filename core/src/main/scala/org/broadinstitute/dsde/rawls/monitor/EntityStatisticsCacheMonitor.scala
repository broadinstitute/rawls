package org.broadinstitute.dsde.rawls.monitor

import akka.actor._
import akka.pattern.pipe
import com.typesafe.scalalogging.LazyLogging
import io.opencensus.scala.Tracing.trace
import io.opencensus.trace.{AttributeValue => OpenCensusAttributeValue}
import org.apache.commons.lang3.exception.ExceptionUtils
import org.broadinstitute.dsde.rawls.dataaccess.SlickDataSource
import org.broadinstitute.dsde.rawls.monitor.EntityStatisticsCacheMonitor._
import org.broadinstitute.dsde.rawls.util.OpenCensusDBIOUtils.traceDBIOWithParent
import slick.dbio.DBIO

import java.sql.Timestamp
import java.util.{Calendar, UUID}
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.language.postfixOps

object EntityStatisticsCacheMonitor {
  def props(datasource: SlickDataSource, timeoutPerWorkspace: Duration, standardPollInterval: FiniteDuration, workspaceCooldown: FiniteDuration)(implicit executionContext: ExecutionContext): Props = {
    Props(new EntityStatisticsCacheMonitorActor(datasource, timeoutPerWorkspace, standardPollInterval, workspaceCooldown))
  }

  sealed trait EntityStatisticsCacheMessage
  case object Sweep extends EntityStatisticsCacheMessage
  case object ScheduleDelayedSweep extends EntityStatisticsCacheMessage

  //Note: Ignored workspaces have a cacheLastUpdated timestamp of 1000ms after epoch
  val MIN_CACHE_TIME = new Timestamp(1000)

}

class EntityStatisticsCacheMonitorActor(val dataSource: SlickDataSource, val timeoutPerWorkspace: Duration, val standardPollInterval: FiniteDuration, val workspaceCooldown: FiniteDuration)(implicit val executionContext: ExecutionContext) extends Actor with EntityStatisticsCacheMonitor with LazyLogging {
  import context.setReceiveTimeout

  setReceiveTimeout(timeoutPerWorkspace)

  override def preStart(): Unit = {
    super.preStart()
    self ! Sweep
  }

  override def receive = {
    case Sweep => sweep() pipeTo self
    case ScheduleDelayedSweep => context.system.scheduler.scheduleOnce(standardPollInterval, self, Sweep)
    case akka.actor.ReceiveTimeout =>
      val pauseLength = standardPollInterval*2
      logger.warn(s"EntityStatisticsCacheMonitor attempt timed out. Pausing for ${pauseLength}.")
      context.system.scheduler.scheduleOnce(pauseLength, self, Sweep)
  }

}

trait EntityStatisticsCacheMonitor extends LazyLogging {

  implicit val executionContext: ExecutionContext
  val dataSource: SlickDataSource
  val standardPollInterval: FiniteDuration
  val workspaceCooldown: FiniteDuration

  def sweep(): Future[EntityStatisticsCacheMessage] = {
    trace("EntityStatisticsCacheMonitor.sweep") { rootSpan =>
      dataSource.inTransaction { dataAccess =>
        // calculate now - workspaceCooldown as the upper bound for workspace last_modified
        val maxModifiedTime = nowMinus(workspaceCooldown)

        dataAccess.entityCacheQuery.findMostOutdatedEntityCachesAfter(MIN_CACHE_TIME, maxModifiedTime) flatMap { candidates =>
          if (candidates.nonEmpty) {
            // pick one of the candidates at random. This randomness ensures that we don't get stuck constantly trying
            // and failing to update the same workspace - until all we have left as candidates are un-updatable workspaces
            val (workspaceId, lastModified, cacheLastUpdated) = candidates.toArray.apply(scala.util.Random.nextInt(candidates.size))
            rootSpan.putAttribute("workspaceId", OpenCensusAttributeValue.stringAttributeValue(workspaceId.toString))
            logger.info(s"EntityStatisticsCacheMonitor starting update attempt for workspace $workspaceId.")
            traceDBIOWithParent("updateStatisticsCache", rootSpan) { _ =>
              DBIO.from(updateStatisticsCache(workspaceId, lastModified).map { _ =>
                val outDated = lastModified.getTime - cacheLastUpdated.getOrElse(MIN_CACHE_TIME).getTime
                logger.info(s"Updated entity cache for workspace $workspaceId. Cache was ${outDated}ms out of date.")
                Sweep
              })
            }
          } else {
            traceDBIOWithParent("nothing-to-update", rootSpan) { _ =>
              logger.info(s"All workspace entity caches are up to date. Sleeping for $standardPollInterval")
              DBIO.successful(ScheduleDelayedSweep)
            }
          }
        }
      }
    }
  }

  def updateStatisticsCache(workspaceId: UUID, timestamp: Timestamp): Future[Unit] = {
    val updateFuture = dataSource.inTransaction { dataAccess =>
      // TODO: beware contention on the approach of delete-all and batch-insert all below
      // if we see contention we could move to encoding the entire metadata object as json
      // and storing in a single column on WORKSPACE_ENTITY_CACHE
      for {
        //update entity statistics
        entityTypesWithCounts <- dataAccess.entityQuery.getEntityTypesWithCounts(workspaceId)
        _ <- dataAccess.entityTypeStatisticsQuery.deleteAllForWorkspace(workspaceId)
        _ <- dataAccess.entityTypeStatisticsQuery.batchInsert(workspaceId, entityTypesWithCounts)
        //update entity attribute statistics
        workspaceShardState <- dataAccess.workspaceQuery.getWorkspaceShardState(workspaceId)
        entityTypesWithAttrNames <- dataAccess.entityQuery.getAttrNamesAndEntityTypes(workspaceId, workspaceShardState)
        _ <- dataAccess.entityAttributeStatisticsQuery.deleteAllForWorkspace(workspaceId)
        _ <- dataAccess.entityAttributeStatisticsQuery.batchInsert(workspaceId, entityTypesWithAttrNames)
        //update cache update date
        _ <- dataAccess.entityCacheQuery.updateCacheLastUpdated(workspaceId, timestamp)
      } yield ()
    }

    updateFuture.recover {
      case t: Throwable =>
        logger.error(s"Error updating statistics cache for workspaceId $workspaceId: ${t.getMessage}", t)
        dataSource.inTransaction { dataAccess =>
          logger.error(s"Workspace $workspaceId will be ignored by the entity cache monitor: ${t.getMessage}")
          //We will set the cacheLastUpdated timestamp to the lowest possible value in MySQL.
          // This is a "magic" value that allows the monitor to skip this problematic workspace
          // so it does not get caught in a loop. These workspaces will require manual intervention.
          val errMsg = s"${t.getMessage} ${ExceptionUtils.getStackTrace(t)}"
          dataAccess.entityCacheQuery.updateCacheLastUpdated(workspaceId, MIN_CACHE_TIME, Some(errMsg))
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
