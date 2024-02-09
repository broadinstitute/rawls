package org.broadinstitute.dsde.rawls.monitor

import akka.actor._
import akka.pattern.pipe
import com.typesafe.scalalogging.LazyLogging
import io.opencensus.trace.{AttributeValue => OpenCensusAttributeValue}
import io.opentelemetry.api.common.AttributeKey
import org.apache.commons.lang3.exception.ExceptionUtils
import org.broadinstitute.dsde.rawls.dataaccess.SlickDataSource
import org.broadinstitute.dsde.rawls.dataaccess.slick.DataAccess
import org.broadinstitute.dsde.rawls.metrics.RawlsInstrumented
import org.broadinstitute.dsde.rawls.model.RawlsTracingContext
import org.broadinstitute.dsde.rawls.monitor.EntityStatisticsCacheMonitor._
import org.broadinstitute.dsde.rawls.util.TracingUtils._
import slick.dbio.DBIO
import slick.jdbc.TransactionIsolation
import slick.jdbc.TransactionIsolation.ReadCommitted

import java.sql.Timestamp
import java.util.concurrent.TimeUnit
import java.util.{Calendar, UUID}
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.language.postfixOps

object EntityStatisticsCacheMonitor {
  def props(datasource: SlickDataSource,
            timeoutPerWorkspace: Duration,
            standardPollInterval: FiniteDuration,
            workspaceCooldown: FiniteDuration,
            workbenchMetricBaseName: String
  )(implicit executionContext: ExecutionContext): Props =
    Props(
      new EntityStatisticsCacheMonitorActor(datasource,
                                            timeoutPerWorkspace,
                                            standardPollInterval,
                                            workspaceCooldown,
                                            workbenchMetricBaseName
      )
    )

  sealed trait EntityStatisticsCacheMessage
  case object Start extends EntityStatisticsCacheMessage
  case object Sweep extends EntityStatisticsCacheMessage
  case object ScheduleDelayedSweep extends EntityStatisticsCacheMessage
  case object DieAndRestart extends EntityStatisticsCacheMessage

  // Note: Ignored workspaces have a cacheLastUpdated timestamp of 1000ms after epoch
  val MIN_CACHE_TIME = new Timestamp(1000)

}

class EntityStatisticsCacheMonitorActor(val dataSource: SlickDataSource,
                                        val timeoutPerWorkspace: Duration,
                                        val standardPollInterval: FiniteDuration,
                                        val workspaceCooldown: FiniteDuration,
                                        override val workbenchMetricBaseName: String
)(implicit val executionContext: ExecutionContext)
    extends Actor
    with EntityStatisticsCacheMonitor
    with LazyLogging {
  import context.setReceiveTimeout

  setReceiveTimeout(timeoutPerWorkspace)

  override def preStart(): Unit = {
    super.preStart()
    self ! Start
  }

  override def receive = {
    case Start =>
      // this Start case assists with unit testing, by providing a case where tests can send this Actor a message
      // and expect a consistent response
      sender ! Sweep
    case Sweep                => sweep() pipeTo self
    case ScheduleDelayedSweep => context.system.scheduler.scheduleOnce(standardPollInterval, self, Sweep)
    case DieAndRestart        =>
      // Kill this actor, and start a new one. This will drain this actor's mailbox to dead letters.
      // The new actor will take over processing caches.
      logger.warn(s"EntityStatisticsCacheMonitor restarting with a new Actor instance.")
      val newActor = context.system.actorOf(
        EntityStatisticsCacheMonitor.props(dataSource,
                                           timeoutPerWorkspace,
                                           standardPollInterval,
                                           workspaceCooldown,
                                           workbenchMetricBaseName
        )
      )
      if (sender != self) sender ! newActor // used by unit tests only
      context.stop(self)
    case akka.actor.ReceiveTimeout =>
      // This actor timed out. We can't be certain if this actor's sweep exited abnormally, or if it is hung,
      // or if it is benignly just taking a while. Because we can't be sure of its state, kill this actor
      // and start up a new one using the same configuration. Pause before starting the new one
      // in case this one is actually still processing.
      val pauseLength = FiniteDuration(timeoutPerWorkspace.toSeconds, TimeUnit.SECONDS) * 2
      logger.warn(s"EntityStatisticsCacheMonitor attempt timed out. Pausing for ${pauseLength}.")
      context.system.scheduler.scheduleOnce(pauseLength, self, DieAndRestart)
  }

}

trait EntityStatisticsCacheMonitor extends LazyLogging with RawlsInstrumented {

  implicit val executionContext: ExecutionContext
  val dataSource: SlickDataSource
  val standardPollInterval: FiniteDuration
  val workspaceCooldown: FiniteDuration
  val timeoutPerWorkspace: Duration

  // default isolation level for queries in this trait
  final val isolationLevel: TransactionIsolation = TransactionIsolation.ReadCommitted

  def sweep(): Future[EntityStatisticsCacheMessage] =
    traceFuture("EntityStatisticsCacheMonitor.sweep") { rootContext =>
      dataSource.inTransaction(
        { dataAccess =>
          // calculate now - workspaceCooldown as the upper bound for workspace last_modified
          val maxModifiedTime = nowMinus(workspaceCooldown)

          dataAccess.entityCacheQuery
            .findMostOutdatedEntityCachesAfter(MIN_CACHE_TIME, maxModifiedTime) flatMap { candidates =>
            if (candidates.nonEmpty) {
              // pick one of the candidates at random. This randomness ensures that we don't get stuck constantly trying
              // and failing to update the same workspace - until all we have left as candidates are un-updatable workspaces
              val (workspaceId, lastModified, cacheLastUpdated) =
                candidates.toArray.apply(scala.util.Random.nextInt(candidates.size))

              setTraceSpanAttribute(rootContext, AttributeKey.stringKey("workspaceId"), workspaceId.toString)
              logger.info(s"EntityStatisticsCacheMonitor starting update attempt for workspace $workspaceId.")
              traceDBIOWithParent("updateStatisticsCache", rootContext) { innerSpan =>
                DBIO.from(updateStatisticsCache(workspaceId, lastModified, innerSpan).map { _ =>
                  val outDated = lastModified.getTime - cacheLastUpdated.getOrElse(MIN_CACHE_TIME).getTime
                  logger.info(s"Updated entity cache for workspace $workspaceId. Cache was ${outDated}ms out of date.")
                  Sweep
                })
              }
            } else {
              traceDBIOWithParent("nothing-to-update", rootContext) { _ =>
                logger.info(s"All workspace entity caches are up to date. Sleeping for $standardPollInterval")
                DBIO.successful(ScheduleDelayedSweep)
              }
            }
          }
        },
        isolationLevel
      )
    }

  private def updateStatisticsCache(workspaceId: UUID,
                                    timestamp: Timestamp,
                                    parentSpan: RawlsTracingContext
  ): Future[Unit] = {
    // allow 80% of the per-workspace timeout to be spent calculating the attribute names.
    // note that other statements do not have timeouts and are unbounded.
    val attrNamesTimeout = (timeoutPerWorkspace * .8).toSeconds.toInt

    val updateFuture = dataSource.inTransaction(
      dataAccess =>
        // TODO: beware contention on the approach of delete-all and batch-insert all below
        // if we see contention we could move to encoding the entire metadata object as json
        // and storing in a single column on WORKSPACE_ENTITY_CACHE
        for {
          // calculate entity statistics
          entityTypesWithCounts <- traceDBIOWithParent("getEntityTypesWithCounts", parentSpan) { _ =>
            dataAccess.entityQuery.getEntityTypesWithCounts(workspaceId)
          }
          // calculate entity attribute statistics
          entityTypesWithAttrNames <- traceDBIOWithParent("getAttrNamesAndEntityTypes", parentSpan) { _ =>
            dataAccess.entityQuery.getAttrNamesAndEntityTypes(workspaceId, attrNamesTimeout)
          }
          _ <- traceDBIOWithParent("saveEntityCache", parentSpan) { _ =>
            dataAccess.entityCacheManagementQuery.saveEntityCache(workspaceId,
                                                                  entityTypesWithCounts,
                                                                  entityTypesWithAttrNames,
                                                                  timestamp
            )
          }
        } yield entityCacheSaveCounter.inc(),
      isolationLevel
    )

    updateFuture.recover { case t: Throwable =>
      logger.error(s"Error updating statistics cache for workspaceId $workspaceId: ${t.getMessage}", t)
      dataSource.inTransaction(
        { dataAccess =>
          logger.error(s"Workspace $workspaceId will be ignored by the entity cache monitor: ${t.getMessage}")
          // We will set the cacheLastUpdated timestamp to the lowest possible value in MySQL.
          // This is a "magic" value that allows the monitor to skip this problematic workspace
          // so it does not get caught in a loop. These workspaces will require manual intervention.
          val errMsg = s"${t.getMessage} ${ExceptionUtils.getStackTrace(t)}"
          dataAccess.entityCacheQuery.updateCacheLastUpdated(workspaceId, MIN_CACHE_TIME, Some(errMsg))
        },
        isolationLevel
      )
    }
  }

  def nowMinus(duration: FiniteDuration): Timestamp = {
    val durationSeconds = duration.toSeconds.toInt
    val nowTime = Calendar.getInstance
    nowTime.add(Calendar.SECOND, durationSeconds * -1)
    new Timestamp(nowTime.getTime.toInstant.toEpochMilli)
  }

}
