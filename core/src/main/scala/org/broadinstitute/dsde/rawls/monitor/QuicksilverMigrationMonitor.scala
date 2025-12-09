package org.broadinstitute.dsde.rawls.monitor

import akka.actor.{Actor, PoisonPill, Props}
import akka.pattern.pipe
import akka.util.Timeout
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.dataaccess.SlickDataSource
import org.broadinstitute.dsde.rawls.dataaccess.slick.{DataAccess, EntityCorrection, ReadWriteAction}
import org.broadinstitute.dsde.rawls.model.AttributeName
import org.broadinstitute.dsde.rawls.monitor.AttributeCorrectionStatus.AttributeCorrectionStatusType
import org.broadinstitute.dsde.rawls.monitor.EntityCorrectionStatus.EntityCorrectionStatusType
import org.broadinstitute.dsde.rawls.monitor.QuicksilverMigrationMonitor._
import slick.dbio.DBIO

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

object QuicksilverMigrationMonitor {

  // config
  final case class QuicksilverMigrationMonitorConfig(startupDelay: FiniteDuration,
                                                     pollInterval: FiniteDuration,
                                                     batchTimeout: Timeout,
                                                     batchSize: Int,
                                                     dryRun: Boolean = true
  )

  // actor messages
  sealed private trait QuicksilverMonitorMessage
  private case object Init extends QuicksilverMonitorMessage
  private case object CountOutstanding extends QuicksilverMonitorMessage
  private case class NextBatch(iteration: Int, expectedIterations: Int) extends QuicksilverMonitorMessage
  private case class ProcessBatch(corrections: List[EntityCorrection], iteration: Int, expectedIterations: Int)
      extends QuicksilverMonitorMessage
  private case object AllDone extends QuicksilverMonitorMessage

  // actor-creator
  def props(
    config: QuicksilverMigrationMonitorConfig,
    dataSource: SlickDataSource
  ): Props =
    Props(
      new QuicksilverMigrationMonitor(config, dataSource)
    )

}

// the actor
private class QuicksilverMigrationMonitor(config: QuicksilverMigrationMonitorConfig, dataSource: SlickDataSource)
    extends Actor
    with QuicksilverMigrationMonitorSupport
    with LazyLogging {

  import context._

  implicit val askTimeout: Timeout = config.batchTimeout

  // routing
  override def receive: Receive = {
    case Init                                               => startAll()
    case CountOutstanding                                   => countOutstanding() pipeTo self
    case NextBatch(iteration: Int, expectedIterations: Int) => nextBatch(iteration, expectedIterations) pipeTo self
    case ProcessBatch(corrections: List[EntityCorrection], iteration: Int, expectedIterations: Int) =>
      processBatch(corrections, iteration, expectedIterations) pipeTo self
    case AllDone => self ! PoisonPill
    case x       =>
      logger.error(s"Unexpected message: ${x.getClass.getName}: $x")
      self ! PoisonPill
  }

  /** kick things off */
  logger.info("initializing ...")
  self ! Init

  /** wait for the startupDelay, start the actor doing work */
  private def startAll() = {
    logger.info("starting ...")
    context.system.scheduler.scheduleOnce(config.startupDelay, self, CountOutstanding)
  }

  /** count the outstanding corrections so log messages can show progress */
  private def countOutstanding(): Future[QuicksilverMonitorMessage] = {
    logger.info("counting outstanding corrections ...")
    dataSource.inTransaction { dataAccess =>
      dataAccess.compactEntityQuery.countOutstandingCorrections map { count =>
        val expectedIterations = Math.ceil(count / config.batchSize).toInt
        NextBatch(1, expectedIterations)
      }
    }
  }

  /** query for the next batch of corrections */
  private def nextBatch(iteration: Int, expectedIterations: Int): Future[QuicksilverMonitorMessage] = {
    logger.debug(s"querying for next batch (${config.batchSize}) ...")
    dataSource.inTransaction { dataAccess =>
      // select the next ${config.batchSize} rows from ENTITY_CORRECTIONS
      dataAccess.compactEntityQuery.getNextCorrectionBatch(config.batchSize) map { batch =>
        if (batch.isEmpty) {
          logger.info("********** all corrections complete **********")
          AllDone
        } else {
          logger.debug(s"... retrieved ${batch.size} corrections ...")
          ProcessBatch(batch, iteration, expectedIterations)
        }
      }
    } recover { case t: Throwable =>
      logger.error(s"${t.getClass.getName}: ${t.getMessage}")
      throw t
    }
  }

  /**
   * Compare this batch of corrections to the current entities.
   */
  private def processBatch(corrections: List[EntityCorrection],
                           iteration: Int,
                           expectedIterations: Int
  ): Future[QuicksilverMonitorMessage] = {
    logger.debug(s"processing batch of ${corrections.size} corrections ...")
    // loop over each corrected entity from ENTITY_CORRECTIONS
    val futures = corrections.map { correction =>
      dataSource.inTransaction { dataAccess =>
        for {
          // retrieve the corresponding current entity from ENTITY
          currentEntity <- dataAccess.compactEntityQuery.getEntity(correction.workspaceId,
                                                                   correction.entityType,
                                                                   correction.entityName
          )
          // compare the corrected entity to the current entity
          (entityStatus, attributeStatusMap) = compareEntities(currentEntity.map(_.toEntity), correction)
          // persist the analysis result
          numUpdated <- persistAnalysisResult(dataAccess, correction, entityStatus, attributeStatusMap)
        } yield numUpdated
      }
    }
    Future.sequence(futures).map { counts =>
      val rowsUpdated = counts.sum
      logger.info(
        f"($iteration%,d/$expectedIterations%,d): processed batch of ${corrections.size} corrections with $rowsUpdated rows updated."
      )
      NextBatch(iteration + 1, expectedIterations)
    } recover { case t: Throwable =>
      logger.error(s"${t.getClass.getName}: ${t.getMessage}")
      throw t
    }
  }

  /**
   * Write the comparison results back to the database, updating ENTITY_CORRECTIONS
   * and upserting to ATTRIBUTE_CORRECTIONS
   */
  private def persistAnalysisResult(dataAccess: DataAccess,
                                    correction: EntityCorrection,
                                    entityStatus: EntityCorrectionStatusType,
                                    attributeStatusMap: Map[AttributeName, AttributeCorrectionStatusType]
  ): ReadWriteAction[Int] = {
    logger.trace(
      s"[${correction.workspaceId}] ${correction.entityType}/${correction.entityName}: $entityStatus (${attributeStatusMap.size})"
    )
    for {
      persistEntityStatus <-
        dataAccess.compactEntityQuery.updateCorrectionStatus(correction.workspaceId,
                                                             correction.entityType,
                                                             correction.entityName,
                                                             entityStatus
        )
      persistAttributeStatuses <-
        dataAccess.compactEntityQuery.updateAttributeStatuses(correction.id, attributeStatusMap)
      _ <-
        if (config.dryRun) {
          DBIO.successful(0)
        } else {
          // TODO: write corrected entities back to the ENTITY table
          DBIO.successful(0)
        }
    } yield persistEntityStatus + persistAttributeStatuses
  }

}
