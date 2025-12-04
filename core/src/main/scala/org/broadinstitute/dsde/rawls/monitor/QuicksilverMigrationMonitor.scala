package org.broadinstitute.dsde.rawls.monitor

import akka.actor.{Actor, PoisonPill, Props}
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.dataaccess.SlickDataSource
import org.broadinstitute.dsde.rawls.dataaccess.slick.{DataAccess, EntityCorrection, ReadWriteAction}
import org.broadinstitute.dsde.rawls.model.AttributeName
import org.broadinstitute.dsde.rawls.monitor.AttributeCorrectionStatusType.AttributeCorrectionStatusType
import org.broadinstitute.dsde.rawls.monitor.EntityCorrectionStatusType.EntityCorrectionStatusType
import org.broadinstitute.dsde.rawls.monitor.QuicksilverMigrationMonitor.{
  AllDone,
  Init,
  NextBatch,
  ProcessBatch,
  QuicksilverMigrationMonitorConfig
}
import slick.dbio.DBIO

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

object QuicksilverMigrationMonitor {

  // config
  final case class QuicksilverMigrationMonitorConfig(startupDelay: FiniteDuration,
                                                     pollInterval: FiniteDuration,
                                                     batchSize: Int,
                                                     dryRun: Boolean = true
  )

  // actor messages
  sealed trait QuicksilverMonitorMessage
  private case object Init extends QuicksilverMonitorMessage
  private case object NextBatch extends QuicksilverMonitorMessage
  private case class ProcessBatch(corrections: List[EntityCorrection])
  private case object AllDone extends QuicksilverMonitorMessage

  def props(
    config: QuicksilverMigrationMonitorConfig,
    dataSource: SlickDataSource
  ): Props =
    Props(
      new QuicksilverMigrationMonitor(config, dataSource)
    )

}

class QuicksilverMigrationMonitor(config: QuicksilverMigrationMonitorConfig, dataSource: SlickDataSource)
    extends Actor
    with QuicksilverMigrationMonitorSupport
    with LazyLogging {

  import context._

  override def receive: Receive = {
    case Init                                              => startAll()
    case NextBatch                                         => nextBatch()
    case ProcessBatch(corrections: List[EntityCorrection]) => processBatch(corrections)
    case AllDone                                           => self ! PoisonPill
  }

  private def startAll() =
    context.system.scheduler.scheduleOnce(config.startupDelay, self, NextBatch)

  private def nextBatch() =
    dataSource.inTransaction { dataAccess =>
      // select the next ${config.batchSize} rows from ENTITY_CORRECTIONS
      dataAccess.compactEntityQuery.getNextCorrectionBatch(config.batchSize) map { batch =>
        if (batch.isEmpty) {
          logger.info("********** all corrections complete **********")
          self ! AllDone
        } else {
          self ! ProcessBatch(batch)
        }
      }
    }

  /**
   * Compare this batch of corrections to the current entities.
   */
  private def processBatch(corrections: List[EntityCorrection]): Future[Int] = {
    // loop over each corrected entity from ENTITY_CORRECTIONS
    val futures = corrections.map { correction =>
      dataSource.inTransaction { dataAccess =>
        // TODO: compare this workspace's last-modified date to its migration date
        for {
          // retrieve the corresponding current entity from ENTITY
          currentEntity <- dataAccess.compactEntityQuery.getEntity(correction.workspaceId,
                                                                   correction.entityType,
                                                                   correction.entityName
          )
          // compare the corrected entity to the current entity
          (entityStatus, attributeStatusMap) = compareEntities(currentEntity, correction)
          // persist the analysis result
          numUpdated <- persistAnalysisResult(dataAccess, correction, entityStatus, attributeStatusMap)
        } yield {
          // move on to the next batch
          context.system.scheduler.scheduleOnce(config.pollInterval, self, NextBatch)
          numUpdated
        }
      }
    }
    Future.sequence(futures).map { counts =>
      val rowsUpdated = counts.sum
      logger.info(s"processed batch of ${corrections.size} corrections with $rowsUpdated rows updated.")
      rowsUpdated
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
    logger.info(
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
        dataAccess.compactEntityQuery.updateAttributeStatuses(correction.workspaceId,
                                                              correction.entityType,
                                                              correction.entityName,
                                                              attributeStatusMap
        )
      performCorrections <-
        if (config.dryRun) {
          DBIO.successful(0)
        } else {
          // TODO: write corrected entities back to the ENTITY table
          DBIO.successful(0)
        }
    } yield persistEntityStatus + persistAttributeStatuses
  }

}
