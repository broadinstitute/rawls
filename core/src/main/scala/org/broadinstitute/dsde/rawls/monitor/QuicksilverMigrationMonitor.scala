package org.broadinstitute.dsde.rawls.monitor

import akka.actor.{Actor, PoisonPill, Props}
import akka.pattern.pipe
import akka.util.Timeout
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.dataaccess.SlickDataSource
import org.broadinstitute.dsde.rawls.dataaccess.slick.{
  CompactEntityRecord,
  DataAccess,
  EntityCorrection,
  ReadWriteAction
}
import org.broadinstitute.dsde.rawls.model.AttributeName
import org.broadinstitute.dsde.rawls.monitor.AttributeCorrectionStatus.AttributeCorrectionStatusType
import org.broadinstitute.dsde.rawls.monitor.EntityCorrectionStatus.EntityCorrectionStatusType
import org.broadinstitute.dsde.rawls.monitor.QuicksilverMigrationMonitor._
import slick.dbio.DBIO

import java.util.UUID
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
                           iteration: Int, // for logging only
                           expectedIterations: Int // for logging only
  ): Future[QuicksilverMonitorMessage] = {
    logger.debug(s"processing batch of ${corrections.size} corrections ...")

    // collapse duplicate corrections (can happen when multiple attrs in the same type)
    val deduplicatedCorrections: List[EntityCorrection] = corrections
      .groupMap(_.id)(identity)
      .values
      .map(_.head)
      .toList
    // group corrections by workspace
    val groupedCorrections: Map[UUID, List[EntityCorrection]] =
      deduplicatedCorrections.groupMap(_.workspaceId)(identity)

    val dbFuture = dataSource.inTransaction { dataAccess =>
      // loop over each workspace represented in this batch of corrections
      val dbActions = groupedCorrections.map { case (workspaceId, workspaceCorrections) =>
        for {

          // re-verify workspace has not been modified since its migration.
          // we have to do this again even though getNextCorrectionBatch checked it already;
          // we do it again inside this db transaction to avoid any race conditions
          isWorkspaceModified <- checkWorkspaceModified(dataAccess, workspaceId)

          correctionAttempts =
            if (isWorkspaceModified) {
              // if the workspace has been modified since the migration, do nothing
              List()
            } else {
              // loop over each corrected entity in this workspace
              workspaceCorrections.map { correction =>
                for {
                  // retrieve the corresponding current entity from ENTITY
                  currentEntity <- dataAccess.compactEntityQuery.getEntity(correction.workspaceId,
                                                                           correction.entityType,
                                                                           correction.entityName
                  )
                  // compare the corrected entity to the current entity
                  (entityStatus, attributeStatusMap) = compareEntities(currentEntity.map(_.toEntity), correction)
                  // write the corrections back to the current entity
                  (numWrites, updatedEntityStatus, updatedAttrStatusMap) <- persistCorrectedEntity(dataAccess,
                                                                                                   correction,
                                                                                                   currentEntity,
                                                                                                   entityStatus,
                                                                                                   attributeStatusMap
                  )
                  // persist the analysis/correction result
                  _ <- persistAnalysisResult(dataAccess, correction, updatedEntityStatus, updatedAttrStatusMap)
                } yield numWrites
              }

            }
          attemptResult <- DBIO.sequence(correctionAttempts)

        } yield attemptResult
      }
      DBIO.sequence(dbActions)
    }

    dbFuture.map { counts =>
      val rowsUpdated = counts.flatten.sum
      logger.info(
        f"($iteration%,d/$expectedIterations%,d): processed batch of ${deduplicatedCorrections.size} corrections with $rowsUpdated attributes corrected."
      )
      NextBatch(iteration + 1, expectedIterations)
    } recover { case t: Throwable =>
      logger.error(s"${t.getClass.getName}: ${t.getMessage}")
      throw t
    }
  }

  private def checkWorkspaceModified(dataAccess: DataAccess, workspaceId: UUID): ReadWriteAction[Boolean] =
    for {
      // re-verify workspace has not been modified since its migration.
      // we have to do this again even though getNextCorrectionBatch checked it already;
      // we do it again inside this db transaction to avoid any race conditions
      isWorkspaceModifiedAfterMigration <- dataAccess.compactEntityQuery.checkWorkspaceLastModified(workspaceId)

      // verify workspace has not run a workflow since its migration;
      // only check this if the last-modified check above returned false (if it returned true or None,
      // this workflow check is redundant)
      isWorkflowRunAfterMigration <-
        if (isWorkspaceModifiedAfterMigration.contains(false)) {
          dataAccess.compactEntityQuery.checkWorkspaceLastWorkflowRun(workspaceId)
        } else {
          DBIO.successful(None)
        }

      // if isWorkspaceModifiedAfterMigration is None, the workspace doesn't exist;
      //     mark all corrections as "the workspace is gone"
      _ <-
        if (isWorkspaceModifiedAfterMigration.isEmpty) {
          dataAccess.compactEntityQuery.updateWorkspaceGone(workspaceId)
        } else {
          DBIO.successful(0)
        }

      // if isWorkspaceModifiedAfterMigration contains true, the workspace has been
      //     modified since it was migrated to Quicksilver;
      // if isWorkflowRunAfterMigration contains true, the workspace has run a workflow
      //     since it was migrated to Quicksilver;
      // in either case, mark all corrections as "the workspace is modified"
      _ <-
        if (isWorkspaceModifiedAfterMigration.contains(true) || isWorkflowRunAfterMigration.contains(true)) {
          dataAccess.compactEntityQuery.updateWorkspaceModified(workspaceId)
        } else {
          DBIO.successful(0)
        }

    } yield isWorkflowRunAfterMigration.contains(true) || isWorkspaceModifiedAfterMigration.contains(true)

  // TODO CTM-256: unit tests
  private def persistCorrectedEntity(dataAccess: DataAccess,
                                     correction: EntityCorrection,
                                     currentEntityOption: Option[CompactEntityRecord],
                                     entityStatus: EntityCorrectionStatusType,
                                     attributeStatusMap: Map[AttributeName, AttributeCorrectionStatusType]
  ): ReadWriteAction[(Int, EntityCorrectionStatusType, Map[AttributeName, AttributeCorrectionStatusType])] =
    if (config.dryRun) {
      // if this is a dry run, don't do anything
      DBIO.successful(0, entityStatus, attributeStatusMap)
    } else {
      currentEntityOption match {
        case None =>
          // if the current entity is gone, just return
          DBIO.successful(0, entityStatus, attributeStatusMap)
        case Some(currentEntityRecord) =>
          // find the correctable attributes
          val correctableAttrNames = attributeStatusMap.filter(_._2 == AttributeCorrectionStatus.Reordered).keySet
          // reduce the correction's AttributeMap to only those which are correctable
          val correctedAttrs = correction.attributes.filter { case (attrName, _) =>
            correctableAttrNames.contains(attrName)
          }
          // layer the corrected attributes on top of the current
          val currentEntity = currentEntityRecord.toEntity
          val entityToSave = currentEntity.copy(attributes = currentEntity.attributes ++ correctedAttrs)

          for {
            // Rewrite the original entity to include the reordered result, then persist as corrected
            saveResult <-
              dataAccess.compactEntityQuery.batchWriteEntities(workspaceId = correction.workspaceId,
                                                               entities = Seq(entityToSave),
                                                               insertOnly = false
              )
            // update the attribute status map to include the corrected attrs
            updatedAttrStatusMap = attributeStatusMap ++ correctableAttrNames
              .map(_ -> AttributeCorrectionStatus.Corrected)
              .toMap
            updatedEntityStatus = calculateEntityStatus(updatedAttrStatusMap)
          } yield (saveResult, updatedEntityStatus, updatedAttrStatusMap)
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
    } yield persistEntityStatus + persistAttributeStatuses
  }

}
