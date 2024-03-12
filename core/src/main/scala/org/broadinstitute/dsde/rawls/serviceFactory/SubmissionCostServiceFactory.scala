package org.broadinstitute.dsde.rawls.serviceFactory

import org.broadinstitute.dsde.rawls.config.RawlsConfigManager
import org.broadinstitute.dsde.rawls.dataaccess.{SubmissionCostService, SubmissionCostServiceImpl}
import org.broadinstitute.dsde.rawls.serviceFactory.DisabledServiceFactory.newDisabledService
import org.broadinstitute.dsde.workbench.google.GoogleBigQueryDAO

import scala.concurrent.ExecutionContext

object SubmissionCostServiceFactory {
  def createSubmissionCostService(appConfigManager: RawlsConfigManager, bigQueryDAO: GoogleBigQueryDAO)(implicit
    executionContext: ExecutionContext
  ): SubmissionCostService =
    appConfigManager.gcsConfig match {
      case Some(gcsConfig) =>
        SubmissionCostServiceImpl.constructor(
          gcsConfig.getString("billingExportTableName"),
          gcsConfig.getString("billingExportDatePartitionColumn"),
          gcsConfig.getString("serviceProject"),
          gcsConfig.getInt("billingSearchWindowDays"),
          bigQueryDAO
        )
      case None =>
        newDisabledService[SubmissionCostService]
    }
}
