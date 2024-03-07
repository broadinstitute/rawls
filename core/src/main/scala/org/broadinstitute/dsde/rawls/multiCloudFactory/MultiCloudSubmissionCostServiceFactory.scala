package org.broadinstitute.dsde.rawls.multiCloudFactory

import org.broadinstitute.dsde.rawls.config.MultiCloudAppConfigManager
import org.broadinstitute.dsde.rawls.dataaccess.{SubmissionCost, SubmissionCostService}
import org.broadinstitute.dsde.rawls.multiCloudFactory.DisabledServiceFactory.newDisabledService
import org.broadinstitute.dsde.workbench.google.GoogleBigQueryDAO

import scala.concurrent.ExecutionContext

object MultiCloudSubmissionCostServiceFactory {
  def createMultiCloudSubmissionCostService(appConfigManager: MultiCloudAppConfigManager,
                                            bigQueryDAO: GoogleBigQueryDAO
  )(implicit executionContext: ExecutionContext): SubmissionCost =
    appConfigManager.gcsConfig match {
      case Some(gcsConfig) =>
        SubmissionCostService.constructor(
          gcsConfig.getString("billingExportTableName"),
          gcsConfig.getString("billingExportDatePartitionColumn"),
          gcsConfig.getString("serviceProject"),
          gcsConfig.getInt("billingSearchWindowDays"),
          bigQueryDAO
        )
      case None =>
        newDisabledService[SubmissionCost]
    }
}
