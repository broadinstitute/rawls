package org.broadinstitute.dsde.rawls.multiCloudFactory

import com.typesafe.config.Config
import org.broadinstitute.dsde.rawls.config.MultiCloudAppConfigManager
import org.broadinstitute.dsde.rawls.dataaccess.{SubmissionCost, SubmissionCostService}
import org.broadinstitute.dsde.rawls.disabled.DisabledSubmissionCostService
import org.broadinstitute.dsde.workbench.google.GoogleBigQueryDAO

import scala.concurrent.ExecutionContext

object MultiCloudSubmissionCostServiceFactory {
  def createMultiCloudSubmissionCostService(appConfigManager: MultiCloudAppConfigManager,
                                            bigQueryDAO: GoogleBigQueryDAO
                                           )(implicit executionContext: ExecutionContext): SubmissionCost = {
    appConfigManager.cloudProvider match{
      case "gcp" =>
        val gcsConfig = appConfigManager.gcsConfig
        SubmissionCostService.constructor(
          gcsConfig.getString("billingExportTableName"),
          gcsConfig.getString("billingExportDatePartitionColumn"),
          gcsConfig.getString("serviceProject"),
          gcsConfig.getInt("billingSearchWindowDays"),
          bigQueryDAO
        )
      case "azure" =>
        DisabledSubmissionCostService.constructor
      case _ => throw new IllegalArgumentException("Invalid cloud provider")
    }
  }
}
