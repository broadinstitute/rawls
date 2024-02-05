package org.broadinstitute.dsde.rawls.multiCloudFactory

import com.typesafe.config.Config
import org.broadinstitute.dsde.rawls.dataaccess.{DisabledSubmissionCostService, SubmissionCost, SubmissionCostService}
import org.broadinstitute.dsde.workbench.google.GoogleBigQueryDAO

import scala.concurrent.ExecutionContext

object MultiCloudSubmissionCostServiceFactory {
  def createMultiCloudSubmissionCostService(bigQueryDAO: GoogleBigQueryDAO, config: Config, cloudProvider: String
                                           )(implicit executionContext: ExecutionContext): SubmissionCost = {
    cloudProvider match{
      case "gcp" =>
        SubmissionCostService.constructor(
          config.getString("billingExportTableName"),
          config.getString("billingExportDatePartitionColumn"),
          config.getString("serviceProject"),
          config.getInt("billingSearchWindowDays"),
          bigQueryDAO
        )
      case "azure" =>
        DisabledSubmissionCostService.constructor
      case _ => throw new IllegalArgumentException("Invalid cloud provider")
    }
  }
}
