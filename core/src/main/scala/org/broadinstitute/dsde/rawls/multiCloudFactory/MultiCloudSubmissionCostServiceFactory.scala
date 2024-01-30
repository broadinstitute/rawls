package org.broadinstitute.dsde.rawls.multiCloudFactory

import akka.actor.ActorSystem
import com.typesafe.config.Config
import org.broadinstitute.dsde.rawls.dataaccess.{DisabledSubmissionCostService, SubmissionCostService}
import org.broadinstitute.dsde.workbench.google.{GoogleBigQueryDAO, HttpGoogleBigQueryDAO}

import scala.concurrent.ExecutionContext

object MultiCloudSubmissionCostServiceFactory {
  def createMultiCloudSubmissionCostService(bigQueryDAO: GoogleBigQueryDAO,
                                            config: Config,
                                            cloudProvider: String
                                           )(implicit system: ActorSystem, executionContext: ExecutionContext): SubmissionCostService = {
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
        DisabledSubmissionCostService.constructor(
          config.getString("billingExportTableName"),
          config.getString("billingExportDatePartitionColumn"),
          config.getString("serviceProject"),
          config.getInt("billingSearchWindowDays"),
          bigQueryDAO
        )
      case _ => throw new IllegalArgumentException("Invalid cloud provider")
    }
  }
}
