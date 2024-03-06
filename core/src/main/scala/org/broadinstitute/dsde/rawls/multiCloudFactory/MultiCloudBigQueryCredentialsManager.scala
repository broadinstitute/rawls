package org.broadinstitute.dsde.rawls.multiCloudFactory

import org.broadinstitute.dsde.rawls.config.MultiCloudAppConfigManager

import scala.util.Using

object MultiCloudBigQueryCredentialsManager {
  def getMultiCloudBucketMigrationService(appConfigManager: MultiCloudAppConfigManager): String = {
    appConfigManager.cloudProvider match {
      case "gcp" =>
        val pathToBqJson = appConfigManager.gcsConfig.getString("pathToBigQueryJson")
        Using(scala.io.Source.fromFile(pathToBqJson))(_.mkString).getOrElse {
          throw new IllegalArgumentException("Unable to read BigQuery JSON credentials")
        }
      case "azure" => "defaultCredentials"
      case _ => throw new IllegalArgumentException("Invalid cloud provider")
    }
  }
}
