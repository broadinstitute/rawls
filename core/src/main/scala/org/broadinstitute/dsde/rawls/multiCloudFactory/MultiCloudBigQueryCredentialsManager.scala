package org.broadinstitute.dsde.rawls.multiCloudFactory

import org.broadinstitute.dsde.rawls.config.MultiCloudAppConfigManager
import org.broadinstitute.dsde.rawls.model.WorkspaceCloudPlatform.{Azure, Gcp}

import scala.util.Using

object MultiCloudBigQueryCredentialsManager {
  def getMultiCloudBucketMigrationService(appConfigManager: MultiCloudAppConfigManager): String = {
    appConfigManager.cloudProvider match {
      case Gcp =>
        val pathToBqJson = appConfigManager.gcsConfig.getString("pathToBigQueryJson")
        Using(scala.io.Source.fromFile(pathToBqJson))(_.mkString).getOrElse {
          throw new IllegalArgumentException("Unable to read BigQuery JSON credentials")
        }
      case Azure => "defaultCredentials"
    }
  }
}
