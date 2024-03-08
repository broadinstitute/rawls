package org.broadinstitute.dsde.rawls.serviceFactory

import org.broadinstitute.dsde.rawls.config.MultiCloudAppConfigManager

import scala.util.Using

object BigQueryCredentialsManager {
  def getBigQueryCredentials(appConfigManager: MultiCloudAppConfigManager): String =
    appConfigManager.gcsConfig match {
      case Some(gcsConfig) =>
        val pathToBqJson = gcsConfig.getString("pathToBigQueryJson")
        Using(scala.io.Source.fromFile(pathToBqJson))(_.mkString).getOrElse {
          throw new IllegalArgumentException("Unable to read BigQuery JSON credentials")
        }
      case None => "unsupported"
    }
}
