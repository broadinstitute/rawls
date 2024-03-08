package org.broadinstitute.dsde.rawls.serviceFactory

import org.broadinstitute.dsde.rawls.config.RawlsConfigManager

import scala.util.Using

object BigQueryCredentialsManager {
  def getBigQueryCredentials(appConfigManager: RawlsConfigManager): String =
    appConfigManager.gcsConfig match {
      case Some(gcsConfig) =>
        val pathToBqJson = gcsConfig.getString("pathToBigQueryJson")
        Using(scala.io.Source.fromFile(pathToBqJson))(_.mkString).getOrElse {
          throw new IllegalArgumentException("Unable to read BigQuery JSON credentials")
        }
      case None => "unsupported"
    }
}
