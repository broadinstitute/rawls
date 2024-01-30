package org.broadinstitute.dsde.rawls.multiCloudFactory
import com.typesafe.config.Config

object MultiCloudStorageConfigFactory {
  def createStorageConfiguration(config:Config, cloudProvider: String): Config = {
    cloudProvider match {
      //azure config has not been created yet
      case "azure" =>
        config.getConfig("azureStorage")
      case "gcp" =>
        config.getConfig("gcs")
    }
  }
}
