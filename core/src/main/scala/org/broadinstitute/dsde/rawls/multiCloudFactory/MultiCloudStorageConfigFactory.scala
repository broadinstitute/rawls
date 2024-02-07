package org.broadinstitute.dsde.rawls.multiCloudFactory
import com.typesafe.config.Config

object MultiCloudStorageConfigFactory {
  def createStorageConfiguration(config:Config, cloudProvider: String): Config = {
    cloudProvider match {
      //azure config has not been created yet - needs to be updated
      case "azure" =>
        config.getConfig("azure")
      case "gcp" =>
        config.getConfig("gcs")
    }
  }
}
