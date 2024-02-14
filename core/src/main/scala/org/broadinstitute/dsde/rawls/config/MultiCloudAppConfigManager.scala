package org.broadinstitute.dsde.rawls.config

import com.typesafe.config.{Config, ConfigFactory}

class MultiCloudAppConfigManager {
  val conf = ConfigFactory.parseResources("version.conf").withFallback(ConfigFactory.load())
  val cloudProvider = getCloudProvider(conf)
  val gcsConfig = getStorageConfiguration(conf, cloudProvider)
  private def getCloudProvider(config:Config): String = {
    if (config.hasPath("cloudProvider")) {
      config.getString("cloudProvider") match {
        case "azure" =>
          "azure"
        case "gcp" =>
          "gcp"
        case _ => throw new IllegalArgumentException("Invalid cloud provider")
      }
    }
    //This can be removed once the variable has been added to config file
    else {
      "gcp"
    }
  }
  private def getStorageConfiguration(config:Config, cloudProvider: String): Config = {
    cloudProvider match {
      //azure config has not been created yet - needs to be updated
      case "azure" =>
        config.getConfig("azure")
      case "gcp" =>
        config.getConfig("gcs")
      case _ => throw new IllegalArgumentException("Invalid cloud provider")
    }
  }
}
