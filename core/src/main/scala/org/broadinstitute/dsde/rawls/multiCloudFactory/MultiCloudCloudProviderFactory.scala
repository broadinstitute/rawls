package org.broadinstitute.dsde.rawls.multiCloudFactory

import com.typesafe.config.Config

object MultiCloudCloudProviderFactory {
  def setCloudProvider(config:Config): String = {
    if (config.hasPath("cloudProvider")) {
      config.getString("cloudProvider") match {
        case "azure" =>
          "azure"
        case "gcp" =>
          "gcp"
      }
    }
    //This can be removed once the variable has been added to config file
    else {
      "gcp"
    }
  }

}
