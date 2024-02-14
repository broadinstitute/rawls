package org.broadinstitute.dsde.rawls.multiCloudFactory

import org.broadinstitute.dsde.rawls.config.{MultiCloudAppConfigManager, ResourceBufferConfig}

object MultiCloudResourceBufferEmailManager {
  def getMultiCloudResourceBufferEmail(appConfigManager: MultiCloudAppConfigManager): String = {
    appConfigManager.cloudProvider match {
      case "gcp" =>
        ResourceBufferConfig(appConfigManager.conf.getConfig("resourceBuffer")).saEmail
      case "azure" =>
        //Might need to change this to throwing exception
        "defaultString"
      case _ => throw new IllegalArgumentException("Invalid cloud provider")
    }
  }
}
