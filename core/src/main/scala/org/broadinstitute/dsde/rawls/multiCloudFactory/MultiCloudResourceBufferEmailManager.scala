package org.broadinstitute.dsde.rawls.multiCloudFactory

import com.typesafe.config.Config
import org.broadinstitute.dsde.rawls.config.ResourceBufferConfig

object MultiCloudResourceBufferEmailManager {
  def getMultiCloudResourceBufferEmail(config: Config, cloudProvider: String
                     ): String = {
    cloudProvider match {
      case "gcp" =>
        ResourceBufferConfig(config.getConfig("resourceBuffer")).saEmail
      case "azure" =>
        //Might need to change this to throwing exception
        "defaultString"
      case _ => throw new IllegalArgumentException("Invalid cloud provider")
    }
  }
}
