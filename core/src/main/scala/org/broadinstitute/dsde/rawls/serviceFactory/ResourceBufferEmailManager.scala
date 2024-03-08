package org.broadinstitute.dsde.rawls.serviceFactory

import org.broadinstitute.dsde.rawls.config.{MultiCloudAppConfigManager, ResourceBufferConfig}
import org.broadinstitute.dsde.rawls.model.WorkspaceCloudPlatform.{Azure, Gcp}

object ResourceBufferEmailManager {
  def getResourceBufferEmail(appConfigManager: MultiCloudAppConfigManager): String =
    appConfigManager.cloudProvider match {
      case Gcp =>
        ResourceBufferConfig(appConfigManager.conf.getConfig("resourceBuffer")).saEmail
      case Azure =>
        // Might need to change this to throwing exception
        "defaultString"
    }
}
