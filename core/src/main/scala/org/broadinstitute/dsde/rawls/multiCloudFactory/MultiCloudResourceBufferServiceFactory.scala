package org.broadinstitute.dsde.rawls.multiCloudFactory

import org.broadinstitute.dsde.rawls.config.{MultiCloudAppConfigManager, ResourceBufferConfig}
import org.broadinstitute.dsde.rawls.dataaccess.ResourceBuffer
import org.broadinstitute.dsde.rawls.dataaccess.disabled.DisabledResourceBufferService
import org.broadinstitute.dsde.rawls.dataaccess.resourcebuffer.ResourceBufferDAO
import org.broadinstitute.dsde.rawls.resourcebuffer.ResourceBufferService

object MultiCloudResourceBufferServiceFactory {
  def createResourceBufferService(appConfigManager: MultiCloudAppConfigManager,
                                  resourceBufferDAO: ResourceBufferDAO
                                  ): ResourceBuffer = {
    appConfigManager.cloudProvider match {
      case "gcp" =>
        new ResourceBufferService(resourceBufferDAO, ResourceBufferConfig(appConfigManager.conf.getConfig("resourceBuffer")))
      case "azure" =>
        new DisabledResourceBufferService
      case _ => throw new IllegalArgumentException("Invalid cloud provider")
    }
  }
}
