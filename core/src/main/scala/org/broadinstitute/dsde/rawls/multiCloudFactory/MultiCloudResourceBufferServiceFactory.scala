package org.broadinstitute.dsde.rawls.multiCloudFactory

import org.broadinstitute.dsde.rawls.config.{MultiCloudAppConfigManager, ResourceBufferConfig}
import org.broadinstitute.dsde.rawls.dataaccess.ResourceBuffer
import org.broadinstitute.dsde.rawls.dataaccess.resourcebuffer.ResourceBufferDAO
import org.broadinstitute.dsde.rawls.model.WorkspaceCloudPlatform.{Azure, Gcp}
import org.broadinstitute.dsde.rawls.multiCloudFactory.DisabledServiceFactory.newDisabledService
import org.broadinstitute.dsde.rawls.resourcebuffer.ResourceBufferService

object MultiCloudResourceBufferServiceFactory {
  def createResourceBufferService(appConfigManager: MultiCloudAppConfigManager,
                                  resourceBufferDAO: ResourceBufferDAO
  ): ResourceBuffer =
    appConfigManager.cloudProvider match {
      case Gcp =>
        new ResourceBufferService(resourceBufferDAO,
                                  ResourceBufferConfig(appConfigManager.conf.getConfig("resourceBuffer"))
        )
      case Azure =>
        newDisabledService[ResourceBuffer]
    }
}
