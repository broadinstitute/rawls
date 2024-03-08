package org.broadinstitute.dsde.rawls.serviceFactory

import org.broadinstitute.dsde.rawls.config.{RawlsConfigManager, ResourceBufferConfig}
import org.broadinstitute.dsde.rawls.dataaccess.ResourceBuffer
import org.broadinstitute.dsde.rawls.dataaccess.resourcebuffer.ResourceBufferDAO
import org.broadinstitute.dsde.rawls.model.WorkspaceCloudPlatform.{Azure, Gcp}
import org.broadinstitute.dsde.rawls.serviceFactory.DisabledServiceFactory.newDisabledService
import org.broadinstitute.dsde.rawls.resourcebuffer.ResourceBufferService

object ResourceBufferServiceFactory {
  def createResourceBufferService(appConfigManager: RawlsConfigManager,
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
