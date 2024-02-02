package org.broadinstitute.dsde.rawls.multiCloudFactory

import org.broadinstitute.dsde.rawls.config.ResourceBufferConfig
import org.broadinstitute.dsde.rawls.dataaccess.{DisabledResourceBufferService, ResourceBuffer}
import org.broadinstitute.dsde.rawls.dataaccess.resourcebuffer.ResourceBufferDAO
import org.broadinstitute.dsde.rawls.resourcebuffer.ResourceBufferService

object MultiCloudResourceBufferServiceFactory {
  def createResourceBufferService(resourceBufferDAO: ResourceBufferDAO,
                                  resourceBufferConfig: ResourceBufferConfig,
                                  cloudProvider: String): ResourceBuffer = {
    cloudProvider match {
      case "gcp" =>
        new ResourceBufferService(resourceBufferDAO, resourceBufferConfig)
      case "azure" =>
        new DisabledResourceBufferService
      case _ => throw new IllegalArgumentException("Invalid cloud provider")
    }
  }
}
