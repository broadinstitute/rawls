package org.broadinstitute.dsde.rawls.multiCloudFactory

import akka.actor.ActorSystem
import org.broadinstitute.dsde.rawls.config.{MultiCloudAppConfigManager, ServicePerimeterServiceConfig}
import org.broadinstitute.dsde.rawls.dataaccess.{GoogleServicesDAO, SlickDataSource}
import org.broadinstitute.dsde.rawls.multiCloudFactory.DisabledServiceFactory.newDisabledService
import org.broadinstitute.dsde.rawls.serviceperimeter.{ServicePerimeter, ServicePerimeterService}

import scala.concurrent.ExecutionContext

object MultiCloudServicePerimeterServiceFactory {
  def createMultiCloudNotificationPubSubDAO(appConfigManager: MultiCloudAppConfigManager,
                                            slickDataSource: SlickDataSource,
                                            gcsDAO: GoogleServicesDAO
  )(implicit system: ActorSystem, executionContext: ExecutionContext): ServicePerimeter =
    appConfigManager.cloudProvider match {
      case "gcp" =>
        val servicePerimeterConfig = ServicePerimeterServiceConfig(appConfigManager.conf)
        new ServicePerimeterService(slickDataSource, gcsDAO, servicePerimeterConfig)
      case "azure" =>
        newDisabledService[ServicePerimeter]
      case _ => throw new IllegalArgumentException("Invalid cloud provider")
    }
}
