package org.broadinstitute.dsde.rawls.serviceFactory

import akka.actor.ActorSystem
import org.broadinstitute.dsde.rawls.config.{RawlsConfigManager, ServicePerimeterServiceConfig}
import org.broadinstitute.dsde.rawls.dataaccess.{GoogleServicesDAO, SlickDataSource}
import org.broadinstitute.dsde.rawls.model.WorkspaceCloudPlatform.{Azure, Gcp}
import org.broadinstitute.dsde.rawls.serviceFactory.DisabledServiceFactory.newDisabledService
import org.broadinstitute.dsde.rawls.serviceperimeter.{ServicePerimeterService, ServicePerimeterServiceImpl}

import scala.concurrent.ExecutionContext

object ServicePerimeterServiceFactory {
  def createServicePerimeter(appConfigManager: RawlsConfigManager,
                             slickDataSource: SlickDataSource,
                             gcsDAO: GoogleServicesDAO
  )(implicit system: ActorSystem, executionContext: ExecutionContext): ServicePerimeterService =
    appConfigManager.gcsConfig match {
      case Some(gcsConfig) =>
        val servicePerimeterConfig = ServicePerimeterServiceConfig(gcsConfig)
        new ServicePerimeterServiceImpl(slickDataSource, gcsDAO, servicePerimeterConfig)
      case None =>
        newDisabledService[ServicePerimeterService]
    }
}
