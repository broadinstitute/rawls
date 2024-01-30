package org.broadinstitute.dsde.rawls.multiCloudFactory

import akka.actor.ActorSystem
import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.config.ServicePerimeterServiceConfig
import org.broadinstitute.dsde.rawls.dataaccess.{GoogleServicesDAO, SlickDataSource}
import org.broadinstitute.dsde.rawls.serviceperimeter.{DisabledServicePerimeterService, ServicePerimeterService}
import org.broadinstitute.dsde.rawls.util.Retry

import scala.concurrent.ExecutionContext

object MultiCloudServicePerimeterServiceFactory {
  def createMultiCloudNotificationPubSubDAO(slickDataSource: SlickDataSource,
                                            gcsDAO: GoogleServicesDAO,
                                            config: Config,
                                            cloudProvider: String
                                           )(implicit system: ActorSystem, executionContext: ExecutionContext): ServicePerimeterService = {
    //TODO:: need to refactor servicePerimeterConfig for Azure
    val servicePerimeterConfig = ServicePerimeterServiceConfig(config)
    cloudProvider match {
      case "gcp" =>
        new ServicePerimeterService(slickDataSource, gcsDAO, servicePerimeterConfig)
      case "azure" =>
        new DisabledServicePerimeterService(slickDataSource, gcsDAO, servicePerimeterConfig)
      case _ => throw new IllegalArgumentException("Invalid cloud provider")
    }
  }
}
