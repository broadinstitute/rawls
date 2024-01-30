package org.broadinstitute.dsde.rawls.multiCloudFactory

import akka.actor.ActorSystem
import cats.effect.IO
import com.typesafe.config.Config
import org.broadinstitute.dsde.rawls.AppDependencies
import org.broadinstitute.dsde.rawls.dataaccess.{DisabledHttpGoogleServicesDAO, GoogleServicesDAO, HttpGoogleServicesDAO}
import org.broadinstitute.dsde.rawls.google.{AccessContextManagerDAO, GoogleUtilities}
import org.broadinstitute.dsde.rawls.util.FutureSupport

import scala.concurrent.ExecutionContext

object MultiCloudServicesDAOFactory {
  def createHttpMultiCloudServicesDAO(config: Config,
                                      appDependencies: AppDependencies[IO],
                                      workbenchMetricBaseName: String,
                                      accessContextManagerDAO: AccessContextManagerDAO,
                                      cloudProvider: String
                                     )(implicit system: ActorSystem, executionContext: ExecutionContext): HttpGoogleServicesDAO = {
    cloudProvider match {
      case "gcp" =>
        new HttpGoogleServicesDAO(
          config,
          200,
          appDependencies,
          workbenchMetricBaseName,
          accessContextManagerDAO
        )
      case "azure" =>
        new DisabledHttpGoogleServicesDAO(
          config,
          200,
          appDependencies,
          workbenchMetricBaseName,
          accessContextManagerDAO
        ).asInstanceOf[HttpGoogleServicesDAO]

      case _ => throw new IllegalArgumentException("Invalid cloud provider")
    }
  }
}
