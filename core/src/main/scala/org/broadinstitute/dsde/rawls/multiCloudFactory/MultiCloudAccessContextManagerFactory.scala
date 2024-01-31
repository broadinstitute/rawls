package org.broadinstitute.dsde.rawls.multiCloudFactory

import akka.actor.ActorSystem
import org.broadinstitute.dsde.rawls.google.{AccessContextManagerDAO, HttpGoogleAccessContextManagerDAO}
import scala.concurrent.ExecutionContext
import com.typesafe.config.Config
import org.broadinstitute.dsde.rawls.dataaccess.DisabledHttpGoogleAccessContextManagerDAO

object MultiCloudAccessContextManagerFactory {
  def createAccessContextManager(metricsPrefix: String, config: Config, cloudProvider: String
                         )(implicit system: ActorSystem, executionContext: ExecutionContext): AccessContextManagerDAO = {
    cloudProvider match {
      case "gcp" =>
        new HttpGoogleAccessContextManagerDAO(
          config,
          metricsPrefix
        )
      case "azure" =>
        new DisabledHttpGoogleAccessContextManagerDAO
      case _ => throw new IllegalArgumentException("Invalid cloud provider")
    }
  }
}
