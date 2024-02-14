package org.broadinstitute.dsde.rawls.multiCloudFactory

import akka.actor.ActorSystem
import org.broadinstitute.dsde.rawls.google.{AccessContextManagerDAO, HttpGoogleAccessContextManagerDAO}

import scala.concurrent.ExecutionContext
import com.typesafe.config.Config
import org.broadinstitute.dsde.rawls.dataaccess.disabled.DisabledHttpGoogleAccessContextManagerDAO
import org.broadinstitute.dsde.rawls.config.MultiCloudAppConfigManager

object MultiCloudAccessContextManagerFactory {
  def createAccessContextManager(metricsPrefix: String, appConfigManager: MultiCloudAppConfigManager
                         )(implicit system: ActorSystem, executionContext: ExecutionContext): AccessContextManagerDAO = {
    appConfigManager.cloudProvider match {
      case "gcp" =>
        new HttpGoogleAccessContextManagerDAO(
          appConfigManager.gcsConfig,
          metricsPrefix
        )
      case "azure" =>
        new DisabledHttpGoogleAccessContextManagerDAO
      case _ => throw new IllegalArgumentException("Invalid cloud provider")
    }
  }
}
