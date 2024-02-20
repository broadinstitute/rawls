package org.broadinstitute.dsde.rawls.multiCloudFactory

import akka.actor.ActorSystem
import org.broadinstitute.dsde.rawls.google.{AccessContextManagerDAO, HttpGoogleAccessContextManagerDAO}

import scala.concurrent.ExecutionContext
import org.broadinstitute.dsde.rawls.config.MultiCloudAppConfigManager
import org.broadinstitute.dsde.rawls.disabled.DisabledHttpGoogleAccessContextManagerDAO

object MultiCloudAccessContextManagerFactory {
  def createAccessContextManager(metricsPrefix: String, appConfigManager: MultiCloudAppConfigManager
                         )(implicit system: ActorSystem, executionContext: ExecutionContext): AccessContextManagerDAO = {
    appConfigManager.cloudProvider match {
      case "gcp" =>
        val gcsConfig = appConfigManager.gcsConfig
        val clientEmail = gcsConfig.getString("serviceClientEmail")
        val serviceProject = gcsConfig.getString("serviceProject")
        val appName = gcsConfig.getString("appName")
        val pemFile = gcsConfig.getString("pathToPem")
        new HttpGoogleAccessContextManagerDAO(
          clientEmail,
          pemFile,
          appName,
          serviceProject,
          metricsPrefix
        )
      case "azure" =>
        new DisabledHttpGoogleAccessContextManagerDAO
      case _ => throw new IllegalArgumentException("Invalid cloud provider")
    }
  }
}
