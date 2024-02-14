package org.broadinstitute.dsde.rawls.multiCloudFactory

import akka.actor.ActorSystem
import org.broadinstitute.dsde.rawls.config.MultiCloudAppConfigManager
import org.broadinstitute.dsde.rawls.google.{DisabledHttpGooglePubSubDAO, GooglePubSubDAO, HttpGooglePubSubDAO}

import scala.concurrent.ExecutionContext

object MultiCloudPubSubDAOFactory {
  def createPubSubDAO(appConfigManager: MultiCloudAppConfigManager, metricsPrefix: String, serviceProject: String
                     )(implicit system: ActorSystem, executionContext: ExecutionContext): GooglePubSubDAO = {
    appConfigManager.cloudProvider match {
      case "gcp" =>
        val gcsConfig = appConfigManager.gcsConfig
        val clientEmail = gcsConfig.getString("serviceClientEmail")
        val appName = gcsConfig.getString("appName")
        val pathToPem = gcsConfig.getString("pathToPem")
        new HttpGooglePubSubDAO(
          clientEmail,
          pathToPem,
          appName,
          serviceProject,
          workbenchMetricBaseName = metricsPrefix
        )
      case "azure" =>
        new DisabledHttpGooglePubSubDAO
      case _ => throw new IllegalArgumentException("Invalid cloud provider")
    }
  }
}
