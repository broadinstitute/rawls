package org.broadinstitute.dsde.rawls.multiCloudFactory

import akka.actor.ActorSystem
import org.broadinstitute.dsde.rawls.config.MultiCloudAppConfigManager
import org.broadinstitute.dsde.rawls.google.{GooglePubSubDAO, HttpGooglePubSubDAO}
import org.broadinstitute.dsde.rawls.model.WorkspaceCloudPlatform.{Azure, Gcp}
import org.broadinstitute.dsde.rawls.multiCloudFactory.DisabledServiceFactory.newDisabledService

import scala.concurrent.ExecutionContext

object MultiCloudPubSubDAOFactory {
  def createPubSubDAO(appConfigManager: MultiCloudAppConfigManager,
                      metricsPrefix: String,
                      serviceProject: String
  )(implicit system: ActorSystem, executionContext: ExecutionContext): GooglePubSubDAO =
    appConfigManager.cloudProvider match {
      case Gcp =>
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
      case Azure =>
        newDisabledService[GooglePubSubDAO]
    }
}
