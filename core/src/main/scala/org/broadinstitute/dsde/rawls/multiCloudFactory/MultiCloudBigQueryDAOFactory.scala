package org.broadinstitute.dsde.rawls.multiCloudFactory

import akka.actor.ActorSystem
import org.broadinstitute.dsde.rawls.config.MultiCloudAppConfigManager
import org.broadinstitute.dsde.rawls.multiCloudFactory.DisabledServiceFactory.newDisabledService
import org.broadinstitute.dsde.workbench.google.GoogleCredentialModes.GoogleCredentialMode
import org.broadinstitute.dsde.workbench.google.{GoogleBigQueryDAO, HttpGoogleBigQueryDAO}

import scala.concurrent.ExecutionContext

object MultiCloudBigQueryDAOFactory {
  def createHttpMultiCloudBigQueryDAO(appConfigManager: MultiCloudAppConfigManager,
                                      googleCredentialMode: GoogleCredentialMode,
                                      metricsPrefix: String
  )(implicit system: ActorSystem, executionContext: ExecutionContext): GoogleBigQueryDAO =
    appConfigManager.cloudProvider match {
      case "gcp" =>
        new HttpGoogleBigQueryDAO(
          appConfigManager.gcsConfig.getString("appName"),
          googleCredentialMode,
          workbenchMetricBaseName = metricsPrefix
        )
      case "azure" =>
        newDisabledService[GoogleBigQueryDAO]
      case _ => throw new IllegalArgumentException("Invalid cloud provider")
    }
}
