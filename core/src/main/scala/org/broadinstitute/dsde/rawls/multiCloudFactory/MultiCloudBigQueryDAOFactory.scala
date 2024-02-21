package org.broadinstitute.dsde.rawls.multiCloudFactory

import akka.actor.ActorSystem
import com.typesafe.config.Config
import org.broadinstitute.dsde.rawls.config.MultiCloudAppConfigManager
import org.broadinstitute.dsde.rawls.disabled.DisabledGoogleBigQueryDAO
import org.broadinstitute.dsde.workbench.google.GoogleCredentialModes.{GoogleCredentialMode, Json}
import org.broadinstitute.dsde.workbench.google.{AbstractHttpGoogleDAO, GoogleBigQueryDAO, HttpGoogleBigQueryDAO}

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
        new DisabledGoogleBigQueryDAO
      case _ => throw new IllegalArgumentException("Invalid cloud provider")
    }
}
