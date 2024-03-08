package org.broadinstitute.dsde.rawls.serviceFactory

import akka.actor.ActorSystem
import org.broadinstitute.dsde.rawls.config.MultiCloudAppConfigManager
import org.broadinstitute.dsde.rawls.serviceFactory.DisabledServiceFactory.newDisabledService
import org.broadinstitute.dsde.workbench.google.GoogleCredentialModes.GoogleCredentialMode
import org.broadinstitute.dsde.workbench.google.{GoogleBigQueryDAO, HttpGoogleBigQueryDAO}

import scala.concurrent.ExecutionContext

object BigQueryDAOFactory {
  def createBigQueryDAO(appConfigManager: MultiCloudAppConfigManager,
                        googleCredentialMode: GoogleCredentialMode,
                        metricsPrefix: String
  )(implicit system: ActorSystem, executionContext: ExecutionContext): GoogleBigQueryDAO =
    appConfigManager.gcsConfig match {
      case Some(gcsConfig) =>
        new HttpGoogleBigQueryDAO(
          gcsConfig.getString("appName"),
          googleCredentialMode,
          workbenchMetricBaseName = metricsPrefix
        )
      case None =>
        newDisabledService[GoogleBigQueryDAO]
    }
}
