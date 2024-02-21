package org.broadinstitute.dsde.rawls.multiCloudFactory

import akka.actor.ActorSystem
import com.typesafe.config.Config
import org.broadinstitute.dsde.rawls.config.MultiCloudAppConfigManager
import org.broadinstitute.dsde.rawls.disabled.DisabledHttpGooglePubSubDAO
import org.broadinstitute.dsde.workbench.google.GooglePubSubDAO

import scala.concurrent.ExecutionContext

object MultiCloudNotificationPubSubDAOFactory {
  def createMultiCloudNotificationPubSubDAO(appConfigManager: MultiCloudAppConfigManager,
                                            workbenchMetricBaseName: String
  )(implicit system: ActorSystem, executionContext: ExecutionContext): GooglePubSubDAO =
    appConfigManager.cloudProvider match {
      case "gcp" =>
        val gcsConfig = appConfigManager.gcsConfig
        new org.broadinstitute.dsde.workbench.google.HttpGooglePubSubDAO(
          gcsConfig.getString("serviceClientEmail"),
          gcsConfig.getString("serviceClientEmail"),
          gcsConfig.getString("appName"),
          gcsConfig.getString("serviceProject"),
          workbenchMetricBaseName
        )
      case "azure" =>
        new DisabledHttpGooglePubSubDAO
      case _ => throw new IllegalArgumentException("Invalid cloud provider")
    }
}
