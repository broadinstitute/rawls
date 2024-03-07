package org.broadinstitute.dsde.rawls.multiCloudFactory

import akka.actor.ActorSystem
import org.broadinstitute.dsde.rawls.config.MultiCloudAppConfigManager
import org.broadinstitute.dsde.rawls.multiCloudFactory.DisabledServiceFactory.newDisabledService
import org.broadinstitute.dsde.workbench.google.GooglePubSubDAO

import scala.concurrent.ExecutionContext

object MultiCloudNotificationPubSubDAOFactory {
  def createMultiCloudNotificationPubSubDAO(appConfigManager: MultiCloudAppConfigManager,
                                            workbenchMetricBaseName: String
  )(implicit system: ActorSystem, executionContext: ExecutionContext): GooglePubSubDAO =
    appConfigManager.gcsConfig match {
      case Some(gcsConfig) =>
        new org.broadinstitute.dsde.workbench.google.HttpGooglePubSubDAO(
          gcsConfig.getString("serviceClientEmail"),
          gcsConfig.getString("serviceClientEmail"),
          gcsConfig.getString("appName"),
          gcsConfig.getString("serviceProject"),
          workbenchMetricBaseName
        )
      case None =>
        newDisabledService[GooglePubSubDAO]
    }
}
