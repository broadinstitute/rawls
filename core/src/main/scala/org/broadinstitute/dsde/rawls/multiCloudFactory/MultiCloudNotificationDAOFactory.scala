package org.broadinstitute.dsde.rawls.multiCloudFactory

import org.broadinstitute.dsde.rawls.config.MultiCloudAppConfigManager
import org.broadinstitute.dsde.rawls.disabled.DisabledPubSubNotificationDAO
import org.broadinstitute.dsde.workbench.dataaccess.{NotificationDAO, PubSubNotificationDAO}
import org.broadinstitute.dsde.workbench.google.GooglePubSubDAO

object MultiCloudNotificationDAOFactory {
  def createMultiCloudNotificationDAO(appConfigManager: MultiCloudAppConfigManager,
                                      notificationPubSubDAO: GooglePubSubDAO
  ): NotificationDAO =
    appConfigManager.cloudProvider match {
      case "gcp" =>
        new PubSubNotificationDAO(
          notificationPubSubDAO,
          appConfigManager.gcsConfig.getString("notifications.topicName")
        )
      case "azure" =>
        new DisabledPubSubNotificationDAO
      case _ => throw new IllegalArgumentException("Invalid cloud provider")
    }
}
