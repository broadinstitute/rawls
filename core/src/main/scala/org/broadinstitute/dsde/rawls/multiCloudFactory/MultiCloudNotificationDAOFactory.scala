package org.broadinstitute.dsde.rawls.multiCloudFactory

import org.broadinstitute.dsde.rawls.config.MultiCloudAppConfigManager
import org.broadinstitute.dsde.rawls.multiCloudFactory.DisabledServiceFactory.newDisabledService
import org.broadinstitute.dsde.workbench.dataaccess.{NotificationDAO, PubSubNotificationDAO}
import org.broadinstitute.dsde.workbench.google.GooglePubSubDAO

object MultiCloudNotificationDAOFactory {
  def createMultiCloudNotificationDAO(appConfigManager: MultiCloudAppConfigManager,
                                      notificationPubSubDAO: GooglePubSubDAO
  ): NotificationDAO =
    appConfigManager.gcsConfig match {
      case Some(gcsConfig) =>
        new PubSubNotificationDAO(
          notificationPubSubDAO,
          gcsConfig.getString("notifications.topicName")
        )
      case None =>
        newDisabledService[NotificationDAO]
    }
}
