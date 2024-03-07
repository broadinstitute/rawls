package org.broadinstitute.dsde.rawls.multiCloudFactory

import org.broadinstitute.dsde.rawls.config.MultiCloudAppConfigManager
import org.broadinstitute.dsde.rawls.model.WorkspaceCloudPlatform.{Azure, Gcp}
import org.broadinstitute.dsde.rawls.multiCloudFactory.DisabledServiceFactory.newDisabledService
import org.broadinstitute.dsde.workbench.dataaccess.{NotificationDAO, PubSubNotificationDAO}
import org.broadinstitute.dsde.workbench.google.GooglePubSubDAO

object MultiCloudNotificationDAOFactory {
  def createMultiCloudNotificationDAO(appConfigManager: MultiCloudAppConfigManager,
                                      notificationPubSubDAO: GooglePubSubDAO
  ): NotificationDAO =
    appConfigManager.cloudProvider match {
      case Gcp =>
        new PubSubNotificationDAO(
          notificationPubSubDAO,
          appConfigManager.gcsConfig.getString("notifications.topicName")
        )
      case Azure =>
        newDisabledService[NotificationDAO]
    }
}
