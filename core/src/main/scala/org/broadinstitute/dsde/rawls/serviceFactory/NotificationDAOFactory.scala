package org.broadinstitute.dsde.rawls.serviceFactory

import org.broadinstitute.dsde.rawls.config.RawlsConfigManager
import org.broadinstitute.dsde.rawls.serviceFactory.DisabledServiceFactory.newDisabledService
import org.broadinstitute.dsde.workbench.dataaccess.{NotificationDAO, PubSubNotificationDAO}
import org.broadinstitute.dsde.workbench.google.GooglePubSubDAO

object NotificationDAOFactory {
  def createNotificationDAO(appConfigManager: RawlsConfigManager,
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
