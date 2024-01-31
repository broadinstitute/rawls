package org.broadinstitute.dsde.rawls.multiCloudFactory

import com.typesafe.config.Config
import org.broadinstitute.dsde.rawls.dataaccess.DisabledPubSubNotificationDAO
import org.broadinstitute.dsde.workbench.dataaccess.{NotificationDAO, PubSubNotificationDAO}
import org.broadinstitute.dsde.workbench.google.GooglePubSubDAO

object MultiCloudNotificationDAOFactory {
  def createMultiCloudNotificationDAO(config: Config,
                                      notificationPubSubDAO: GooglePubSubDAO,
                                      cloudProvider:String
                                     ): NotificationDAO = {
    cloudProvider match {
      case "gcp" =>
        new PubSubNotificationDAO(
          notificationPubSubDAO,
          config.getString("notifications.topicName")
        )
      case "azure" =>
        new DisabledPubSubNotificationDAO
      case _ => throw new IllegalArgumentException("Invalid cloud provider")
    }
  }
}
