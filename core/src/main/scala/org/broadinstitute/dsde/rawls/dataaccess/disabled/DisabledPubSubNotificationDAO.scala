package org.broadinstitute.dsde.rawls.dataaccess.disabled

import org.broadinstitute.dsde.workbench.dataaccess.NotificationDAO

import scala.concurrent.Future

class DisabledPubSubNotificationDAO extends NotificationDAO {
  protected def sendNotifications(notification: Traversable[String]): Future[Unit] =
    throw new NotImplementedError("sendNotifications is not implemented for Azure.")
}
