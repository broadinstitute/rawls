package org.broadinstitute.dsde.rawls.dataaccess

import org.broadinstitute.dsde.workbench.dataaccess.NotificationDAO
import org.broadinstitute.dsde.workbench.google.GooglePubSubDAO

import scala.concurrent.Future

class DisabledPubSubNotificationDAO(googlePubSubDAO: GooglePubSubDAO, topicName: String) extends NotificationDAO {
  import scala.concurrent.ExecutionContext.Implicits.global
  // attempt to create the topic, if it already exists this will log a message and then move on
  googlePubSubDAO.createTopic(topicName).map { created =>
    if (!created) logger.info(s"The topic $topicName was not created because it already exists.")
  }

  protected def sendNotifications(notification: Traversable[String]): Future[Unit] =
    throw new NotImplementedError("sendNotifications is not implemented for Azure.")
}
