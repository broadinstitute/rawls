package org.broadinstitute.dsde.rawls.disabled

import com.google.api.services.pubsub.model._
import org.broadinstitute.dsde.workbench.google.GooglePubSubDAO
import org.broadinstitute.dsde.workbench.google.GooglePubSubDAO._
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail

import scala.concurrent._

class DisabledHttpGooglePubSubDAO extends GooglePubSubDAO {
  implicit override val executionContext: ExecutionContext = throw new NotImplementedError(
    "executionContext is not implemented for Azure."
  )
  override def createTopic(topicName: String) =
    throw new NotImplementedError("createTopic is not implemented for Azure.")
  override def deleteTopic(topicName: String): Future[Boolean] =
    throw new NotImplementedError("deleteTopic is not implemented for Azure.")
  override def getTopic(topicName: String)(implicit executionContext: ExecutionContext): Future[Option[Topic]] =
    throw new NotImplementedError("getTopic is not implemented for Azure.")
  override def setTopicIamPermissions(topicName: String, permissions: Map[WorkbenchEmail, String]): Future[Unit] =
    throw new NotImplementedError("setTopicIamPermissions is not implemented for Azure.")
  override def createSubscription(topicName: String, subscriptionName: String, ackDeadlineSeconds: Option[Int] = None) =
    throw new NotImplementedError("createSubscription is not implemented for Azure.")
  override def deleteSubscription(subscriptionName: String): Future[Boolean] =
    throw new NotImplementedError("deleteSubscription is not implemented for Azure.")
  override def publishMessages(topicName: String, messages: scala.collection.Seq[MessageRequest]) =
    throw new NotImplementedError("publishMessages is not implemented for Azure.")
  override def acknowledgeMessages(subscriptionName: String, messages: scala.collection.Seq[PubSubMessage]) =
    throw new NotImplementedError("acknowledgeMessages is not implemented for Azure.")
  override def acknowledgeMessagesById(subscriptionName: String, ackIds: scala.collection.Seq[String]) =
    throw new NotImplementedError("acknowledgeMessagesById is not implemented for Azure.")
  override def extendDeadline(subscriptionName: String,
                              messages: scala.collection.Seq[PubSubMessage],
                              extendDeadlineBySeconds: Int
  ): Future[Unit] =
    throw new NotImplementedError("extendDeadline is not implemented for Azure.")
  override def extendDeadlineById(subscriptionName: String,
                                  ackIds: scala.collection.Seq[String],
                                  extendDeadlineBySeconds: Int
  ): Future[Unit] =
    throw new NotImplementedError("extendDeadlineById is not implemented for Azure.")
  override def pullMessages(subscriptionName: String, maxMessages: Int): Future[scala.collection.Seq[PubSubMessage]] =
    throw new NotImplementedError("pullMessages is not implemented for Azure.")
}
