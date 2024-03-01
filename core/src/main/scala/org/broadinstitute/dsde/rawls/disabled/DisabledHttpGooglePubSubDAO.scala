package org.broadinstitute.dsde.rawls.disabled

import com.google.api.client.auth.oauth2.Credential
import com.google.api.services.pubsub.model._
import org.broadinstitute.dsde.workbench.google.GooglePubSubDAO._
import org.broadinstitute.dsde.workbench.google.GooglePubSubDAO
import org.broadinstitute.dsde.rawls.model.SubsystemStatus
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail

import scala.concurrent._

class DisabledHttpGooglePubSubDAO(implicit val executionContext: ExecutionContext) extends GooglePubSubDAO {
  override def createTopic(topicName: String) =
    throw new NotImplementedError("createTopic is not implemented for Azure.")
  override def deleteTopic(topicName: String): Future[Boolean] =
    throw new NotImplementedError("deleteTopic is not implemented for Azure.")
  override def getTopic(topicName: String)(implicit executionContext: ExecutionContext): Future[Option[Topic]] =
    Future.successful(None)
  override def createSubscription(topicName: String, subscriptionName: String, ackDeadlineSeconds: Option[Int] = None) =
    throw new NotImplementedError("createSubscription is not implemented for Azure.")
  override def deleteSubscription(subscriptionName: String): Future[Boolean] =
    throw new NotImplementedError("deleteSubscription is not implemented for Azure.")
 def publishMessages(topicName: String, messages: scala.collection.immutable.Seq[MessageRequest]) =
    throw new NotImplementedError("publishMessages is not implemented for Azure.")
  def acknowledgeMessages(subscriptionName: String, messages: scala.collection.immutable.Seq[PubSubMessage]) =
    throw new NotImplementedError("acknowledgeMessages is not implemented for Azure.")
 def acknowledgeMessagesById(subscriptionName: String, ackIds: scala.collection.immutable.Seq[String]) =
    throw new NotImplementedError("acknowledgeMessagesById is not implemented for Azure.")
  override def pullMessages(subscriptionName: String, maxMessages: Int): Future[scala.collection.immutable.Seq[PubSubMessage]] =
    throw new NotImplementedError("pullMessages is not implemented for Azure.")
  def getPubSubServiceAccountCredential: Credential =
    throw new NotImplementedError("getPubSubServiceAccountCredential is not implemented for Azure.")
  def setTopicIamPermissions(topicName: String, permissions: Map[WorkbenchEmail, String]): Future[Unit] =
    throw new NotImplementedError("getPubSubServiceAccountCredential is not implemented for Azure.")

  def publishMessages(topicName: String, messages: collection.Seq[MessageRequest]): Future[Unit] =
    throw new NotImplementedError("getPubSubServiceAccountCredential is not implemented for Azure.")

  def acknowledgeMessages(subscriptionName: String, messages: collection.Seq[PubSubMessage]): Future[Unit] =
    throw new NotImplementedError("getPubSubServiceAccountCredential is not implemented for Azure.")

  def acknowledgeMessagesById(subscriptionName: String, ackIds: collection.Seq[String]): Future[Unit] =
    throw new NotImplementedError("getPubSubServiceAccountCredential is not implemented for Azure.")

  def extendDeadline(subscriptionName: String, messages: collection.Seq[PubSubMessage], extendDeadlineBySeconds: Int): Future[Unit] =
    throw new NotImplementedError("getPubSubServiceAccountCredential is not implemented for Azure.")

  def extendDeadlineById(subscriptionName: String, ackIds: collection.Seq[String], extendDeadlineBySeconds: Int): Future[Unit] =
    throw new NotImplementedError("getPubSubServiceAccountCredential is not implemented for Azure.")
}

