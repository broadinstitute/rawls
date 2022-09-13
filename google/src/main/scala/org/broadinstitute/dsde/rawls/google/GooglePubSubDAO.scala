package org.broadinstitute.dsde.rawls.google

import com.google.api.client.auth.oauth2.Credential
import com.google.api.services.pubsub.model.Topic
import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.google.GooglePubSubDAO._

import scala.collection.Map
import scala.concurrent.{ExecutionContext, Future}

/**
 * Created by dvoet on 12/7/16.
 */
object GooglePubSubDAO {
  sealed trait AckStatus

  case object MessageAcknowledged extends AckStatus
  case object MessageNotAcknowledged extends AckStatus

  sealed trait HandledStatus

  case object MessageHandled extends HandledStatus
  case object MessageNotHandled extends HandledStatus
  case object NoMessage extends HandledStatus

  case class PubSubMessage(ackId: String, contents: String, attributes: Map[String, String] = Map.empty)
  case class MessageRequest(text: String, attributes: Map[String, String] = Map.empty)

}

trait GooglePubSubDAO {
  implicit val executionContext: ExecutionContext

  def createTopic(topicName: String): Future[Boolean]

  def deleteTopic(topicName: String): Future[Boolean]

  def getTopic(topicName: String)(implicit executionContext: ExecutionContext): Future[Option[Topic]]

  def createSubscription(topicName: String, subscriptionName: String, ackDeadlineSeconds: Option[Int]): Future[Boolean]

  def deleteSubscription(subscriptionName: String): Future[Boolean]

  def publishMessages(topicName: String, messages: scala.collection.immutable.Seq[MessageRequest]): Future[Unit]

  def acknowledgeMessages(subscriptionName: String,
                          messages: scala.collection.immutable.Seq[PubSubMessage]
  ): Future[Unit]

  def acknowledgeMessagesById(subscriptionName: String, ackIds: scala.collection.immutable.Seq[String]): Future[Unit]

  def pullMessages(subscriptionName: String, maxMessages: Int): Future[scala.collection.immutable.Seq[PubSubMessage]]

  def withMessage(subscriptionName: String)(op: (String) => Future[AckStatus]): Future[HandledStatus] =
    withMessages(subscriptionName, 1) {
      case Seq(msg) => op(msg)
      case _        => throw new RawlsException(s"Unable to process message from subscription ${subscriptionName}")
    }

  def withMessages(subscriptionName: String, maxMessages: Int)(
    op: (scala.collection.immutable.Seq[String]) => Future[AckStatus]
  ): Future[HandledStatus] =
    pullMessages(subscriptionName, maxMessages) flatMap {
      case Seq() => Future.successful(NoMessage)
      case messages =>
        op(messages.map(msg => msg.contents)) flatMap {
          case MessageAcknowledged    => acknowledgeMessages(subscriptionName, messages).map(_ => MessageHandled)
          case MessageNotAcknowledged => Future.successful(MessageNotHandled)
        }
    }

  def getPubSubServiceAccountCredential: Credential
}
