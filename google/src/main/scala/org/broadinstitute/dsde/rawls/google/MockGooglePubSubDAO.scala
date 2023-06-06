package org.broadinstitute.dsde.rawls.google

import com.google.api.client.auth.oauth2.Credential
import com.google.api.client.googleapis.testing.auth.oauth2.MockGoogleCredential
import com.google.api.services.pubsub.model.Topic
import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.google.GooglePubSubDAO.{MessageRequest, PubSubMessage}

import java.util
import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue}
import java.util.{Collections, UUID}
import scala.collection.{mutable, _}
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._

/**
 * Created by dvoet on 12/7/16.
 */
class MockGooglePubSubDAO extends GooglePubSubDAO {
  implicit val executionContext: ExecutionContext = ExecutionContext.global

  val topics: concurrent.Map[String, mutable.Set[Subscription]] =
    new ConcurrentHashMap[String, mutable.Set[Subscription]]().asScala
  val subscriptionsByName = collection.concurrent.TrieMap.empty[String, Subscription]

  val messageLog = new ConcurrentLinkedQueue[String]
  val acks = new ConcurrentLinkedQueue[String]

  def logMessage(topic: String, message: String) = messageLog.add(s"$topic|$message")
  def receivedMessage(topic: String, message: String, count: Int = 1) =
    messageLog.toArray.filter(_ == s"$topic|$message").size == count

  override def createTopic(topicName: String): Future[Boolean] = {
    val initialCount = topics.size
    topics += (topicName -> Collections.synchronizedSet(new util.HashSet[Subscription]()).asScala)
    Future.successful(topics.size != initialCount)
  }

  override def pullMessages(subscriptionName: String,
                            maxMessages: Int
  ): Future[scala.collection.immutable.Seq[PubSubMessage]] = Future {
    val subscription =
      subscriptionsByName.getOrElse(subscriptionName,
                                    throw new RawlsException(s"no subscription named $subscriptionName")
      )
    (0 until maxMessages).map(_ => Option(subscription.queue.poll())).collect { case Some(message) =>
      PubSubMessage(UUID.randomUUID().toString, message.text, message.attributes)
    }
  }

  override def acknowledgeMessages(subscriptionName: String,
                                   messages: scala.collection.immutable.Seq[PubSubMessage]
  ): Future[Unit] = Future.successful(messages.foreach(m => acks.add(m.ackId)))

  override def acknowledgeMessagesById(subscriptionName: String,
                                       ackIds: scala.collection.immutable.Seq[String]
  ): Future[Unit] = Future.successful(ackIds.foreach(acks.add))

  override def publishMessages(topicName: String,
                               messages: scala.collection.immutable.Seq[MessageRequest]
  ): Future[Unit] = Future {
    val subscriptions = topics.getOrElse(topicName, throw new RawlsException(s"no topic named $topicName"))
    messages.foreach(message => logMessage(topicName, message.text))
    for {
      sub <- subscriptions
      message <- messages
    } yield sub.queue.add(message)
  }

  override def getPubSubServiceAccountCredential: Credential = getPreparedMockGoogleCredential()

  override def deleteSubscription(subscriptionName: String): Future[Boolean] = Future {
    subscriptionsByName.get(subscriptionName) match {
      case None =>
        false

      case Some(subscription) =>
        topics(subscription.topic) -= subscription
        subscriptionsByName -= subscriptionName
        true
    }
  }

  override def deleteTopic(topicName: String): Future[Boolean] = Future {
    val startingLength = topics.size
    topics -= topicName
    startingLength != topics.size
  }

  override def createSubscription(topicName: String,
                                  subscriptionName: String,
                                  ackDeadlineSeconds: Option[Int] = None
  ): Future[Boolean] = Future {
    if (!topics.contains(topicName)) throw new RawlsException(s"no topic named $topicName")
    if (subscriptionsByName.contains(subscriptionName)) {
      false
    } else {
      val subscription = Subscription(subscriptionName, topicName, new ConcurrentLinkedQueue[MessageRequest]())
      topics(topicName) += subscription
      subscriptionsByName += subscriptionName -> subscription
      true
    }
  }

  def getPreparedMockGoogleCredential(): MockGoogleCredential = {
    val credential = new MockGoogleCredential.Builder().build()
    credential.setAccessToken(MockGoogleCredential.ACCESS_TOKEN)
//    credential.setRefreshToken(token)
    credential.setExpiresInSeconds(1000000L) // make sure not to refresh this token
    credential
  }

  override def getTopic(topicName: String)(implicit executionContext: ExecutionContext): Future[Option[Topic]] =
    Future {
      if (topics.contains(topicName)) {
        val topic = new Topic
        topic.setName(topicName)
        Some(topic)
      } else None
    }

  case class Subscription(name: String, topic: String, queue: ConcurrentLinkedQueue[MessageRequest])
}
