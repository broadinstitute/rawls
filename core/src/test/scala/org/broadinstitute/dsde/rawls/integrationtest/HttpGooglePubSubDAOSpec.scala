package org.broadinstitute.dsde.rawls.integrationtest

/**
 * Created by mbemis on 5/10/16.
 */

import java.io.StringReader
import java.util.concurrent.atomic.AtomicInteger

import akka.actor.ActorSystem
import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets
import com.google.api.client.json.jackson2.JacksonFactory
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.dataaccess.{Retry, _}
import org.broadinstitute.dsde.rawls.dataaccess.slick.TestDriverComponent
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

class HttpGooglePubSubDAOSpec extends FlatSpec with Matchers with IntegrationTestConfig with BeforeAndAfterAll with Retry with TestDriverComponent with LazyLogging {
  implicit val system = ActorSystem("HttpGooglePubSubDAOSpec")
  val gpsDAO = new HttpGooglePubSubDAO(
    GoogleClientSecrets.load(
      JacksonFactory.getDefaultInstance, new StringReader(gcsConfig.getString("secrets"))),
    gcsConfig.getString("pathToPem"),
    gcsConfig.getString("appName"),
    gcsConfig.getString("serviceProject")
  )

  val defaultTopicName = "integration-tests"
  val defaultSubscriptionName = "test-subscription"

  override def beforeAll() = {
    super.beforeAll()
    //if this test ever fails, there will likely be some leftover garbage. clean it up first
    //cleanup uses Await.ready because we don't care what the result is, just that it happened
    Await.ready(gpsDAO.deleteSubscription(defaultSubscriptionName), Duration.Inf)
    Await.ready(gpsDAO.deleteTopic(defaultTopicName), Duration.Inf)

    Await.result(gpsDAO.createTopic(defaultTopicName), Duration.Inf)
    Await.result(gpsDAO.createSubscription(defaultTopicName, defaultSubscriptionName), Duration.Inf)
  }

  override def afterAll() = {
    super.afterAll()
    Await.result(gpsDAO.deleteSubscription(defaultSubscriptionName), Duration.Inf)
    Await.result(gpsDAO.deleteTopic(defaultTopicName), Duration.Inf)
  }

  "HttpGooglePubSubDAOSpec" should "do all of the things" in {
    //publish a few messages to the topic
    val messages = Seq("test-1")
    Await.result(gpsDAO.publishMessages(defaultTopicName, messages), Duration.Inf)

    Await.result(gpsDAO.withMessage(defaultSubscriptionName) { msg =>
      assertResult(true) {
        messages.contains(msg)
      }
      Future.successful(GooglePubSubDAO.MessageAcknowledged)
    }, Duration.Inf)
  }

  it should "submit more than 1000 messages" in {
    //publish a lot of messages to the topic
    val numMessages = 2877
    val messages = Seq.fill(numMessages)("foo")
    Await.result(gpsDAO.publishMessages(defaultTopicName, messages), Duration.Inf)

    while (Await.result(gpsDAO.withMessages(defaultSubscriptionName, numMessages) { msgs =>
        Future.successful(GooglePubSubDAO.MessageAcknowledged)
      }, Duration.Inf) != GooglePubSubDAO.NoMessage) {}
  }

  it should "gracefully handle there being no messages in the queue" in {
    Await.result(gpsDAO.withMessage(defaultSubscriptionName) { msg =>
      assertResult(None) {
        msg
      }
      Future.successful(GooglePubSubDAO.MessageNotAcknowledged)
    }, Duration.Inf)
  }

  it should "do all of the things with multiple messages" in {
    //publish a few messages to the topic
    val messages = Seq("test-1", "test-2", "test-3", "test-4", "test-5")
    Await.result(gpsDAO.publishMessages(defaultTopicName, messages), Duration.Inf)

    Await.result(gpsDAO.withMessages(defaultSubscriptionName, 5) { msg =>
      assertResult(messages) {
        msg
      }
      Future.successful(GooglePubSubDAO.MessageAcknowledged)
    }, Duration.Inf)
  }

}
