package org.broadinstitute.dsde.rawls.integrationtest

/**
 * Created by mbemis on 5/10/16.
 */

import akka.actor.ActorSystem
import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets
import com.google.api.client.json.gson.GsonFactory
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.google.GooglePubSubDAO.MessageRequest
import org.broadinstitute.dsde.rawls.google.{GooglePubSubDAO, HttpGooglePubSubDAO}
import org.broadinstitute.dsde.rawls.metrics.StatsDTestUtils
import org.broadinstitute.dsde.rawls.util.{MockitoTestUtils, Retry}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.{File, StringReader}
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

class HttpGooglePubSubDAOSpec
    extends AnyFlatSpec
    with Matchers
    with BeforeAndAfterAll
    with Retry
    with LazyLogging
    with Eventually
    with MockitoTestUtils
    with StatsDTestUtils {
  implicit val system: ActorSystem = ActorSystem("HttpGooglePubSubDAOSpec")

  val etcConf = ConfigFactory.load()
  val jenkinsConf = ConfigFactory.parseFile(new File("jenkins.conf"))
  val gcsConfig = jenkinsConf.withFallback(etcConf).getConfig("gcs")

  import scala.concurrent.ExecutionContext.Implicits.global
  val gpsDAO = new HttpGooglePubSubDAO(
    GoogleClientSecrets
      .load(GsonFactory.getDefaultInstance, new StringReader(gcsConfig.getString("secrets")))
      .getDetails
      .get("client_email")
      .toString,
    gcsConfig.getString("pathToPem"),
    gcsConfig.getString("appName"),
    gcsConfig.getString("serviceProject"),
    workbenchMetricBaseName
  )

  val defaultTopicName = "integration-tests"
  val defaultSubscriptionName = "test-subscription"

  override def beforeAll() = {
    super.beforeAll()
    // if this test ever fails, there will likely be some leftover garbage. clean it up first
    // cleanup uses Await.ready because we don't care what the result is, just that it happened
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
    // publish a few messages to the topic
    val messages = List(MessageRequest("test-1"))
    Await.result(gpsDAO.publishMessages(defaultTopicName, messages), Duration.Inf)

    Await.result(
      gpsDAO.withMessage(defaultSubscriptionName) { msg =>
        assertResult(true) {
          messages.contains(msg)
        }
        Future.successful(GooglePubSubDAO.MessageAcknowledged)
      },
      Duration.Inf
    )
  }

  it should "submit more than 1000 messages" in {
    // publish a lot of messages to the topic
    val numMessages = 2877
    val messages = List.fill(numMessages)(MessageRequest("foo"))
    Await.result(gpsDAO.publishMessages(defaultTopicName, messages), Duration.Inf)

    while (
      Await.result(gpsDAO.withMessages(defaultSubscriptionName, numMessages) { msgs =>
                     Future.successful(GooglePubSubDAO.MessageAcknowledged)
                   },
                   Duration.Inf
      ) != GooglePubSubDAO.NoMessage
    ) {}
  }

  it should "gracefully handle there being no messages in the queue" in {
    Await.result(gpsDAO.withMessage(defaultSubscriptionName) { msg =>
                   assertResult(None) {
                     msg
                   }
                   Future.successful(GooglePubSubDAO.MessageNotAcknowledged)
                 },
                 Duration.Inf
    )
  }

  it should "do all of the things with multiple messages" in {
    // publish a few messages to the topic
    val messages = List("test-1", "test-2", "test-3", "test-4", "test-5").map(MessageRequest(_))
    Await.result(gpsDAO.publishMessages(defaultTopicName, messages), Duration.Inf)

    Await.result(gpsDAO.withMessages(defaultSubscriptionName, 5) { msg =>
                   assertResult(messages) {
                     msg
                   }
                   Future.successful(GooglePubSubDAO.MessageAcknowledged)
                 },
                 Duration.Inf
    )
  }

}
