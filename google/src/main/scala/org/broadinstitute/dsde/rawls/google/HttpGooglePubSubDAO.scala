package org.broadinstitute.dsde.rawls.google

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import com.google.api.client.auth.oauth2.Credential
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport
import com.google.api.client.http.HttpResponseException
import com.google.api.client.json.gson.GsonFactory
import com.google.api.services.pubsub.model._
import com.google.api.services.pubsub.{Pubsub, PubsubScopes}
import com.typesafe.config.Config
import org.broadinstitute.dsde.rawls.google.GooglePubSubDAO._
import org.broadinstitute.dsde.rawls.metrics.GoogleInstrumentedService
import org.broadinstitute.dsde.rawls.util.FutureSupport

import scala.concurrent._
import scala.jdk.CollectionConverters._

/**
 * Created by mbemis on 5/6/16.
 */

class HttpGooglePubSubDAO(config: Config,
                          serviceProject: String,
                          override val workbenchMetricBaseName: String
)(implicit val system: ActorSystem, implicit val executionContext: ExecutionContext)
    extends FutureSupport
    with GoogleUtilities
    with GooglePubSubDAO
    {
      val clientEmail = config.getString("serviceClientEmail")
      val appName = config.getString("appName")
      val pemFile = config.getString("pathToPem")
  val pubSubScopes = Seq(PubsubScopes.PUBSUB)

  val httpTransport = GoogleNetHttpTransport.newTrustedTransport
  val jsonFactory = GsonFactory.getDefaultInstance

  private val characterEncoding = "UTF-8"
  implicit val service: GoogleInstrumentedService.Value = GoogleInstrumentedService.PubSub

  override def createTopic(topicName: String) =
    retryWithRecoverWhen500orGoogleError { () =>
      executeGoogleRequest(getPubSubDirectory.projects().topics().create(topicToFullPath(topicName), new Topic()))
      true
    } {
      case t: HttpResponseException if t.getStatusCode == 409 => false
    }

  override def deleteTopic(topicName: String): Future[Boolean] =
    retryWithRecoverWhen500orGoogleError { () =>
      executeGoogleRequest(getPubSubDirectory.projects().topics().delete(topicToFullPath(topicName)))
      true
    } {
      case e: HttpResponseException if e.getStatusCode == StatusCodes.NotFound.intValue => false
    }

  override def getTopic(topicName: String)(implicit executionContext: ExecutionContext): Future[Option[Topic]] =
    retryWithRecoverWhen500orGoogleError { () =>
      Option(executeGoogleRequest(getPubSubDirectory.projects().topics().get(topicToFullPath(topicName))))
    } {
      case e: HttpResponseException if e.getStatusCode == StatusCodes.NotFound.intValue => None
    }

  override def createSubscription(topicName: String, subscriptionName: String, ackDeadlineSeconds: Option[Int] = None) =
    retryWithRecoverWhen500orGoogleError { () =>
      val subscription = new Subscription().setTopic(topicToFullPath(topicName))
      ackDeadlineSeconds.map { secs =>
        subscription.setAckDeadlineSeconds(secs)
      }
      executeGoogleRequest(
        getPubSubDirectory.projects().subscriptions().create(subscriptionToFullPath(subscriptionName), subscription)
      )
      true
    } {
      case t: HttpResponseException if t.getStatusCode == 409 => false
    }

  override def deleteSubscription(subscriptionName: String): Future[Boolean] =
    retryWithRecoverWhen500orGoogleError { () =>
      executeGoogleRequest(
        getPubSubDirectory.projects().subscriptions().delete(subscriptionToFullPath(subscriptionName))
      )
      true
    } {
      case e: HttpResponseException if e.getStatusCode == StatusCodes.NotFound.intValue => false
    }

  override def publishMessages(topicName: String, messages: scala.collection.immutable.Seq[MessageRequest]) = {
    logger.debug(s"publishing to google pubsub topic $topicName, messages [${messages.mkString(", ")}]")
    Future
      .traverse(messages.grouped(1000)) { messageBatch =>
        retryWhen500orGoogleError { () =>
          val pubsubMessages = messageBatch.map(messageRequest =>
            new PubsubMessage()
              .encodeData(messageRequest.text.getBytes(characterEncoding))
              .setAttributes(messageRequest.attributes.asJava)
          )
          val pubsubRequest = new PublishRequest().setMessages(pubsubMessages.asJava)
          executeGoogleRequest(
            getPubSubDirectory.projects().topics().publish(topicToFullPath(topicName), pubsubRequest)
          )
        }
      }
      .map(_ => ())
  }

  override def acknowledgeMessages(subscriptionName: String, messages: scala.collection.immutable.Seq[PubSubMessage]) =
    acknowledgeMessagesById(subscriptionName, messages.map(_.ackId))

  override def acknowledgeMessagesById(subscriptionName: String, ackIds: scala.collection.immutable.Seq[String]) =
    retryWhen500orGoogleError { () =>
      val ackRequest = new AcknowledgeRequest().setAckIds(ackIds.asJava)
      executeGoogleRequest(
        getPubSubDirectory.projects().subscriptions().acknowledge(subscriptionToFullPath(subscriptionName), ackRequest)
      )
    }

  override def pullMessages(subscriptionName: String,
                            maxMessages: Int
  ): Future[scala.collection.immutable.Seq[PubSubMessage]] =
    retryWhen500orGoogleError { () =>
      val pullRequest = new PullRequest()
        .setReturnImmediately(true)
        .setMaxMessages(maxMessages) // won't keep the connection open if there's no msgs available
      val messages = executeGoogleRequest(
        getPubSubDirectory.projects().subscriptions().pull(subscriptionToFullPath(subscriptionName), pullRequest)
      ).getReceivedMessages
      if (messages == null)
        scala.collection.immutable.Seq.empty
      else
        messages.asScala.toList.map { message =>
          val messageBody = Option(message.getMessage.decodeData()).map(new String(_, characterEncoding)).getOrElse("")
          PubSubMessage(message.getAckId, messageBody, message.getMessage.getAttributes.asScala.toMap)
        }
    }

  def topicToFullPath(topicName: String) = s"projects/${serviceProject}/topics/${topicName}"
  def subscriptionToFullPath(subscriptionName: String) = s"projects/${serviceProject}/subscriptions/${subscriptionName}"

  def getPubSubDirectory =
    new Pubsub.Builder(httpTransport, jsonFactory, getPubSubServiceAccountCredential)
      .setApplicationName(appName)
      .build()

  def getPubSubServiceAccountCredential: Credential =
    new GoogleCredential.Builder()
      .setTransport(httpTransport)
      .setJsonFactory(jsonFactory)
      .setServiceAccountId(clientEmail)
      .setServiceAccountScopes(pubSubScopes.asJava) // grant pub sub powers
      .setServiceAccountPrivateKeyFromPemFile(new java.io.File(pemFile))
      .build()

}
