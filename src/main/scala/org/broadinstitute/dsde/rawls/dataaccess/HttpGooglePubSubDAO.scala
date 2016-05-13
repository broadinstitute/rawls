package org.broadinstitute.dsde.rawls.dataaccess

import akka.actor.ActorSystem
import com.google.api.client.auth.oauth2.Credential
import com.google.api.client.googleapis.auth.oauth2.{GoogleClientSecrets, GoogleCredential}
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport
import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.services.pubsub.model._
import com.google.api.services.pubsub.{Pubsub, PubsubScopes}
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.model.RawlsEnumeration
import org.broadinstitute.dsde.rawls.util.FutureSupport

import scala.collection.JavaConversions._
import scala.concurrent._

/**
 * Created by mbemis on 5/6/16.
 */
class HttpGooglePubSubDAO(
  val clientSecrets: GoogleClientSecrets,
  pemFile: String,
  appName: String,
  serviceProject: String)( implicit val system: ActorSystem, implicit val executionContext: ExecutionContext ) extends Retry with FutureSupport with LazyLogging with GoogleUtilities {

  val pubSubScopes = Seq(PubsubScopes.PUBSUB)

  val httpTransport = GoogleNetHttpTransport.newTrustedTransport
  val jsonFactory = JacksonFactory.getDefaultInstance

  val serviceAccountClientId: String = clientSecrets.getDetails.get("client_email").toString

  sealed trait AckStatus

  case object MessageAcknowledged extends AckStatus
  case object MessageNotAcknowledged extends AckStatus

  sealed trait HandledStatus

  case object MessageHandled extends HandledStatus
  case object MessageNotHandled extends HandledStatus
  case object NoMessage extends HandledStatus

  def createTopic(topicName: String) = {
    retry(when500)(() => Future { blocking {
      executeGoogleRequest(getPubSubDirectory.projects().topics().create(topicToFullPath(topicName), new Topic()))
    }})
  }

  def deleteTopic(topicName: String) = {
    retry(when500)(() => Future { blocking {
      executeGoogleRequest(getPubSubDirectory.projects().topics().delete(topicToFullPath(topicName)))
    }})
  }

  def createSubscription(topicName: String, subscriptionName: String) = {
    retry(when500)(() => Future { blocking {
      val subscription = new Subscription().setTopic(topicToFullPath(topicName))
      executeGoogleRequest(getPubSubDirectory.projects().subscriptions().create(subscriptionToFullPath(subscriptionName), subscription))
    }})
  }

  def deleteSubscription(subscriptionName: String) = {
    retry(when500)(() => Future { blocking {
      executeGoogleRequest(getPubSubDirectory.projects().subscriptions().delete(subscriptionToFullPath(subscriptionName)))
    }})
  }

  def publishMessages(topicName: String, messages: Seq[String]) = {
    retry(when500)(() => Future { blocking {
      val pubsubMessages = messages.map(text => new PubsubMessage().encodeData(text.getBytes("UTF-8")))
      val pubsubRequest = new PublishRequest().setMessages(pubsubMessages)
      executeGoogleRequest(getPubSubDirectory.projects().topics().publish(topicToFullPath(topicName), pubsubRequest))
    }})
  }

  def acknowledgeMessages(subscriptionName: String, ackIds: Seq[String]) = {
    retry(when500)(() => Future { blocking {
      val ackRequest = new AcknowledgeRequest().setAckIds(ackIds)
      executeGoogleRequest(getPubSubDirectory.projects().subscriptions().acknowledge(subscriptionToFullPath(subscriptionName), ackRequest))
    }})
  }

  def pullMessages(subscriptionName: String, maxMessages: Int): Future[Seq[ReceivedMessage]] = {
    retry(when500)(() => Future { blocking {
      val pullRequest = new PullRequest().setReturnImmediately(true).setMaxMessages(maxMessages) //won't keep the connection open if there's no msgs available
      val messages = executeGoogleRequest(getPubSubDirectory.projects().subscriptions().pull(subscriptionToFullPath(subscriptionName), pullRequest)).getReceivedMessages
      if(messages == null)
        Seq.empty
      else messages.toList.toSeq
    }})
  }

  def withMessage(subscriptionName: String)(op: (String) => Future[AckStatus]): Future[HandledStatus] = {
    withMessages(subscriptionName, 1) {
      case Seq(msg) => op(msg)
      case _ => throw new RawlsException(s"Unable to process message from subscription ${subscriptionName}")
    }
  }

  def withMessages(subscriptionName: String, maxMessages: Int)(op: (Seq[String]) => Future[AckStatus]): Future[HandledStatus] = {
    pullMessages(subscriptionName, maxMessages) flatMap {
      case Seq() => Future.successful(NoMessage)
      case messages => op(messages.map(msg => decodeMessage(msg))) flatMap {
        case MessageAcknowledged => acknowledgeMessages(subscriptionName, messages.map(msg => msg.getAckId)).map(_ => MessageHandled)
        case MessageNotAcknowledged => Future.successful(MessageNotHandled)
      }
    }
  }

  def decodeMessage(message: ReceivedMessage): String = {
    new String(java.util.Base64.getUrlDecoder.decode(message.getMessage.getData), "UTF-8")
  }

  def topicToFullPath(topicName: String) = s"projects/${serviceProject}/topics/${topicName}"
  def subscriptionToFullPath(subscriptionName: String) = s"projects/${serviceProject}/subscriptions/${subscriptionName}"

  def getPubSubDirectory = {
    new Pubsub.Builder(httpTransport, jsonFactory, getPubSubServiceAccountCredential).setApplicationName(appName).build()
  }

  def getPubSubServiceAccountCredential: Credential = {
    new GoogleCredential.Builder()
      .setTransport(httpTransport)
      .setJsonFactory(jsonFactory)
      .setServiceAccountId(serviceAccountClientId)
      .setServiceAccountScopes(pubSubScopes) // grant pub sub powers
      .setServiceAccountPrivateKeyFromPemFile(new java.io.File(pemFile))
      .build()
  }

}
