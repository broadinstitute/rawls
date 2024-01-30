package org.broadinstitute.dsde.rawls.google

import akka.actor.ActorSystem
import com.google.api.client.auth.oauth2.Credential
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport
import com.google.api.client.http.javanet.NetHttpTransport
import com.google.api.client.json.gson.GsonFactory
import com.google.api.services.pubsub.model._
import com.google.api.services.pubsub.PubsubScopes
import com.typesafe.config.Config
import org.broadinstitute.dsde.rawls.google.GooglePubSubDAO._
import org.broadinstitute.dsde.rawls.metrics.GoogleInstrumentedService
import org.broadinstitute.dsde.rawls.util.FutureSupport

import scala.concurrent._

class DisabledHttpGooglePubSubDAO(config: Config,
                                  serviceProject: String,
                                  override val workbenchMetricBaseName: String
                                 )(implicit val system: ActorSystem, implicit val executionContext: ExecutionContext)
  extends FutureSupport
    with GoogleUtilities
    with GooglePubSubDAO
{
  val clientEmail: String = config.getString("serviceClientEmail")
  val appName: String = config.getString("appName")
  val pemFile: String = config.getString("pathToPem")
  val pubSubScopes: Seq[String] = Seq(PubsubScopes.PUBSUB)

  val httpTransport: NetHttpTransport = GoogleNetHttpTransport.newTrustedTransport
  val jsonFactory: GsonFactory = GsonFactory.getDefaultInstance

  private val characterEncoding = "UTF-8"
  implicit val service: GoogleInstrumentedService.Value = GoogleInstrumentedService.PubSub

  override def createTopic(topicName: String) =
    throw new NotImplementedError("createTopic is not implemented for Azure.")

  override def deleteTopic(topicName: String): Future[Boolean] =
    throw new NotImplementedError("deleteTopic is not implemented for Azure.")

  override def getTopic(topicName: String)(implicit executionContext: ExecutionContext): Future[Option[Topic]] =
    throw new NotImplementedError("getTopic is not implemented for Azure.")

  override def createSubscription(topicName: String, subscriptionName: String, ackDeadlineSeconds: Option[Int] = None) =
    throw new NotImplementedError("createSubscription is not implemented for Azure.")

  override def deleteSubscription(subscriptionName: String): Future[Boolean] =
    throw new NotImplementedError("deleteSubscription is not implemented for Azure.")

  override def publishMessages(topicName: String, messages: scala.collection.immutable.Seq[MessageRequest]) =
    throw new NotImplementedError("publishMessages is not implemented for Azure.")

  override def acknowledgeMessages(subscriptionName: String, messages: scala.collection.immutable.Seq[PubSubMessage]) =
    throw new NotImplementedError("acknowledgeMessages is not implemented for Azure.")

  override def acknowledgeMessagesById(subscriptionName: String, ackIds: scala.collection.immutable.Seq[String]) =
    throw new NotImplementedError("acknowledgeMessagesById is not implemented for Azure.")

  override def pullMessages(subscriptionName: String,
                            maxMessages: Int
                           ): Future[scala.collection.immutable.Seq[PubSubMessage]] =
    throw new NotImplementedError("pullMessages is not implemented for Azure.")

  def topicToFullPath(topicName: String) =
    throw new NotImplementedError("topicToFullPath is not implemented for Azure.")
  def subscriptionToFullPath(subscriptionName: String) =
    throw new NotImplementedError("subscriptionToFullPath is not implemented for Azure.")

  def getPubSubDirectory =
    throw new NotImplementedError("getPubSubDirectory is not implemented for Azure.")

  def getPubSubServiceAccountCredential: Credential =
    throw new NotImplementedError("getPubSubServiceAccountCredential is not implemented for Azure.")
}

