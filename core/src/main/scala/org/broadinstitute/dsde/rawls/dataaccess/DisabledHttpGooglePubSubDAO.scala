package org.broadinstitute.dsde.rawls.dataaccess

import java.io.File
import akka.actor.ActorSystem
import com.google.api.services.pubsub.model._
import com.google.api.services.pubsub.{Pubsub, PubsubScopes}
import org.broadinstitute.dsde.workbench.google.{AbstractHttpGoogleDAO, GooglePubSubDAO}
import org.broadinstitute.dsde.workbench.google.GoogleCredentialModes._
import org.broadinstitute.dsde.workbench.google.GooglePubSubDAO._
import org.broadinstitute.dsde.workbench.metrics.GoogleInstrumentedService
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail

import scala.concurrent._

class DisabledHttpGooglePubSubDAO(appName: String,
                          googleCredentialMode: GoogleCredentialMode,
                          workbenchMetricBaseName: String,
                          serviceProject: String
                         )(implicit system: ActorSystem, executionContext: ExecutionContext)
  extends AbstractHttpGoogleDAO(appName, googleCredentialMode, workbenchMetricBaseName)
    with GooglePubSubDAO {

  @deprecated(
    message =
      "This way of instantiating HttpGooglePubSubDAO has been deprecated. Please update to use the primary constructor.",
    since = "0.15"
  )
  def this(clientEmail: String,
           pemFile: String,
           appName: String,
           serviceProject: String,
           workbenchMetricBaseName: String
          )(implicit system: ActorSystem, executionContext: ExecutionContext) =
    this(appName, Pem(WorkbenchEmail(clientEmail), new File(pemFile)), workbenchMetricBaseName, serviceProject)

  override val scopes = Seq(PubsubScopes.PUBSUB)

  implicit override val service: GoogleInstrumentedService.Value = GoogleInstrumentedService.PubSub

  private val characterEncoding = "UTF-8"

  private lazy val pubSub =
    new Pubsub.Builder(httpTransport, jsonFactory, googleCredential).setApplicationName(appName).build()

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

  def topicToFullPath(topicName: String) =
    throw new NotImplementedError("topicToFullPath is not implemented for Azure.")
  def subscriptionToFullPath(subscriptionName: String) =
    throw new NotImplementedError("subscriptionToFullPath is not implemented for Azure.")
}
