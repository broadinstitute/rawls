package org.broadinstitute.dsde.rawls.monitor

import akka.actor.SupervisorStrategy.{Escalate, Stop}
import akka.actor._
import akka.pattern._
import cats.effect.{ContextShift, IO}
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.dataaccess.{GoogleServicesDAO, SamDAO}
import org.broadinstitute.dsde.rawls.google.GooglePubSubDAO
import org.broadinstitute.dsde.rawls.google.GooglePubSubDAO.PubSubMessage
import org.broadinstitute.dsde.rawls.model.AttributeUpdateOperations.EntityUpdateDefinition
import org.broadinstitute.dsde.rawls.model.{RawlsUserEmail, UserInfo, WorkspaceName}
import org.broadinstitute.dsde.rawls.workspace.WorkspaceService
import org.broadinstitute.dsde.workbench.google2.{GcsBlobName, GoogleStorageService}
import org.broadinstitute.dsde.workbench.model.{UserInfo => _, _}
import org.broadinstitute.dsde.workbench.model.google.{GcsBucketName, GoogleProject}
import org.broadinstitute.dsde.workbench.util.FutureSupport
import spray.json._
import spray.json.DefaultJsonProtocol._

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.{FiniteDuration, _}
import scala.language.postfixOps

/**
  * Created by mbemis on 10/17/19.
  */
object AvroUpsertMonitorSupervisor {
  sealed trait AvroUpsertMonitorSupervisorMessage
  case object Init extends AvroUpsertMonitorSupervisorMessage
  case object Start extends AvroUpsertMonitorSupervisorMessage

  def props(
             pollInterval: FiniteDuration,
             pollIntervalJitter: FiniteDuration,
             workspaceService: UserInfo => WorkspaceService,
             googleServicesDAO: GoogleServicesDAO,
             samDAO: SamDAO,
             googleStorage: GoogleStorageService[IO],
             pubSubDao: GooglePubSubDAO,
             avroUpsertPubSubProject: GoogleProject,
             pubSubTopicName: String,
             pubSubSubscriptionName: String,
             avroUpsertBucketName: String,
             workerCount: Int)(implicit executionContext: ExecutionContext, cs: ContextShift[IO]): Props =
    Props(
      new AvroUpsertMonitorSupervisor(
        pollInterval,
        pollIntervalJitter,
        workspaceService,
        googleServicesDAO,
        samDAO,
        googleStorage,
        pubSubDao,
        avroUpsertPubSubProject,
        pubSubTopicName,
        pubSubSubscriptionName,
        avroUpsertBucketName,
        workerCount))
}

class AvroUpsertMonitorSupervisor(
                                       val pollInterval: FiniteDuration,
                                       pollIntervalJitter: FiniteDuration,
                                       workspaceService: UserInfo => WorkspaceService,
                                       googleServicesDAO: GoogleServicesDAO,
                                       samDAO: SamDAO,
                                       googleStorage: GoogleStorageService[IO],
                                       pubSubDao: GooglePubSubDAO,
                                       avroUpsertPubSubProject: GoogleProject,
                                       pubSubTopicName: String,
                                       pubSubSubscriptionName: String,
                                       avroUpsertBucketName: String,
                                       workerCount: Int)
  extends Actor
    with LazyLogging {
  import AvroUpsertMonitorSupervisor._
  import context._

  self ! Init

  override def receive = {
    case Init => init pipeTo self
    case Start => for (i <- 1 to workerCount) startOne()
    case Status.Failure(t) => logger.error("error initializing avro upsert monitor", t)
  }

  def topicToFullPath(topicName: String) = s"projects/${avroUpsertPubSubProject.value}/topics/${pubSubTopicName}" //TODO: read from config

  def init =
    for {
      _ <- pubSubDao.createSubscription(pubSubTopicName, pubSubSubscriptionName) //TODO: read from config
    } yield Start

  def startOne(): Unit = {
    logger.info("starting AvroUpsertMonitorActor")
    actorOf(AvroUpsertMonitor.props(pollInterval, pollIntervalJitter, workspaceService, googleServicesDAO, samDAO, googleStorage, pubSubDao, pubSubSubscriptionName, avroUpsertBucketName))
  }

  override val supervisorStrategy =
    OneForOneStrategy() {
      case e => {
        logger.error("unexpected error in avro upsert monitor", e)
        // start one to replace the error, stop the errored child so that we also drop its mailbox (i.e. restart not good enough)
        startOne()
        Stop
      }
    }
}

object AvroUpsertMonitor {
  case object StartMonitorPass

  def props(
             pollInterval: FiniteDuration,
             pollIntervalJitter: FiniteDuration,
             workspaceService: UserInfo => WorkspaceService,
             googleServicesDAO: GoogleServicesDAO,
             samDAO: SamDAO,
             googleStorage: GoogleStorageService[IO],
             pubSubDao: GooglePubSubDAO,
             pubSubSubscriptionName: String,
             avroUpsertBucketName: String): Props =
    Props(new AvroUpsertMonitorActor(pollInterval, pollIntervalJitter, workspaceService, googleServicesDAO, samDAO, googleStorage, pubSubDao, pubSubSubscriptionName, avroUpsertBucketName))
}

class AvroUpsertMonitorActor(
                                  val pollInterval: FiniteDuration,
                                  pollIntervalJitter: FiniteDuration,
                                  workspaceService: UserInfo => WorkspaceService,
                                  googleServicesDAO: GoogleServicesDAO,
                                  samDAO: SamDAO,
                                  googleStorage: GoogleStorageService[IO],
                                  pubSubDao: GooglePubSubDAO,
                                  pubSubSubscriptionName: String,
                                  avroUpsertBucketName: String)
  extends Actor
    with LazyLogging
    with FutureSupport {
  import AvroUpsertMonitor._
  import context._

  case class AvroUpsertJson(namespace: String, name: String, userSubjectId: String, userEmail: String, payload: Array[EntityUpdateDefinition])

  implicit val avroUpsertJsonFormat = jsonFormat5(AvroUpsertJson)

  self ! StartMonitorPass

  // fail safe in case this actor is idle too long but not too fast (1 second lower limit)
  setReceiveTimeout(max((pollInterval + pollIntervalJitter) * 10, 1 second))

  private def max(durations: FiniteDuration*): FiniteDuration = {
    implicit val finiteDurationIsOrdered = scala.concurrent.duration.FiniteDuration.FiniteDurationIsOrdered
    durations.max
  }

  override def receive = {
    case StartMonitorPass =>
      // start the process by pulling a message and sending it back to self
      pubSubDao.pullMessages(pubSubSubscriptionName, 1).map(_.headOption) pipeTo self

    case Some(message: PubSubMessage) =>
      logger.info(s"received avro upsert message: $message")
      val (jobId, file) = parseMessage(message)
      initAvroUpsert(jobId, file).map(response => (response, message.ackId)) pipeTo self

    case None =>
      // there was no message so wait and try again
      val nextTime = org.broadinstitute.dsde.workbench.util.addJitter(pollInterval, pollIntervalJitter)
      system.scheduler.scheduleOnce(nextTime.asInstanceOf[FiniteDuration], self, StartMonitorPass)

    case ((), ackId: String) =>
      acknowledgeMessage(ackId).map(_ => StartMonitorPass) pipeTo self

    case Status.Failure(t) => throw t

    case ReceiveTimeout =>
      throw new WorkbenchException("AvroUpsertMonitorActor has received no messages for too long")

    case x =>
      logger.info(s"unhandled $x")
//      self ! StartMonitorPass TODO: should this restart the monitor if we've got an unhandled message? I think so.
  }

  private def initAvroUpsert(jobId: String, file: String): Future[Unit] = {
    for {
      avroJson <- readUpsertObject(s"$jobId/$file")
      petSAJson <- samDAO.getPetServiceAccountKeyForUser(avroJson.namespace, RawlsUserEmail(avroJson.userEmail))
      petUserInfo <- googleServicesDAO.getUserInfoUsingJson(petSAJson)
      _ <- workspaceService.apply(petUserInfo).batchUpdateEntities(WorkspaceName(avroJson.namespace, avroJson.name), avroJson.payload.toSeq, true)
    } yield ()
  }

  private def acknowledgeMessage(ackId: String) =
    pubSubDao.acknowledgeMessagesById(pubSubSubscriptionName, Seq(ackId))

  private def parseMessage(message: PubSubMessage) = {
    val objectIdPattern = """"([^/]+)/([^/]+)"""".r

    message.contents.parseJson.asJsObject.getFields("name").head.compactPrint match {
      case objectIdPattern(jobId, file) => (jobId, file)
      case m => throw new Exception(s"unable to parse message $m")
    }
  }

  private def readUpsertObject(path: String): Future[AvroUpsertJson] = {
    googleStorage.getBlobBody(GcsBucketName(avroUpsertBucketName), GcsBlobName(path)).compile.to[Array].unsafeToFuture().map { byteArray =>
      byteArray.map(_.toChar).mkString.parseJson.convertTo[AvroUpsertJson]
    }
  }

  override val supervisorStrategy =
    OneForOneStrategy() {
      case e => {
        Escalate
      }
    }
}
