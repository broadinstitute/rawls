package org.broadinstitute.dsde.rawls.monitor

import akka.actor.SupervisorStrategy.{Escalate, Stop}
import akka.actor._
import akka.pattern._
import cats.effect.{ContextShift, IO}
import cats.implicits._
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.dataaccess.{GoogleServicesDAO, SamDAO}
import org.broadinstitute.dsde.rawls.google.GooglePubSubDAO
import org.broadinstitute.dsde.rawls.google.GooglePubSubDAO.PubSubMessage
import org.broadinstitute.dsde.rawls.model.AttributeUpdateOperations.EntityUpdateDefinition
import org.broadinstitute.dsde.rawls.model.{RawlsUserEmail, UserInfo, WorkspaceName}
import org.broadinstitute.dsde.rawls.monitor.AvroUpsertMonitorSupervisor.AvroUpsertMonitorConfig
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

  final case class AvroUpsertMonitorConfig(pollInterval: FiniteDuration,
                                           pollIntervalJitter: FiniteDuration,
                                           pubSubProject: GoogleProject,
                                           pubSubTopic: String,
                                           pubSubSubscription: String,
                                           bucketName: String,
                                           batchSize: Int,
                                           workerCount: Int)

  def props(workspaceService: UserInfo => WorkspaceService,
             googleServicesDAO: GoogleServicesDAO,
             samDAO: SamDAO,
             googleStorage: GoogleStorageService[IO],
             pubSubDao: GooglePubSubDAO,
             avroUpsertMonitorConfig: AvroUpsertMonitorConfig)(implicit executionContext: ExecutionContext, cs: ContextShift[IO]): Props =
    Props(
      new AvroUpsertMonitorSupervisor(
        workspaceService,
        googleServicesDAO,
        samDAO,
        googleStorage,
        pubSubDao,
        avroUpsertMonitorConfig))
}

class AvroUpsertMonitorSupervisor(workspaceService: UserInfo => WorkspaceService,
                                  googleServicesDAO: GoogleServicesDAO,
                                  samDAO: SamDAO,
                                  googleStorage: GoogleStorageService[IO],
                                  pubSubDao: GooglePubSubDAO,
                                  avroUpsertMonitorConfig: AvroUpsertMonitorConfig)(implicit cs: ContextShift[IO])
  extends Actor
    with LazyLogging {
  import AvroUpsertMonitorSupervisor._
  import context._

  self ! Init

  override def receive = {
    case Init => init pipeTo self
    case Start => for (i <- 1 to avroUpsertMonitorConfig.workerCount) startOne()
    case Status.Failure(t) => logger.error("error initializing avro upsert monitor", t)
  }

  def topicToFullPath(topicName: String) = s"projects/${avroUpsertMonitorConfig.pubSubProject.value}/topics/${avroUpsertMonitorConfig.pubSubTopic}"

  def init =
    for {
      _ <- pubSubDao.createSubscription(avroUpsertMonitorConfig.pubSubTopic, avroUpsertMonitorConfig.pubSubSubscription, Some(600)) //TODO: read from config
    } yield Start

  def startOne(): Unit = {
    logger.info("starting AvroUpsertMonitorActor")
    actorOf(AvroUpsertMonitor.props(avroUpsertMonitorConfig.pollInterval, avroUpsertMonitorConfig.pollIntervalJitter, workspaceService, googleServicesDAO, samDAO, googleStorage, pubSubDao, avroUpsertMonitorConfig.pubSubSubscription, avroUpsertMonitorConfig.bucketName, avroUpsertMonitorConfig.batchSize))
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
  case object ImportComplete

  val objectIdPattern = """"([^/]+)/([^/]+)"""".r

  def props(
             pollInterval: FiniteDuration,
             pollIntervalJitter: FiniteDuration,
             workspaceService: UserInfo => WorkspaceService,
             googleServicesDAO: GoogleServicesDAO,
             samDAO: SamDAO,
             googleStorage: GoogleStorageService[IO],
             pubSubDao: GooglePubSubDAO,
             pubSubSubscriptionName: String,
             avroUpsertBucketName: String,
             batchSize: Int)(implicit cs: ContextShift[IO]): Props =
    Props(new AvroUpsertMonitorActor(pollInterval, pollIntervalJitter, workspaceService, googleServicesDAO, samDAO, googleStorage, pubSubDao, pubSubSubscriptionName, avroUpsertBucketName, batchSize))
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
                                  avroUpsertBucketName: String,
                                  batchSize: Int)(implicit cs: ContextShift[IO])
  extends Actor
    with LazyLogging
    with FutureSupport {
  import AvroUpsertMonitor._
  import context._

  case class AvroMetadataJson(namespace: String, name: String, userSubjectId: String, userEmail: String, jobId: String)
  implicit val avroMetadataJsonFormat = jsonFormat5(AvroMetadataJson)

  self ! StartMonitorPass

  // fail safe in case this actor is idle too long but not too fast (1 second lower limit)
  setReceiveTimeout(max((pollInterval + pollIntervalJitter) * 20, 1 second))

  private def max(durations: FiniteDuration*): FiniteDuration = {
    implicit val finiteDurationIsOrdered = scala.concurrent.duration.FiniteDuration.FiniteDurationIsOrdered
    durations.max
  }

  override def receive = {
    case StartMonitorPass =>
      // start the process by pulling a message and sending it back to self
      pubSubDao.pullMessages(pubSubSubscriptionName, 1).map(_.headOption) pipeTo self

    case Some(message: PubSubMessage) =>
      //we received a message, so we will parse it and try to upsert it
      logger.info(s"received avro upsert message: $message")
      val (jobId, file) = parseMessage(message)

      file match {
        case "upsert.json" => initAvroUpsert(jobId, message.ackId).map(_ => ImportComplete) pipeTo self
        case _ => acknowledgeMessage(message.ackId).map(_ => None) pipeTo self //some other file (i.e. success/failure log, metadata.json)
      }

    case None =>
      // there was no message so wait and try again
      val nextTime = org.broadinstitute.dsde.workbench.util.addJitter(pollInterval, pollIntervalJitter)
      system.scheduler.scheduleOnce(nextTime, self, StartMonitorPass)

    case ImportComplete => self ! StartMonitorPass

    case Status.Failure(t) => throw t

    case ReceiveTimeout => throw new WorkbenchException("AvroUpsertMonitorActor has received no messages for too long")

    case x =>
      logger.info(s"unhandled $x")
      self ! None
  }

  private def initAvroUpsert(jobId: String, ackId: String): Future[Unit] = {
    //start this Future asap because it can be long-running
    val readUpsertObjectFuture = readUpsertObject(s"$jobId/upsert.json")

    for {
      avroMetadataJson <- readMetadataObject(s"$jobId/metadata.json")
      //ack the response after we load the json into memory. pro: don't have to worry about ack timeouts for long operations, con: if someone restarts rawls here the uspert is lost
      petSAJson <- samDAO.getPetServiceAccountKeyForUser(avroMetadataJson.namespace, RawlsUserEmail(avroMetadataJson.userEmail))
      petUserInfo <- googleServicesDAO.getUserInfoUsingJson(petSAJson)
      avroUpsertJson <- readUpsertObjectFuture
      ackResponse <- acknowledgeMessage(ackId)
      upsertResults <-
        avroUpsertJson.grouped(batchSize).toList.traverse { upsertBatch =>
          IO.fromFuture(IO(workspaceService.apply(petUserInfo).batchUpdateEntities(WorkspaceName(avroMetadataJson.namespace, avroMetadataJson.name), upsertBatch, true)))
        }.unsafeToFuture
    } yield {
      logger.info(s"completed Avro upsert job ${avroMetadataJson.jobId} for user: ${avroMetadataJson.userEmail} with size ${avroUpsertJson.size} entities")
      ()
    }
  }

  private def acknowledgeMessage(ackId: String) =
    pubSubDao.acknowledgeMessagesById(pubSubSubscriptionName, Seq(ackId))

  private def parseMessage(message: PubSubMessage) = {
    message.contents.parseJson.asJsObject.getFields("name").head.compactPrint match {
      case objectIdPattern(jobId, file) => (jobId, file)
      case m => throw new Exception(s"unable to parse message $m")
    }
  }

  private def readUpsertObject(path: String): Future[Seq[EntityUpdateDefinition]] = {
    readObject[Seq[EntityUpdateDefinition]](path)
  }

  private def readMetadataObject(path: String): Future[AvroMetadataJson] = {
    readObject[AvroMetadataJson](path)
  }

  private def readObject[T](path: String)(implicit reader: JsonReader[T]): Future[T] = {
    googleStorage.getBlobBody(GcsBucketName(avroUpsertBucketName), GcsBlobName(path)).compile.to[Array].unsafeToFuture().map { byteArray =>
      byteArray.map(_.toChar).mkString.parseJson.convertTo[T]
    }
  }

  override val supervisorStrategy =
    OneForOneStrategy() {
      case e => {
        Escalate
      }
    }
}
