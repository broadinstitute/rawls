package org.broadinstitute.dsde.rawls.monitor

import java.io.ByteArrayInputStream
import java.util.UUID
import java.util.zip.GZIPInputStream

import akka.actor.SupervisorStrategy.{Escalate, Stop}
import akka.actor._
import akka.pattern._
import cats.effect.{ContextShift, IO}
import cats.implicits._
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.dataaccess.ImportStatuses.ImportStatus
import org.broadinstitute.dsde.rawls.dataaccess.{GoogleServicesDAO, ImportServiceDAO, ImportStatuses, SamDAO}
import org.broadinstitute.dsde.rawls.google.GooglePubSubDAO
import org.broadinstitute.dsde.rawls.google.GooglePubSubDAO.PubSubMessage
import org.broadinstitute.dsde.rawls.model.AttributeUpdateOperations.EntityUpdateDefinition
import org.broadinstitute.dsde.rawls.model.{RawlsUserEmail, WorkspaceName}
import org.broadinstitute.dsde.rawls.monitor.AvroUpsertMonitorSupervisor.AvroUpsertMonitorConfig
import org.broadinstitute.dsde.rawls.workspace.WorkspaceService
import org.broadinstitute.dsde.workbench.google2.{GcsBlobName, GoogleStorageService}
import org.broadinstitute.dsde.workbench.model.{UserInfo => _, _}
import org.broadinstitute.dsde.workbench.model.google.{GcsBucketName, GoogleProject}
import org.broadinstitute.dsde.workbench.util.FutureSupport
import spray.json._
import spray.json.DefaultJsonProtocol._

import scala.concurrent.duration.{FiniteDuration, _}
import scala.language.postfixOps
import scala.util.{Failure, Success}
import org.broadinstitute.dsde.rawls.model.UserInfo
import org.joda.time.DateTime

import scala.concurrent.{ExecutionContext, Future}

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
                                           importRequestPubSubTopic: String,
                                           importRequestPubSubSubscription: String,
                                           importStatusPubSubTopic: String,
                                           importStatusPubSubSubscription: String,
                                           bucketName: String,
                                           batchSize: Int,
                                           workerCount: Int)

  def props(workspaceService: UserInfo => WorkspaceService,
             googleServicesDAO: GoogleServicesDAO,
             samDAO: SamDAO,
             googleStorage: GoogleStorageService[IO],
             pubSubDao: GooglePubSubDAO,
             importServiceDAO: ImportServiceDAO,
             avroUpsertMonitorConfig: AvroUpsertMonitorConfig)(implicit executionContext: ExecutionContext, cs: ContextShift[IO]): Props =
    Props(
      new AvroUpsertMonitorSupervisor(
        workspaceService,
        googleServicesDAO,
        samDAO,
        googleStorage,
        pubSubDao,
        importServiceDAO,
        avroUpsertMonitorConfig))
}

class AvroUpsertMonitorSupervisor(workspaceService: UserInfo => WorkspaceService,
                                  googleServicesDAO: GoogleServicesDAO,
                                  samDAO: SamDAO,
                                  googleStorage: GoogleStorageService[IO],
                                  pubSubDao: GooglePubSubDAO,
                                  importServiceDAO: ImportServiceDAO,
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

//  def topicToFullPath(topicName: String) = s"projects/${avroUpsertMonitorConfig.pubSubProject.value}/topics/${avroUpsertMonitorConfig.importRequestPubSubTopic}"

  def init =
    for {
      _ <- pubSubDao.createSubscription(avroUpsertMonitorConfig.importRequestPubSubTopic, avroUpsertMonitorConfig.importRequestPubSubSubscription, Some(600)) //TODO: read from config
      _ <- pubSubDao.createSubscription(avroUpsertMonitorConfig.importStatusPubSubTopic, avroUpsertMonitorConfig.importStatusPubSubSubscription, Some(600)) //TODO: read from config
    } yield Start

  def startOne(): Unit = {
    logger.info("starting AvroUpsertMonitorActor")
    actorOf(AvroUpsertMonitor.props(avroUpsertMonitorConfig.pollInterval, avroUpsertMonitorConfig.pollIntervalJitter, workspaceService, googleServicesDAO, samDAO, googleStorage, pubSubDao, avroUpsertMonitorConfig.importRequestPubSubSubscription, avroUpsertMonitorConfig.importStatusPubSubTopic, importServiceDAO, avroUpsertMonitorConfig.bucketName, avroUpsertMonitorConfig.batchSize))
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
             importServicePubSubTopic: String,
             importServiceDAO: ImportServiceDAO,
             avroUpsertBucketName: String,
             batchSize: Int)(implicit cs: ContextShift[IO]): Props =
    Props(new AvroUpsertMonitorActor(pollInterval, pollIntervalJitter, workspaceService, googleServicesDAO, samDAO, googleStorage, pubSubDao, pubSubSubscriptionName, importServicePubSubTopic, importServiceDAO, avroUpsertBucketName, batchSize))
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
                                  importServicePubSubTopic: String,
                                  importServiceDAO: ImportServiceDAO,
                                  avroUpsertBucketName: String,
                                  batchSize: Int)(implicit cs: ContextShift[IO])
  extends Actor
    with LazyLogging
    with FutureSupport {
  import AvroUpsertMonitor._
  import context._

  case class AvroMetadataJson(namespace: String, name: String, userSubjectId: String, userEmail: String, jobId: String, startTime: String)
  implicit val avroMetadataJsonFormat = jsonFormat6(AvroMetadataJson)

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
      logger.info(s"received async upsert message: $message")
      //post to import service pubsub topic
      val (workspaceName, userEmail, importId, upsertFile) = parseMessage(message)

      for {
        petUserInfo <- getPetServiceAccountUserInfo(workspaceName, userEmail)
        importStatus <- importServiceDAO.getImportStatus(importId, workspaceName, petUserInfo)
        _ <- importStatus match {
            case Some(ImportStatuses.ReadyForUpsert) => {
              publishMessageToUpdateImportStatus(ImportStatuses.Upserting)
              toFutureTry(initUpsert(upsertFile, importId, message.ackId, workspaceName, petUserInfo)) map {
                case Success(_) => markImportAsDone(importId, workspaceName, petUserInfo)
                case Failure(t) => publishMessageToUpdateImportStatus(ImportStatuses.Error(t.getMessage))
              }
            }
            case None => publishMessageToUpdateImportStatus(ImportStatuses.Error("Status Not Found")) // !!!
            case _ => publishMessageToUpdateImportStatus(ImportStatuses.Error("Status Was Wrong")) // !!!
          }
        } yield ()

    case None =>
      // there was no message so wait and try again
      val nextTime = org.broadinstitute.dsde.workbench.util.addJitter(pollInterval, pollIntervalJitter)
      system.scheduler.scheduleOnce(nextTime, self, StartMonitorPass)

    case ImportComplete => self ! StartMonitorPass

    case Status.Failure(t) => {
      throw t
      self ! StartMonitorPass
    }

    case ReceiveTimeout => throw new WorkbenchException("AvroUpsertMonitorActor has received no messages for too long")

    case x =>
      logger.info(s"unhandled $x")
      self ! None
  }

  private def getPetServiceAccountUserInfo(workspaceName: WorkspaceName, userEmail: RawlsUserEmail) = {
    for {
      petSAJson <- samDAO.getPetServiceAccountKeyForUser(workspaceName.namespace, userEmail)
      petUserInfo <- googleServicesDAO.getUserInfoUsingJson(petSAJson)
    } yield {
      petUserInfo
    }
  }

  private def markImportAsDone(importId: UUID, workspaceName: WorkspaceName, userInfo: UserInfo) = {
    for {
      importStatus <- importServiceDAO.getImportStatus(importId, workspaceName, userInfo)
      _ <- importStatus match {
        case Some(ImportStatuses.Upserting) => publishMessageToUpdateImportStatus(ImportStatuses.Done)
        case None => publishMessageToUpdateImportStatus(ImportStatuses.Error("Status Not Found"))
        case _ => publishMessageToUpdateImportStatus(ImportStatuses.Error("Status Was Wrong"))
      }
    } yield ()
  }

  private def publishMessageToUpdateImportStatus(importStatus: ImportStatus) = {
    val now = DateTime.now
    val requiredAttributes = Map("updateStatus" -> ImportStatuses.Done.toString, "updatedDate" -> now.toString)
    val message = requiredAttributes ++ {importStatus match {
      case error: ImportStatuses.Error => Map("errorMessage" -> error.message)
      case _ => Map()
    }}
    pubSubDao.publishMessages(importServicePubSubTopic, Seq(message.toJson.compactPrint))
  }


  private def initUpsert(upsertFile: String, jobId: UUID, ackId: String, workspaceName: WorkspaceName, userInfo: UserInfo): Future[Unit] = {
    for {
      //ack the response after we load the json into memory. pro: don't have to worry about ack timeouts for long operations, con: if someone restarts rawls here the upsert is lost
      upsertJson <- readUpsertObject(upsertFile)
      ackResponse <- acknowledgeMessage(ackId)
      upsertResults <-
        upsertJson.grouped(batchSize).toList.traverse { upsertBatch =>
          logger.info(s"starting upsert for $jobId with ${upsertBatch.size} entities ...")
          IO.fromFuture(IO(workspaceService.apply(userInfo).batchUpdateEntities(WorkspaceName(workspaceName.namespace, workspaceName.name), upsertBatch, true)))
        }.unsafeToFuture
    } yield {
      logger.info(s"completed async upsert job ${jobId} for user: ${userInfo.userEmail} with ${upsertJson.size} entities")
      ()
    }
  }

//  private def writeResult(path: String, message: String): Future[Unit] = {
//    logger.info(s"writing import result to $path")
//    googleStorage.createBlob(GcsBucketName(avroUpsertBucketName), GcsBlobName(path), message.getBytes).compile.drain.unsafeToFuture()
//  }

//  private def resultString(message: String) = s"""{\"message\":\"${message}\"}"""

  private def acknowledgeMessage(ackId: String) = {
    logger.info(s"acking message with ackId $ackId")
    pubSubDao.acknowledgeMessagesById(pubSubSubscriptionName, Seq(ackId))
  }

  private def parseMessage(message: PubSubMessage) = {
    val attributes = List("workspaceNamespace", "workspaceName", "userEmail", "jobId", "upsertFile")
    val extractedAttributes = attributes.map(att => att -> message.attributes.get(att)).toMap
    if (extractedAttributes.values.toList.contains(None)) {
      publishMessageToUpdateImportStatus(ImportStatuses.Error("Attributes were wrong")) // !!!
      throw new Exception(s"unable to parse message $message")
    }
    else
      extractedAttributes.map(att => att._2.get) match {
        case List(workspaceNamespace, workspaceName, userEmail, jobId, upsertFile) =>
          (WorkspaceName(workspaceNamespace, workspaceName), RawlsUserEmail(userEmail), UUID.fromString(jobId), upsertFile)
      }
  }

  private def readUpsertObject(path: String): Future[Seq[EntityUpdateDefinition]] = {
    readObject[Seq[EntityUpdateDefinition]](path, decompress = true)
  }

  private def readObject[T](path: String, decompress: Boolean = false)(implicit reader: JsonReader[T]): Future[T] = {
    logger.info(s"reading ${if (decompress) "compressed " else ""}object $path ...")
    googleStorage.getBlobBody(GcsBucketName(avroUpsertBucketName), GcsBlobName(path)).compile.to[Array].unsafeToFuture().map { byteArray =>
      val bytes = if (decompress) decompressGzip(byteArray) else byteArray
      logger.info(s"successfully read $path; parsing ...")
      val obj = bytes.map(_.toChar).mkString.parseJson.convertTo[T]
      logger.info(s"successfully parsed $path")
      obj
    }
  }

  private def decompressGzip(compressed: Array[Byte]): Array[Byte] = {
    val inputStream = new GZIPInputStream(new ByteArrayInputStream(compressed))
    val result = org.apache.commons.io.IOUtils.toByteArray(inputStream)
    inputStream.close()
    result
  }


  override val supervisorStrategy =
    OneForOneStrategy() {
      case e => {
        Escalate
      }
    }
}