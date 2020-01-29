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
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.google.GooglePubSubDAO
import org.broadinstitute.dsde.rawls.google.GooglePubSubDAO.PubSubMessage
import org.broadinstitute.dsde.rawls.model.AttributeUpdateOperations.EntityUpdateDefinition
import org.broadinstitute.dsde.rawls.model.{RawlsUserEmail, WorkspaceName}
import org.broadinstitute.dsde.rawls.monitor.AvroUpsertMonitorSupervisor.AvroUpsertMonitorConfig
import org.broadinstitute.dsde.rawls.workspace.WorkspaceService
import org.broadinstitute.dsde.workbench.google2.{GcsBlobName, GoogleStorageService}
import org.broadinstitute.dsde.workbench.model.{UserInfo => _, _}
import org.broadinstitute.dsde.workbench.model.google.GcsBucketName
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
                                           importRequestPubSubTopic: String,
                                           importRequestPubSubSubscription: String,
                                           importStatusPubSubTopic: String,
                                           importStatusPubSubSubscription: String,
                                           bucketName: GcsBucketName,
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

  def init =
    for {
      _ <- pubSubDao.createSubscription(avroUpsertMonitorConfig.importRequestPubSubTopic, avroUpsertMonitorConfig.importRequestPubSubSubscription, Some(600))
      _ <- pubSubDao.createSubscription(avroUpsertMonitorConfig.importStatusPubSubTopic, avroUpsertMonitorConfig.importStatusPubSubSubscription, Some(600))
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
             importStatusPubSubTopic: String,
             importServiceDAO: ImportServiceDAO,
             avroUpsertBucketName: GcsBucketName,
             batchSize: Int)(implicit cs: ContextShift[IO]): Props =
    Props(new AvroUpsertMonitorActor(pollInterval, pollIntervalJitter, workspaceService, googleServicesDAO, samDAO, googleStorage, pubSubDao, pubSubSubscriptionName, importStatusPubSubTopic, importServiceDAO, avroUpsertBucketName, batchSize))
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
                              importStatusPubSubTopic: String,
                              importServiceDAO: ImportServiceDAO,
                              avroUpsertBucketName: GcsBucketName,
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
      val attributes = parseMessage(message)
      for {
        petUserInfo <- getPetServiceAccountUserInfo(attributes.workspace, attributes.userEmail)
        importStatus <- importServiceDAO.getImportStatus(attributes.importId, attributes.workspace, petUserInfo)
        _ <- importStatus match {
            case Some(ImportStatuses.ReadyForUpsert) => {
              publishMessageToUpdateImportStatus(attributes.importId, ImportStatuses.Upserting)
              toFutureTry(initUpsert(attributes.upsertFile, attributes.importId, message.ackId, attributes.workspace, petUserInfo)) map {
                case Success(_) => markImportAsDone(attributes.importId, attributes.workspace, petUserInfo)
                case Failure(t) => publishMessageToUpdateImportStatus(attributes.importId, ImportStatuses.Error(t.getMessage))
              }
            }
            case None => publishMessageToUpdateImportStatus(attributes.importId, ImportStatuses.Error("Import status not found"))
            case _ => publishMessageToUpdateImportStatus(attributes.importId, ImportStatuses.Error("Import was not ready for upsert"))
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
        case Some(ImportStatuses.Upserting) => publishMessageToUpdateImportStatus(importId, ImportStatuses.Done)
        case None => publishMessageToUpdateImportStatus(importId, ImportStatuses.Error("Import status not found"))
        case _ => publishMessageToUpdateImportStatus(importId, ImportStatuses.Error("Upsert finished, but import status was not correct."))
      }
    } yield ()
  }

  private def publishMessageToUpdateImportStatus(importId: UUID, importStatus: ImportStatus) = {
    val now = DateTime.now
    val requiredAttributes = Map("importId" -> importId.toString, "updateStatus" -> importStatus.toString, "updatedDate" -> now.toString)
    val attributes = requiredAttributes ++ {importStatus match {
      case error: ImportStatuses.Error => Map("errorMessage" -> error.message)
      case _ => Map()
    }}
    val message = GooglePubSubDAO.MessageRequest("", attributes)
    pubSubDao.publishMessages(importStatusPubSubTopic, Seq(message))
  }


  private def initUpsert(upsertFile: String, jobId: UUID, ackId: String, workspaceName: WorkspaceName, userInfo: UserInfo): Future[Unit] = {
    for {
      //ack the response after we load the json into memory. pro: don't have to worry about ack timeouts for long operations, con: if someone restarts rawls here the upsert is lost
      upsertJson <- readUpsertObject(upsertFile)
      ackResponse <- acknowledgeMessage(ackId)
      ws = workspaceService.apply(userInfo)
      upsertResults <- {
        upsertJson.grouped(batchSize).toList.traverse { upsertBatch =>
          logger.info(s"starting upsert for $jobId with ${upsertBatch.size} entities ...")
          IO.fromFuture(IO(ws.batchUpdateEntities(workspaceName, upsertBatch, true)))
        }.unsafeToFuture}
    } yield {
      logger.info(s"completed async upsert job ${jobId} for user: ${userInfo.userEmail} with ${upsertJson.size} entities")
      ()
    }
  }


  private def acknowledgeMessage(ackId: String) = {
    logger.info(s"acking message with ackId $ackId")
    pubSubDao.acknowledgeMessagesById(pubSubSubscriptionName, Seq(ackId))
  }

  case class AvroUpsertAttributes(workspace: WorkspaceName, userEmail: RawlsUserEmail, importId: UUID, upsertFile: String)

  private def parseMessage(message: PubSubMessage) = {
    val workspaceNamespace = "workspaceNamespace"
    val workspaceName = "workspaceName"
    val userEmail = "userEmail"
    val jobId = "jobId"
    val upsertFile = "upsertFile"

    def attributeNotFoundException(attribute: String) =  throw new Exception(s"unable to parse message - attribute $attribute not found in ${message.attributes}")

    AvroUpsertAttributes(
      WorkspaceName(message.attributes.getOrElse(workspaceNamespace, attributeNotFoundException(workspaceNamespace)),
        message.attributes.getOrElse(workspaceName, attributeNotFoundException(workspaceName))),
      RawlsUserEmail(message.attributes.getOrElse(userEmail, attributeNotFoundException(userEmail))),
      UUID.fromString(message.attributes.getOrElse(jobId, attributeNotFoundException(jobId))),
      message.attributes.getOrElse(upsertFile, attributeNotFoundException(upsertFile))
    )
  }

  private def readUpsertObject(path: String): Future[Seq[EntityUpdateDefinition]] = {
    readObject[Seq[EntityUpdateDefinition]](path, decompress = true)
  }

  private def readObject[T](path: String, decompress: Boolean = false)(implicit reader: JsonReader[T]): Future[T] = {
    logger.info(s"reading ${if (decompress) "compressed " else ""}object $path ...")
    googleStorage.getBlobBody(avroUpsertBucketName, GcsBlobName(path)).compile.to[Array].unsafeToFuture().map { byteArray =>
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