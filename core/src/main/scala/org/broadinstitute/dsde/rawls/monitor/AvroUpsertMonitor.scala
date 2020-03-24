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
import org.broadinstitute.dsde.rawls.model.ImportStatuses.ImportStatus
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.google.GooglePubSubDAO
import org.broadinstitute.dsde.rawls.google.GooglePubSubDAO.PubSubMessage
import org.broadinstitute.dsde.rawls.model.AttributeUpdateOperations.EntityUpdateDefinition
import org.broadinstitute.dsde.rawls.model.{ImportStatuses, RawlsUserEmail, UserInfo, WorkspaceName}
import org.broadinstitute.dsde.rawls.monitor.AvroUpsertMonitorSupervisor.{AvroUpsertMonitorConfig, updateImportStatusFormat}
import org.broadinstitute.dsde.rawls.util.AuthUtil
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
                                           arrowPubSubTopic: String,        // remove when cutting over to import service
                                           arrowPubSubSubscription: String, // remove when cutting over to import service
                                           arrowBucketName: String,         // remove when cutting over to import service
                                           importRequestPubSubTopic: String,
                                           importRequestPubSubSubscription: String,
                                           updateImportStatusPubSubTopic: String,
                                           ackDeadlineSeconds: Int,
                                           batchSize: Int,
                                           workerCount: Int)

  def props(workspaceService: UserInfo => WorkspaceService,
            googleServicesDAO: GoogleServicesDAO,
            samDAO: SamDAO,
            googleStorage: GoogleStorageService[IO],
            pubSubDAO: GooglePubSubDAO,
            arrowPubSubDAO: GooglePubSubDAO, // remove when cutting over to import service
            importServiceDAO: ImportServiceDAO,
            avroUpsertMonitorConfig: AvroUpsertMonitorConfig)(implicit executionContext: ExecutionContext, cs: ContextShift[IO]): Props =
    Props(
      new AvroUpsertMonitorSupervisor(
        workspaceService,
        googleServicesDAO,
        samDAO,
        googleStorage,
        pubSubDAO,
        arrowPubSubDAO, // remove when cutting over to import service
        importServiceDAO,
        avroUpsertMonitorConfig))


  import spray.json.DefaultJsonProtocol._
  implicit val updateImportStatusFormat = jsonFormat5(UpdateImportStatus)
}

case class UpdateImportStatus(importId: String,
                              newStatus: String,
                              currentStatus: Option[String] = None,
                              errorMessage: Option[String] = None,
                              action: String = "status")


class AvroUpsertMonitorSupervisor(workspaceService: UserInfo => WorkspaceService,
                                  googleServicesDAO: GoogleServicesDAO,
                                  samDAO: SamDAO,
                                  googleStorage: GoogleStorageService[IO],
                                  pubSubDAO: GooglePubSubDAO,
                                  arrowPubSubDAO: GooglePubSubDAO, // remove when cutting over to import service
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
      _ <- arrowPubSubDAO.createTopic(avroUpsertMonitorConfig.arrowPubSubTopic)
      _ <- arrowPubSubDAO.createSubscription(avroUpsertMonitorConfig.arrowPubSubTopic, avroUpsertMonitorConfig.arrowPubSubSubscription, Some(avroUpsertMonitorConfig.ackDeadlineSeconds)) // remove when cutting over to import service
      _ <- pubSubDAO.createTopic(avroUpsertMonitorConfig.importRequestPubSubTopic)
      _ <- pubSubDAO.createSubscription(avroUpsertMonitorConfig.importRequestPubSubTopic, avroUpsertMonitorConfig.importRequestPubSubSubscription, Some(avroUpsertMonitorConfig.ackDeadlineSeconds))
    } yield Start

  def startOne(): Unit = {
    logger.info("starting AvroUpsertMonitorActor")
    actorOf(AvroUpsertMonitor.props(avroUpsertMonitorConfig.pollInterval, avroUpsertMonitorConfig.pollIntervalJitter, workspaceService, googleServicesDAO, samDAO, googleStorage, pubSubDAO, arrowPubSubDAO, avroUpsertMonitorConfig.arrowPubSubSubscription, avroUpsertMonitorConfig.arrowBucketName, avroUpsertMonitorConfig.importRequestPubSubSubscription, avroUpsertMonitorConfig.updateImportStatusPubSubTopic, importServiceDAO, avroUpsertMonitorConfig.batchSize))
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
             arrowPubSubDAO: GooglePubSubDAO,     // remove when cutting over to import service
             arrowPubSubSubscriptionName: String, // remove when cutting over to import service
             arrowUpsertBucketName: String,       // remove when cutting over to import service
             pubSubSubscriptionName: String,
             importStatusPubSubTopic: String,
             importServiceDAO: ImportServiceDAO,
             batchSize: Int)(implicit cs: ContextShift[IO]): Props =
    Props(new AvroUpsertMonitorActor(pollInterval, pollIntervalJitter, workspaceService, googleServicesDAO, samDAO, googleStorage, pubSubDao, arrowPubSubDAO, arrowPubSubSubscriptionName, arrowUpsertBucketName,
      pubSubSubscriptionName, importStatusPubSubTopic, importServiceDAO, batchSize))
}

class AvroUpsertMonitorActor(
                              val pollInterval: FiniteDuration,
                              pollIntervalJitter: FiniteDuration,
                              workspaceService: UserInfo => WorkspaceService,
                              val googleServicesDAO: GoogleServicesDAO,
                              val samDAO: SamDAO,
                              googleStorage: GoogleStorageService[IO],
                              pubSubDao: GooglePubSubDAO,
                              arrowPubSubDAO: GooglePubSubDAO,     // remove when cutting over to import service
                              arrowPubSubSubscriptionName: String, // remove when cutting over to import service
                              arrowUpsertBucketName: String,       // remove when cutting over to import service
                              pubSubSubscriptionName: String,
                              importStatusPubSubTopic: String,
                              importServiceDAO: ImportServiceDAO,
                              batchSize: Int)(implicit cs: ContextShift[IO])
  extends Actor
    with LazyLogging
    with FutureSupport
    with AuthUtil {
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

  var pullArrow = true

  /* Note there is a (rare) possibility for data overwrite as follows:
   * 1. Pub/Sub double-delivers an import request to Rawls. Rawls processes the first request and sends a message to
   *    import service to update the import job status.
   * 2. Import service has been asleep and a VM cold boots, taking a few extra seconds.
   * 3. While this is happening, a user modifies data through the API.
   * 4. Rawls processes the second import request. Import service has not updated its status, so Rawls proceeds.
   * 5. Rawls just wiped out the user's changes from #3.
   *
   * It is conceivable (with a large change in technology - think serializing all imports and guaranteeing in-order,
   * only-once delivery) that we could eliminate the kind of data overwrites that Pub/Sub makes us liable to see.
   * However, it is worth noting that we already accept the possibility that Terra users will overwrite each other's
   * data. We also acknowledge that the the risk of data overwrite is small, requiring pubsub to double-deliver a
   * message at the same time as Rawls receives another user request modifying the same entities. */
  override def receive = {
    case StartMonitorPass =>
      pullArrow = !pullArrow
      // start the process by pulling a message and sending it back to self
      if (pullArrow)
        arrowPubSubDAO.pullMessages(arrowPubSubSubscriptionName, 1).map(_.headOption) pipeTo self
      else
        pubSubDao.pullMessages(pubSubSubscriptionName, 1).map(_.headOption) pipeTo self

    case Some(message: PubSubMessage) =>
      // we received a message, so we will parse it and try to upsert it
      logger.info(s"received async upsert message: $message")

      // We decide whether to go down the CF path or the import service path based on whether the attributes contain a jobId.
      // The CF solution has the metadata in the message body, while the import service has the metadata as attributes on the message
      if (message.attributes.contains("jobId")) {
        importEntities(message)
      } else {
        arrowImportEntities(message)  // remove when cutting over to import service
      }

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

  private def importEntities(message: PubSubMessage) = {
    val attributes = parseMessage(message)
    val importFuture = for {
      petUserInfo <- getPetServiceAccountUserInfo(attributes.workspace.namespace, attributes.userEmail)
      importStatus <- importServiceDAO.getImportStatus(attributes.importId, attributes.workspace, petUserInfo)
      _ <- importStatus match {
        // Currently, there is only one upsert monitor thread - but if we decide to make more threads, we might
        // end up with a race condition where multiple threads are attempting the same import / updating the status
        // of the same import.
        case Some(status) if status == ImportStatuses.ReadyForUpsert => {
          publishMessageToUpdateImportStatus(attributes.importId, Option(status), ImportStatuses.Upserting, None)
          toFutureTry(initUpsert(attributes.upsertFile, attributes.importId, message.ackId, attributes.workspace, petUserInfo)) map {
            case Success(_) => publishMessageToUpdateImportStatus(attributes.importId, Option(status), ImportStatuses.Done, None)
            case Failure(t) => publishMessageToUpdateImportStatus(attributes.importId, Option(status), ImportStatuses.Error, Option(t.getMessage))
          }
        }
        case Some(_) => {
          logger.warn(s"Received a double message delivery for import ID [${attributes.importId}]")
          Future.unit
        }
        case None => publishMessageToUpdateImportStatus(attributes.importId, None, ImportStatuses.Error, Option("Import status not found"))
      }
    } yield ()
    importFuture.map(_ => ImportComplete) pipeTo self
  }

  private def publishMessageToUpdateImportStatus(importId: UUID, currentImportStatus: Option[ImportStatus], newImportStatus: ImportStatus, errorMessage: Option[String]) = {
    logger.info(s"asking to change import job $importId from $currentImportStatus to $newImportStatus ${errorMessage.getOrElse("")}")
    val updateImportStatus = UpdateImportStatus(importId.toString, newImportStatus.toString, currentImportStatus.map(_.toString), errorMessage)
    val attributes: Map[String, String] = updateImportStatus.toJson.asJsObject.fields.collect {
     case (attName:String, attValue:JsString) => (attName, attValue.value)
   }
    pubSubDao.publishMessages(importStatusPubSubTopic, Seq(GooglePubSubDAO.MessageRequest("", attributes)))
  }


  private def initUpsert(upsertFile: String, jobId: UUID, ackId: String, workspaceName: WorkspaceName, userInfo: UserInfo): Future[Unit] = {
    for {
      //ack the response after we load the json into memory. pro: don't have to worry about ack timeouts for long operations, con: if someone restarts rawls here the upsert is lost
      upsertEntityUpdates <- readUpsertObject(upsertFile)
      ackResponse <- acknowledgeMessage(ackId)
      upsertResults <- {
        val batches = upsertEntityUpdates.grouped(batchSize).zipWithIndex.toList
        val numBatches = batches.size
        batches.traverse {
          case (upsertBatch, idx) =>
            logger.info(s"starting upsert $idx of $batchSize for $jobId with ${upsertBatch.size} entities ...")
            IO.fromFuture(IO(workspaceService.apply(userInfo).batchUpdateEntities(workspaceName, upsertBatch, true)))
        }.unsafeToFuture
      }
    } yield {
      logger.info(s"completed async upsert job ${jobId} for user: ${userInfo.userEmail} with ${upsertEntityUpdates.size} entities")
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


  private def readUpsertObject(path: String) = {
    val pathArray = path.split("/")
    val bucketName = GcsBucketName(pathArray(0))
    val blobName = GcsBlobName(pathArray.tail.mkString("/"))
    readObject[Seq[EntityUpdateDefinition]](bucketName, blobName)
  }


  private def readObject[T](bucketName: GcsBucketName, blobName: GcsBlobName, decompress: Boolean = false)(implicit reader: JsonReader[T]): Future[T] = {
    logger.info(s"reading ${if (decompress) "compressed " else ""}object $blobName from bucket $bucketName ...")
    val compiled = googleStorage.getBlobBody(bucketName, blobName).compile

    compiled.to[Array].unsafeToFuture().map { byteArray =>
      val bytes = if (decompress) decompressGzip(byteArray) else byteArray
      logger.info(s"successfully read $blobName from bucket $bucketName; parsing ...")
      val obj = bytes.map(_.toChar).mkString.parseJson.convertTo[T]
      logger.info(s"successfully parsed $blobName from bucket $bucketName")
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


  //----------------------------  remove when cutting over to import service ---------------------------

  private def arrowImportEntities(message: PubSubMessage) = {
    val (jobId, file) = arrowParseMessage(message)
    file match {
      case "upsert.json" =>
        logger.info(s"processing $file message for job $jobId with ackId ${message.ackId} in message: $message")
        toFutureTry(initAvroUpsert(jobId, message.ackId)) map {
          case Success(_) => writeResult(s"$jobId/success.json", resultString("import successful")).map(_ => ImportComplete) pipeTo self
          case Failure(t) => writeResult(s"$jobId/error.json", resultString(t.getMessage)).map(_ => ImportComplete) pipeTo self
        }
      case _ =>
        logger.info(s"found ignorable file $file message for job $jobId with ackId ${message.ackId} in message: $message")
        arrowAcknowledgeMessage(message.ackId).map(_ => ImportComplete) pipeTo self //some other file (i.e. success/failure log, metadata.json)
    }
  }

  private def writeResult(path: String, message: String): Future[Unit] = {
    logger.info(s"writing import result to $path")
    googleStorage.createBlob(GcsBucketName(arrowUpsertBucketName), GcsBlobName(path), message.getBytes).compile.drain.unsafeToFuture()
  }

  private def resultString(message: String) = s"""{\"message\":\"${message}\"}"""

  private def arrowParseMessage(message: PubSubMessage) = {
    message.contents.parseJson.asJsObject.getFields("name").head.compactPrint match {
      case objectIdPattern(jobId, file) => (jobId, file)
      case m => throw new Exception(s"unable to parse message $m")
    }
  }

  private def arrowAcknowledgeMessage(ackId: String) = {
    logger.info(s"acking message with ackId $ackId")
    arrowPubSubDAO.acknowledgeMessagesById(arrowPubSubSubscriptionName, Seq(ackId))
  }

  private def arrowReadMetadataObject(blobName: String): Future[AvroMetadataJson] = {
    readObject[AvroMetadataJson](GcsBucketName(arrowUpsertBucketName), GcsBlobName(blobName))
  }

  private def arrowReadUpsertObject(blobName: String): Future[Seq[EntityUpdateDefinition]] = {
    readObject[Seq[EntityUpdateDefinition]](GcsBucketName(arrowUpsertBucketName), GcsBlobName(blobName), decompress = true)
  }

  private def initAvroUpsert(jobId: String, ackId: String): Future[Unit] = {
    for {
      avroMetadataJson <- arrowReadMetadataObject(s"$jobId/metadata.json")
      //ack the response after we load the json into memory. pro: don't have to worry about ack timeouts for long operations, con: if someone restarts rawls here the uspert is lost
      petSAJson <- samDAO.getPetServiceAccountKeyForUser(avroMetadataJson.namespace, RawlsUserEmail(avroMetadataJson.userEmail))
      petUserInfo <- googleServicesDAO.getUserInfoUsingJson(petSAJson)
      avroUpsertJson <- arrowReadUpsertObject(s"$jobId/upsert.json")
      ackResponse <- arrowAcknowledgeMessage(ackId)
      upsertResults <-
        avroUpsertJson.grouped(batchSize).toList.traverse { upsertBatch =>
          logger.info(s"starting upsert for $jobId with ${upsertBatch.size} entities ...")
          IO.fromFuture(IO(workspaceService.apply(petUserInfo).batchUpdateEntities(WorkspaceName(avroMetadataJson.namespace, avroMetadataJson.name), upsertBatch, true)))
        }.unsafeToFuture
    } yield {
      logger.info(s"completed Avro upsert job ${avroMetadataJson.jobId} for user: ${avroMetadataJson.userEmail} with ${avroUpsertJson.size} entities")
      ()
    }
  }


  //------------------------------------------------------------------------------------------------
}