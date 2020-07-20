package org.broadinstitute.dsde.rawls.monitor

import java.util.UUID

import akka.actor.SupervisorStrategy.{Escalate, Stop}
import akka.actor._
import akka.pattern._
import cats.effect.{ContextShift, IO}
import com.typesafe.scalalogging.LazyLogging
import io.circe.fs2._
import io.circe.generic.auto._
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.entities.EntityService
import org.broadinstitute.dsde.rawls.google.GooglePubSubDAO
import org.broadinstitute.dsde.rawls.google.GooglePubSubDAO.PubSubMessage
import org.broadinstitute.dsde.rawls.model.AttributeUpdateOperations.EntityUpdateDefinition
import org.broadinstitute.dsde.rawls.model.ImportStatuses.ImportStatus
import org.broadinstitute.dsde.rawls.model.{ImportStatuses, RawlsUserEmail, UserInfo, WorkspaceName}
import org.broadinstitute.dsde.rawls.monitor.AvroUpsertMonitorSupervisor.{AvroUpsertMonitorConfig, updateImportStatusFormat}
import org.broadinstitute.dsde.rawls.util.AuthUtil
import org.broadinstitute.dsde.workbench.google2.{GcsBlobName, GoogleStorageService}
import org.broadinstitute.dsde.workbench.model.google.GcsBucketName
import org.broadinstitute.dsde.workbench.model.{UserInfo => _, _}
import org.broadinstitute.dsde.workbench.util.FutureSupport
import spray.json.DefaultJsonProtocol._
import spray.json._

import scala.concurrent.duration.{FiniteDuration, _}
import scala.concurrent.{ExecutionContext, Future}
import scala.language.postfixOps
import scala.util.{Failure, Success}

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
                                           updateImportStatusPubSubTopic: String,
                                           ackDeadlineSeconds: Int,
                                           batchSize: Int,
                                           workerCount: Int)

  def props(entityService: UserInfo => EntityService,
            googleServicesDAO: GoogleServicesDAO,
            samDAO: SamDAO,
            googleStorage: GoogleStorageService[IO],
            pubSubDAO: GooglePubSubDAO,
            importServicePubSubDAO: GooglePubSubDAO,
            importServiceDAO: ImportServiceDAO,
            avroUpsertMonitorConfig: AvroUpsertMonitorConfig)(implicit executionContext: ExecutionContext, cs: ContextShift[IO]): Props =
    Props(
      new AvroUpsertMonitorSupervisor(
        entityService,
        googleServicesDAO,
        samDAO,
        googleStorage,
        pubSubDAO,
        importServicePubSubDAO,
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


class AvroUpsertMonitorSupervisor(entityService: UserInfo => EntityService,
                                  googleServicesDAO: GoogleServicesDAO,
                                  samDAO: SamDAO,
                                  googleStorage: GoogleStorageService[IO],
                                  pubSubDAO: GooglePubSubDAO,
                                  importServicePubSubDAO: GooglePubSubDAO,
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
      _ <- pubSubDAO.createTopic(avroUpsertMonitorConfig.importRequestPubSubTopic)
      _ <- pubSubDAO.createSubscription(avroUpsertMonitorConfig.importRequestPubSubTopic, avroUpsertMonitorConfig.importRequestPubSubSubscription, Some(avroUpsertMonitorConfig.ackDeadlineSeconds))
    } yield Start

  def startOne(): Unit = {
    logger.info("starting AvroUpsertMonitorActor")
    actorOf(AvroUpsertMonitor.props(avroUpsertMonitorConfig.pollInterval, avroUpsertMonitorConfig.pollIntervalJitter, entityService, googleServicesDAO, samDAO, googleStorage, pubSubDAO, importServicePubSubDAO, avroUpsertMonitorConfig.importRequestPubSubSubscription, avroUpsertMonitorConfig.updateImportStatusPubSubTopic, importServiceDAO, avroUpsertMonitorConfig.batchSize))
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
             entityService: UserInfo => EntityService,
             googleServicesDAO: GoogleServicesDAO,
             samDAO: SamDAO,
             googleStorage: GoogleStorageService[IO],
             pubSubDao: GooglePubSubDAO,
             importServicePubSubDAO: GooglePubSubDAO,
             pubSubSubscriptionName: String,
             importStatusPubSubTopic: String,
             importServiceDAO: ImportServiceDAO,
             batchSize: Int)(implicit cs: ContextShift[IO]): Props =
    Props(new AvroUpsertMonitorActor(pollInterval, pollIntervalJitter, entityService, googleServicesDAO, samDAO, googleStorage, pubSubDao, importServicePubSubDAO,
      pubSubSubscriptionName, importStatusPubSubTopic, importServiceDAO, batchSize))
}

class AvroUpsertMonitorActor(
                              val pollInterval: FiniteDuration,
                              pollIntervalJitter: FiniteDuration,
                              entityService: UserInfo => EntityService,
                              val googleServicesDAO: GoogleServicesDAO,
                              val samDAO: SamDAO,
                              googleStorage: GoogleStorageService[IO],
                              pubSubDao: GooglePubSubDAO,
                              importServicePubSubDAO: GooglePubSubDAO,
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
      // start the process by pulling a message and sending it back to self
      pubSubDao.pullMessages(pubSubSubscriptionName, 1).map(_.headOption) pipeTo self

    case Some(message: PubSubMessage) =>
      // we received a message, so we will parse it and try to upsert it
      logger.info(s"received async upsert message: $message")
      importEntities(message)

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
    importServicePubSubDAO.publishMessages(importStatusPubSubTopic, Seq(GooglePubSubDAO.MessageRequest("", attributes)))
  }


  private def initUpsert(upsertFile: String, jobId: UUID, ackId: String, workspaceName: WorkspaceName, userInfo: UserInfo): Future[Boolean] = {
    logger.info(s"beginning upsert process for $jobId ...")

    /* TODO:
        - wrap this entire thing in a try/catch, or otherwise ensure that ALL errors are caught, and mark the
          import job as Failed on error. You could do the try/catch in importEntities(), which calls this method.
        - write unit tests for this method, which may require factoring functionality out into smaller, more
          easily-testable methods
        - clean up the code, this is just a sketch!
        - figure out what this method should return; it's currently a Future[Boolean] and we could do better
        - add lots of logging to help debugging in the future
        - manually test this against some of the known-bad use cases
     */

    // Start reading the file. This should throw an error if we can't read the file; it returns a stream
    val upsertStream = getUpsertStream(upsertFile)
    // Ack the pubsub message as soon as we know we can read the file. pro: don't have to worry about ack timeouts for long operations, con: if someone restarts rawls here the upsert is lost
    acknowledgeMessage(ackId)
    // translate the stream of bytes to a stream of json
    val jsonStream = upsertStream.through(byteArrayParser)
    // translate the stream of json to a stream of EntityUpdateDefinition
    val entityUpdateDefinitionStream = jsonStream.through(decoder[IO, EntityUpdateDefinition])
    // chunk the stream into batches.
    // TODO: should we reduce batch size, currently 1000?
    val batchStream = entityUpdateDefinitionStream.chunkN(batchSize)

    // upsert each chunk
    val upsertFuturesStream = batchStream map { chunk =>
      // TODO: this definition of final will be false-negative if the number of upserts is an clean multiple of batchSize
      val isFinalChunk = (chunk.size < batchSize) // if final, we may want to log something or do something?
      toFutureTry(entityService.apply(userInfo).batchUpdateEntities(workspaceName, chunk.iterator.toSeq, true))
    }

    // wait for all upserts
    Future.sequence(upsertFuturesStream.compile.toList.unsafeRunSync()) map { upsertResults =>
      upsertResults.forall(_.isSuccess) // do something more elegant than returning true/false here!
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

  private def getUpsertStream(path: String) = {
    val pathArray = path.split("/")
    val bucketName = GcsBucketName(pathArray(0))
    val blobName = GcsBlobName(pathArray.tail.mkString("/"))
    logger.info(s"reading object ${blobName.value} from bucket ${bucketName.value} ...")
    googleStorage.getBlobBody(bucketName, blobName)
  }

//  private def readUpsertObject(path: String) = {
//    val pathArray = path.split("/")
//    val bucketName = GcsBucketName(pathArray(0))
//    val blobName = GcsBlobName(pathArray.tail.mkString("/"))
//    readObject[Seq[EntityUpdateDefinition]](bucketName, blobName)
//  }
//
//
//  // TODO: stream-read the batchUpsert json from the bucket. Can use https://github.com/circe/circe-fs2
//  // to interpret the results of googleStorage.getBlobBody as a stream, instead of attempting to
//  // read the whole thing into memory and parse it as a huge string.
//  private def readObject[T](bucketName: GcsBucketName, blobName: GcsBlobName, decompress: Boolean = false)(implicit reader: JsonReader[T]): Future[T] = {
//    logger.info(s"reading ${if (decompress) "compressed " else ""}object $blobName from bucket $bucketName ...")
//    val compiled = googleStorage.getBlobBody(bucketName, blobName).compile
//
//    compiled.toList.unsafeToFuture().map { byteList =>
//      val byteArray = byteList.toArray
//      val bytes = if (decompress) decompressGzip(byteArray) else byteArray
//      logger.info(s"successfully read $blobName from bucket $bucketName; parsing ...")
//      val obj = bytes.map(_.toChar).mkString.parseJson.convertTo[T]
//      logger.info(s"successfully parsed $blobName from bucket $bucketName")
//      obj
//    }
//  }
//
//
//
//  private def decompressGzip(compressed: Array[Byte]): Array[Byte] = {
//    val inputStream = new GZIPInputStream(new ByteArrayInputStream(compressed))
//    val result = org.apache.commons.io.IOUtils.toByteArray(inputStream)
//    inputStream.close()
//    result
//  }


  override val supervisorStrategy =
    OneForOneStrategy() {
      case e => {
        Escalate
      }
    }

}