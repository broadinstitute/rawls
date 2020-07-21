package org.broadinstitute.dsde.rawls.monitor

import java.util.UUID

import akka.actor.SupervisorStrategy.{Escalate, Stop}
import akka.actor._
import akka.http.scaladsl.model.StatusCodes
import akka.pattern._
import cats.effect.{ContextShift, IO}
import com.typesafe.scalalogging.LazyLogging
import io.circe.fs2._
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.entities.EntityService
import org.broadinstitute.dsde.rawls.google.GooglePubSubDAO
import org.broadinstitute.dsde.rawls.google.GooglePubSubDAO.PubSubMessage
import org.broadinstitute.dsde.rawls.model.AttributeUpdateOperations._
import org.broadinstitute.dsde.rawls.model.{ErrorReport => RawlsErrorReport}
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
import scala.util.{Failure, Success, Try}

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

case class ImportUpsertResults(successes: Int, failures: List[RawlsErrorReport])


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

    try {
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
              case Success(importUpsertResults) =>
                val msg = s"Successfully updated ${importUpsertResults.successes} entities; ${importUpsertResults.failures.size} updates failed."
                publishMessageToUpdateImportStatus(attributes.importId, Option(status), ImportStatuses.Done, Option(msg))
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

      importFuture recover {
        case ex:Exception =>
          logger.error(s"import jobid ${attributes.importId} failed unexpectedly, in recover: ${ex.getMessage}", ex)
          publishMessageToUpdateImportStatus(attributes.importId, None, ImportStatuses.Error, Option(ex.getMessage))
          // potential for retry here, in case the error was transient
      } map(_ => ImportComplete) pipeTo self

    } catch {
      case ex:Exception =>
        logger.error(s"import jobid ${attributes.importId} failed unexpectedly, in catch: ${ex.getMessage}", ex)
        publishMessageToUpdateImportStatus(attributes.importId, None, ImportStatuses.Error, Option(ex.getMessage)) map (_ =>
          ImportComplete) pipeTo self
        // potential for retry here, in case the error was transient
    }
  }

  private def publishMessageToUpdateImportStatus(importId: UUID, currentImportStatus: Option[ImportStatus], newImportStatus: ImportStatus, errorMessage: Option[String]) = {
    logger.info(s"asking to change import job $importId from $currentImportStatus to $newImportStatus ${errorMessage.getOrElse("")}")
    val updateImportStatus = UpdateImportStatus(importId.toString, newImportStatus.toString, currentImportStatus.map(_.toString), errorMessage)
    val attributes: Map[String, String] = updateImportStatus.toJson.asJsObject.fields.collect {
     case (attName:String, attValue:JsString) => (attName, attValue.value)
   }
    importServicePubSubDAO.publishMessages(importStatusPubSubTopic, Seq(GooglePubSubDAO.MessageRequest("", attributes)))
  }


  private def initUpsert(upsertFile: String, jobId: UUID, ackId: String, workspaceName: WorkspaceName, userInfo: UserInfo): Future[ImportUpsertResults] = {
    logger.info(s"beginning upsert process for $jobId ...")

    // Start reading the file. This returns a stream
    logger.info(s"checking access to $upsertFile for jobId ${jobId.toString} ...")
    val upsertStream = getUpsertStream(upsertFile)

    // Ensure that the file has some contents. Our implementation of GoogleStorageInterpreter will return an empty
    // stream instead of an error in cases where it could not read the file. We check to see if there is at least
    // one valid byte in the stream.
    val nonEmptyStream = Try(upsertStream.exists(_.isValidByte).compile.toList.unsafeRunSync().head) match {
      case Success(true) => true
      case _ => false
    }
    if (!nonEmptyStream) {
      throw new RawlsExceptionWithErrorReport(RawlsErrorReport(StatusCodes.BadRequest,
        s"Intermediate batch upsert file $upsertFile not found, or file was empty for jobId $jobId"))
    }

    // Ack the pubsub message as soon as we know we can read the file. pro: don't have to worry about ack timeouts for long operations, con: if someone restarts rawls here the upsert is lost
    logger.info(s"acking upsert pubsub message for jobId ${jobId.toString} ...")
    acknowledgeMessage(ackId)
    // translate the stream of bytes to a stream of json
    val jsonStream = upsertStream.through(byteArrayParser)

    // translate the stream of json to a stream of EntityUpdateDefinition. This is awkward, because we break out
    // of Circe and use spray-json parsing, since we have complex custom spray-json decoders for EntityUpdateDefinition
    // and I would prefer not to have multiple complex custom decoders
    val entityUpdateDefinitionStream = jsonStream.map { circeJson =>
      circeJson.toString().parseJson.convertTo[EntityUpdateDefinition]
    }

    // chunk the stream into batches.
    val batchStream = entityUpdateDefinitionStream.chunkN(batchSize)

    // upsert each chunk
    val upsertFuturesStream = batchStream map { chunk =>
      val upsertBatch: List[EntityUpdateDefinition] = chunk.toList
      logger.info(s"inserting a batch of ${upsertBatch.size} for jobId ${jobId.toString} ...")
      toFutureTry(entityService.apply(userInfo).batchUpdateEntitiesInternal(workspaceName, upsertBatch, true))
    }

    // wait for all upserts
    Future.sequence(upsertFuturesStream.compile.toList.unsafeRunSync()) map { upsertResults =>

      val numSuccesses:Int = upsertResults.collect {
        case Success(entityList) => entityList.size
      }.sum

      val failureReports:List[RawlsErrorReport] = upsertResults collect {
        case Failure(regrets:RawlsExceptionWithErrorReport) => regrets.errorReport.causes
      } flatten

      // fail if nothing at all succeeded
      if (numSuccesses == 0) {
        // TODO: this could be a LOT of error reports, should we omit if large?
        throw new RawlsExceptionWithErrorReport(RawlsErrorReport(StatusCodes.BadRequest, "All entities could not be updated.", failureReports))
      }

      ImportUpsertResults(numSuccesses, failureReports)
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

  override val supervisorStrategy =
    OneForOneStrategy() {
      case e => {
        Escalate
      }
    }

}