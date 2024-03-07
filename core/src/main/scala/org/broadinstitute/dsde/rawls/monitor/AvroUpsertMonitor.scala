package org.broadinstitute.dsde.rawls.monitor

import akka.actor.SupervisorStrategy.{Escalate, Stop}
import akka.actor._
import akka.http.scaladsl.model.StatusCodes
import akka.pattern._
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.typesafe.scalalogging.LazyLogging
import fs2.concurrent.SignallingRef
import io.circe.fs2._
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.entities.EntityService
import org.broadinstitute.dsde.rawls.google.GooglePubSubDAO
import org.broadinstitute.dsde.rawls.google.GooglePubSubDAO.PubSubMessage
import org.broadinstitute.dsde.rawls.model.AttributeUpdateOperations._
import org.broadinstitute.dsde.rawls.model.ImportStatuses.ImportStatus
import org.broadinstitute.dsde.rawls.model.{
  Entity,
  ErrorReport => RawlsErrorReport,
  ImportStatuses,
  RawlsRequestContext,
  RawlsUserEmail,
  Workspace,
  WorkspaceName
}
import org.broadinstitute.dsde.rawls.monitor.AvroUpsertMonitorSupervisor.{
  updateImportStatusFormat,
  AvroUpsertMonitorConfig,
  KeepAlive
}
import org.broadinstitute.dsde.rawls.util.AuthUtil
import org.broadinstitute.dsde.rawls.{RawlsException, RawlsExceptionWithErrorReport}
import org.broadinstitute.dsde.workbench.google2.{GcsBlobName, GoogleStorageService}
import org.broadinstitute.dsde.workbench.model.google.GcsBucketName
import org.broadinstitute.dsde.workbench.model.{UserInfo => _, _}
import org.broadinstitute.dsde.workbench.util.FutureSupport
import spray.json.DefaultJsonProtocol._
import spray.json._

import java.util.UUID
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
  case object KeepAlive extends AvroUpsertMonitorSupervisorMessage

  final case class AvroUpsertMonitorConfig(pollInterval: FiniteDuration,
                                           pollIntervalJitter: FiniteDuration,
                                           importRequestPubSubTopic: String,
                                           importRequestPubSubSubscription: String,
                                           updateImportStatusPubSubTopic: String,
                                           ackDeadlineSeconds: Int,
                                           batchSize: Int,
                                           workerCount: Int
  )

  def props(entityService: RawlsRequestContext => EntityService,
            googleServicesDAO: GoogleServicesDAO,
            samDAO: SamDAO,
            googleStorage: GoogleStorageService[IO],
            pubSubDAO: GooglePubSubDAO,
            importServicePubSubDAO: GooglePubSubDAO,
            importServiceDAO: ImportServiceDAO,
            avroUpsertMonitorConfig: AvroUpsertMonitorConfig,
            dataSource: SlickDataSource
  )(implicit executionContext: ExecutionContext): Props =
    Props(
      new AvroUpsertMonitorSupervisor(entityService,
                                      googleServicesDAO,
                                      samDAO,
                                      googleStorage,
                                      pubSubDAO,
                                      importServicePubSubDAO,
                                      importServiceDAO,
                                      avroUpsertMonitorConfig,
                                      dataSource
      )
    )

  import spray.json.DefaultJsonProtocol._
  implicit val updateImportStatusFormat: RootJsonFormat[UpdateImportStatus] = jsonFormat5(UpdateImportStatus)
}

case class UpdateImportStatus(importId: String,
                              newStatus: String,
                              currentStatus: Option[String] = None,
                              errorMessage: Option[String] = None,
                              action: String = "status"
)

case class ImportUpsertResults(successes: Int, failures: List[RawlsErrorReport])

case class AvroUpsertAttributes(workspace: WorkspaceName,
                                userEmail: RawlsUserEmail,
                                importId: UUID,
                                upsertFile: String,
                                isUpsert: Boolean,
                                isCwds: Boolean
)
class AvroUpsertMonitorSupervisor(entityService: RawlsRequestContext => EntityService,
                                  googleServicesDAO: GoogleServicesDAO,
                                  samDAO: SamDAO,
                                  googleStorage: GoogleStorageService[IO],
                                  pubSubDAO: GooglePubSubDAO,
                                  importServicePubSubDAO: GooglePubSubDAO,
                                  importServiceDAO: ImportServiceDAO,
                                  avroUpsertMonitorConfig: AvroUpsertMonitorConfig,
                                  dataSource: SlickDataSource
) extends Actor
    with LazyLogging {
  import AvroUpsertMonitorSupervisor._
  import context._

  self ! Init

  override def receive = {
    case Init              => init pipeTo self
    case Start             => for (i <- 1 to avroUpsertMonitorConfig.workerCount) startOne()
    case Status.Failure(t) => logger.error("error initializing avro upsert monitor", t)
  }

  def init =
    for {
      _ <- pubSubDAO.createTopic(avroUpsertMonitorConfig.importRequestPubSubTopic)
      _ <- pubSubDAO.createSubscription(
        avroUpsertMonitorConfig.importRequestPubSubTopic,
        avroUpsertMonitorConfig.importRequestPubSubSubscription,
        Some(avroUpsertMonitorConfig.ackDeadlineSeconds)
      )
    } yield Start

  def startOne(): Unit = {
    logger.info("starting AvroUpsertMonitorActor")
    actorOf(
      AvroUpsertMonitor.props(
        avroUpsertMonitorConfig.pollInterval,
        avroUpsertMonitorConfig.pollIntervalJitter,
        entityService,
        googleServicesDAO,
        samDAO,
        googleStorage,
        pubSubDAO,
        importServicePubSubDAO,
        avroUpsertMonitorConfig.importRequestPubSubSubscription,
        avroUpsertMonitorConfig.updateImportStatusPubSubTopic,
        importServiceDAO,
        avroUpsertMonitorConfig.batchSize,
        dataSource
      )
    )
  }

  override val supervisorStrategy =
    OneForOneStrategy() { case e =>
      logger.error("unexpected error in avro upsert monitor", e)
      // start one to replace the error, stop the errored child so that we also drop its mailbox (i.e. restart not good enough)
      startOne()
      Stop
    }
}

object AvroUpsertMonitor {
  case object StartMonitorPass
  case object ImportComplete

  val objectIdPattern = """"([^/]+)/([^/]+)"""".r

  def props(pollInterval: FiniteDuration,
            pollIntervalJitter: FiniteDuration,
            entityService: RawlsRequestContext => EntityService,
            googleServicesDAO: GoogleServicesDAO,
            samDAO: SamDAO,
            googleStorage: GoogleStorageService[IO],
            pubSubDao: GooglePubSubDAO,
            importServicePubSubDAO: GooglePubSubDAO,
            pubSubSubscriptionName: String,
            importStatusPubSubTopic: String,
            importServiceDAO: ImportServiceDAO,
            batchSize: Int,
            dataSource: SlickDataSource
  ): Props =
    Props(
      new AvroUpsertMonitorActor(
        pollInterval,
        pollIntervalJitter,
        entityService,
        googleServicesDAO,
        samDAO,
        googleStorage,
        pubSubDao,
        importServicePubSubDAO,
        pubSubSubscriptionName,
        importStatusPubSubTopic,
        importServiceDAO,
        batchSize,
        dataSource
      )
    )
}

class AvroUpsertMonitorActor(val pollInterval: FiniteDuration,
                             pollIntervalJitter: FiniteDuration,
                             entityService: RawlsRequestContext => EntityService,
                             val googleServicesDAO: GoogleServicesDAO,
                             val samDAO: SamDAO,
                             googleStorage: GoogleStorageService[IO],
                             pubSubDao: GooglePubSubDAO,
                             importServicePubSubDAO: GooglePubSubDAO,
                             pubSubSubscriptionName: String,
                             importStatusPubSubTopic: String,
                             importServiceDAO: ImportServiceDAO,
                             batchSize: Int,
                             dataSource: SlickDataSource
) extends Actor
    with LazyLogging
    with FutureSupport
    with AuthUtil {
  import AvroUpsertMonitor._
  import context._

  case class AvroMetadataJson(namespace: String,
                              name: String,
                              userSubjectId: String,
                              userEmail: String,
                              jobId: String,
                              startTime: String
  )
  implicit val avroMetadataJsonFormat: RootJsonFormat[AvroMetadataJson] = jsonFormat6(AvroMetadataJson)

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

    case KeepAlive =>
      // noop, here to ensure the actor does not time out from not receiving messages for too long
      logger.info("received keepalive message")

    case None =>
      // there was no message so wait and try again
      val nextTime = org.broadinstitute.dsde.workbench.util.addJitter(pollInterval, pollIntervalJitter)
      system.scheduler.scheduleOnce(nextTime, self, StartMonitorPass)

    case ImportComplete => self ! StartMonitorPass

    case Status.Failure(t) =>
      throw t
      self ! StartMonitorPass

    case ReceiveTimeout => throw new WorkbenchException("AvroUpsertMonitorActor has received no messages for too long")

    case x =>
      logger.info(s"unhandled $x")
      self ! None
  }

  private def importEntities(message: PubSubMessage) =
    new AvroUpsertMonitorMessageParser(message, dataSource).parse flatMap { attributes =>
      val importFuture = for {
        workspace <- dataSource.inTransaction { dataAccess =>
          dataAccess.workspaceQuery.findByName(attributes.workspace).map {
            case Some(workspace) => workspace
            case None =>
              throw new RawlsException(s"Workspace ${attributes.workspace} not found while importing entities")
          }
        }
        petUserInfo <- getPetServiceAccountUserInfo(workspace.googleProjectId, attributes.userEmail)
        importStatus <- importServiceDAO.getImportStatus(attributes.importId, attributes.workspace, petUserInfo)
        _ <- importStatus match {
          // Currently, there is only one upsert monitor thread - but if we decide to make more threads, we might
          // end up with a race condition where multiple threads are attempting the same import / updating the status
          // of the same import.
          case Some(status) if status == ImportStatuses.ReadyForUpsert =>
            publishMessageToUpdateImportStatus(attributes.importId, Option(status), ImportStatuses.Upserting, None)
            toFutureTry(
              initUpsert(attributes.upsertFile,
                         attributes.importId,
                         message.ackId,
                         workspace,
                         attributes.userEmail,
                         attributes.isUpsert
              )
            ) map {
              case Success(importUpsertResults) =>
                val failureMessages = stringMessageFromFailures(importUpsertResults.failures, 100)
                val baseMsg =
                  s"Successfully updated ${importUpsertResults.successes} entities; ${importUpsertResults.failures.size} updates failed."
                if (importUpsertResults.failures.isEmpty)
                  publishMessageToUpdateImportStatus(attributes.importId,
                                                     Option(status),
                                                     ImportStatuses.Done,
                                                     Option(baseMsg)
                  )
                else {
                  val msg = baseMsg + s" First 100 failures are: $failureMessages"
                  publishMessageToUpdateImportStatus(attributes.importId,
                                                     Option(status),
                                                     ImportStatuses.Error,
                                                     Option(msg)
                  )
                }
              case Failure(t) =>
                val errMsg = t match {
                  case errRpt: RawlsExceptionWithErrorReport => errRpt.errorReport.message
                  case x                                     => x.getMessage
                }
                publishMessageToUpdateImportStatus(attributes.importId,
                                                   Option(status),
                                                   ImportStatuses.Error,
                                                   Option(errMsg)
                )
            }
          case Some(status) =>
            logger.warn(
              s"Received a double message delivery for import ID [${attributes.importId}] which is already in status [$status].  Acking message."
            )
            acknowledgeMessage(message.ackId)
          case None =>
            publishMessageToUpdateImportStatus(attributes.importId,
                                               None,
                                               ImportStatuses.Error,
                                               Option("Import status not found")
            )
        }
      } yield ()

      // Make sure message is acknowledged in the case of any failure while trying to construct importFuture
      importFuture.map(_ => ImportComplete) recover { case t =>
        logger.error(s"unexpected error in importFuture for ${attributes.importId}: ${t.getMessage}", t)
        publishMessageToUpdateImportStatus(
          attributes.importId,
          None,
          ImportStatuses.Error,
          Option(s"Failed to import data. The underlying error message is: ${t.getMessage}")
        )
        acknowledgeMessage(message.ackId)
      } pipeTo self

    }

  private def publishMessageToUpdateImportStatus(importId: UUID,
                                                 currentImportStatus: Option[ImportStatus],
                                                 newImportStatus: ImportStatus,
                                                 errorMessage: Option[String]
  ) = {
    logger.info(
      s"asking to change import job $importId from $currentImportStatus to $newImportStatus ${errorMessage.getOrElse("")}"
    )
    val updateImportStatus =
      UpdateImportStatus(importId.toString, newImportStatus.toString, currentImportStatus.map(_.toString), errorMessage)
    val attributes: Map[String, String] = updateImportStatus.toJson.asJsObject.fields.collect {
      case (attName: String, attValue: JsString) =>
        // pubsub message attribute value limit is 1024 bytes, we'll use 1000 here to be safe
        val usableValue = if (attValue.value.length < 1000) attValue.value else attValue.value.take(1000) + " ..."
        (attName, usableValue)
    }
    importServicePubSubDAO.publishMessages(
      importStatusPubSubTopic,
      scala.collection.immutable.Seq(GooglePubSubDAO.MessageRequest("", attributes))
    )
  }

  private def initUpsert(upsertFile: String,
                         jobId: UUID,
                         ackId: String,
                         workspace: Workspace,
                         userEmail: RawlsUserEmail,
                         isUpsert: Boolean
  ): Future[ImportUpsertResults] = {
    val startTime = System.currentTimeMillis()
    logger.info(s"beginning upsert process for $jobId ...")

    try {

      // Start reading the file. This returns a stream
      logger.info(s"checking access to $upsertFile for jobId ${jobId.toString} ...")
      val upsertStream = getUpsertStream(upsertFile)

      // Ensure that the file has some contents. Our implementation of GoogleStorageInterpreter will return an empty
      // stream instead of an error in cases where it could not read the file. We check to see if there is at least
      // one valid byte in the stream.
      val nonEmptyStream = Try(upsertStream.exists(_.isValidByte).compile.toList.unsafeRunSync().head) match {
        case Success(true) => true
        case _             => false
      }
      if (!nonEmptyStream) {
        throw new RawlsExceptionWithErrorReport(
          RawlsErrorReport(StatusCodes.BadRequest,
                           s"Intermediate batch upsert file $upsertFile not found, or file was empty for jobId $jobId"
          )
        )
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

      // convenience method to encapsulate the call to EntityService's batchUpdateEntitiesInternal
      def performUpsertBatch(idx: Long, upsertBatch: Seq[EntityUpdateDefinition]): Future[Traversable[Entity]] = {
        logger.info(s"upserting batch #$idx of ${upsertBatch.size} entities for jobId ${jobId.toString} ...")
        for {
          petUserInfo <- getPetServiceAccountUserInfo(workspace.googleProjectId, userEmail)
          upsertResults <- entityService(RawlsRequestContext(petUserInfo)).batchUpdateEntitiesInternal(
            workspace.toWorkspaceName,
            upsertBatch,
            upsert = isUpsert,
            None,
            None
          )
        } yield upsertResults
      }

      // create our pause signal. We use this to control when the stream should pause and resume.
      val sig = SignallingRef[IO, Boolean](false).unsafeRunSync()

      // tell the stream what to execute for each batch.
      // we must ensure that each batch is upserted sequentially, because later entities may reference earlier entities.
      // if we try to upsert them in parallel, the referenced entity may not exist yet, and the "later" upsert would fail.
      // therefore, for each batch, we:
      //  1. pause the stream by setting the signal to true
      //  2. perform this batch's upsert
      //  3. resume the stream by setting the signal to false
      // TODO: I _think_ that by using evalMapChunk here, we can eliminate the pause/resume and trust the stream
      // to handle it natively. This will require more testing so I am leaving the pause/resume in place, as it does
      // no harm.
      val upsertFuturesStream = batchStream.zipWithIndex
        .evalMapChunk { case (chunk, idx) =>
          for {
            _ <- sig.set(true)
            _ = self ! KeepAlive // keep actor alive during this loop
            attempt <- IO.fromFuture(IO(toFutureTry(performUpsertBatch(idx, chunk.toList))))
            _ <- sig.set(false)
          } yield {
            attempt match {
              case Failure(regrets: RawlsExceptionWithErrorReport) =>
                val loggedErrors = stringMessageFromFailures(regrets.errorReport.causes.toList, 100)
                logger.warn(
                  s"upsert batch #$idx for jobId ${jobId.toString} contained errors. The first 100 errors are: $loggedErrors"
                )
              case _ => // noop; here for completeness of matching
            }
            logger.info(s"completed upsert batch #$idx for jobId ${jobId.toString}...")
            attempt
          }
        }
        .pauseWhen(sig)

      // finally, after all the stream setup, tell the stream to execute
      val upsertResults = upsertFuturesStream.compile.toList.unsafeRunSync()

      val numSuccesses: Int = upsertResults.collect { case Success(entityList) =>
        entityList.size
      }.sum

      // if the failure has underlying causes, use those; else use the parent failure.
      // unresolved entity references only have useful error messages in the underlying causes, not the parent failure
      // therefore to deliver the best messages to the user we need to handle both cases
      val failureReports: List[RawlsErrorReport] = upsertResults collect {
        case Failure(regrets: RawlsExceptionWithErrorReport) if regrets.errorReport.causes.nonEmpty =>
          regrets.errorReport.causes
        case Failure(regrets: RawlsExceptionWithErrorReport) => Seq(regrets.errorReport)
      } flatten

      // this could be a LOT of error reports, we don't want to send an enormous packet back to the caller.
      // Cap the failure reports at 100.
      val failureReportsForCaller = failureReports.take(100)
      val additionalErrorString = if (failureReports.size > failureReportsForCaller.size) {
        "; only the first 100 errors are shown."
      } else {
        "."
      }

      // fail if nothing at all succeeded
      if (numSuccesses == 0) {
        throw new RawlsExceptionWithErrorReport(
          RawlsErrorReport(
            StatusCodes.BadRequest,
            s"All entities failed to update. There were ${failureReports.size} errors in total$additionalErrorString" +
              s" Error messages: ${stringMessageFromFailures(failureReportsForCaller, 100)}"
          )
        )
      }

      val elapsed = System.currentTimeMillis() - startTime
      logger.info(
        s"upsert process for $jobId succeeded after $elapsed ms: $numSuccesses upserted," +
          s" ${failureReports.size} failed$additionalErrorString Errors: ${failureReportsForCaller.map(_.message).mkString(", ")}"
      )

      Future.successful(ImportUpsertResults(numSuccesses, failureReports))

    } catch {
      // try/catch for any synchronous exceptions, not covered by a Future.recover(). Potential for retry here, in case of transient failures
      case ex: Exception =>
        val elapsed = System.currentTimeMillis() - startTime
        logger.error(s"upsert process for $jobId FAILED after $elapsed ms: ${ex.getMessage}")
        acknowledgeMessage(ackId) // just in case
        Future.failed(ex)
    }
  }

  private def stringMessageFromFailures(errorReports: List[RawlsErrorReport], maxFailures: Int = 100): String =
    errorReports.take(maxFailures).map(_.message).mkString("; ")

  private def acknowledgeMessage(ackId: String) = {
    logger.info(s"acking message with ackId $ackId")
    pubSubDao.acknowledgeMessagesById(pubSubSubscriptionName, scala.collection.immutable.Seq(ackId))
  }

  private def getUpsertStream(path: String) = {
    val pathArray = path.split("/")
    val bucketName = GcsBucketName(pathArray(0))
    val blobName = GcsBlobName(pathArray.tail.mkString("/"))
    logger.info(s"reading object ${blobName.value} from bucket ${bucketName.value} ...")
    googleStorage.getBlobBody(bucketName, blobName)
  }

  override val supervisorStrategy =
    OneForOneStrategy() { case e =>
      Escalate
    }

}
