package org.broadinstitute.dsde.rawls.monitor

import akka.actor.{Actor, Props}
import akka.pattern.{after, pipe}
import cats._
import cats.implicits._
import com.typesafe.scalalogging.LazyLogging
import cromwell.client.ApiException
import org.broadinstitute.dsde.rawls.billing.BillingProfileManagerDAO
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.WorkspaceManagerDAO
import org.broadinstitute.dsde.rawls.google.GooglePubSubDAO
import org.broadinstitute.dsde.rawls.model.Subsystems._
import org.broadinstitute.dsde.rawls.model.{StatusCheckResponse, SubsystemStatus}
import org.broadinstitute.dsde.rawls.monitor.HealthMonitor._

import java.util.concurrent.TimeoutException
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.control.NonFatal
import scala.jdk.CollectionConverters._

/**
  * Created by rtitle on 5/17/17.
  */
object HealthMonitor {
  val DefaultFutureTimeout = 1 minute
  val DefaultStaleThreshold = 15 minutes

  val OkStatus = SubsystemStatus(true, None)
  val UnknownStatus = SubsystemStatus(false, Some(List("Unknown status")))
  def failedStatus(message: String) = SubsystemStatus(false, Some(List(message)))

  // Actor API:

  sealed trait HealthMonitorMessage

  /** Triggers subsystem checking */
  case object CheckAll extends HealthMonitorMessage

  /** Stores status for a particular subsystem */
  case class Store(subsystem: Subsystem, status: SubsystemStatus) extends HealthMonitorMessage

  /** Retrieves current status and sends back to caller */
  case object GetCurrentStatus extends HealthMonitorMessage

  def props(slickDataSource: SlickDataSource,
            googleServicesDAO: GoogleServicesDAO,
            googlePubSubDAO: GooglePubSubDAO,
            methodRepoDAO: MethodRepoDAO,
            samDAO: SamDAO,
            billingProfileManagerDAO: BillingProfileManagerDAO,
            workspaceManagerDAO: WorkspaceManagerDAO,
            executionServiceServers: Map[ExecutionServiceId, ExecutionServiceDAO],
            groupsToCheck: Seq[String],
            topicsToCheck: Seq[String],
            bucketsToCheck: Seq[String],
            futureTimeout: FiniteDuration = DefaultFutureTimeout,
            staleThreshold: FiniteDuration = DefaultStaleThreshold
  ): Props =
    Props(
      new HealthMonitor(
        slickDataSource,
        googleServicesDAO,
        googlePubSubDAO,
        methodRepoDAO,
        samDAO,
        billingProfileManagerDAO,
        workspaceManagerDAO,
        executionServiceServers,
        groupsToCheck,
        topicsToCheck,
        bucketsToCheck,
        futureTimeout,
        staleThreshold
      )
    )
}

/**
  * This actor periodically checks the health of each Rawls subsystem and reports on the results.
  * It is used for Rawls system monitoring.
  *
  * For a list of the Rawls subsystems, see the [[Subsystem]] enum.
  *
  * The actor lifecyle is as follows:
  * 1. Periodically receives a [[CheckAll]] message from the Akka scheduler. Receipt of this message
  * triggers independent, asynchronous checks of each Rawls subsystem. The results of these futures
  * are piped to self via...
  *
  * 2. the [[Store]] message. This updates the actor state for the given subsystem status. Note the current
  * timestamp is also stored to ensure the returned statuses are current (see staleThreshold param).
  *
  * 3. [[GetCurrentStatus]] looks up the current actor state and sends it back to the caller wrapped in
  * a [[StatusCheckResponse]] case class. This message is purely for retrieving state; it does not
  * trigger any asynchronous operations.
  *
  * Note we structure status checks in this asynchronous way for a couple of reasons:
  * - The /status endpoint is unauthenticated - we don't want each call to /status to trigger DB queries,
  * Google calls, etc. It opens us up to DDoS.
  *
  * - The /status endpoint should be reliable, and decoupled from the status checks themselves. For example,
  * if there is a problem with the DB that is causing hanging queries, that behavior shouldn't leak out to
  * the /status API call. Instead, the call to /status will return quickly and report there is a problem
  * with the DB (but not other subsystems because the checks are independent).
  *
  * @param slickDataSource the slick data source for DB operations
  * @param googleServicesDAO the GCS DAO for Google API calls
  * @param googlePubSubDAO the GPS DAO for Google PubSub API calls
  * @param methodRepoDAO the method repo DAO for Agora calls
  * @param samDAO the IAM DAO for Sam calls
  * @param executionServiceServers the execution service DAOs for Cromwell calls
  * @param groupsToCheck Set of Google groups to check for existence
  * @param topicsToCheck Set of Google PubSub topics to check for existence
  * @param bucketsToCheck Set of Google bucket IDs to check for existence
  * @param futureTimeout amount of time after which subsystem check futures will time out (default 1 minute)
  * @param staleThreshold amount of time after which statuses are considered "stale". If a status is stale
  *                       then it won't be returned to the caller; instead a failing status with an "Unknown"
  *                       message will be returned. This shouldn't normally happen in practice if we have
  *                       reasonable future timeouts; however it is still a defensive check in case something
  *                       unexpected goes wrong. Default 15 minutes.
  */
class HealthMonitor private (val slickDataSource: SlickDataSource,
                             val googleServicesDAO: GoogleServicesDAO,
                             val googlePubSubDAO: GooglePubSubDAO,
                             val methodRepoDAO: MethodRepoDAO,
                             val samDAO: SamDAO,
                             val billingProfileManagerDAO: BillingProfileManagerDAO,
                             val workspaceManagerDAO: WorkspaceManagerDAO,
                             val executionServiceServers: Map[ExecutionServiceId, ExecutionServiceDAO],
                             val groupsToCheck: Seq[String],
                             val topicsToCheck: Seq[String],
                             val bucketsToCheck: Seq[String],
                             val futureTimeout: FiniteDuration,
                             val staleThreshold: FiniteDuration
) extends Actor
    with LazyLogging {
  // Use the execution context for this actor's dispatcher for all asynchronous operations.
  // We define a separate execution context (a fixed thread pool) for health checking to
  // not interfere with user facing operations.
  import context.dispatcher

  /**
    * Contains each subsystem status along with a timestamp of when the entry was made.
    * Initialized with unknown status.
    */
  private var data: Map[Subsystem, (SubsystemStatus, Long)] = {
    val now = System.currentTimeMillis
    AllSubsystems.map(_ -> (UnknownStatus, now)).toMap
  }

  override def receive: Receive = {
    case CheckAll                 => checkAll
    case Store(subsystem, status) => store(subsystem, status)
    case GetCurrentStatus         => sender ! getCurrentStatus
  }

  private def checkAll: Unit =
    List(
      (Agora, checkAgora),
      (Cromwell, checkCromwell),
      (Database, checkDB),
      (GoogleBilling, checkGoogleBilling),
      (GoogleBuckets, checkGoogleBuckets),
      (GoogleGenomics, checkGoogleGenomics),
      (GoogleGroups, checkGoogleGroups),
      (GooglePubSub, checkGooglePubsub),
      (Sam, checkSam),
      (BillingProfileManager, checkBPM),
      (WorkspaceManager, checkWSM)
    ).foreach(processSubsystemResult)

  /**
    * Checks Agora status by calling the /status endpoint.
    */
  private def checkAgora: Future[SubsystemStatus] = {
    logger.debug("Checking Agora...")
    methodRepoDAO.getStatus
  }

  /**
    * A monoid used for combining SubsystemStatuses.
    * Zero is an ok status with no messages.
    * Append uses && on the ok flag, and ++ on the messages.
    */
  implicit private val SubsystemStatusMonoid = new Monoid[SubsystemStatus] {
    def combine(a: SubsystemStatus, b: SubsystemStatus): SubsystemStatus =
      SubsystemStatus(a.ok && b.ok, a.messages |+| b.messages)
    def empty: SubsystemStatus = OkStatus
  }

  /**
    * Checks Cromwell status.
    * Calls each Cromwell's /status endpoint and returns true if all Cromwells return true
    */
  private def checkCromwell: Future[SubsystemStatus] = {
    logger.debug("Checking Cromwell(s)...")

    def annotateSubsystem(serviceId: ExecutionServiceId,
                          execSubsystem: String,
                          subsystemStatus: SubsystemStatus
    ): SubsystemStatus =
      subsystemStatus.copy(messages = subsystemStatus.messages.map { msgList =>
        msgList.map(msg => s"${serviceId.id}-$execSubsystem: $msg")
      })

    // Note: calls to `foldMap` depend on SubsystemStatusMonoid
    executionServiceServers.toList.foldMap { case (id, dao) =>
      dao.getStatus() map {
        _.toList.foldMap { case (execSubsystem, execSubStatus) =>
          annotateSubsystem(id, execSubsystem, execSubStatus)
        }
      }
    }
  }

  /**
    * Checks database status by running a "select version()" query.
    * We don't care about the result, besides checking that the query succeeds.
    */
  private def checkDB: Future[SubsystemStatus] = {
    // Note: this uses the slick thread pool, not this actor's dispatcher.
    logger.debug("Checking Database...")
    slickDataSource.inTransaction(_.sqlDBStatus).map(_ => OkStatus)
  }

  /**
    * Checks Google PubSub status by doing a Get call on the notification and group monitor topics
    * using the pubsub service account.
    */
  private def checkGooglePubsub: Future[SubsystemStatus] = {
    logger.debug("Checking Google PubSub...")
    // Note: call to `foldMap` depends on SubsystemStatusMonoid
    topicsToCheck.toList.foldMap { topic =>
      googlePubSubDAO.getTopic(topic).map {
        case Some(_) => OkStatus
        case None    => failedStatus(s"Could not find topic: $topic")
      }
    }
  }

  /**
    * Checks Google groups status by doing a Get on the admin and curator groups using the groups
    * service account.
    */
  private def checkGoogleGroups: Future[SubsystemStatus] = {
    logger.debug("Checking Google Groups...")
    // Note: call to `foldMap` depends on SubsystemStatusMonoid
    groupsToCheck.toList.foldMap { group =>
      googleServicesDAO.getGoogleGroup(group).map {
        case Some(_) => OkStatus
        case None    => failedStatus(s"Could not find group: $group")
      }
    }
  }

  /**
    * Checks Google bucket status by doing a Get on the token bucket using the buckets service account.
    */
  private def checkGoogleBuckets: Future[SubsystemStatus] = {
    logger.debug("Checking Google Buckets...")
    // Note: call to `foldMap` depends on SubsystemStatusMonoid
    bucketsToCheck.toList.foldMap { bucket =>
      googleServicesDAO.getBucket(bucket, None).map {
        case Right(_)      => OkStatus
        case Left(message) => failedStatus(s"Could not find bucket: $bucket. $message")
      }
    }
  }

  /**
    * Checks Google billing status by doing a list() using the billing service account.
    * Expects at least one account to be returned.
    */
  private def checkGoogleBilling: Future[SubsystemStatus] = {
    logger.debug("Checking Google Billing...")
    googleServicesDAO.listBillingAccountsUsingServiceCredential.map { accts =>
      if (accts.isEmpty) failedStatus("Could not find any Rawls billing accounts")
      else OkStatus
    }
  }

  /**
    * Checks Google genomics status by doing a list() using the genomics service account.
    * Does not validate the results; only that the API call succeeds.
    */
  private def checkGoogleGenomics: Future[SubsystemStatus] = {
    logger.debug("Checking Google Genomics...")
    googleServicesDAO.checkGenomicsOperationsHealth.map { _ =>
      OkStatus
    }
  }

  private def checkSam: Future[SubsystemStatus] = {
    logger.debug("Checking Sam...")
    samDAO.getStatus()
  }

  private def checkBPM: Future[SubsystemStatus] = {
    logger.debug("Checking Billing Profile Manager...")
    Future(
      SubsystemStatus(billingProfileManagerDAO.getStatus().isOk,
        Option(billingProfileManagerDAO.getStatus().getSystems).map { subSystemStatuses =>
          val messages = for {
            (subSystem, subSystemStatus) <- subSystemStatuses.asScala
            message <- Option(subSystemStatus.getMessages).map(_.asScala).getOrElse(Seq("none"))
          } yield s"$subSystem: (ok: ${subSystemStatus.isOk}, message: $message)"
          messages.toList
        }))
  }

  private def checkWSM: Future[SubsystemStatus] = {
    logger.debug("Checking Workspace Manager...")
    Future(
      try {
        workspaceManagerDAO.getStatus()
        OkStatus
      } catch {
        case exception: ApiException => failedStatus(s"WorkspaceManager: (ok: false, message: $exception)")
      }
    )
  }

  private def processSubsystemResult(subsystemAndResult: (Subsystem, Future[SubsystemStatus])): Unit = {
    val (subsystem, result) = subsystemAndResult
    result
      .withTimeout(futureTimeout,
                   s"Timed out after ${futureTimeout.toString} waiting for a response from ${subsystem.toString}"
      )
      .recover { case NonFatal(ex) =>
        failedStatus(ex.getMessage)
      } map {
      Store(subsystem, _)
    } pipeTo self
  }

  private def store(subsystem: Subsystem, status: SubsystemStatus): Unit = {
    data = data + (subsystem -> (status, System.currentTimeMillis))
    logger.debug(s"New health monitor state: $data")
  }

  private def getCurrentStatus: StatusCheckResponse = {
    val now = System.currentTimeMillis()
    // Convert any expired statuses to unknown
    val processed = data.mapValues {
      case (_, t) if now - t > staleThreshold.toMillis => UnknownStatus
      case (status, _)                                 => status
    }
    // overall status is ok iff all subsystems are ok
    val overall = processed.forall(_._2.ok)
    StatusCheckResponse(overall, processed.toMap)
  }

  /**
    * Adds non-blocking timeout support to futures.
    * Example usage:
    * {{{
    *   val future = Future(Thread.sleep(1000*60*60*24*365)) // 1 year
    *   Await.result(future.withTimeout(5 seconds, "Timed out"), 365 days)
    *   // returns in 5 seconds
    * }}}
    */
  implicit private class FutureWithTimeout[A](f: Future[A]) {
    def withTimeout(duration: FiniteDuration, errMsg: String): Future[A] =
      Future.firstCompletedOf(
        Seq(f, after(duration, context.system.scheduler)(Future.failed(new TimeoutException(errMsg))))
      )
  }
}
