package org.broadinstitute.dsde.rawls.monitor

import java.util.concurrent.TimeoutException

import akka.actor.{Actor, Props}
import akka.pattern.{after, pipe}
import cats._
import cats.implicits._
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.dataaccess.{GoogleServicesDAO, MethodRepoDAO, SlickDataSource, UserDirectoryDAO}
import org.broadinstitute.dsde.rawls.google.GooglePubSubDAO
import org.broadinstitute.dsde.rawls.model.Subsystems._
import org.broadinstitute.dsde.rawls.model.{StatusCheckResponse, SubsystemStatus}
import org.broadinstitute.dsde.rawls.monitor.HealthMonitor._

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.control.NonFatal

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

  /** Triggers subsystem checking */
  case object CheckAll
  /** Stores status for a particular subsystem */
  case class Store(subsystem: Subsystem, status: SubsystemStatus)
  /** Retrieves current status and sends back to caller */
  case object GetCurrentStatus

  def props(slickDataSource: SlickDataSource, googleServicesDAO: GoogleServicesDAO, googlePubSubDAO: GooglePubSubDAO, userDirectoryDAO: UserDirectoryDAO, methodRepoDAO: MethodRepoDAO,
            groupsToCheck: Seq[String], topicsToCheck: Seq[String], bucketsToCheck: Seq[String],
            futureTimeout: FiniteDuration = DefaultFutureTimeout, staleThreshold: FiniteDuration = DefaultStaleThreshold): Props =
    Props(new HealthMonitor(slickDataSource, googleServicesDAO, googlePubSubDAO, userDirectoryDAO, methodRepoDAO, groupsToCheck, topicsToCheck, bucketsToCheck, futureTimeout, staleThreshold))
}

class HealthMonitor private (val slickDataSource: SlickDataSource, val googleServicesDAO: GoogleServicesDAO, val googlePubSubDAO: GooglePubSubDAO, val userDirectoryDAO: UserDirectoryDAO, val methodRepoDAO: MethodRepoDAO,
                             val groupsToCheck: Seq[String], val topicsToCheck: Seq[String], val bucketsToCheck: Seq[String],
                             val futureTimeout: FiniteDuration, val staleThreshold: FiniteDuration) extends Actor with LazyLogging {
  // Use the execution context for this actor's dispatcher for all asynchronous operations.
  // We define a separate execution context (a fixed thread pool) for health checking to
  // not interfere with user facing operations.
  import context.dispatcher

  /**
    * Contains each subsystem status along with a timestamp of when the entry was made.
    * Initialized with unknown status.
    */
  var data: Map[Subsystem, (SubsystemStatus, Long)] = {
    val now = System.currentTimeMillis
    AllSubsystems.map(_ -> (UnknownStatus, now)).toMap
  }

  override def receive: Receive = {
    case CheckAll => checkAll
    case Store(subsystem, status) => store(subsystem, status)
    case GetCurrentStatus => sender ! getCurrentStatus
  }

  private def checkAll: Unit = {
    List(
      (Agora, checkAgora),
      (Cromwell, checkCromwell),
      (Database, checkDB),
      (GoogleBilling, checkGoogleBilling),
      (GoogleBuckets, checkGoogleBuckets),
      (GoogleGenomics, checkGoogleGenomics),
      (GoogleGroups, checkGoogleGroups),
      (GooglePubSub, checkGooglePubsub),
      (LDAP, checkLDAP)
    ).foreach(processSubsystemResult _ tupled)
  }

  /**
    * Checks Agora status by calling the /status endpoint.
    */
  private def checkAgora: Future[SubsystemStatus] = {
    logger.debug("Checking Agora...")
    methodRepoDAO.getStatus.map(agoraStatus => SubsystemStatus(agoraStatus.up, if (agoraStatus.messages.nonEmpty) Some(agoraStatus.messages.toList) else None))
  }

  /**
    * Checks Cromwell status.
    * TODO: Cromwell doesn't have a status API yet so this is stubbed out for now.
    */
  private def checkCromwell: Future[SubsystemStatus] = {
    logger.debug("Checking Cromwell...")
    Future.successful(OkStatus)
  }

  /**
    * Checks database status by running a "select version()" query.
    * We don't care about the result, besides checking that the query succeeds.
    */
  private def checkDB: Future[SubsystemStatus] = {
    logger.debug("Checking Database...")
    slickDataSource.inTransaction(_.sqlDBStatus).map(_ => OkStatus)
  }

  /**
    * Checks Google PubSub status by doing a Get call on the notification and group monitor topics
    * using the pubsub service account.
    */
  private def checkGooglePubsub: Future[SubsystemStatus] = {
    logger.debug("Checking Google PubSub...")
    // Note: call to `foldMap` depends on SubsystemStatusMonoid, defined implicitly below
    topicsToCheck.toList.foldMap { topic =>
      googlePubSubDAO.getTopic(topic).map {
        case Some(_) => OkStatus
        case None => failedStatus(s"Could not find topic: $topic")
      }
    }
  }

  /**
    * Checks Google groups status by doing a Get on the admin and curator groups using the groups
    * service account.
    */
  private def checkGoogleGroups: Future[SubsystemStatus] = {
    logger.debug("Checking Google Groups...")
    // Note: call to `foldMap` depends on SubsystemStatusMonoid, defined implicitly below
    groupsToCheck.toList.foldMap { group =>
      googleServicesDAO.getGoogleGroup(group).map {
        case Some(_) => OkStatus
        case None => failedStatus(s"Could not find group: $group")
      }
    }
  }

  /**
    * Checks Google bucket status by doing a Get on the token bucket using the buckets service account.
    */
  private def checkGoogleBuckets: Future[SubsystemStatus] = {
    logger.debug("Checking Google Buckets...")
    // Note: call to `foldMap` depends on SubsystemStatusMonoid, defined implicitly below
    bucketsToCheck.toList.foldMap { bucket =>
      googleServicesDAO.getBucket(bucket).map {
        case Some(_) => OkStatus
        case None => failedStatus(s"Could not find bucket: $bucket")
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
    googleServicesDAO.listGenomicsOperations.map { _ =>
      OkStatus
    }
  }

  /**
    * Checks LDAP status by doing a search and validating that we can retrieve at least one user.
    */
  private def checkLDAP: Future[SubsystemStatus] = {
    logger.debug("Checking LDAP...")
    userDirectoryDAO.listUsers.map { users =>
      if (users.isEmpty) failedStatus("Could not find any users in LDAP")
      else OkStatus
    }
  }

  private def processSubsystemResult(subSystem: Subsystem, result: Future[SubsystemStatus]): Unit = {
    result.withTimeout(futureTimeout, s"Timed out after ${futureTimeout.toString} waiting for a response from ${subSystem.toString}")
    .recover { case NonFatal(ex) =>
      failedStatus(ex.getMessage)
    } map {
      Store(subSystem, _)
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
      case (status, _) => status
    }
    // overall status is ok iff all subsystems are ok
    val overall = processed.forall(_._2.ok)
    StatusCheckResponse(overall, processed)
  }

  /**
    * A monoid used for combining SubsystemStatuses.
    * Zero is an ok status with no messages.
    * Append uses && on the ok flag, and ++ on the messages.
    */
  private implicit val SubsystemStatusMonoid = new Monoid[SubsystemStatus] {
    def combine(a: SubsystemStatus, b: SubsystemStatus): SubsystemStatus = {
      SubsystemStatus(a.ok && b.ok, a.messages |+| b.messages)
    }
    def empty: SubsystemStatus = OkStatus
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
  private implicit class FutureWithTimeout[A](f: Future[A]) {
    def withTimeout(duration: FiniteDuration, errMsg: String): Future[A] =
      Future.firstCompletedOf(Seq(f, after(duration, context.system.scheduler)(Future.failed(new TimeoutException(errMsg)))))
  }
}