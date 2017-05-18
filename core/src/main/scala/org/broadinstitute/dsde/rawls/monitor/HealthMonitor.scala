package org.broadinstitute.dsde.rawls.monitor

import akka.actor.{Actor, Props}
import akka.pattern.pipe
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.dataaccess.{GoogleServicesDAO, MethodRepoDAO, SlickDataSource, UserDirectoryDAO}
import org.broadinstitute.dsde.rawls.google.GooglePubSubDAO
import org.broadinstitute.dsde.rawls.model.Subsystems._
import org.broadinstitute.dsde.rawls.model.{AgoraStatus, StatusCheckResponse, SubsystemStatus}
import org.broadinstitute.dsde.rawls.monitor.HealthMonitor._

import scala.collection.GenTraversableOnce
import scala.concurrent.Future
import scala.util.control.NonFatal

import cats._
import cats.implicits._

/**
  * Created by rtitle on 5/17/17.
  */
object HealthMonitor {
  case object Check
  case class Store(subsystem: Subsystem, status: SubsystemStatus)
  case object GetCurrentStatus

  def props(slickDataSource: SlickDataSource, googleServicesDAO: GoogleServicesDAO, googlePubSubDAO: GooglePubSubDAO, userDirectoryDAO: UserDirectoryDAO, methodRepoDAO: MethodRepoDAO,
            groupsToCheck: Seq[String], topicsToCheck: Seq[String], bucketsToCheck: Seq[String]): Props =
    Props(new HealthMonitor(slickDataSource, googleServicesDAO, googlePubSubDAO, userDirectoryDAO, methodRepoDAO, groupsToCheck, topicsToCheck, bucketsToCheck))
}

class HealthMonitor(val slickDataSource: SlickDataSource, val googleServicesDAO: GoogleServicesDAO, val googlePubSubDAO: GooglePubSubDAO, val userDirectoryDAO: UserDirectoryDAO, val methodRepoDAO: MethodRepoDAO,
                    val groupsToCheck: Seq[String], val bucketsToCheck: Seq[String], val topicsToCheck: Seq[String]) extends Actor with LazyLogging {

  import context.dispatcher

  var data: Map[Subsystem, SubsystemStatus] = _

  override def receive: Receive = {
    case Check => checkAll

    case Store(subsystem, status) => store(subsystem, status)

    case GetCurrentStatus => sender ! getCurrentStatus
  }

  private def checkAll: Unit = {
    checkAgora
    checkLDAP
    checkCromwell
    checkDB
    checkGoogle
  }

  private def checkAgora = {
    checkSubSystem(Agora, methodRepoDAO.getStatus) { agoraStatus =>
      SubsystemStatus(agoraStatus.up, agoraStatus.messages)
    }
  }

  private def checkCromwell = {
    // Note: cromwell doesn't have a health check
    checkSubSystem(Cromwell, Future.successful(()))(noopMapper)
  }

  private def checkDB = {
    // Note: uses the slick thread pool
    checkSubSystem(Database, slickDataSource.inTransaction(_.sqlDBStatus))(noopMapper)
  }


  private def checkGoogle = {
    // pubsub
    val topicChecks = Future.traverse(topicsToCheck.toList) { topic =>
      googlePubSubDAO.getTopic(topic).map {
        case Some(_) => SubsystemStatus(true, Seq.empty)
        case None => SubsystemStatus(false, Seq(s"Could not find topic $topic"))
      }
    }
    checkSubSystem(GooglePubSub, topicChecks)(_.combineAll)

    // groups
    val groupChecks = Future.traverse(groupsToCheck.toList) { group =>
      googleServicesDAO.getGoogleGroup(group).map {
        case Some(_) => SubsystemStatus(true, Seq.empty)
        case None => SubsystemStatus(false, Seq(s"Could not find group $group"))
      }
    }
    checkSubSystem(GoogleGroups, groupChecks)(_.combineAll)

    // buckets
    val bucketChecks = Future.traverse(bucketsToCheck.toList) { bucket =>
      googleServicesDAO.getBucket(bucket).map {
        case Some(_) => SubsystemStatus(true, Seq.empty)
        case None => SubsystemStatus(false, Seq(s"Could not find bucket $bucket"))
      }
    }
    checkSubSystem(GoogleBuckets, bucketChecks)(_.combineAll)

    // billing
    checkSubSystem(GoogleBilling, googleServicesDAO.listBillingAccountsUsingServiceCredential) { accts =>
      if (accts.isEmpty) SubsystemStatus(false, Seq("Could not find any Rawls billing accounts"))
      else SubsystemStatus(true, Seq.empty)
    }

    // genomics
    checkSubSystem(GoogleGenomics, googleServicesDAO.listGenomicsOperations) { ops =>
      // don't validate nonempty
      SubsystemStatus(true, Seq.empty)
    }

  }

  private def checkLDAP = {
    checkSubSystem(LDAP, userDirectoryDAO.getAnyUser)(noopMapper)
  }

  private def checkSubSystem[A <: Subsystem, B](subSystem: A, result: => Future[B])(mapper: B => SubsystemStatus): Unit = {
    result.map(mapper).recover { case NonFatal(ex) =>
      SubsystemStatus(false, Seq(ex.getMessage))
    } map {
      Store(subSystem, _)
    } pipeTo self
  }

  private def noopMapper = (_: Any) => SubsystemStatus(true, Seq.empty)

  private def store(subsystem: Subsystem, status: SubsystemStatus): Unit = {
    data = data + (subsystem -> status)
  }

  private def getCurrentStatus: StatusCheckResponse = {
    val overall = data.values.forall(_.ok)
    StatusCheckResponse(overall, data)
  }
  
  private implicit val SubsystemStatusMonoid = new Monoid[SubsystemStatus] {
    def combine(a: SubsystemStatus, b: SubsystemStatus): SubsystemStatus = {
      SubsystemStatus(a.ok && b.ok, a.messages ++ b.messages)
    }
    def empty: SubsystemStatus = SubsystemStatus(true, Seq.empty)
  }

}

