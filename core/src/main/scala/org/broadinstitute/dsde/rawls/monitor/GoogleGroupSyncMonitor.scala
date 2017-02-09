package org.broadinstitute.dsde.rawls.monitor

import akka.actor.SupervisorStrategy.{Escalate, Stop}
import akka.actor._
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.dataaccess.GooglePubSubDAO
import org.broadinstitute.dsde.rawls.dataaccess.GooglePubSubDAO.PubSubMessage
import org.broadinstitute.dsde.rawls.model.{UserInfo, SyncReport, RawlsGroupRef}
import org.broadinstitute.dsde.rawls.monitor.GoogleGroupSyncMonitor.StartMonitorPass
import org.broadinstitute.dsde.rawls.monitor.GoogleGroupSyncMonitorSupervisor.{Init, Start}
import org.broadinstitute.dsde.rawls.user.UserService

import scala.concurrent.{Future, ExecutionContext}
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.duration._

import akka.pattern._

import spray.json._
import spray.json.DefaultJsonProtocol._
import org.broadinstitute.dsde.rawls.model.UserAuthJsonSupport._

/**
 * Created by dvoet on 12/6/16.
 */
object GoogleGroupSyncMonitorSupervisor {
  sealed trait GoogleGroupSyncMonitorSupervisorMessage
  case object Init extends GoogleGroupSyncMonitorSupervisorMessage
  case object Start extends GoogleGroupSyncMonitorSupervisorMessage

  def props(pollInterval: FiniteDuration, pollIntervalJitter: FiniteDuration, pubSubDao: GooglePubSubDAO, pubSubTopicName: String, pubSubSubscriptionName: String, workerCount: Int, userServiceConstructor: UserInfo => UserService)(implicit executionContext: ExecutionContext): Props = {
    Props(new GoogleGroupSyncMonitorSupervisor(pollInterval, pollIntervalJitter, pubSubDao, pubSubTopicName, pubSubSubscriptionName, workerCount, userServiceConstructor))
  }
}

class GoogleGroupSyncMonitorSupervisor(val pollInterval: FiniteDuration, pollIntervalJitter: FiniteDuration, pubSubDao: GooglePubSubDAO, pubSubTopicName: String, pubSubSubscriptionName: String, workerCount: Int, userServiceConstructor: UserInfo => UserService)(implicit executionContext: ExecutionContext) extends Actor with LazyLogging {
  import context._

  self ! Init

  override def receive = {
    case Init => init pipeTo self
    case Start => for(i <- 1 to workerCount) startOne()
    case Status.Failure(t) => logger.error("error initializing google group sync monitor", t)
  }

  def init = {
    for {
      _ <- pubSubDao.createTopic(pubSubTopicName)
      _ <- pubSubDao.createSubscription(pubSubTopicName, pubSubSubscriptionName)
    } yield Start
  }

  def startOne(): Unit = {
    logger.info("starting GoogleGroupSyncMonitorActor")
    actorOf(GoogleGroupSyncMonitor.props(pollInterval, pollIntervalJitter, pubSubDao, pubSubSubscriptionName, userServiceConstructor))
  }

  override val supervisorStrategy =
    OneForOneStrategy() {
      case e => {
        logger.error("error syncing google group", e)
        // start one to replace the error, stop the errored child so that we also drop its mailbox (i.e. restart not good enough)
        startOne()
        Stop
      }
    }

}

object GoogleGroupSyncMonitor {
  case object StartMonitorPass

  def props(pollInterval: FiniteDuration, pollIntervalJitter: FiniteDuration, pubSubDao: GooglePubSubDAO, pubSubSubscriptionName: String, userServiceConstructor: UserInfo => UserService)(implicit executionContext: ExecutionContext): Props = {
    Props(new GoogleGroupSyncMonitorActor(pollInterval, pollIntervalJitter, pubSubDao, pubSubSubscriptionName, userServiceConstructor))
  }
}

class GoogleGroupSyncMonitorActor(val pollInterval: FiniteDuration, pollIntervalJitter: FiniteDuration, pubSubDao: GooglePubSubDAO, pubSubSubscriptionName: String, userServiceConstructor: UserInfo => UserService)(implicit executionContext: ExecutionContext) extends Actor with LazyLogging {
  import context._

  self ! StartMonitorPass

  // fail safe in case this actor is idle too long but not too fast (1 second lower limit)
  setReceiveTimeout(max((pollInterval + pollIntervalJitter) * 10, 1 second))

  private def max(durations: FiniteDuration*): FiniteDuration = durations.max

  override def receive = {
    case StartMonitorPass =>
      // start the process by pulling a message and sending it back to self
      pubSubDao.pullMessages(pubSubSubscriptionName, 1).map(_.headOption) pipeTo self

    case Some(message: PubSubMessage) =>
      logger.debug(s"pulled $message")
      // send a message to the user service, it will send back a SyncReport message
      // note the message ack id used as the actor name
      // note that the UserInfo passed in probably is not used
      import org.broadinstitute.dsde.rawls.model.UserModelJsonSupport.RawlsGroupRefFormat
      val userServiceRef = actorOf(UserService.props(userServiceConstructor, UserInfo.buildFromTokens(pubSubDao.getPubSubServiceAccountCredential)), message.ackId)
      logger.debug(s"received sync message: ${message.contents}")
      userServiceRef ! UserService.InternalSynchronizeGroupMembers(message.contents.parseJson.convertTo[RawlsGroupRef])

    case None =>
      // there was no message to wait and try again
      val nextTime = pollInterval + pollIntervalJitter * Math.random()
      system.scheduler.scheduleOnce(nextTime.asInstanceOf[FiniteDuration], self, StartMonitorPass)

    case report: SyncReport =>
      // sync done, log it and try again immediately
      // sender should be the user service actor instantiated up above, name of which is the message ack id
      val messageAckId = sender().path.name
      stop(sender())
      logger.info(s"synchronized google group ${report.groupEmail.value}: ${report.items.toJson.compactPrint}")
      pubSubDao.acknowledgeMessagesById(pubSubSubscriptionName, Seq(messageAckId)).map(_ => StartMonitorPass) pipeTo self

    case Status.Failure(t) =>
      // an error happened in some future, let the supervisor handle it
      throw t

    case ReceiveTimeout =>
      throw new RawlsException("GoogleGroupSyncMonitorActor has received no messages for too long")
  }

  override val supervisorStrategy =
    OneForOneStrategy() {
      case e => {
        Escalate
      }
    }
}
