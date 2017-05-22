package org.broadinstitute.dsde.rawls.status

import akka.actor.{Actor, ActorRef, Props}
import akka.pattern._
import akka.util.Timeout
import org.broadinstitute.dsde.rawls.model.ExecutionJsonSupport.StatusCheckResponseFormat
import org.broadinstitute.dsde.rawls.model.StatusCheckResponse
import org.broadinstitute.dsde.rawls.monitor.HealthMonitor.GetCurrentStatus
import org.broadinstitute.dsde.rawls.status.StatusService.GetStatus
import org.broadinstitute.dsde.rawls.webservice.PerRequest.{PerRequestMessage, RequestComplete}
import spray.http.StatusCodes
import spray.httpx.SprayJsonSupport._

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

/**
  * Created by rtitle on 5/20/17.
  */
object StatusService {
  def props(statusServiceConstructor: () => StatusService): Props = {
    Props(statusServiceConstructor())
  }

  def constructor(healthMonitor: ActorRef)()(implicit executionContext: ExecutionContext): StatusService = {
    new StatusService(healthMonitor)
  }

  case object GetStatus
}

class StatusService(val healthMonitor: ActorRef)(implicit val executionContext: ExecutionContext) extends Actor {
  implicit val timeout = Timeout(1 minute)

  override def receive = {
    case GetStatus => getStatus pipeTo sender
  }

  def getStatus: Future[PerRequestMessage] = {
    (healthMonitor ? GetCurrentStatus).mapTo[StatusCheckResponse].map { statusResponse =>
      RequestComplete(StatusCodes.OK, statusResponse)
    }
  }
}
