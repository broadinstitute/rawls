package org.broadinstitute.dsde.rawls.status

import akka.actor.ActorRef
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.StatusCodes
import akka.pattern._
import akka.util.Timeout
import org.broadinstitute.dsde.rawls.model.StatusJsonSupport.StatusCheckResponseFormat
import org.broadinstitute.dsde.rawls.model.{StatusCheckResponse, Subsystems}
import org.broadinstitute.dsde.rawls.monitor.HealthMonitor.GetCurrentStatus
import org.broadinstitute.dsde.rawls.webservice.PerRequest.{PerRequestMessage, RequestComplete}

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.language.postfixOps

/**
  * Created by rtitle on 5/20/17.
  */
object StatusService {
  def constructor(healthMonitor: ActorRef): StatusService = {
    new StatusService(healthMonitor)
  }
}

class StatusService(val healthMonitor: ActorRef) {
  implicit val timeout = Timeout(1 minute)

  def getStatus: Future[PerRequestMessage] = {
    (healthMonitor ? GetCurrentStatus).mapTo[StatusCheckResponse].map { statusCheckResponse =>
      val criticalStatusOk = Subsystems.CriticalSubsystems.forall { subsystem =>
        statusCheckResponse.systems.get(subsystem).exists(_.ok)
      }
      val httpStatus = if (criticalStatusOk) StatusCodes.OK else StatusCodes.InternalServerError
      RequestComplete(httpStatus, statusCheckResponse)
    }
  }
}
