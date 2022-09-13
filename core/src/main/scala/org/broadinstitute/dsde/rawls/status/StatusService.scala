package org.broadinstitute.dsde.rawls.status

import akka.actor.ActorRef
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.{StatusCode, StatusCodes}
import akka.pattern._
import akka.util.Timeout
import org.broadinstitute.dsde.rawls.model.StatusJsonSupport.StatusCheckResponseFormat
import org.broadinstitute.dsde.rawls.model.{StatusCheckResponse, Subsystems}
import org.broadinstitute.dsde.rawls.monitor.HealthMonitor.GetCurrentStatus

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.language.postfixOps

/**
  * Created by rtitle on 5/20/17.
  */
object StatusService {
  def constructor(healthMonitor: ActorRef)()(implicit executionContext: ExecutionContext): StatusService =
    new StatusService(healthMonitor)
}

class StatusService(val healthMonitor: ActorRef)(implicit val executionContext: ExecutionContext) {
  implicit val timeout = Timeout(1 minute)

  def getStatus: Future[(StatusCode, StatusCheckResponse)] =
    (healthMonitor ? GetCurrentStatus).mapTo[StatusCheckResponse].map { statusCheckResponse =>
      val criticalStatusOk = Subsystems.CriticalSubsystems.forall { subsystem =>
        statusCheckResponse.systems.get(subsystem).exists(_.ok)
      }
      val httpStatus = if (criticalStatusOk) StatusCodes.OK else StatusCodes.InternalServerError
      (httpStatus, statusCheckResponse)
    }
}
