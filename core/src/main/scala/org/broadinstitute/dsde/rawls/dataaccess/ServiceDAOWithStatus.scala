package org.broadinstitute.dsde.rawls.dataaccess

import akka.http.scaladsl.client.RequestBuilding._
import akka.http.scaladsl.unmarshalling.Unmarshal
import org.broadinstitute.dsde.rawls.model.SubsystemStatus

import scala.concurrent.Future

trait ServiceDAOWithStatus {
  this: DsdeHttpDAO =>

  protected def statusUrl: String

  def getStatus(): Future[SubsystemStatus] =
    for {
      response <- http.singleRequest(Get(statusUrl))
      ok = response.status.isSuccess
      message <-
        if (ok) {
          response.discardEntityBytes() // ensure the entity is subscribed!
          Future.successful(None)
        } else {
          Unmarshal(response.entity).to[String].map(Option(_))
        }
    } yield SubsystemStatus(ok, message.map(List(_)))
}
