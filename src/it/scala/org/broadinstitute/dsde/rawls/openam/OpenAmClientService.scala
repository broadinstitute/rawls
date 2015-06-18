package scala.org.broadinstitute.dsde.rawls.openam

import java.util.concurrent.TimeUnit

import akka.actor.Actor
import akka.event.Logging
import org.broadinstitute.dsde.rawls.IntegrationTestConfig
import spray.client.pipelining._
import spray.http.{HttpEntity, MediaTypes}
import spray.json._

import scala.concurrent.Await
import scala.concurrent.duration.{Duration, FiniteDuration}

import OpenAmClientService.{OpenAmAuthRequest,OpenAmResponse}

object OpenAmClientService {
  case class OpenAmAuthRequest(username: String, password: String)
  case class OpenAmResponse(tokenId: String, successUrl: String)
}

object OpenAmResponseJsonProtocol extends DefaultJsonProtocol {
  implicit val impOpenAmResponse = jsonFormat2(OpenAmResponse)
}

class OpenAmClientService extends Actor with IntegrationTestConfig {

  implicit val system = context.system
  import system.dispatcher
  val log = Logging(system, getClass)
  val duration: Duration = new FiniteDuration(5, TimeUnit.SECONDS)

  override def receive: Receive = {
    case OpenAmAuthRequest(username, password) =>
      log.debug("Querying the OpenAM Server for access token for username: " + username)
      sender ! {
        import OpenAmResponseJsonProtocol._
        import spray.httpx.SprayJsonSupport._

        val pipeline = sendReceive ~> unmarshal[OpenAmResponse]
        val responseFuture = pipeline {
          Post(openAmUrl, HttpEntity(MediaTypes.`application/json`, """{}""")) ~>
            addHeader("X-OpenAM-Username", username) ~>
            addHeader("X-OpenAM-Password", password)
        }
        Await.result(responseFuture, duration)
      }
  }
}
