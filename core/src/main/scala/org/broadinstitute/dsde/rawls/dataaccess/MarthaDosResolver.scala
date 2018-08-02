package org.broadinstitute.dsde.rawls.dataaccess

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.client.RequestBuilding._
import akka.http.scaladsl.marshalling.Marshal
import akka.http.scaladsl.model.RequestEntity
import akka.stream.Materializer
import org.broadinstitute.dsde.rawls.util.{HttpClientUtilsStandard, Retry}

import scala.concurrent.{ExecutionContext, Future}

case class ServiceAccountEmail(client_email: String)
case class MarthaV2Response(googleServiceAccount: Option[ServiceAccountEmail])

object MarthaJsonSupport {
  import spray.json.DefaultJsonProtocol._

  implicit val ServiceAccountEmailFormat = jsonFormat1(ServiceAccountEmail)
  implicit val MarthaV2ResponseFormat = jsonFormat1(MarthaV2Response)
}

class MarthaDosResolver(url: String)(implicit val system: ActorSystem, val materializer: Materializer, val executionContext: ExecutionContext) extends DosResolver with DsdeHttpDAO with Retry {

  val http = Http(system)
  val httpClientUtils = HttpClientUtilsStandard()

  override def dosServiceAccountEmail(dos: String): Future[Option[String]] = {
    import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
    import spray.json.DefaultJsonProtocol._
    import MarthaJsonSupport._

    val content = Map("url" -> dos)
    val marthaResponse: Future[MarthaV2Response] = Marshal(content).to[RequestEntity] flatMap { entity =>
      retry[MarthaV2Response](when500) { () =>
        httpClientUtils.executeRequestUnmarshalResponse[MarthaV2Response](http, Post(url, entity))
      }
    }

    marthaResponse.map(_.googleServiceAccount.map(_.client_email))
  }
}
