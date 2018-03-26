package org.broadinstitute.dsde.rawls.dataaccess

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.client.RequestBuilding._
import akka.http.scaladsl.marshalling.Marshal
import akka.http.scaladsl.model.RequestEntity
import akka.stream.Materializer
import org.broadinstitute.dsde.rawls.util.{HttpClientUtilsStandard, Retry}

import scala.concurrent.{ExecutionContext, Future}

class HttpMarthaDAO(url: String)(implicit val system: ActorSystem, val materializer: Materializer, val executionContext: ExecutionContext) extends MarthaDAO with DsdeHttpDAO with Retry {

  val http = Http(system)
  val httpClientUtils = HttpClientUtilsStandard()

  override def dosToGs(dos: String): Future[String] = {
    import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
    import spray.json.DefaultJsonProtocol._

    val content = Map("url" -> dos, "pattern" -> "gs://")
    Marshal(content).to[RequestEntity] flatMap { entity =>
      retry[String](when500) { () =>
        httpClientUtils.executeRequestUnmarshalResponse[String](http, Post(url, entity))
      }
    }
  }
}
