package org.broadinstitute.dsde.rawls.dataaccess.martha

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.client.RequestBuilding._
import akka.http.scaladsl.marshalling.Marshal
import akka.http.scaladsl.model.RequestEntity
import akka.stream.Materializer
import org.broadinstitute.dsde.rawls.dataaccess.DsdeHttpDAO
import org.broadinstitute.dsde.rawls.model.UserInfo
import org.broadinstitute.dsde.rawls.util.{HttpClientUtilsStandard, Retry}

import scala.concurrent.{ExecutionContext, Future}

class MarthaDosResolver(marthaUrl: String)(implicit val system: ActorSystem, val materializer: Materializer, val executionContext: ExecutionContext) extends DosResolver with DsdeHttpDAO with Retry {

  val http = Http(system)
  val httpClientUtils = HttpClientUtilsStandard()

  override def dosServiceAccountEmail(dos: String, userInfo: UserInfo): Future[Option[String]] = {
    import MarthaJsonSupport._
    import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
    import spray.json.DefaultJsonProtocol._

    // TODO: when this goes to code review, make sure that logging these fields is OK with compliance
    logger.info(s"Calling $marthaUrl to look up $dos on behalf of user ${userInfo.userEmail.value}")

    val content = Map("url" -> dos)
    val marthaResponse: Future[MarthaMinimalResponse] = Marshal(content).to[RequestEntity] flatMap { entity =>
      retry[MarthaMinimalResponse](when500) { () =>
        executeRequestWithToken[MarthaMinimalResponse](userInfo.accessToken)(Post(marthaUrl, entity))
      }
    }

    marthaResponse.map { resp =>
      //FIXME: investigate changing the Martha response formats to not contain Option, since Martha should always return an email if provided a bearer token
      val saEmail = resp.googleServiceAccount.flatMap(_.data.map(_.client_email))
      if(saEmail.isEmpty) {
        logger.warn(s"MarthaDosResolver.dosServiceAccountEmail returned no SA for dos URL $dos")
      }
      saEmail
    }
  }
}
