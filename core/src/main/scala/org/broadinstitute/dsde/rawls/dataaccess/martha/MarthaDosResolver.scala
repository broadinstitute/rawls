package org.broadinstitute.dsde.rawls.dataaccess.martha

import akka.actor.ActorSystem
import akka.http.scaladsl.{Http, HttpExt}
import akka.http.scaladsl.client.RequestBuilding._
import akka.http.scaladsl.marshalling.Marshal
import akka.http.scaladsl.model.RequestEntity
import akka.stream.Materializer
import org.broadinstitute.dsde.rawls.dataaccess.DsdeHttpDAO
import org.broadinstitute.dsde.rawls.model.UserInfo
import org.broadinstitute.dsde.rawls.util.{HttpClientUtilsStandard, Retry}

import scala.concurrent.{ExecutionContext, Future}

class MarthaDosResolver(marthaUrl: String, excludeJDRDomain: Boolean)(implicit val system: ActorSystem, val materializer: Materializer, val executionContext: ExecutionContext) extends DosResolver with DsdeHttpDAO with Retry {
  import MarthaJsonSupport._
  import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
  import spray.json.DefaultJsonProtocol._

  val http: HttpExt = Http(system)
  val httpClientUtils: HttpClientUtilsStandard = HttpClientUtilsStandard()

  def getClientEmailFromMartha(dos: String, userInfo: UserInfo): Future[Option[String]] = {
    val content = Map("url" -> dos)
    val marthaResponse: Future[MarthaMinimalResponse] = Marshal(content).to[RequestEntity] flatMap { entity =>
      retry[MarthaMinimalResponse](when500) { () =>
        executeRequestWithToken[MarthaMinimalResponse](userInfo.accessToken)(Post(marthaUrl, entity))
      }
    }

    // Evan idea 2020-09-08:
    // Have Rawls call an "SA-only" endpoint in Martha because it doesn't need any URI info (calls Bond but not overloaded DRS servers)
    marthaResponse.map { resp =>
      // The email field must remain an `Option` because DRS servers that do not use Bond (HCA, JDR) do not return a service account
      // AEN 2020-09-08 [WA-325]
      val saEmail: Option[String] = resp.googleServiceAccount.flatMap(_.data.map(_.client_email))

      if (saEmail.isEmpty) {
        logger.info(s"MarthaDosResolver.dosServiceAccountEmail returned no SA for dos URL $dos")
      }

      saEmail
    }
  }

  override def dosServiceAccountEmail(dos: String, userInfo: UserInfo): Future[Option[String]] = {
    if (excludeJDRDomain && MarthaUtils.isJDRDomain(dos)) {
      // Temporarily decline to look up TDR DRS objects in Martha because no SA is returned (no-op in Rawls)
      Future.successful(None)
    } else {
      getClientEmailFromMartha(dos, userInfo)
    }
  }
}
