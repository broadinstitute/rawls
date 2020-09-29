package org.broadinstitute.dsde.rawls.dataaccess.martha

import akka.actor.ActorSystem
import akka.http.scaladsl.client.RequestBuilding._
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.marshalling.Marshal
import akka.http.scaladsl.model.RequestEntity
import akka.http.scaladsl.{Http, HttpExt}
import akka.stream.Materializer
import org.broadinstitute.dsde.rawls.dataaccess.DsdeHttpDAO
import org.broadinstitute.dsde.rawls.dataaccess.martha.MarthaJsonSupport._
import org.broadinstitute.dsde.rawls.model.UserInfo
import org.broadinstitute.dsde.rawls.util.{HttpClientUtilsStandard, Retry}

import scala.concurrent.{ExecutionContext, Future}

class MarthaResolver(marthaUrl: String)(implicit val system: ActorSystem, val materializer: Materializer, val executionContext: ExecutionContext) extends DosResolver with DsdeHttpDAO with Retry {

  // the list of fields we want in Martha response
  private val MarthaRequestFieldsKey: Array[String] = Array("googleServiceAccount")

  val http: HttpExt = Http(system)
  val httpClientUtils: HttpClientUtilsStandard = HttpClientUtilsStandard()

  override def dosServiceAccountEmail(drsUrl: String, userInfo: UserInfo): Future[Option[String]] = {
    val requestObj = MarthaRequest(drsUrl, MarthaRequestFieldsKey)
    val marthaResponse: Future[MarthaMinimalResponse] = Marshal(requestObj).to[RequestEntity] flatMap { entity =>
      retry[MarthaMinimalResponse](when500) { () =>
        executeRequestWithToken[MarthaMinimalResponse](userInfo.accessToken)(Post(marthaUrl, entity))
      }
    }

    marthaResponse.map { resp =>
      // The email field must remain an `Option` because DRS servers that do not use Bond (HCA, JDR) do not return a service account
      // AEN 2020-09-08 [WA-325]
      val saEmail: Option[String] = resp.googleServiceAccount.flatMap(_.data.map(_.client_email))

      if (saEmail.isEmpty) {
        logger.info(s"MarthaResolver.dosServiceAccountEmail returned no SA for drs URL $drsUrl")
      }

      saEmail
    }
  }
}
