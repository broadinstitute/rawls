package org.broadinstitute.dsde.rawls.dataaccess.drs

import akka.actor.ActorSystem
import akka.http.scaladsl.client.RequestBuilding._
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.marshalling.Marshal
import akka.http.scaladsl.model.RequestEntity
import akka.http.scaladsl.{Http, HttpExt}
import akka.stream.Materializer
import org.broadinstitute.dsde.rawls.dataaccess.DsdeHttpDAO
import org.broadinstitute.dsde.rawls.dataaccess.drs.MarthaJsonSupport._
import org.broadinstitute.dsde.rawls.model.UserInfo
import org.broadinstitute.dsde.rawls.util.{HttpClientUtilsStandard, Retry}

import scala.concurrent.{ExecutionContext, Future}

class MarthaResolver(marthaUrl: String)(implicit
  val system: ActorSystem,
  val materializer: Materializer,
  val executionContext: ExecutionContext
) extends DrsResolver
    with DsdeHttpDAO
    with Retry {

  // the list of fields we want in Martha response. More info can be found here: https://github.com/broadinstitute/martha#martha-v3
  private val MarthaRequestFieldsKey: Array[String] = Array("googleServiceAccount")

  val http: HttpExt = Http(system)
  val httpClientUtils: HttpClientUtilsStandard = HttpClientUtilsStandard()

  def resolveDrsThroughMartha(drsUrl: String, userInfo: UserInfo): Future[MarthaMinimalResponse] = {
    // Evan idea 2020-09-08:
    // Have Rawls call an "SA-only" endpoint in Martha because it doesn't need any URI info (calls Bond but not overloaded DRS servers)
    val requestObj = MarthaRequest(drsUrl, MarthaRequestFieldsKey)
    Marshal(requestObj).to[RequestEntity] flatMap { entity =>
      retry[MarthaMinimalResponse](when5xx) { () =>
        executeRequestWithToken[MarthaMinimalResponse](userInfo.accessToken)(Post(marthaUrl, entity))
      }
    }
  }

  override def drsServiceAccountEmail(drsUrl: String, userInfo: UserInfo): Future[Option[String]] =
    resolveDrsThroughMartha(drsUrl, userInfo).map { resp =>
      // The email field must remain an `Option` because DRS servers that do not use Bond (HCA, JDR) do not return a service account
      // AEN 2020-09-08 [WA-325]
      val saEmail: Option[String] = resp.googleServiceAccount.flatMap(_.data.map(_.client_email))

      if (saEmail.isEmpty) {
        logger.info(s"MarthaResolver.drsServiceAccountEmail returned no SA for DRS url $drsUrl")
      }

      saEmail
    }
}
