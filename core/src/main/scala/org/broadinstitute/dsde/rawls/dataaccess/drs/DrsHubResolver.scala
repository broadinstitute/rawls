package org.broadinstitute.dsde.rawls.dataaccess.drs

import akka.actor.ActorSystem
import akka.http.scaladsl.client.RequestBuilding._
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.marshalling.Marshal
import akka.http.scaladsl.model.RequestEntity
import akka.http.scaladsl.{Http, HttpExt}
import akka.stream.Materializer
import org.broadinstitute.dsde.rawls.dataaccess.DsdeHttpDAO
import org.broadinstitute.dsde.rawls.model.UserInfo
import org.broadinstitute.dsde.rawls.util.{HttpClientUtilsStandard, Retry}
import org.broadinstitute.dsde.rawls.dataaccess.drs.DrsHubJsonSupport._

import scala.concurrent.{ExecutionContext, Future}

class DrsHubResolver(drsHubUrl: String)(implicit
                                        val system: ActorSystem,
                                        val materializer: Materializer,
                                        val executionContext: ExecutionContext
) extends DrsResolver
    with DsdeHttpDAO
    with Retry {

  // the list of fields we want in DrsHub response. More info can be found here: https://github.com/broadinstitute/drsHub#drsHub-v3
  private val DrsHubRequestFieldsKey: Array[String] = Array("googleServiceAccount")

  val http: HttpExt = Http(system)
  val httpClientUtils: HttpClientUtilsStandard = HttpClientUtilsStandard()

  private def resolveDrs(drsUrl: String, userInfo: UserInfo): Future[DrsHubMinimalResponse] = {
    // Evan idea 2020-09-08:
    // Have Rawls call an "SA-only" endpoint in DrsHub because it doesn't need any URI info (calls Bond but not overloaded DRS servers)
    val requestObj = DrsHubRequest(drsUrl, DrsHubRequestFieldsKey)
    Marshal(requestObj).to[RequestEntity] flatMap { entity =>
      retry[DrsHubMinimalResponse](when5xx) { () =>
        executeRequestWithToken[DrsHubMinimalResponse](userInfo.accessToken)(Post(drsHubUrl, entity))
      }
    }
  }

  override def drsServiceAccountEmail(drsUrl: String, userInfo: UserInfo): Future[Option[String]] =
    resolveDrs(drsUrl, userInfo).map { resp =>
      // The email field must remain an `Option` because DRS servers that do not use Bond (HCA, JDR) do not return a service account
      // AEN 2020-09-08 [WA-325]
      val saEmail: Option[String] = resp.googleServiceAccount.flatMap(_.data.map(_.client_email))

      if (saEmail.isEmpty) {
        logger.info(s"DrsHubResolver.drsServiceAccountEmail returned no SA for DRS url $drsUrl")
      }

      saEmail
    }
}
