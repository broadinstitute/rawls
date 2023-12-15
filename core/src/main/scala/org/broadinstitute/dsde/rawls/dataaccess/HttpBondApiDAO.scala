package org.broadinstitute.dsde.rawls.dataaccess

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.client.RequestBuilding._
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.StatusCodes
import akka.stream.Materializer
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.dataaccess.BondJsonSupport._
import org.broadinstitute.dsde.rawls.model.UserInfo
import org.broadinstitute.dsde.rawls.util.{HttpClientUtilsStandard, Retry}
import spray.json.RootJsonFormat

import scala.concurrent.{ExecutionContext, Future}

case class BondServiceAccountEmail(client_email: String)
case class BondResponseData(data: BondServiceAccountEmail)

case class Providers(providers: List[String])

object BondJsonSupport {
  import spray.json.DefaultJsonProtocol._

  implicit val BondServiceAccountEmailFormat: RootJsonFormat[BondServiceAccountEmail] = jsonFormat1(
    BondServiceAccountEmail
  )
  implicit val BondResponseDataFormat: RootJsonFormat[BondResponseData] = jsonFormat1(BondResponseData)

  implicit val providersFormat: RootJsonFormat[Providers] = jsonFormat1(Providers)

}

trait BondApiDAO {
  def getBondProviders(): Future[List[String]]
  def getServiceAccountKey(provider: String, userInfo: UserInfo): Future[Option[BondResponseData]]
}

class HttpBondApiDAO(bondBaseUrl: String)(implicit
  val system: ActorSystem,
  val materializer: Materializer,
  val executionContext: ExecutionContext
) extends DsdeHttpDAO
    with Retry
    with BondApiDAO {
  val http = Http(system)
  val httpClientUtils = HttpClientUtilsStandard()

  def getBondProviders(): Future[List[String]] = {
    val bondProviderUrl = s"$bondBaseUrl/api/link/v1/providers"
    val providerResponse: Future[Providers] = executeRequest[Providers](Get(bondProviderUrl))
    providerResponse.map { resp =>
      resp.providers
    }
  }

  def getServiceAccountKey(provider: String, userInfo: UserInfo): Future[Option[BondResponseData]] = {
    val providerUrl = s"$bondBaseUrl/api/link/v1/$provider/serviceaccount/key"
    retry(when5xx) { () =>
      executeRequestWithToken[BondResponseData](userInfo.accessToken)(Get(providerUrl)).map(Option(_)).recover {
        case t: RawlsExceptionWithErrorReport if t.errorReport.statusCode.contains(StatusCodes.NotFound) => None
      }
    }
  }

}
