package org.broadinstitute.dsde.rawls.dataaccess

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.client.RequestBuilding
import akka.http.scaladsl.client.RequestBuilding._
import akka.http.scaladsl.marshalling.Marshal
import akka.http.scaladsl.model.{HttpResponse, RequestEntity}
import akka.http.scaladsl.model.headers.Authorization
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.Materializer
import org.broadinstitute.dsde.rawls.model.UserInfo
import org.broadinstitute.dsde.rawls.util.{HttpClientUtilsStandard, Retry}

import scala.concurrent.{ExecutionContext, Future}

case class BondServiceAccountEmail(client_email: String)
case class BondResponseData(data: Option[BondServiceAccountEmail])

case class Providers(providers: List[String])


object BondJsonSupport {
  import spray.json.DefaultJsonProtocol._

  implicit val BondServiceAccountEmailFormat = jsonFormat1(BondServiceAccountEmail)
  implicit val BondResponseDataFormat = jsonFormat1(BondResponseData)

  implicit val providersFormat = jsonFormat1(Providers)

}

// todo: dosresolver and marthadosresolver can be removed after these changes.


class BondTalker()(implicit val system: ActorSystem, val materializer: Materializer, val executionContext: ExecutionContext) extends DsdeHttpDAO with Retry {
  val http = Http(system)
  val httpClientUtils = HttpClientUtilsStandard()

  // todo: make private
  def getBondProviders(): Future[List[String]] = {
    import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
    import BondJsonSupport._

    val bondProviderUrl = "https://broad-bond-dev.appspot.com/api/link/v1/providers"

    val providerResponse: Future[Providers] = executeRequest[Providers](Get(bondProviderUrl))

    providerResponse.map { resp =>
      println(resp)
      resp.providers
    }

  }


  def getBondProviderServiceAccountEmails(userInfo: UserInfo): Future[List[BondServiceAccountEmail]] = {
    import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
    import spray.json.DefaultJsonProtocol._
    import BondJsonSupport._


    for {
      bondProviderList <- getBondProviders()
      bondResponses <- Future.traverse(bondProviderList) { provider => // todo: is traverse the simplest/best way to do this?
        val bondProviderUrl = s"https://broad-bond-dev.appspot.com/api/link/v1/$provider/serviceaccount/key"
        println(bondProviderUrl)

        retryableFutureToFuture(
          retry[BondResponseData](when500) { () =>
            executeRequestWithToken[BondResponseData](userInfo.accessToken)(Get(bondProviderUrl))
          }
        )
      }
    } yield {
      bondResponses collect {
        case BondResponseData(Some(email)) => email
      }
    }

    // todo: this returns a List(Future(<not completed>), Future(<not completed>))


  }

  // todo: is this the right place? or should it go in under workspace somewhere.
  def addBondProvidersToWorkspace(userInfo: UserInfo) = {
    val emails = getBondProviderServiceAccountEmails(userInfo)
    emails.map { emails =>
      println(emails) // works!

    }


  }
}
