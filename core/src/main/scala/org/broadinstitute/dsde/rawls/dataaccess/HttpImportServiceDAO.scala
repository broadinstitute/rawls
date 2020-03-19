package org.broadinstitute.dsde.rawls.dataaccess

import java.util.UUID

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.client.RequestBuilding.Get
import akka.http.scaladsl.model.StatusCodes
import akka.stream.Materializer
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.model.ImportStatuses
import org.broadinstitute.dsde.rawls.model.ImportStatuses.ImportStatus
import org.broadinstitute.dsde.rawls.model.{UserInfo, WorkspaceName}
import org.broadinstitute.dsde.rawls.util.{HttpClientUtilsStandard, Retry}

import scala.concurrent.{ExecutionContext, Future}

object ImportServiceJsonSupport {
  import spray.json.DefaultJsonProtocol._

  implicit val importServiceResponseFormat = jsonFormat2(ImportServiceResponse)
}

class HttpImportServiceDAO(url: String)(implicit val system: ActorSystem, val materializer: Materializer, val executionContext: ExecutionContext) extends ImportServiceDAO with DsdeHttpDAO with Retry {

  val http = Http(system)
  val httpClientUtils = HttpClientUtilsStandard()

  def getImportStatus(importId: UUID, workspaceName: WorkspaceName, userInfo: UserInfo): Future[Option[ImportStatus]] = {
    import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
    import ImportServiceJsonSupport._

    val requestUrl = s"$url/${workspaceName.namespace}/${workspaceName.name}/imports/$importId"

    val importStatusResponse: Future[Option[ImportServiceResponse]] =  retry[Option[ImportServiceResponse]](when500) { () =>
        executeRequestWithToken[Option[ImportServiceResponse]](userInfo.accessToken)(Get(requestUrl)) recover {
          case notOK: RawlsExceptionWithErrorReport if notOK.errorReport.statusCode.contains(StatusCodes.NotFound) => None
        }
    }

    importStatusResponse.map { response =>
      response.map( statusString => ImportStatuses.withName(statusString.status))
    }
  }

}


case class ImportServiceResponse(jobId: String, status: String)


