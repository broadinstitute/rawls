package org.broadinstitute.dsde.rawls.dataaccess

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.client.RequestBuilding.Get
import akka.http.scaladsl.model.Uri.Path
import akka.http.scaladsl.model.{StatusCodes, Uri}
import akka.stream.Materializer
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.model.ImportStatuses.ImportStatus
import org.broadinstitute.dsde.rawls.model.{ImportStatuses, UserInfo}
import org.broadinstitute.dsde.rawls.util.{HttpClientUtilsStandard, Retry}
import spray.json.RootJsonFormat

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

object CwdsJsonSupport {
  import spray.json.DefaultJsonProtocol._

  implicit val jobStatusResponseFormat: RootJsonFormat[JobStatusResponse] = jsonFormat2(JobStatusResponse)
}

/**
  * DAO to talk to cWDS.
  *
  * @param cwdsUrl base url for cWDS
  * @param system ActorSystem
  * @param materializer Materializer
  * @param executionContext ExecutionContext
  */
class HttpCwdsDAO(cwdsUrl: String)(implicit
  val system: ActorSystem,
  val materializer: Materializer,
  val executionContext: ExecutionContext
) extends CwdsDAO
    with DsdeHttpDAO
    with Retry {

  val http = Http(system)
  val httpClientUtils = HttpClientUtilsStandard()

  // retrieve an import job's status from cWDS, and translate its status into the known values for Import Service
  override def getImportStatus(importId: UUID, workspaceId: UUID, userInfo: UserInfo): Future[Option[ImportStatus]] = {
    val requestUrl =
      Uri(cwdsUrl).withPath(Path(s"/job/v1/$importId"))

    doImportStatusRequest(requestUrl, userInfo).map { response =>
      response.map(statusString => ImportStatuses.fromCwdsStatus(statusString.status))
    }

  }

  private def doImportStatusRequest(requestUrl: Uri, userInfo: UserInfo): Future[Option[JobStatusResponse]] = {
    import CwdsJsonSupport._
    import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
    retry[Option[JobStatusResponse]](when5xx) { () =>
      executeRequestWithToken[Option[JobStatusResponse]](userInfo.accessToken)(Get(requestUrl)) recover {
        case notOK: RawlsExceptionWithErrorReport if notOK.errorReport.statusCode.contains(StatusCodes.NotFound) =>
          None
      }
    }
  }

}

case class JobStatusResponse(jobId: String, status: String)
