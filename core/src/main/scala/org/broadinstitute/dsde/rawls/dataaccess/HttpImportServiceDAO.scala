package org.broadinstitute.dsde.rawls.dataaccess

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.client.RequestBuilding.Get
import akka.http.scaladsl.model.Uri.Path
import akka.http.scaladsl.model.{StatusCodes, Uri}
import akka.stream.Materializer
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.model.ImportStatuses.ImportStatus
import org.broadinstitute.dsde.rawls.model.{ImportStatuses, UserInfo, WorkspaceName}
import org.broadinstitute.dsde.rawls.util.{HttpClientUtilsStandard, Retry}
import spray.json.RootJsonFormat

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

object ImportServiceJsonSupport {
  import spray.json.DefaultJsonProtocol._

  implicit val importServiceResponseFormat: RootJsonFormat[ImportServiceResponse] = jsonFormat2(ImportServiceResponse)
}

/**
  * This ImportServiceDAO talks to both cWDS (Java/Spring, new) and Import Service (Python, deprecated). Yes, this
  * violates encapsulation for a DAO, but it allows for smooth migration from Import Service to cWDS without large
  * code churn. When Import Service is truly sunset, this DAO will return to form and only talk to one external service.
  *
  * @param importServiceUrl base url for Import Service
  * @param cwdsUrl base url for cWDS
  * @param system ActorSystem
  * @param materializer Materializer
  * @param executionContext ExecutionContext
  */
class HttpImportServiceDAO(importServiceUrl: String, cwdsUrl: String)(implicit
  val system: ActorSystem,
  val materializer: Materializer,
  val executionContext: ExecutionContext
) extends ImportServiceDAO
    with DsdeHttpDAO
    with Retry {

  val http = Http(system)
  val httpClientUtils = HttpClientUtilsStandard()

  // retrieve an import job's status from Import Service
  override def getImportStatus(importId: UUID,
                               workspaceName: WorkspaceName,
                               userInfo: UserInfo
  ): Future[Option[ImportStatus]] = {

    val requestUrl =
      Uri(importServiceUrl).withPath(Path(s"/${workspaceName.namespace}/${workspaceName.name}/imports/$importId"))

    doImportStatusRequest(requestUrl, userInfo).map { response =>
      response.map(statusString => ImportStatuses.withName(statusString.status))
    }
  }

  // retrieve an import job's status from cWDS, and translate its status into the known values for Import Service
  override def getCwdsStatus(importId: UUID, workspaceId: UUID, userInfo: UserInfo): Future[Option[ImportStatus]] = {
    val requestUrl =
      Uri(cwdsUrl).withPath(Path(s"/job/v1/$importId"))

    doImportStatusRequest(requestUrl, userInfo).map { response =>
      response.map(statusString => ImportStatuses.fromCwdsStatus(statusString.status))
    }

  }

  private def doImportStatusRequest(requestUrl: Uri, userInfo: UserInfo): Future[Option[ImportServiceResponse]] = {
    import ImportServiceJsonSupport._
    import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
    retry[Option[ImportServiceResponse]](when5xx) { () =>
      executeRequestWithToken[Option[ImportServiceResponse]](userInfo.accessToken)(Get(requestUrl)) recover {
        case notOK: RawlsExceptionWithErrorReport if notOK.errorReport.statusCode.contains(StatusCodes.NotFound) =>
          None
      }
    }
  }

}

case class ImportServiceResponse(jobId: String, status: String)
