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

class HttpImportServiceDAO(importServiceUrl: String, cwdsUrl: String)(implicit
  val system: ActorSystem,
  val materializer: Materializer,
  val executionContext: ExecutionContext
) extends ImportServiceDAO
    with DsdeHttpDAO
    with Retry {

  val http = Http(system)
  val httpClientUtils = HttpClientUtilsStandard()

  def getImportStatus(importId: UUID,
                      workspaceName: WorkspaceName,
                      userInfo: UserInfo
  ): Future[Option[ImportStatus]] = {
    import ImportServiceJsonSupport._
    import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._

    val requestUrl =
      Uri(importServiceUrl).withPath(Path(s"/${workspaceName.namespace}/${workspaceName.name}/imports/$importId"))

    val importStatusResponse: Future[Option[ImportServiceResponse]] = retry[Option[ImportServiceResponse]](when5xx) {
      () =>
        executeRequestWithToken[Option[ImportServiceResponse]](userInfo.accessToken)(Get(requestUrl)) recover {
          case notOK: RawlsExceptionWithErrorReport if notOK.errorReport.statusCode.contains(StatusCodes.NotFound) =>
            None
        }
    }

    importStatusResponse.map { response =>
      response.map(statusString => ImportStatuses.withName(statusString.status))
    }
  }

  override def getCwdsStatus(importId: UUID, workspaceId: UUID, userInfo: UserInfo): Future[Option[ImportStatus]] = ???
}

case class ImportServiceResponse(jobId: String, status: String)
