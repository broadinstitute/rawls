package org.broadinstitute.dsde.rawls.dataaccess

import java.util.UUID

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.client.RequestBuilding.Get
import akka.stream.Materializer
import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.dataaccess.ImportStatuses.ImportStatus
import org.broadinstitute.dsde.rawls.model.{RawlsEnumeration, UserInfo, WorkspaceName}
import org.broadinstitute.dsde.rawls.util.{HttpClientUtilsStandard, Retry}

import scala.concurrent.{ExecutionContext, Future}

object ImportServiceJsonSupport {
  import spray.json.DefaultJsonProtocol._

  implicit val importServiceResponseFormat = jsonFormat1(ImportServiceResponse)
}

class HttpImportServiceDAO(url: String)(implicit val system: ActorSystem, val materializer: Materializer, val executionContext: ExecutionContext) extends ImportServiceDAO with DsdeHttpDAO with Retry {

  val http = Http(system)
  val httpClientUtils = HttpClientUtilsStandard()

  def getImportStatus(importId: UUID, workspaceName: WorkspaceName, userInfo: UserInfo): Future[Option[ImportStatus]] = {
    import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
    import spray.json.DefaultJsonProtocol._
    import ImportServiceJsonSupport._

    val requestUrl = s"$url/iservice/${workspaceName.namespace}/${workspaceName.name}/imports/$importId"
    val importStatusResponse: Future[ImportServiceResponse] =  retry[ImportServiceResponse](when500) { () =>
        executeRequestWithToken[ImportServiceResponse](userInfo.accessToken)(Get(requestUrl))
    }

    importStatusResponse.map { response =>
      if( response.status.isEmpty ) {
        logger.warn(s"Status not found for import with id $importId into workspace ${workspaceName.name} in namespace ${workspaceName.namespace} for user ${userInfo.userEmail}.")
      }
      response.status.map( statusString => ImportStatuses.withName(statusString))
    }
  }
}


case class ImportServiceResponse(status: Option[String])



object ImportStatuses {
  sealed trait ImportStatus extends RawlsEnumeration[ImportStatus] {
    override def toString = getClass.getSimpleName.stripSuffix("$")
    override def withName(name: String): ImportStatus = ImportStatuses.withName(name)
  }

  def withName(name: String): ImportStatus = name.toLowerCase match {
    case "readyforupsert" => ReadyForUpsert
    case "upserting" => Upserting
    case "done" => Done
    case "error" => Error()
    case _ => throw new RawlsException(s"invalid ImportStatus [${name}]")
  }

  case object ReadyForUpsert extends ImportStatus
  case object Upserting extends ImportStatus
  case object Done extends ImportStatus
  case class Error(message: String = "") extends ImportStatus
}
