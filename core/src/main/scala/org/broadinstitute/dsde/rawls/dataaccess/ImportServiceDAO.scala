package org.broadinstitute.dsde.rawls.dataaccess

import java.util.UUID

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.client.RequestBuilding.Post
import akka.http.scaladsl.marshalling.Marshal
import akka.http.scaladsl.model.RequestEntity
import akka.stream.Materializer
import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.model.{RawlsEnumeration, UserInfo, WorkspaceName}
import org.broadinstitute.dsde.rawls.util.{HttpClientUtilsStandard, Retry}

import scala.concurrent.{ExecutionContext, Future}

object ImportServiceJsonSupport {
  import spray.json.DefaultJsonProtocol._

  implicit val importServiceResponseFormat = jsonFormat1(ImportServiceResponse)
}

class ImportServiceDAO(url: String)(implicit val system: ActorSystem, val materializer: Materializer, val executionContext: ExecutionContext) extends DsdeHttpDAO with Retry {

  val http = Http(system)
  val httpClientUtils = HttpClientUtilsStandard()

  def getImportStatus(importId: UUID, workspaceName: WorkspaceName, userInfo: UserInfo) = {
    import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
    import spray.json.DefaultJsonProtocol._
    import ImportServiceJsonSupport._

    val content = Map("importId" -> importId.toString)
    val importStatusResponse: Future[ImportServiceResponse] = Marshal(content).to[RequestEntity] flatMap { entity =>
      retry[ImportServiceResponse](when500) { () =>
        executeRequestWithToken[ImportServiceResponse](userInfo.accessToken)(Post(url, entity))
      }
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
