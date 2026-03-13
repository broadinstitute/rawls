package org.broadinstitute.dsde.rawls.dataaccess.drs

import spray.json.RootJsonFormat

case class DrsHubRequest(url: String, fields: Array[String], userProject: Option[String])

case class DrsHubAccessUrl(url: Option[String], headers: Option[Map[String, String]])
case class DrsHubMinimalResponse(accessUrl: Option[DrsHubAccessUrl])

object DrsHubJsonSupport {
  import spray.json.DefaultJsonProtocol._

  implicit val DrsHubRequestFormat: RootJsonFormat[DrsHubRequest] = jsonFormat3(DrsHubRequest)
  implicit val DrsHubAccessUrlFormat: RootJsonFormat[DrsHubAccessUrl] = jsonFormat2(DrsHubAccessUrl)
  implicit val DrsHubV2ResponseFormat: RootJsonFormat[DrsHubMinimalResponse] = jsonFormat1(DrsHubMinimalResponse)
}
