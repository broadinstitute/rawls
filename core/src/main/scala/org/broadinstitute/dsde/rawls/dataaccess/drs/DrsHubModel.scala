package org.broadinstitute.dsde.rawls.dataaccess.drs

import spray.json.RootJsonFormat

case class DrsHubRequest(url: String, fields: Array[String], serviceName: String = "rawls")

case class DrsHubMinimalResponse(googleServiceAccount: Option[ServiceAccountPayload])

object DrsHubJsonSupport {
  import spray.json.DefaultJsonProtocol._

  implicit val DrsHubRequestFormat: RootJsonFormat[DrsHubRequest] = jsonFormat3(DrsHubRequest)
  implicit val ServiceAccountEmailFormat: RootJsonFormat[ServiceAccountEmail] = jsonFormat1(ServiceAccountEmail)
  implicit val DrsHubV2ResponseDataFormat: RootJsonFormat[ServiceAccountPayload] = jsonFormat1(ServiceAccountPayload)
  implicit val DrsHubV2ResponseFormat: RootJsonFormat[DrsHubMinimalResponse] = jsonFormat1(DrsHubMinimalResponse)
}
