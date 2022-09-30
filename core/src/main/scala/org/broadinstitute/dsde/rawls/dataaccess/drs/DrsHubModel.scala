package org.broadinstitute.dsde.rawls.dataaccess.drs

import spray.json.RootJsonFormat

case class DrsHubRequest(url: String, fields: Array[String])

case class DrsHubMinimalResponse(googleServiceAccount: Option[ServiceAccountPayload])
case class ServiceAccountPayload(data: Option[ServiceAccountEmail])
case class ServiceAccountEmail(client_email: String)

object DrsHubJsonSupport {
  import spray.json.DefaultJsonProtocol._

  implicit val DrsHubRequestFormat: RootJsonFormat[DrsHubRequest] = jsonFormat2(DrsHubRequest)
  implicit val ServiceAccountEmailFormat: RootJsonFormat[ServiceAccountEmail] = jsonFormat1(ServiceAccountEmail)
  implicit val DrsHubV2ResponseDataFormat: RootJsonFormat[ServiceAccountPayload] = jsonFormat1(ServiceAccountPayload)
  implicit val DrsHubV2ResponseFormat: RootJsonFormat[DrsHubMinimalResponse] = jsonFormat1(DrsHubMinimalResponse)
}
