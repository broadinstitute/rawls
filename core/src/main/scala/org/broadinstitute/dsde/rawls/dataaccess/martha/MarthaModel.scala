package org.broadinstitute.dsde.rawls.dataaccess.martha

import spray.json.RootJsonFormat

case class MarthaMinimalRequest(url: String, fields: List[String])

// Both Martha v2 and v3 return a JSON that includes `googleServiceAccount` as a top-level key
// The rest of the response in both versions is currently unused
case class MarthaMinimalResponse(googleServiceAccount: Option[ServiceAccountPayload])
case class ServiceAccountPayload(data: Option[ServiceAccountEmail])
case class ServiceAccountEmail(client_email: String)

object MarthaJsonSupport {
  import spray.json.DefaultJsonProtocol._

  implicit val MarthaMinimalRequestFormat: RootJsonFormat[MarthaMinimalRequest] = jsonFormat2(MarthaMinimalRequest)
  implicit val ServiceAccountEmailFormat: RootJsonFormat[ServiceAccountEmail] = jsonFormat1(ServiceAccountEmail)
  implicit val ServiceAccountPayloadFormat: RootJsonFormat[ServiceAccountPayload] = jsonFormat1(ServiceAccountPayload)
  implicit val MarthaMinimalResponseFormat: RootJsonFormat[MarthaMinimalResponse] = jsonFormat1(MarthaMinimalResponse)
}
