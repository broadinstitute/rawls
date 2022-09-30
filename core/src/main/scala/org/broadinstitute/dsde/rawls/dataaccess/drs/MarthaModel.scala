package org.broadinstitute.dsde.rawls.dataaccess.drs

import spray.json.RootJsonFormat

case class MarthaRequest(url: String, fields: Array[String])

// Both Martha v2 and v3 return a JSON that includes `googleServiceAccount` as a top-level key
// The rest of the response in both versions is currently unused
case class MarthaMinimalResponse(googleServiceAccount: Option[ServiceAccountPayload])
object MarthaJsonSupport {
  import spray.json.DefaultJsonProtocol._

  implicit val MarthaRequestFormat: RootJsonFormat[MarthaRequest] = jsonFormat2(MarthaRequest)
  implicit val ServiceAccountEmailFormat: RootJsonFormat[ServiceAccountEmail] = jsonFormat1(ServiceAccountEmail)
  implicit val MarthaV2ResponseDataFormat: RootJsonFormat[ServiceAccountPayload] = jsonFormat1(ServiceAccountPayload)
  implicit val MarthaV2ResponseFormat: RootJsonFormat[MarthaMinimalResponse] = jsonFormat1(MarthaMinimalResponse)
}
