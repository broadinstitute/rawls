package org.broadinstitute.dsde.rawls.dataaccess.martha

// Both Martha v2 and v3 return a JSON that includes `googleServiceAccount` as a top-level key
// The rest of the response in both versions is currently unused
case class MarthaMinimalResponse(googleServiceAccount: Option[ServiceAccountPayload])
case class ServiceAccountPayload(data: Option[ServiceAccountEmail])
case class ServiceAccountEmail(client_email: String)

object MarthaJsonSupport {
  import spray.json.DefaultJsonProtocol._

  implicit val ServiceAccountEmailFormat = jsonFormat1(ServiceAccountEmail)
  implicit val MarthaV2ResponseDataFormat = jsonFormat1(ServiceAccountPayload)
  implicit val MarthaV2ResponseFormat = jsonFormat1(MarthaMinimalResponse)
}
