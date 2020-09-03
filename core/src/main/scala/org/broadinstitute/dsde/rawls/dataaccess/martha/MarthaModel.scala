package org.broadinstitute.dsde.rawls.dataaccess.martha

object MarthaV2JsonSupport {
  import spray.json.DefaultJsonProtocol._

  implicit val ServiceAccountEmailFormat = jsonFormat1(ServiceAccountEmail)
  implicit val MarthaV2ResponseDataFormat = jsonFormat1(MarthaV2ResponseData)
  implicit val MarthaV2ResponseFormat = jsonFormat1(MarthaV2Response)
}

final case class MarthaV3Response(size: Option[Long],
                                  timeUpdated: Option[String],
                                  bucket: Option[String],
                                  name: Option[String],
                                  gsUri: Option[String],
                                  googleServiceAccount: Option[SADataObject],
                                  hashes: Option[Map[String, String]])