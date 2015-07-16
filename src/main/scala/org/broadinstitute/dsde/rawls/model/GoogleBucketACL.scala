package org.broadinstitute.dsde.rawls.model

import org.broadinstitute.dsde.rawls.acl.AccessLevel
import spray.json.{DeserializationException, JsString, JsValue, RootJsonFormat}

object GoogleBucketACLJsonSupport extends JsonSupport {

  implicit object AccessLevelStatusFormat extends RootJsonFormat[AccessLevel.Value] {
    override def write(value: AccessLevel.Value): JsValue = JsString(AccessLevel.toGoogleString(value))
    override def read(json: JsValue): AccessLevel.Value = json match {
      case JsString(name) => AccessLevel.fromGoogleString(name)
      case x => throw new DeserializationException("invalid value: " + x)
    }
  }

  implicit val BucketAccessControlFormat = jsonFormat8(BucketAccessControl)
  implicit val BucketAccessControlsFormat = jsonFormat2(BucketAccessControls)
}

//TODO: use com.google.api.services.storage.model.BucketAccessControl

case class BucketAccessControl(
  kind: String,
  id: String,
  selfLink: String,
  bucket: String,
  entity: String,
  role: AccessLevel.Value,
  projectTeam: Map[String,String],
  etag: String
)

case class BucketAccessControls(
  kind: String,
  items: Seq[BucketAccessControl]
)
{
  def maximumAccessLevel: AccessLevel.Value = {
    val levels = items map { _.role }
    levels.max
  }
}


