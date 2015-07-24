package org.broadinstitute.dsde.rawls.model

import com.google.api.client.util.Value
import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.model.GCSAccessLevel.GCSAccessLevel
import spray.json.{DeserializationException, JsString, JsValue, RootJsonFormat}

// based on https://cloud.google.com/storage/docs/json_api/v1/bucketAccessControls

object GCSAccessLevel extends Enumeration {
  type GCSAccessLevel = Value
  val UnknownUser = Value(0)
  val NoAccess = Value(1)
  val Read = Value(2)
  val Write = Value(3)
  val Owner = Value(4)

  def toGoogleString(v: GCSAccessLevel): String = {
    v match {
      case Owner => "OWNER"
      case Write => "WRITER"
      case Read => "READER"
      case NoAccess => "NO ACCESS"
      case UnknownUser => "UNKNOWN USER"
      case _ => throw new RawlsException(s"invalid GCSAccessLevel [${v}]")
    }
  }

  def fromGoogleString(s: String): GCSAccessLevel = {
    s match {
      case "OWNER" => Owner
      case "WRITER" => Write
      case "READER" => Read
      case "NO ACCESS" => NoAccess
      case "UNKNOWN USER" => UnknownUser
      case _ => throw new RawlsException(s"invalid GCSAccessLevel [${s}]")
    }
  }
}

case class BucketAccessControl(
  bucket: String,
  domain: Option[String],
  email: Option[String],
  entity: String,
  entityId: Option[String],
  etag: String,
  id: String,
  kind: String,
  projectTeam: Option[Map[String,String]],
  role: GCSAccessLevel,
  selfLink: String
)

case class BucketAccessControls(
  kind: String,
  items: Seq[BucketAccessControl]
)
{
  def maximumAccessLevel: GCSAccessLevel = {
    if (items.isEmpty) GCSAccessLevel.NoAccess
    else (items map { _.role }).max
  }
}

object BucketAccessControlJsonSupport extends JsonSupport {

  implicit object GCSAccessLevelFormat extends RootJsonFormat[GCSAccessLevel] {
    override def write(value: GCSAccessLevel): JsValue = JsString(GCSAccessLevel.toGoogleString(value))
    override def read(json: JsValue): GCSAccessLevel = json match {
      case JsString(name) => GCSAccessLevel.fromGoogleString(name)
      case x => throw new DeserializationException("invalid value: " + x)
    }
  }

  implicit val BucketAccessControlFormat = jsonFormat11(BucketAccessControl)
  implicit val BucketAccessControlsFormat = jsonFormat2(BucketAccessControls)
}

