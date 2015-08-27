package org.broadinstitute.dsde.rawls.model

import com.google.api.client.util.Value
import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.model.WorkspaceAccessLevel.WorkspaceAccessLevel
import spray.json.{DeserializationException, JsString, JsValue, RootJsonFormat}

case class WorkspaceACL(acl: Map[String, WorkspaceAccessLevel])

case class WorkspaceACLUpdate(userId: String, accessLevel: WorkspaceAccessLevel)

object WorkspaceAccessLevel extends Enumeration {
  type WorkspaceAccessLevel = Value
  val UnknownUser = Value(0)
  val NoAccess = Value(1)
  val Read = Value(2)
  val Write = Value(3)
  val Owner = Value(4)

  // note that the canonical string must match the format for GCS ACL roles,
  // because we use it to set the role of entities in the ACL.
  // (see https://cloud.google.com/storage/docs/json_api/v1/bucketAccessControls)

  def toCanonicalString(v: WorkspaceAccessLevel): String = {
    v match {
      case Owner => "OWNER"
      case Write => "WRITER"
      case Read => "READER"
      case NoAccess => "NO ACCESS"
      case UnknownUser => "UNKNOWN USER"
      case _ => throw new RawlsException(s"invalid WorkspaceAccessLevel [${v}]")
    }
  }

  def fromCanonicalString(s: String): WorkspaceAccessLevel = {
    s match {
      case "OWNER" => Owner
      case "WRITER" => Write
      case "READER" => Read
      case "NO ACCESS" => NoAccess
      case "UNKNOWN USER" => UnknownUser
      case _ => throw new RawlsException(s"invalid WorkspaceAccessLevel [${s}]")
    }
  }
}

object WorkspaceACLJsonSupport extends JsonSupport {
  implicit object WorkspaceAccessLevelFormat extends RootJsonFormat[WorkspaceAccessLevel] {
    override def write(value: WorkspaceAccessLevel): JsValue = JsString(WorkspaceAccessLevel.toCanonicalString(value))
    override def read(json: JsValue): WorkspaceAccessLevel = json match {
      case JsString(name) => WorkspaceAccessLevel.fromCanonicalString(name)
      case x => throw new DeserializationException("invalid value: " + x)
    }
  }

  implicit val WorkspaceACLFormat = jsonFormat1(WorkspaceACL)

  implicit val WorkspaceACLUpdateFormat = jsonFormat2(WorkspaceACLUpdate)
}

