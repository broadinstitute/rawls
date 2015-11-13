package org.broadinstitute.dsde.rawls.model

import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.model.WorkspaceAccessLevels.WorkspaceAccessLevel
import spray.json._

case class WorkspaceACL(acl: Map[String, WorkspaceAccessLevel])

case class WorkspaceACLUpdate(email: String, accessLevel: WorkspaceAccessLevel)

object WorkspaceAccessLevels {
  sealed trait WorkspaceAccessLevel extends RawlsEnumeration[WorkspaceAccessLevel] with Ordered[WorkspaceAccessLevel] {
    val all = Seq(UnknownUser, NoAccess, Read, Write, Owner)

    def compare(that: WorkspaceAccessLevel) = { all.indexOf(this).compare(all.indexOf(that)) }

    override def toString = WorkspaceAccessLevels.toString(this)
    override def withName(name: String) = WorkspaceAccessLevels.withName(name)
  }

  case object UnknownUser extends WorkspaceAccessLevel
  case object NoAccess extends WorkspaceAccessLevel
  case object Read extends WorkspaceAccessLevel
  case object Write extends WorkspaceAccessLevel
  case object Owner extends WorkspaceAccessLevel

  val groupAccessLevelsAscending = Seq(Read, Write, Owner)

  // note that the canonical string must match the format for GCS ACL roles,
  // because we use it to set the role of entities in the ACL.
  // (see https://cloud.google.com/storage/docs/json_api/v1/bucketAccessControls)
  def toString(v: WorkspaceAccessLevel): String = {
    v match {
      case Owner => "OWNER"
      case Write => "WRITER"
      case Read => "READER"
      case NoAccess => "NO ACCESS"
      case UnknownUser => "UNKNOWN USER"
      case _ => throw new RawlsException(s"invalid WorkspaceAccessLevel [${v}]")
    }
  }

  def withName(s: String): WorkspaceAccessLevel = {
    s match {
      case "OWNER" => Owner
      case "WRITER" => Write
      case "READER" => Read
      case "NO ACCESS" => NoAccess
      case "UNKNOWN USER" => UnknownUser
      case _ => throw new RawlsException(s"invalid WorkspaceAccessLevel [${s}]")
    }
  }

  def max(a: WorkspaceAccessLevel, b: WorkspaceAccessLevel): WorkspaceAccessLevel = {
    if( a <= b ) {
      b
    } else {
      a
    }
  }
}

object WorkspaceACLJsonSupport extends JsonSupport {
  implicit object WorkspaceAccessLevelFormat extends RootJsonFormat[WorkspaceAccessLevel] {
    override def write(value: WorkspaceAccessLevel): JsValue = JsString(value.toString)
    override def read(json: JsValue): WorkspaceAccessLevel = json match {
      case JsString(name) => WorkspaceAccessLevels.withName(name)
      case x => throw new DeserializationException("invalid value: " + x)
    }
  }

  implicit val WorkspaceACLFormat = jsonFormat1(WorkspaceACL)

  implicit val WorkspaceACLUpdateFormat = jsonFormat2(WorkspaceACLUpdate)
}

