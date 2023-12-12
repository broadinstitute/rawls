package org.broadinstitute.dsde.rawls.model

import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.model.WorkspaceAccessLevels.WorkspaceAccessLevel
import spray.json._

import scala.util.Try

case class AccessEntry(accessLevel: WorkspaceAccessLevel, pending: Boolean, canShare: Boolean, canCompute: Boolean)

case class WorkspaceACL(acl: Map[String, AccessEntry])

case class WorkspaceACLUpdate(email: String,
                              accessLevel: WorkspaceAccessLevel,
                              canShare: Option[Boolean] = None,
                              canCompute: Option[Boolean] = None
)

case class WorkspaceACLUpdateResponseList(usersUpdated: Set[WorkspaceACLUpdate],
                                          invitesSent: Set[WorkspaceACLUpdate],
                                          usersNotFound: Set[WorkspaceACLUpdate]
)

case class WorkspaceCatalog(email: String, catalog: Boolean)

case class WorkspaceCatalogResponse(subjectId: String, catalog: Boolean)

case class WorkspaceCatalogUpdateResponseList(usersUpdated: Seq[WorkspaceCatalogResponse], emailsNotFound: Seq[String])

object WorkspaceAccessLevels {
  sealed trait WorkspaceAccessLevel extends RawlsEnumeration[WorkspaceAccessLevel] with Ordered[WorkspaceAccessLevel] {
    def compare(that: WorkspaceAccessLevel) = all.indexOf(this).compare(all.indexOf(that))

    override def toString = WorkspaceAccessLevels.toString(this)
    def toPolicyName = WorkspaceAccessLevels.toPolicyName(this)
    override def withName(name: String) = WorkspaceAccessLevels.withName(name)
  }

  case object NoAccess extends WorkspaceAccessLevel
  case object Read extends WorkspaceAccessLevel
  case object Write extends WorkspaceAccessLevel
  case object Owner extends WorkspaceAccessLevel
  case object ProjectOwner extends WorkspaceAccessLevel

  val all: Seq[WorkspaceAccessLevel] = Seq(NoAccess, Read, Write, Owner, ProjectOwner)
  val groupAccessLevelsAscending = Seq(Read, Write, Owner, ProjectOwner)

  // note that the canonical string must match the format for GCS ACL roles,
  // because we use it to set the role of entities in the ACL.
  // (see https://cloud.google.com/storage/docs/json_api/v1/bucketAccessControls)
  def toString(v: WorkspaceAccessLevel): String =
    v match {
      case ProjectOwner => "PROJECT_OWNER"
      case Owner        => "OWNER"
      case Write        => "WRITER"
      case Read         => "READER"
      case NoAccess     => "NO ACCESS"
      case _            => throw new RawlsException(s"invalid WorkspaceAccessLevel [${v}]")
    }

  def toPolicyName(v: WorkspaceAccessLevel): Option[String] =
    v match {
      case ProjectOwner => Option("project-owner")
      case Owner        => Option("owner")
      case Write        => Option("writer")
      case Read         => Option("reader")
      case _            => None
    }

  def withName(s: String): WorkspaceAccessLevel =
    s match {
      case accessLevel if accessLevel.equalsIgnoreCase("PROJECT_OWNER") => ProjectOwner
      case accessLevel if accessLevel.equalsIgnoreCase("OWNER")         => Owner
      case accessLevel if accessLevel.equalsIgnoreCase("WRITER")        => Write
      case accessLevel if accessLevel.equalsIgnoreCase("READER")        => Read
      case accessLevel if accessLevel.equalsIgnoreCase("NO ACCESS")     => NoAccess
      case _ => throw new RawlsException(s"invalid WorkspaceAccessLevel [${s}]")
    }

  def withPolicyName(policyName: String): Option[WorkspaceAccessLevel] =
    Try(withName(policyName.replace("-", "_"))).toOption

  def withRoleName(roleName: String): Option[WorkspaceAccessLevel] =
    Try(withName(roleName.replace("-", "_"))).toOption

  def max(a: WorkspaceAccessLevel, b: WorkspaceAccessLevel): WorkspaceAccessLevel =
    if (a <= b) {
      b
    } else {
      a
    }
}

class WorkspaceACLJsonSupport extends JsonSupport {
  import spray.json.DefaultJsonProtocol._

  implicit object WorkspaceAccessLevelFormat extends RootJsonFormat[WorkspaceAccessLevel] {
    override def write(value: WorkspaceAccessLevel): JsValue = JsString(value.toString)
    override def read(json: JsValue): WorkspaceAccessLevel = json match {
      case JsString(name) => WorkspaceAccessLevels.withName(name)
      case x              => throw new DeserializationException("invalid value: " + x)
    }
  }

  implicit val AccessEntryFormat: RootJsonFormat[AccessEntry] = jsonFormat4(AccessEntry)

  implicit val WorkspaceACLFormat: RootJsonFormat[WorkspaceACL] = jsonFormat1(WorkspaceACL)

  implicit val WorkspaceACLUpdateFormat: RootJsonFormat[WorkspaceACLUpdate] = jsonFormat4(WorkspaceACLUpdate)

  implicit val WorkspaceACLUpdateResponseListFormat: RootJsonFormat[WorkspaceACLUpdateResponseList] = jsonFormat3(WorkspaceACLUpdateResponseList)

  implicit val WorkspaceCatalogFormat: RootJsonFormat[WorkspaceCatalog] = jsonFormat2(WorkspaceCatalog)

  implicit val WorkspaceCatalogResponseFormat: RootJsonFormat[WorkspaceCatalogResponse] = jsonFormat2(WorkspaceCatalogResponse)

  implicit val WorkspaceCatalogUpdateResponseListFormat: RootJsonFormat[WorkspaceCatalogUpdateResponseList] = jsonFormat2(WorkspaceCatalogUpdateResponseList)
}

object WorkspaceACLJsonSupport extends WorkspaceACLJsonSupport
