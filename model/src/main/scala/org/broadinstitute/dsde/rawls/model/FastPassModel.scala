package org.broadinstitute.dsde.rawls.model

import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.model.GcpResourceTypes.GcpResourceType
import org.broadinstitute.dsde.rawls.model.MemberTypes.MemberType
import org.joda.time.DateTime

/**
  * Created by tlangs on 3/16/2023.
  */

object FastPassGrant {
  def newFastPassGrant(workspaceId: String,
                       userSubjectId: RawlsUserSubjectId,
                       accountEmail: RawlsUserEmail,
                       accountType: MemberType,
                       resourceType: GcpResourceType,
                       resourceName: String,
                       organizationRole: String,
                       expiration: DateTime
  ) = FastPassGrant(-1L,
                    workspaceId,
                    userSubjectId,
                    accountEmail,
                    accountType,
                    resourceType,
                    resourceName,
                    organizationRole,
                    expiration,
                    DateTime.now()
  )
}
case class FastPassGrant(
  id: Long,
  workspaceId: String,
  userSubjectId: RawlsUserSubjectId,
  accountEmail: RawlsUserEmail,
  accountType: MemberType,
  resourceType: GcpResourceType,
  resourceName: String,
  organizationRole: String,
  expiration: DateTime,
  created: DateTime
)

object GcpResourceTypes {

  sealed trait GcpResourceType extends RawlsEnumeration[GcpResourceType] {
    override def withName(name: String) = GcpResourceTypes.withName(name)
    override def toString = getClass.getSimpleName.stripSuffix("$")
    def toName(gcpResourceType: GcpResourceType) = GcpResourceTypes.toName(gcpResourceType)
  }

  case object Bucket extends GcpResourceType
  case object Project extends GcpResourceType

  def withName(name: String): GcpResourceType =
    name match {
      case "bucket"  => Bucket
      case "project" => Project
      case _         => throw new RawlsException(s"invalid GcpResourceType [$name]")
    }

  def toName(gcpResourceType: GcpResourceType): String =
    gcpResourceType match {
      case Bucket  => "bucket"
      case Project => "project"
    }
}

/**
  * Mirrors org.broadinstitute.dsde.workbench.google.GoogleIamDAO.MemberType
  */
object MemberTypes {

  sealed trait MemberType extends RawlsEnumeration[MemberType] {
    override def withName(name: String) = MemberTypes.withName(name)
    override def toString = getClass.getSimpleName.stripSuffix("$")
    def toName(memberType: MemberType) = MemberTypes.toName(memberType)
  }

  case object User extends MemberType
  case object ServiceAccount extends MemberType

  def withName(name: String): MemberType =
    name match {
      case "user"           => User
      case "serviceAccount" => ServiceAccount
      case _                => throw new RawlsException(s"invalid MemberType [$name]")
    }

  def toName(memberType: MemberType): String =
    memberType match {
      case User           => "user"
      case ServiceAccount => "serviceAccount"
    }
}
