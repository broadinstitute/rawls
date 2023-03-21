package org.broadinstitute.dsde.rawls.model

import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.model.GcpResourceTypes.GcpResourceType
import org.joda.time.DateTime

/**
  * Created by tlangs on 3/16/2023.
  */

object FastPassGrant {
  def newFastPassGrant(workspaceId: String,
                       userSubjectId: RawlsUserSubjectId,
                       resourceType: GcpResourceType,
                       resourceName: String,
                       organizationRole: String,
                       expiration: DateTime
  ) = FastPassGrant(-1L,
                    workspaceId,
                    userSubjectId,
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
