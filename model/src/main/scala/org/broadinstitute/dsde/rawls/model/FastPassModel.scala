package org.broadinstitute.dsde.rawls.model

import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.model.GcpResourceTypes.GcpResourceType
import org.broadinstitute.dsde.rawls.model.IamRoles.IamRole
import org.joda.time.DateTime

import java.sql.Timestamp

/**
  * Created by tlangs on 3/16/2023.
  */

case class FastPassGrant(
  id: Long,
  workspaceId: String,
  userSubjectId: RawlsUserSubjectId,
  resourceType: GcpResourceType,
  resourceName: String,
  roleName: IamRole,
  expiration: DateTime,
  created: DateTime
)

object IamRoles {

  sealed trait IamRole extends RawlsEnumeration[IamRole] {
    override def withName(name: String) = IamRoles.withName(name)
    override def toString = getClass.getSimpleName.stripSuffix("$")
    def toName(iamRole: IamRole) = IamRoles.toName(iamRole)
  }

  // Roles on Projects
  case object RequesterPays extends IamRole
  case object TerraBillingProjectOwner extends IamRole
  case object TerraWorkspaceCanCompute extends IamRole
  case object TerraWorkspaceNextflow extends IamRole

  // Roles on buckets
  case object TerraBucketReader extends IamRole
  case object TerraBucketWriter extends IamRole

  def withName(name: String): IamRole =
    name match {
      case "RequesterPays"                 => RequesterPays
      case "terra_billing_project_owner"   => TerraBillingProjectOwner
      case "terra_workspace_can_compute"   => TerraWorkspaceCanCompute
      case "terra_workspace_nextflow_role" => TerraWorkspaceNextflow
      case "terraBucketReader"             => TerraBucketReader
      case "terraBucketWriter"             => TerraBucketWriter
      case _                               => throw new RawlsException(s"invalid IamRole [$name]")
    }

  def toName(iamRole: IamRole): String =
    iamRole match {
      case RequesterPays            => "RequesterPays"
      case TerraBillingProjectOwner => "terra_billing_project_owner"
      case TerraWorkspaceCanCompute => "terra_workspace_can_compute"
      case TerraWorkspaceNextflow   => "terra_workspace_nextflow_role"
      case TerraBucketReader        => "terraBucketReader"
      case TerraBucketWriter        => "terraBucketWriter"
    }
}

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
