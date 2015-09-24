package org.broadinstitute.dsde.rawls.dataaccess

import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.model.WorkspaceAccessLevel.WorkspaceAccessLevel
import WorkspaceACLJsonSupport._
import spray.json._

object MockGoogleCloudStorageDAO extends GoogleCloudStorageDAO {

  val mockPermissions: Map[String, WorkspaceAccessLevel] = Map(
    "test@broadinstitute.org" -> WorkspaceAccessLevel.Owner,
    "test_token" -> WorkspaceAccessLevel.Owner,
    "owner-access" -> WorkspaceAccessLevel.Owner,
    "write-access" -> WorkspaceAccessLevel.Write,
    "read-access" -> WorkspaceAccessLevel.Read,
    "no-access" -> WorkspaceAccessLevel.NoAccess
  )

  private def getAccessLevelOrDieTrying(userId: String) = {
    mockPermissions get userId getOrElse {
      throw new RuntimeException(s"Need to add ${userId} to MockGoogleCloudStorageDAO.mockPermissions map")
    }
  }

  override def createBucket(userInfo: UserInfo, projectId: String, workspaceId: String, workspaceName: WorkspaceName): Unit = {}

  override def deleteBucket(userInfo: UserInfo, workspaceId: String): Unit = {}

  override def getACL(workspaceId: String): WorkspaceACL = {
    WorkspaceACL(mockPermissions)
  }

  override def updateACL(workspaceId: String, aclUpdates: Seq[WorkspaceACLUpdate]) = Map.empty

  override def getOwners(workspaceId: String): Seq[String] = mockPermissions.filter(_._2 == WorkspaceAccessLevel.Owner).keys.toSeq

  override def getMaximumAccessLevel(userId: String, workspaceId: String): WorkspaceAccessLevel = {
    getAccessLevelOrDieTrying(userId)
  }

  override def getWorkspaces(userId: String): Seq[WorkspacePermissionsPair] = {
    Seq(
      WorkspacePermissionsPair("bucket1", WorkspaceAccessLevel.Owner),
      WorkspacePermissionsPair("bucket2", WorkspaceAccessLevel.Write),
      WorkspacePermissionsPair("bucket3", WorkspaceAccessLevel.Read)
    )
  }

  override def getBucketName(workspaceId: String) = s"rawls-${workspaceId}"
}
