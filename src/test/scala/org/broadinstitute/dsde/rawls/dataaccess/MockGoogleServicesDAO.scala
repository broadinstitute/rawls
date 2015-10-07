package org.broadinstitute.dsde.rawls.dataaccess

import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.model.WorkspaceAccessLevel.WorkspaceAccessLevel
import WorkspaceACLJsonSupport._
import spray.json._

object MockGoogleServicesDAO extends GoogleServicesDAO {

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
      throw new RuntimeException(s"Need to add ${userId} to MockGoogleServicesDAO.mockPermissions map")
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
      WorkspacePermissionsPair("workspaceId1", WorkspaceAccessLevel.Owner),
      WorkspacePermissionsPair("workspaceId2", WorkspaceAccessLevel.Write),
      WorkspacePermissionsPair("workspaceId3", WorkspaceAccessLevel.Read)
    )
  }

  override def getBucketName(workspaceId: String) = s"rawls-${workspaceId}"

  val adminList = scala.collection.mutable.Set("test_token")

  override def isAdmin(userId: String): Boolean = { adminList.contains(userId) }

  override def addAdmin(userId: String): Unit = { adminList += userId }

  override def deleteAdmin(userId: String): Unit = { adminList -= userId }

  override def listAdmins(): Seq[String] = { adminList.toSeq }
}
