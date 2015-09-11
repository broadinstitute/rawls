package org.broadinstitute.dsde.rawls.dataaccess

import org.broadinstitute.dsde.rawls.model.WorkspaceAccessLevel._
import org.broadinstitute.dsde.rawls.model.{WorkspacePermissionsPair, UserInfo, WorkspaceACLUpdate, WorkspaceACL, WorkspaceName}
import scala.util.Try

trait GoogleCloudStorageDAO {

  // returns a workspaceID
  def createBucket(userInfo: UserInfo, projectId: String, workspaceId: String): Unit

  def deleteBucket(userInfo: UserInfo, workspaceId: String): Unit

  def createACLGroups(userInfo: UserInfo, workspaceId: String, workspaceName: WorkspaceName): Unit

  def deleteACLGroups(workspaceId: String): Unit

  def getACL(workspaceId: String): WorkspaceACL

  def updateACL(workspaceId: String, aclUpdates: Seq[WorkspaceACLUpdate]): Map[String, String]

  def getOwners(workspaceId: String): Seq[String]

  def getMaximumAccessLevel(userId: String, workspaceId: String): WorkspaceAccessLevel

  def getWorkspaces(userId: String): Seq[WorkspacePermissionsPair]

  def getBucketName(workspaceId: String): String
}
