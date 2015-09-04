package org.broadinstitute.dsde.rawls.dataaccess

import org.broadinstitute.dsde.rawls.model.WorkspaceAccessLevel._
import org.broadinstitute.dsde.rawls.model.{WorkspacePermissionsPair, UserInfo, WorkspaceACLUpdate, WorkspaceACL, WorkspaceName}

trait GoogleCloudStorageDAO {
  def createBucket(userInfo: UserInfo, projectId: String, bucketName: String): Unit

  def deleteBucket(userInfo: UserInfo, projectId: String, bucketName: String): Unit

  def setupACL(userInfo: UserInfo, bucketName: String, workspaceName: WorkspaceName): Unit

  def teardownACL(bucketName: String, workspaceName: WorkspaceName): Unit

  def getACL(bucketName: String, workspaceName: WorkspaceName): WorkspaceACL

  def updateACL(bucketName: String, workspaceName: WorkspaceName, aclUpdates: Seq[WorkspaceACLUpdate]): Map[String, String]

  def getOwners(workspaceName: WorkspaceName): Seq[String]

  def getMaximumAccessLevel(userId: String, workspaceName: WorkspaceName): WorkspaceAccessLevel

  def getWorkspaces(userId: String): Seq[WorkspacePermissionsPair]

  def getWorkspace(userId: String, workspaceName: WorkspaceName): Seq[WorkspacePermissionsPair]

}
