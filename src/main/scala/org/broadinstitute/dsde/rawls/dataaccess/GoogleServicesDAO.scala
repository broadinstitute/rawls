package org.broadinstitute.dsde.rawls.dataaccess

import org.broadinstitute.dsde.rawls.model.WorkspaceAccessLevel._
import org.broadinstitute.dsde.rawls.model.{WorkspacePermissionsPair, UserInfo, WorkspaceACLUpdate, WorkspaceACL, WorkspaceName}
import scala.util.Try

trait GoogleServicesDAO {

  // returns a workspaceID
  def createBucket(userInfo: UserInfo, projectId: String, workspaceId: String, workspaceName: WorkspaceName): Unit

  def deleteBucket(userInfo: UserInfo, workspaceId: String): Unit

  def getACL(workspaceId: String): WorkspaceACL

  def updateACL(workspaceId: String, aclUpdates: Seq[WorkspaceACLUpdate]): Map[String, String]

  def getOwners(workspaceId: String): Seq[String]

  def getMaximumAccessLevel(userId: String, workspaceId: String): WorkspaceAccessLevel

  def getWorkspaces(userId: String): Seq[WorkspacePermissionsPair]

  def getBucketName(workspaceId: String): String

  def isAdmin(userId: String): Boolean

  def addAdmin(userId: String): Unit

  def deleteAdmin(userId: String): Unit

  def listAdmins(): Seq[String]
}
