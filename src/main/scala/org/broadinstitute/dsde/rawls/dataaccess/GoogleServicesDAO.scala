package org.broadinstitute.dsde.rawls.dataaccess

import org.broadinstitute.dsde.rawls.model.WorkspaceAccessLevel._
import org.broadinstitute.dsde.rawls.model.{WorkspacePermissionsPair, UserInfo, WorkspaceACLUpdate, WorkspaceACL, WorkspaceName}
import scala.concurrent.Future
import scala.util.Try

trait GoogleServicesDAO {

  // returns a workspaceID
  def createBucket(userInfo: UserInfo, projectId: String, workspaceId: String, workspaceName: WorkspaceName): Future[Unit]

  def deleteBucket(userInfo: UserInfo, workspaceId: String): Future[Any]

  def getACL(workspaceId: String): Future[WorkspaceACL]

  def updateACL(workspaceId: String, aclUpdates: Seq[WorkspaceACLUpdate]): Future[Map[String, String]]

  def getOwners(workspaceId: String): Future[Seq[String]]

  def getMaximumAccessLevel(userId: String, workspaceId: String): Future[WorkspaceAccessLevel]

  def getWorkspaces(userId: String): Future[Seq[WorkspacePermissionsPair]]

  def getBucketName(workspaceId: String): String

  def isAdmin(userId: String): Future[Boolean]

  def addAdmin(userId: String): Future[Unit]

  def deleteAdmin(userId: String): Future[Unit]

  def listAdmins(): Future[Seq[String]]
}
