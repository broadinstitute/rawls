package org.broadinstitute.dsde.rawls.dataaccess

import org.broadinstitute.dsde.rawls.model.WorkspaceAccessLevel._
import org.broadinstitute.dsde.rawls.model.{WorkspaceACLUpdate, WorkspaceACL, WorkspaceName}

trait GoogleCloudStorageDAO {
  def getRawlsRedirectURI(callbackPath: String): String

  def getGoogleRedirectURI(userId: String, callbackPath: String): String

  def storeUser(userId: String, authCode: String, state: String, callbackPath: String): Unit

  def createBucket(ownerId: String, projectId: String, bucketName: String): Unit

  def deleteBucket(ownerId: String, projectId: String, bucketName: String): Unit

  def setupACL(ownerId: String, bucketName: String, workspaceName: WorkspaceName): Unit

  def teardownACL(ownerId: String, bucketName: String, workspaceName: WorkspaceName): Unit

  def getACL(bucketName: String, workspaceName: WorkspaceName): WorkspaceACL

  def updateACL(bucketName: String, workspaceName: WorkspaceName, aclUpdates: Seq[WorkspaceACLUpdate]): Map[String, String]

  def getOwners(workspaceName: WorkspaceName): Seq[String]

  def getMaximumAccessLevel(userId: String, workspaceName: WorkspaceName): WorkspaceAccessLevel

  def getWorkspaces(userId: String): Seq[(WorkspaceName, WorkspaceAccessLevel)]

}
