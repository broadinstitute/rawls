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
      throw new RuntimeException("Need to add %s to MockGoogleCloudStorageDAO.mockPermissions map".format(userId))
    }
  }

  override def getRawlsRedirectURI(callbackPath: String): String = "http://localhost.broadinstitute.org:8080/"+callbackPath

  override def getGoogleRedirectURI(userId: String, callbackPath: String): String = "https://google.com/other_stuff"

  override def storeUser(userId: String, authCode: String, state: String, callbackPath: String): Unit = {}

  override def createBucket(ownerId: String, projectId: String, bucketName: String): Unit = {}

  override def deleteBucket(ownerId: String, projectId: String, bucketName: String): Unit = {}

  override def setupACL(ownerId: String, bucketName: String, workspaceName: WorkspaceName): Unit = {}

  override def teardownACL(ownerId: String, bucketName: String, workspaceName: WorkspaceName): Unit = {}

  override def getACL(bucketName: String, workspaceName: WorkspaceName): WorkspaceACL = {
    WorkspaceACL(mockPermissions)
  }

  override def updateACL(bucketName: String, workspaceName: WorkspaceName, aclUpdates: Seq[WorkspaceACLUpdate]) = Map.empty

  override def getMaximumAccessLevel(userId: String, workspaceName: WorkspaceName): WorkspaceAccessLevel = {
    getAccessLevelOrDieTrying(userId)
  }

  override def getWorkspaces(userId: String): Seq[(WorkspaceName, WorkspaceAccessLevel)] = {
    Seq(
      (WorkspaceName("ns", "owner"), WorkspaceAccessLevel.Owner),
      (WorkspaceName("ns", "writer"), WorkspaceAccessLevel.Write),
      (WorkspaceName("ns", "reader"), WorkspaceAccessLevel.Read)
    )
  }

  override def getOwners(workspaceName: WorkspaceName): Seq[String] = mockPermissions.filter(_._2 == WorkspaceAccessLevel.Owner).keys.toSeq
}
