package org.broadinstitute.dsde.rawls.dataaccess

import com.google.api.services.admin.directory.model.Group
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.model.WorkspaceAccessLevel.WorkspaceAccessLevel
import WorkspaceACLJsonSupport._
import org.joda.time.DateTime
import spray.json._
import scala.concurrent.Future
import scala.util.Try

class MockGoogleServicesDAO extends GoogleServicesDAO {

  private var token: String = null
  private var tokenDate: DateTime = null

  override def storeToken(userInfo: UserInfo, refreshToken: String): Future[Unit] = {
    this.token = refreshToken
    this.tokenDate = DateTime.now
    Future.successful(Unit)
  }

  override def getTokenDate(userInfo: UserInfo): Future[Option[DateTime]] = {
    Future.successful(Option(tokenDate))
  }

  override def deleteToken(userInfo: UserInfo): Future[Unit] = {
    token = null
    tokenDate = null
    Future.successful(Unit)
  }

  override def getToken(userInfo: UserInfo): Future[Option[String]] = {
    Future.successful(Option(token))
  }

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

  override def createBucket(userInfo: UserInfo, projectId: String, workspaceId: String, workspaceName: WorkspaceName): Future[Unit] = Future.successful(Unit)

  override def deleteBucket(userInfo: UserInfo, workspaceId: String) = Future.successful(Unit)

  override def getACL(workspaceId: String) = Future.successful(WorkspaceACL(mockPermissions))

  override def updateACL(userEmail: String, workspaceId: String, aclUpdates: Seq[WorkspaceACLUpdate]) = Future.successful(None)

  override def getOwners(workspaceId: String) = Future.successful(mockPermissions.filter(_._2 == WorkspaceAccessLevel.Owner).keys.toSeq)

  override def getMaximumAccessLevel(userId: String, workspaceId: String) = Future.successful(getAccessLevelOrDieTrying(userId))

  override def getWorkspaces(userId: String) = Future.successful(
    Seq(
      WorkspacePermissionsPair("workspaceId1", WorkspaceAccessLevel.Owner),
      WorkspacePermissionsPair("workspaceId2", WorkspaceAccessLevel.Write),
      WorkspacePermissionsPair("workspaceId3", WorkspaceAccessLevel.Read)
    )
  )

  override def getBucketName(workspaceId: String) = s"rawls-${workspaceId}"

  val adminList = scala.collection.mutable.Set("test_token")

  override def isAdmin(userId: String): Future[Boolean] = Future.successful(adminList.contains(userId))

  override def addAdmin(userId: String): Future[Unit] = Future.successful(adminList += userId)

  override def deleteAdmin(userId: String): Future[Unit] = Future.successful(adminList -= userId)

  override def listAdmins(): Future[Seq[String]] = Future.successful(adminList.toSeq)

  override def createProxyGroup(userInfo: UserInfo): Future[Unit] = Future.successful(Unit)
}
