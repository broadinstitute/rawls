package org.broadinstitute.dsde.rawls.dataaccess

import akka.actor.ActorRef
import com.google.api.client.auth.oauth2.Credential
import com.google.api.client.googleapis.testing.auth.oauth2.MockGoogleCredential
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.model.WorkspaceAccessLevels.WorkspaceAccessLevel
import org.joda.time.DateTime
import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

class MockGoogleServicesDAO(groupsPrefix: String) extends GoogleServicesDAO(groupsPrefix) {

  private var token: String = null
  private var tokenDate: DateTime = null

  private val groups: TrieMap[RawlsGroupRef, Set[Either[RawlsUser, RawlsGroup]]] = TrieMap()

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

  override def getUserCredentials(rawlsUserRef: RawlsUserRef): Future[Option[Credential]] = {
    Future.successful(Option(new MockGoogleCredential.Builder().build()))
  }

  override def getToken(rawlsUserRef: RawlsUserRef): Future[Option[String]] = {
    Future.successful(Option(token))
  }

  val mockPermissions: Map[String, WorkspaceAccessLevel] = Map(
    "test@broadinstitute.org" -> WorkspaceAccessLevels.Owner,
    "test_token" -> WorkspaceAccessLevels.Owner,
    "owner-access" -> WorkspaceAccessLevels.Owner,
    "write-access" -> WorkspaceAccessLevels.Write,
    "read-access" -> WorkspaceAccessLevels.Read,
    "no-access" -> WorkspaceAccessLevels.NoAccess
  )

  private def getAccessLevelOrDieTrying(userId: String) = {
    mockPermissions get userId getOrElse {
      throw new RuntimeException(s"Need to add ${userId} to MockGoogleServicesDAO.mockPermissions map")
    }
  }

  var mockProxyGroups = mutable.Map[RawlsUser, Boolean]()

  override def setupWorkspace(userInfo: UserInfo, projectId: String, workspaceId: String, workspaceName: WorkspaceName): Future[Unit] = Future.successful(Unit)

  override def createCromwellAuthBucket(billingProject: RawlsBillingProjectName): Future[String] = Future.successful("mockBucket")

  override def deleteWorkspace(bucketName: String, monitorRef: ActorRef) = Future.successful(Unit)

  override def deleteBucket(bucketName: String, monitorRef: ActorRef) = Future.successful(Unit)

  override def getACL(workspaceId: String) = Future.successful(WorkspaceACL(mockPermissions))

  override def updateACL(currentUser: UserInfo, workspaceId: String, aclUpdates: Map[Either[RawlsUser, RawlsGroup], WorkspaceAccessLevel]): Future[Option[Seq[ErrorReport]]] = Future.successful(None)

  override def getMaximumAccessLevel(userId: String, workspaceId: String) = Future.successful(getAccessLevelOrDieTrying(userId))

  val adminList = scala.collection.mutable.Set("test_token")

  override def isAdmin(userId: String): Future[Boolean] = Future.successful(adminList.contains(userId))

  override def addAdmin(userId: String): Future[Unit] = Future.successful(adminList += userId)

  override def deleteAdmin(userId: String): Future[Unit] = Future.successful(adminList -= userId)

  override def listAdmins(): Future[Seq[String]] = Future.successful(adminList.toSeq)

  override def createProxyGroup(user: RawlsUser): Future[Unit] = {
    mockProxyGroups += (user -> false)
    Future.successful(Unit)
  }

  def containsProxyGroup(user: RawlsUser) = mockProxyGroups.keySet.contains(user)

  override def addUserToProxyGroup(user: RawlsUser): Future[Unit] = Future.successful(mockProxyGroups += (user -> true))

  override def removeUserFromProxyGroup(user: RawlsUser): Future[Unit] = Future.successful(mockProxyGroups += (user -> false))

  override def isUserInProxyGroup(user: RawlsUser): Future[Boolean] = Future.successful(mockProxyGroups.getOrElse(user, false))

  override def createGoogleGroup(groupRef: RawlsGroupRef): Future[Unit] = Future {
    groups.putIfAbsent(groupRef, Set()) match {
      case Some(_) => throw new RuntimeException(s"group $groupRef already exists")
      case None =>
    }
  }

  override def deleteGoogleGroup(groupRef: RawlsGroupRef): Future[Unit] = Future {
    groups.remove(groupRef)
  }

  override def addMemberToGoogleGroup(groupRef: RawlsGroupRef, member: Either[RawlsUser, RawlsGroup]) = Future {
    groups.get(groupRef) match {
      case Some(members) => groups.update(groupRef, members + member)
      case None => throw new RuntimeException(s"group $groupRef does not exists")
    }
  }

  override def removeMemberFromGoogleGroup(groupRef: RawlsGroupRef, member: Either[RawlsUser, RawlsGroup]) = Future {
    groups.get(groupRef) match {
      case Some(members) => groups.update(groupRef, members - member)
      case None => throw new RuntimeException(s"group $groupRef does not exists")
    }
  }

  override def listGroupMembers(groupRef: RawlsGroupRef): Future[Option[Set[Either[RawlsUserRef, RawlsGroupRef]]]] = Future {
    groups.get(groupRef) map ( _.map {
      case Left(user) => Left(RawlsUser.toRef(user))
      case Right(group) => Right(RawlsGroup.toRef(group))
    })
  }

  override def toGoogleGroupName(groupName: RawlsGroupName): String = s"GROUP_${groupName.value}@dev.firecloud.org"

  override def toProxyFromUser(userSubjectId: RawlsUserSubjectId): String = s"PROXY_${userSubjectId}"

  override def toUserFromProxy(proxy: String): String = "joe.biden@whitehouse.gov"
}
