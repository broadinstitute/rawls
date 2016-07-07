package org.broadinstitute.dsde.rawls.dataaccess

import akka.actor.ActorRef
import com.google.api.client.auth.oauth2.Credential
import com.google.api.client.googleapis.testing.auth.oauth2.MockGoogleCredential
import com.google.api.services.admin.directory.model.Group
import com.google.api.services.storage.model.{BucketAccessControl, Bucket}
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.model.WorkspaceAccessLevels._
import org.joda.time.DateTime
import spray.http.OAuth2BearerToken
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

  override def deleteToken(rawlsUserRef: RawlsUserRef): Future[Unit] = {
    token = null
    tokenDate = null
    Future.successful(Unit)
  }

  override def getUserCredentials(rawlsUserRef: RawlsUserRef): Future[Option[Credential]] = {
    Future.successful(Option(getPreparedMockGoogleCredential()))
  }

  def getPreparedMockGoogleCredential(): MockGoogleCredential = {
    val credential = new MockGoogleCredential.Builder().build()
    credential.setAccessToken(MockGoogleCredential.ACCESS_TOKEN)
    credential.setRefreshToken(token)
    credential.setExpiresInSeconds(1000000L) // make sure not to refresh this token
    credential
  }

  override def getBucketServiceAccountCredential: Credential = getPreparedMockGoogleCredential()

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

  override def setupWorkspace(userInfo: UserInfo, projectId: String, workspaceId: String, workspaceName: WorkspaceName, realm: Option[RawlsGroupRef]): Future[GoogleWorkspaceInfo] = {

    def workspaceAccessGroup(workspaceId: String, accessLevel: WorkspaceAccessLevel, users: Set[RawlsUserRef]) = {
      RawlsGroup(RawlsGroupName(s"fc-${workspaceId}-${accessLevel.toString}"), RawlsGroupEmail(s"$accessLevel@$workspaceId"), users, Set.empty)
    }

    def intersectionGroup(workspaceId: String, realm: String, accessLevel: WorkspaceAccessLevel, users: Set[RawlsUserRef]) = {
      RawlsGroup(RawlsGroupName(s"fc-${realm}-${workspaceId}-${accessLevel.toString}"), RawlsGroupEmail(s"$realm-$accessLevel@$workspaceId"), users, Set.empty)
    }

    val accessGroups: Map[WorkspaceAccessLevel, RawlsGroup] = groupAccessLevelsAscending.map { accessLevel =>
      val users: Set[RawlsUserRef] = if (accessLevel == WorkspaceAccessLevels.Owner) Set(RawlsUser(userInfo)) else Set.empty
      accessLevel -> workspaceAccessGroup(workspaceId, accessLevel, users)
    }.toMap

    val intersectionGroups: Option[Map[WorkspaceAccessLevel, RawlsGroup]] = realm map { realmGroupRef =>
      groupAccessLevelsAscending.map { accessLevel =>
        val users: Set[RawlsUserRef] = if (accessLevel == WorkspaceAccessLevels.Owner) Set(RawlsUser(userInfo)) else Set.empty
        accessLevel -> intersectionGroup(workspaceId, realmGroupRef.groupName.value, accessLevel, users)
      }.toMap
    }

    val googleWorkspaceInfo: GoogleWorkspaceInfo = GoogleWorkspaceInfo(s"fc-$workspaceId", accessGroups, intersectionGroups)
    googleWorkspaceInfo.accessGroupsByLevel.values.foreach(createGoogleGroup(_))
    googleWorkspaceInfo.intersectionGroupsByLevel.map(_.values.foreach(createGoogleGroup(_)))
    Future.successful(googleWorkspaceInfo)
  }

  override def createCromwellAuthBucket(billingProject: RawlsBillingProjectName): Future[String] = Future.successful("mockBucket")

  override def deleteBucket(bucketName: String, monitorRef: ActorRef) = Future.successful(Unit)

  override def getBucket(bucketName: String): Future[Option[Bucket]] = Future.successful(Some(new Bucket))

  override def getBucketACL(bucketName: String): Future[Option[List[BucketAccessControl]]] = Future.successful(Some(List.fill(5)(new BucketAccessControl)))

  override def diagnosticBucketWrite(user: RawlsUser, bucketName: String) = Future.successful(None)

  override def diagnosticBucketRead(userInfo: UserInfo, bucketName: String) = Future.successful(None)

  val adminList = scala.collection.mutable.Set("test_token")

  override def isAdmin(userId: String): Future[Boolean] = Future.successful(adminList.contains(userId))

  override def createProxyGroup(user: RawlsUser): Future[Unit] = {
    mockProxyGroups += (user -> false)
    Future.successful(Unit)
  }

  override def deleteProxyGroup(user: RawlsUser): Future[Unit] = {
    mockProxyGroups -= user
    Future.successful(Unit)
  }

  def containsProxyGroup(user: RawlsUser) = mockProxyGroups.keySet.contains(user)

  override def addUserToProxyGroup(user: RawlsUser): Future[Unit] = Future.successful(mockProxyGroups += (user -> true))

  override def removeUserFromProxyGroup(user: RawlsUser): Future[Unit] = Future.successful(mockProxyGroups += (user -> false))

  override def isUserInProxyGroup(user: RawlsUser): Future[Boolean] = Future.successful(mockProxyGroups.getOrElse(user, false))

  override def createGoogleGroup(groupRef: RawlsGroupRef): Future[RawlsGroup] = Future {
    groups.putIfAbsent(groupRef, Set()) match {
      case Some(_) => throw new RuntimeException(s"group $groupRef already exists")
      case None => RawlsGroup(groupRef.groupName, RawlsGroupEmail(toGoogleGroupName(groupRef.groupName)), Set.empty[RawlsUserRef], Set.empty[RawlsGroupRef])
    }
  }
  override def isEmailInGoogleGroup(email: String, groupName: String): Future[Boolean] = Future.successful(true)

  override def getGoogleGroup(groupName: String): Future[Option[Group]] = Future.successful(Some(new Group))

  override def deleteGoogleGroup(group: RawlsGroup): Future[Unit] = Future {
    groups.remove(group)
  }

  override def addMemberToGoogleGroup(group: RawlsGroup, member: Either[RawlsUser, RawlsGroup]) = Future {
    groups.get(group) match {
      case Some(members) => groups.update(group, members + member)
      case None => throw new RuntimeException(s"group $group does not exist")
    }
  }

  override def removeMemberFromGoogleGroup(group: RawlsGroup, member: Either[RawlsUser, RawlsGroup]) = Future {
    groups.get(group) match {
      case Some(members) => groups.update(group, members - member)
      case None => throw new RuntimeException(s"group $group does not exist")
    }
  }

  override def listGroupMembers(group: RawlsGroup): Future[Option[Set[Either[RawlsUserRef, RawlsGroupRef]]]] = Future {
    groups.get(group) map ( _.map {
      case Left(user) => Left(RawlsUser.toRef(user))
      case Right(group) => Right(RawlsGroup.toRef(group))
    })
  }

  def toGoogleGroupName(groupName: RawlsGroupName): String = s"GROUP_${groupName.value}@dev.firecloud.org"

  override def toProxyFromUser(userSubjectId: RawlsUserSubjectId): String = s"PROXY_${userSubjectId}"

  override def toUserFromProxy(proxy: String): String = "joe.biden@whitehouse.gov"

  override def getServiceAccountRawlsUser(): Future[RawlsUser] = Future.successful(RawlsUser(RawlsUserSubjectId("12345678000"), RawlsUserEmail("foo@bar.com")))

  def getServiceAccountUserInfo(): Future[UserInfo] = Future.successful(UserInfo("foo@bar.com", OAuth2BearerToken("test_token"), 0, "12345678000"))

  override def revokeToken(rawlsUserRef: RawlsUserRef): Future[Unit] = Future.successful(Unit)
}
