package org.broadinstitute.dsde.rawls.dataaccess

import akka.actor.ActorRef
import com.google.api.client.auth.oauth2.Credential
import com.google.api.client.googleapis.testing.auth.oauth2.MockGoogleCredential
import com.google.api.services.admin.directory.model.Group
import com.google.api.services.storage.model.{BucketAccessControl, Bucket}
import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.model.WorkspaceAccessLevels._
import org.joda.time.DateTime
import spray.http.OAuth2BearerToken
import spray.json.JsObject
import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Try

class MockGoogleServicesDAO(groupsPrefix: String) extends GoogleServicesDAO(groupsPrefix) {

  val billingEmail: String = "billing@test.firecloud.org"
  private var token: String = null
  private var tokenDate: DateTime = null

  private val groups: TrieMap[RawlsGroupRef, Set[Either[RawlsUser, RawlsGroup]]] = TrieMap()

  val accessibleBillingAccountName = RawlsBillingAccountName("billingAccounts/firecloudHasThisOne")
  val inaccessibleBillingAccountName = RawlsBillingAccountName("billingAccounts/firecloudDoesntHaveThisOne")

  override def listBillingAccounts(userInfo: UserInfo): Future[Seq[RawlsBillingAccount]] = {
    val firecloudHasThisOne = RawlsBillingAccount(accessibleBillingAccountName, true, "testBillingAccount")
    val firecloudDoesntHaveThisOne = RawlsBillingAccount(inaccessibleBillingAccountName, false, "testBillingAccount")
    Future.successful(Seq(firecloudHasThisOne, firecloudDoesntHaveThisOne))
  }

  override def storeToken(userInfo: UserInfo, refreshToken: String): Future[Unit] = {
    this.token = refreshToken
    this.tokenDate = DateTime.now
    Future.successful(Unit)
  }

  override def getTokenDate(rawlsUserRef: RawlsUserRef): Future[Option[DateTime]] = {
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

  override def setupWorkspace(userInfo: UserInfo, project: RawlsBillingProject, workspaceId: String, workspaceName: WorkspaceName, realm: Option[RawlsGroupRef], realmProjectOwnerIntersection: Option[Set[RawlsUserRef]]): Future[GoogleWorkspaceInfo] = {

    def workspaceAccessGroup(workspaceId: String, accessLevel: WorkspaceAccessLevel, users: Set[RawlsUserRef]) = {
      RawlsGroup(RawlsGroupName(s"fc-${workspaceId}-${accessLevel.toString}"), RawlsGroupEmail(s"$accessLevel@$workspaceId"), users, Set.empty)
    }

    def intersectionGroup(workspaceId: String, realm: String, accessLevel: WorkspaceAccessLevel, users: Set[RawlsUserRef]) = {
      RawlsGroup(RawlsGroupName(s"fc-${realm}-${workspaceId}-${accessLevel.toString}"), RawlsGroupEmail(s"$realm-$accessLevel@$workspaceId"), users, Set.empty)
    }

    val accessGroups: Map[WorkspaceAccessLevel, RawlsGroup] = groupAccessLevelsAscending.map { accessLevel =>
      accessLevel -> (accessLevel match {
        case WorkspaceAccessLevels.Owner => workspaceAccessGroup(workspaceId, accessLevel, Set(RawlsUser(userInfo)))
        case WorkspaceAccessLevels.ProjectOwner => project.groups(ProjectRoles.Owner)
        case _ => workspaceAccessGroup(workspaceId, accessLevel, Set.empty)
      })
    }.toMap

    val intersectionGroups: Option[Map[WorkspaceAccessLevel, RawlsGroup]] = realm map { realmGroupRef =>
      groupAccessLevelsAscending.map { accessLevel =>
        val users: Set[RawlsUserRef] = accessLevel match {
          case WorkspaceAccessLevels.Owner => Set(RawlsUser(userInfo))
          case WorkspaceAccessLevels.ProjectOwner => realmProjectOwnerIntersection.getOrElse(Set.empty)
          case _ => Set.empty
        }
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

  val adminList = scala.collection.mutable.Set("owner-access")
  val curatorList = scala.collection.mutable.Set("owner-access")

  val googleGroups = TrieMap(
    "fc-ADMINS@dev.test.firecloud.org" -> adminList,
    "fc-CURATORS@dev.test.firecloud.org" -> curatorList
  )

  override def isAdmin(userEmail: String): Future[Boolean] = hasGoogleRole("fc-ADMINS@dev.test.firecloud.org", userEmail)

  def removeAdmin(userEmail: String): Future[Unit] = {
    if(adminList.contains(userEmail)) {
      adminList -= userEmail
      Future.successful(())
    }
    else Future.failed(new RawlsException("Unable to remove user"))
  }

  override def isLibraryCurator(userEmail: String): Future[Boolean] = hasGoogleRole("fc-CURATORS@dev.test.firecloud.org", userEmail)

  override def hasGoogleRole(roleGroupName: String, userEmail: String): Future[Boolean] = Future.successful(googleGroups(roleGroupName).contains(userEmail))

  override def addLibraryCurator(userEmail: String): Future[Unit] = {
    curatorList += userEmail
    Future.successful(())
  }

  override def removeLibraryCurator(userEmail: String): Future[Unit] = {
    if(curatorList.contains(userEmail)) {
      curatorList -= userEmail
      Future.successful(())
    }
    else Future.failed(new RawlsException("Unable to remove user"))
  }

  override def createProxyGroup(user: RawlsUser): Future[Unit] = {
    mockProxyGroups += (user -> false)
    Future.successful(())
  }

  override def deleteProxyGroup(user: RawlsUser): Future[Unit] = {
    mockProxyGroups -= user
    Future.successful(())
  }

  def containsProxyGroup(user: RawlsUser) = mockProxyGroups.keySet.contains(user)

  override def addUserToProxyGroup(user: RawlsUser): Future[Unit] = Future.successful(mockProxyGroups += (user -> true))

  override def removeUserFromProxyGroup(user: RawlsUser): Future[Unit] = Future.successful(mockProxyGroups += (user -> false))

  override def isUserInProxyGroup(user: RawlsUser): Future[Boolean] = Future.successful(mockProxyGroups.getOrElse(user, false))

  override def createGoogleGroup(groupRef: RawlsGroupRef): Future[RawlsGroup] = Future {
    groups.putIfAbsent(groupRef, Set()) match {
      case Some(_) => throw new RuntimeException(s"group $groupRef already exists")
      case None =>
        val group = RawlsGroup(groupRef.groupName, RawlsGroupEmail(toGoogleGroupName(groupRef.groupName)), Set.empty[RawlsUserRef], Set.empty[RawlsGroupRef])
        googleGroups.put(group.groupEmail.value, scala.collection.mutable.Set.empty)
        group

    }
  }
  override def isEmailInGoogleGroup(email: String, groupName: String): Future[Boolean] = Future.successful(true)

  override def getGoogleGroup(groupName: String): Future[Option[Group]] = Future.successful(Some(new Group))

  def getBucketUsage(projectName: RawlsBillingProjectName, bucketName: String, maxResults: Option[Long]): Future[BigInt] = Future.successful(42)

  override def deleteGoogleGroup(group: RawlsGroup): Future[Unit] = Future {
    googleGroups.remove(group.groupEmail.value)
    groups.remove(group)
  }

  override def addEmailToGoogleGroup(groupEmail: String, emailToAdd: String): Future[Unit] = {
    googleGroups(groupEmail) += emailToAdd
    Future.successful(())
  }

  override def addMemberToGoogleGroup(group: RawlsGroup, member: Either[RawlsUser, RawlsGroup]) = Future {
    groups.get(group) match {
      case Some(members) =>
        groups.update(group, members + member)
        googleGroups(group.groupEmail.value) += (member match {
          case Left(user) => toProxyFromUser(user.userSubjectId)
          case Right(group) => toGoogleGroupName(group.groupName)
        })
      case None => throw new RuntimeException(s"group $group does not exist")
    }
  }

  override def removeEmailFromGoogleGroup(groupEmail: String, emailToRemove: String): Future[Unit] = {
    googleGroups(groupEmail) -= emailToRemove
    Future.successful(())
  }

  override def removeMemberFromGoogleGroup(group: RawlsGroup, member: Either[RawlsUser, RawlsGroup]) = Future {
    groups.get(group) match {
      case Some(members) =>
        groups.update(group, members - member)
        googleGroups(group.groupEmail.value) -= (member match {
          case Left(user) => toProxyFromUser(user.userSubjectId)
          case Right(group) => toGoogleGroupName(group.groupName)
        })
      case None => throw new RuntimeException(s"group $group does not exist")
    }
  }

  override def listGroupMembers(group: RawlsGroup): Future[Option[Map[String, Option[Either[RawlsUserRef, RawlsGroupRef]]]]] = Future {
    groups.get(group) map ( _.map {
      case Left(user) => user.userEmail.value -> Option(Left(RawlsUser.toRef(user)))
      case Right(group) => group.groupEmail.value -> Option(Right(RawlsGroup.toRef(group)))
    } toMap)
  }

  def toGoogleGroupName(groupName: RawlsGroupName): String = s"GROUP_${groupName.value}@dev.firecloud.org"

  override def toProxyFromUser(userSubjectId: RawlsUserSubjectId): String = s"PROXY_${userSubjectId}"

  override def toUserFromProxy(proxy: String): String = "joe.biden@whitehouse.gov"

  override def getServiceAccountRawlsUser(): Future[RawlsUser] = Future.successful(RawlsUser(RawlsUserSubjectId("12345678000"), RawlsUserEmail("foo@bar.com")))

  def getServiceAccountUserInfo(): Future[UserInfo] = Future.successful(UserInfo("foo@bar.com", OAuth2BearerToken("test_token"), 0, "12345678000"))

  override def revokeToken(rawlsUserRef: RawlsUserRef): Future[Unit] = Future.successful(Unit)

  override def getGenomicsOperation(jobId: String): Future[Option[JsObject]] = {
    import spray.json._
    Future.successful(Some("""{"foo":"bar"}""".parseJson.asJsObject))
  }

  override def createProject(projectName: RawlsBillingProjectName, billingAccount: RawlsBillingAccount): Future[Unit] = Future.successful(Unit)

  override def deleteProject(projectName: RawlsBillingProjectName): Future[Unit] = Future.successful(Unit)
  override def setupProject(project: RawlsBillingProject, projectTemplate: ProjectTemplate, groupEmailsByRef: Map[RawlsGroupRef, RawlsGroupEmail]): Future[Try[Unit]] = Future.successful {
    project.groups.values.foreach(group => createGoogleGroup(group))
    Try(Unit)
  }
}
