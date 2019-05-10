package org.broadinstitute.dsde.rawls.dataaccess

import java.util.UUID

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import cats.effect.IO
import com.google.api.client.auth.oauth2.Credential
import com.google.api.client.googleapis.testing.auth.oauth2.MockGoogleCredential
import com.google.api.services.admin.directory.model.Group
import com.google.api.services.cloudresourcemanager.model.Project
import com.google.api.services.genomics.v2alpha1.model.Operation
import com.google.api.services.storage.model.{Bucket, BucketAccessControl, StorageObject}
import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.dataaccess.slick.RawlsBillingProjectOperationRecord
import org.broadinstitute.dsde.rawls.model.WorkspaceAccessLevels._
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.workbench.google2.GcsBlobName
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.joda.time.DateTime
import spray.json._

import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Success, Try}

class MockGoogleServicesDAO(groupsPrefix: String) extends GoogleServicesDAO(groupsPrefix) {

  val billingEmail: String = "billing@test.firecloud.org"
  private var token: String = null
  private var tokenDate: DateTime = null

  private val groups: TrieMap[RawlsGroupRef, Set[Either[RawlsUser, RawlsGroup]]] = TrieMap()
  val policies: TrieMap[RawlsBillingProjectName, Map[String, List[String]]] = TrieMap()

  val accessibleBillingAccountName = RawlsBillingAccountName("billingAccounts/firecloudHasThisOne")
  val inaccessibleBillingAccountName = RawlsBillingAccountName("billingAccounts/firecloudDoesntHaveThisOne")

  val mockJobIds = Seq("operations/dummy-job-id", "projects/dummy-project/operations/dummy-job-id")

  override def listBillingAccounts(userInfo: UserInfo): Future[Seq[RawlsBillingAccount]] = {
    val firecloudHasThisOne = RawlsBillingAccount(accessibleBillingAccountName, true, "testBillingAccount")
    val firecloudDoesntHaveThisOne = RawlsBillingAccount(inaccessibleBillingAccountName, false, "testBillingAccount")
    Future.successful(Seq(firecloudHasThisOne, firecloudDoesntHaveThisOne))
  }

  override def listBillingAccountsUsingServiceCredential(implicit executionContext: ExecutionContext): Future[Seq[RawlsBillingAccount]] = {
    val firecloudHasThisOne = RawlsBillingAccount(accessibleBillingAccountName, true, "testBillingAccount")
    Future.successful(Seq(firecloudHasThisOne))
  }

  override def storeToken(userInfo: UserInfo, refreshToken: String): Future[Unit] = {
    this.token = refreshToken
    this.tokenDate = DateTime.now
    Future.successful(())
  }

  override def getTokenDate(rawlsUserRef: RawlsUserRef): Future[Option[DateTime]] = {
    Future.successful(Option(tokenDate))
  }

  override def deleteToken(rawlsUserRef: RawlsUserRef): Future[Unit] = {
    token = null
    tokenDate = null
    Future.successful(())
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

  override def setupWorkspace(userInfo: UserInfo, project: RawlsBillingProject, policyGroupsByAccessLevel: Map[WorkspaceAccessLevel, WorkbenchEmail], bucketName: String, labels: Map[String, String]): Future[GoogleWorkspaceInfo] = {

    val googleWorkspaceInfo: GoogleWorkspaceInfo = GoogleWorkspaceInfo(bucketName, policyGroupsByAccessLevel)
    Future.successful(googleWorkspaceInfo)
  }

  override def getAccessTokenUsingJson(saKey: String): Future[String] = Future.successful("token")
  override def getUserInfoUsingJson(saKey: String): Future[UserInfo] = Future.successful(UserInfo(RawlsUserEmail("foo@bar.com"), OAuth2BearerToken("test_token"), 0, RawlsUserSubjectId("12345678000")))

  override def getGoogleProject(billingProjectName: RawlsBillingProjectName): Future[Project] = Future.successful(new Project())

  override def deleteBucket(bucketName: String) = Future.successful(true)

  override def getBucket(bucketName: String)(implicit executionContext: ExecutionContext): Future[Option[Bucket]] = Future.successful(Some(new Bucket))

  override def getBucketACL(bucketName: String): Future[Option[List[BucketAccessControl]]] = Future.successful(Some(List.fill(5)(new BucketAccessControl)))

  override def diagnosticBucketRead(userInfo: UserInfo, bucketName: String) = Future.successful(None)

  override def listObjectsWithPrefix(bucketName: String, objectNamePrefix: String): Future[List[StorageObject]] = Future.successful(List.empty)

  override def storeCromwellMetadata(objectName: GcsBlobName, body: fs2.Stream[fs2.Pure, Byte]): IO[Unit] = IO.unit

  override def copyFile(sourceBucket: String, sourceObject: String, destinationBucket: String, destinationObject: String): Future[Option[StorageObject]] = Future.successful(None)

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

  def containsProxyGroup(user: RawlsUser) = mockProxyGroups.keySet.contains(user)

  override def getGoogleGroup(groupName: String)(implicit executionContext: ExecutionContext): Future[Option[Group]] = Future.successful(Some(new Group))

  def getBucketUsage(projectName: RawlsBillingProjectName, bucketName: String, maxResults: Option[Long]): Future[BigInt] = Future.successful(42)

  override def addEmailToGoogleGroup(groupEmail: String, emailToAdd: String): Future[Unit] = {
    googleGroups(groupEmail) += emailToAdd
    Future.successful(())
  }

  override def removeEmailFromGoogleGroup(groupEmail: String, emailToRemove: String): Future[Unit] = {
    googleGroups(groupEmail) -= emailToRemove
    Future.successful(())
  }

  def toGoogleGroupName(groupName: RawlsGroupName): String = s"GROUP_${groupName.value}@dev.firecloud.org"

  override def getServiceAccountRawlsUser(): Future[RawlsUser] = Future.successful(RawlsUser(RawlsUserSubjectId("12345678000"), RawlsUserEmail("foo@bar.com")))

  def getServiceAccountUserInfo(): Future[UserInfo] = Future.successful(UserInfo(RawlsUserEmail("foo@bar.com"), OAuth2BearerToken("test_token"), 0, RawlsUserSubjectId("12345678000")))

  override def revokeToken(rawlsUserRef: RawlsUserRef): Future[Unit] = Future.successful(())

  override def getGenomicsOperation(jobId: String): Future[Option[JsObject]] = Future {
    if (mockJobIds.contains(jobId)) {
      Some("""{"foo":"bar"}""".parseJson.asJsObject)
    } else {
      None
    }
  }

  override def checkGenomicsOperationsHealth(implicit executionContext: ExecutionContext): Future[Boolean] = {
    Future.successful(true)
  }

  override def createProject(projectName: RawlsBillingProjectName, billingAccount: RawlsBillingAccount, dmTemplatePath: String, highSecurityNetwork: Boolean, requesterPaysRole: String, ownerGroupEmail: WorkbenchEmail, computeUserGroupEmail: WorkbenchEmail, projectTemplate: ProjectTemplate): Future[RawlsBillingProjectOperationRecord] =
    Future.successful(RawlsBillingProjectOperationRecord(projectName.value, DEPLOYMENT_MANAGER_CREATE_PROJECT, "opid", false, None, "create"))

  override def cleanupDMProject(projectName: RawlsBillingProjectName): Future[Unit] = Future.successful(())

  override def getBucketDetails(bucket: String, project: RawlsBillingProjectName): Future[WorkspaceBucketOptions] = {
    Future.successful(WorkspaceBucketOptions(false))
  }
  override def addPolicyBindings(projectName: RawlsBillingProjectName, policiesToAdd: Map[String, List[String]]): Future[Boolean] = Future.successful {
    import cats.implicits._
    val existingPolicies = policies.getOrElse(projectName, Map.empty)
    val newPolicies = existingPolicies |+| policiesToAdd
    if (newPolicies.equals(existingPolicies)) {
      false
    } else {
      policies.put(projectName, newPolicies)
      true
    }
  }

  override def grantReadAccess(billingProject: RawlsBillingProjectName,
                               bucketName: String,
                               readers: Set[WorkbenchEmail]): Future[String] = Future(bucketName)

  override def pollOperation(rawlsBillingProjectOperation: RawlsBillingProjectOperationRecord): Future[RawlsBillingProjectOperationRecord] = {
    Future.successful(rawlsBillingProjectOperation.copy(done = true))
  }

  override def deleteProject(projectName: RawlsBillingProjectName): Future[Unit] = Future.successful(())

  override def addRoleToGroup(projectName: RawlsBillingProjectName, groupEmail: WorkbenchEmail, role: String): Future[Boolean] = Future.successful(false)

  override def removeRoleFromGroup(projectName: RawlsBillingProjectName, groupEmail: WorkbenchEmail, role: String): Future[Unit] = Future.successful(())
}
