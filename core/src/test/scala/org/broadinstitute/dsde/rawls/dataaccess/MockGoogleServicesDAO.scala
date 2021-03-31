package org.broadinstitute.dsde.rawls.dataaccess

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import com.google.api.client.auth.oauth2.Credential
import com.google.api.client.googleapis.testing.auth.oauth2.MockGoogleCredential
import com.google.api.services.admin.directory.model.Group
import com.google.api.services.cloudbilling.model.ProjectBillingInfo
import com.google.api.services.cloudresourcemanager.model.Project
import com.google.api.services.storage.model.{Bucket, BucketAccessControl, StorageObject}
import io.opencensus.trace.Span
import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.dataaccess.slick.RawlsBillingProjectOperationRecord
import org.broadinstitute.dsde.rawls.google.{AccessContextManagerDAO, MockGoogleAccessContextManagerDAO}
import org.broadinstitute.dsde.rawls.model.WorkspaceAccessLevels._
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.joda.time.DateTime
import spray.json._

import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Random

class MockGoogleServicesDAO(groupsPrefix: String,
                            override val accessContextManagerDAO: AccessContextManagerDAO = new MockGoogleAccessContextManagerDAO) extends GoogleServicesDAO(groupsPrefix) {

  val billingEmail: String = "billing@test.firecloud.org"
  val billingGroupEmail: String = "terra-billing@test.firecloud.org"
  private var token: String = null
  private var tokenDate: DateTime = null

  private val groups: TrieMap[RawlsGroupRef, Set[Either[RawlsUser, RawlsGroup]]] = TrieMap()
  val policies: TrieMap[GoogleProjectId, Map[String, Set[String]]] = TrieMap()

  val accessibleBillingAccountName = RawlsBillingAccountName("billingAccounts/firecloudHasThisOne")
  val inaccessibleBillingAccountName = RawlsBillingAccountName("billingAccounts/firecloudDoesntHaveThisOne")

  val mockJobIds = Seq("operations/dummy-job-id", "projects/dummy-project/operations/dummy-job-id")

  override def listBillingAccounts(userInfo: UserInfo): Future[Seq[RawlsBillingAccount]] = {
    val firecloudHasThisOne = RawlsBillingAccount(accessibleBillingAccountName, true, "testBillingAccount")
    val firecloudDoesntHaveThisOne = RawlsBillingAccount(inaccessibleBillingAccountName, false, "testBillingAccount")
    Future.successful(Seq(firecloudHasThisOne, firecloudDoesntHaveThisOne))
  }

  override def testDMBillingAccountAccess(billingAccountName: RawlsBillingAccountName): Future[Boolean] = {
    if (billingAccountName == inaccessibleBillingAccountName)
      Future.successful(false)
    else
      Future.successful(true)
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
  lazy val getResourceBufferServiceAccountCredential: Credential = getPreparedMockGoogleCredential()

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

  var mockProxyGroups = mutable.Map[RawlsUser, Boolean]()

  override def setupWorkspace(userInfo: UserInfo,
                              googleProject: GoogleProjectId,
                              policyGroupsByAccessLevel: Map[WorkspaceAccessLevel, WorkbenchEmail],
                              bucketName: String,
                              labels: Map[String, String],
                              parentSpan: Span =  null,
                              bucketLocation: Option[String]): Future[GoogleWorkspaceInfo] = {

    val googleWorkspaceInfo: GoogleWorkspaceInfo = GoogleWorkspaceInfo(bucketName, policyGroupsByAccessLevel)
    Future.successful(googleWorkspaceInfo)
  }

  override def getAccessTokenUsingJson(saKey: String): Future[String] = Future.successful("token")
  override def getUserInfoUsingJson(saKey: String): Future[UserInfo] = Future.successful(UserInfo(RawlsUserEmail("foo@bar.com"), OAuth2BearerToken("test_token"), 0, RawlsUserSubjectId("12345678000")))

  override def getGoogleProject(billingProjectName: GoogleProjectId): Future[Project] = Future.successful(new Project().setProjectNumber(Random.nextLong()))

  override def deleteBucket(bucketName: String) = Future.successful(true)

  override def getBucket(bucketName: String, userProject: Option[GoogleProjectId])(implicit executionContext: ExecutionContext): Future[Option[Bucket]] = Future.successful(Some(new Bucket))

  override def getBucketACL(bucketName: String): Future[Option[List[BucketAccessControl]]] = Future.successful(Some(List.fill(5)(new BucketAccessControl)))

  override def diagnosticBucketRead(userInfo: UserInfo, bucketName: String) = Future.successful(None)

  override def listObjectsWithPrefix(bucketName: String, objectNamePrefix: String, userProject: Option[GoogleProjectId]): Future[List[StorageObject]] = Future.successful(List.empty)

  override def copyFile(sourceBucket: String, sourceObject: String, destinationBucket: String, destinationObject: String, userProject: Option[GoogleProjectId]): Future[Option[StorageObject]] = Future.successful(None)

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

  def getBucketUsage(googleProject: GoogleProjectId, bucketName: String, maxResults: Option[Long]): Future[BigInt] = Future.successful(42)

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

  override def createProject(googleProject: GoogleProjectId, billingAccount: RawlsBillingAccount, dmTemplatePath: String, highSecurityNetwork: Boolean, enableFlowLogs: Boolean, privateIpGoogleAccess: Boolean, requesterPaysRole: String, ownerGroupEmail: WorkbenchEmail, computeUserGroupEmail: WorkbenchEmail, projectTemplate: ProjectTemplate, parentFolderId: Option[String]): Future[RawlsBillingProjectOperationRecord] =
    Future.successful(RawlsBillingProjectOperationRecord(googleProject.value, GoogleOperationNames.DeploymentManagerCreateProject, "opid", false, None, GoogleApiTypes.DeploymentManagerApi))

  override def cleanupDMProject(googleProject: GoogleProjectId): Future[Unit] = Future.successful(())

  override def getBucketDetails(bucket: String, project: GoogleProjectId): Future[WorkspaceBucketOptions] = {
    Future.successful(WorkspaceBucketOptions(false))
  }

  protected def updatePolicyBindings(googleProject: GoogleProjectId)(updatePolicies: Map[String, Set[String]] => Map[String, Set[String]]): Future[Boolean] = Future.successful {
    val existingPolicies = policies.getOrElse(googleProject, Map.empty)
    val updatedPolicies = updatePolicies(existingPolicies)
    if (updatedPolicies.equals(existingPolicies)) {
      false
    } else {
      policies.put(googleProject, updatedPolicies)
      true
    }
  }

  override def grantReadAccess(bucketName: String, readers: Set[WorkbenchEmail]): Future[String] = Future(bucketName)

  override def pollOperation(operationId: OperationId): Future[OperationStatus] = {
    Future.successful(OperationStatus(true, None))
  }


  override def addGoogleProjectLabels(googleProject: GoogleProjectId, labels: Map[String, String]): Future[Unit] = Future.successful(())

  override def updateGoogleProject(googleProjectId: GoogleProjectId, googleProjectWithUpdates: Project): Future[Project] = Future.successful(new Project())

  override def updateGoogleProjectName(googleProject: GoogleProjectId, name: String): Future[Unit] = Future.successful(())

  override def deleteGoogleProject(googleProject: GoogleProjectId): Future[Unit] = Future.successful(())

  override def deleteV1Project(googleProject: GoogleProjectId): Future[Unit] = Future.successful(())

  override def addProjectToFolder(googleProject: GoogleProjectId, folderName: String): Future[Unit] = Future.successful(())

  override def getFolderId(folderName: String): Future[Option[String]] = Future.successful(Option("folders/1234567"))

  override def testBillingAccountAccess(billingAccount: RawlsBillingAccountName, userInfo: UserInfo): Future[Boolean] = {
    Future.successful(billingAccount == accessibleBillingAccountName)
  }

  override def updateGoogleProjectBillingAccount(googleProjectId: GoogleProjectId, newBillingAccount: Option[RawlsBillingAccountName]): Future[ProjectBillingInfo] = {
    Future.successful(new ProjectBillingInfo().setBillingAccountName(newBillingAccount.map(_.value).getOrElse("")).setProjectId(googleProjectId.value))
  }

  override def getRegionForRegionalBucket(bucketName: String, userProject: Option[GoogleProjectId]): Future[Option[String]] = {
    Future.successful {
      bucketName match {
        case "fc-regional-bucket" => Option("EUROPE-NORTH1")
        case _ => None
      }
    }
  }

  override def getComputeZonesForRegion(googleProject: GoogleProjectId, region: String): Future[List[String]] = {
    Future.successful {
      region.toLowerCase match {
        case "europe-north1" => List("europe-north1-a", "europe-north1-b", "europe-north1-c")
        case _ => List("us-central1-b", "us-central1-c", "us-central1-f")
      }
    }
  }
}
