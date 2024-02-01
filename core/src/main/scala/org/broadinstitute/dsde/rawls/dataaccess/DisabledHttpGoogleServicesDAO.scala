package org.broadinstitute.dsde.rawls.dataaccess

import com.google.api.client.auth.oauth2.Credential
import com.google.api.services.cloudbilling.model.ProjectBillingInfo
import com.google.api.services.cloudresourcemanager.model._
import com.google.api.services.directory.model.Group
import com.google.api.services.storage.model._
import com.typesafe.config.Config
import org.broadinstitute.dsde.rawls.google.AccessContextManagerDAO
import org.broadinstitute.dsde.rawls.model.WorkspaceAccessLevels._
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.workbench.model.google.{GcsBucketName, GoogleProject, IamPermission}
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import spray.json._

import scala.concurrent._

class DisabledHttpGoogleServicesDAO(config:Config) extends GoogleServicesDAO(config.getString("groupsPrefix")) {
  override val billingEmail: String = config.getString("billingEmail")
  override val billingGroupEmail: String = config.getString("billingGroupEmail")
  def adminGroupName =
    throw new NotImplementedError("adminGroupName method is not implemented for Azure.")
  def curatorGroupName =
    throw new NotImplementedError("curatorGroupName method is not implemented for Azure.")
  override def updateBucketIam(bucketName: GcsBucketName, policyGroupsByAccessLevel: Map[WorkspaceAccessLevel, WorkbenchEmail],
                               userProject: Option[GoogleProjectId], iamPolicyVersion: Int = 1): Future[Unit] =
    throw new NotImplementedError("updateBucketIam method is not implemented for Azure.")
  override def grantReadAccess(bucketName: String, authBucketReaders: Set[WorkbenchEmail]): Future[String] =
    throw new NotImplementedError("grantReadAccess method is not implemented for Azure.")
  override def deleteBucket(bucketName: String): Future[Boolean] =
    throw new NotImplementedError("deleteBucket method is not implemented for Azure.")
  override def isAdmin(userEmail: String): Future[Boolean] =
    throw new NotImplementedError("isAdmin method is not implemented for Azure.")
  override def isLibraryCurator(userEmail: String): Future[Boolean] =
    throw new NotImplementedError("isLibraryCurator method is not implemented for Azure.")
  override def addLibraryCurator(userEmail: String): Future[Unit] =
    throw new NotImplementedError("addLibraryCurator method is not implemented for Azure.")
  override def removeLibraryCurator(userEmail: String): Future[Unit] =
    throw new NotImplementedError("removeLibraryCurator method is not implemented for Azure.")
  override def hasGoogleRole(roleGroupName: String, userEmail: String): Future[Boolean] =
    throw new NotImplementedError("hasGoogleRole method is not implemented for Azure.")
  override def getGoogleGroup(groupName: String)(implicit executionContext: ExecutionContext): Future[Option[Group]] =
    throw new NotImplementedError("getGoogleGroup method is not implemented for Azure.")
  override def getBucketUsage(googleProject: GoogleProjectId, bucketName: String, maxResults: Option[Long]): Future[BucketUsageResponse] =
    throw new NotImplementedError("getBucketUsage method is not implemented for Azure.")
  override def getBucketACL(bucketName: String): Future[Option[List[BucketAccessControl]]] =
    throw new NotImplementedError("getBucketACL method is not implemented for Azure.")
  override def getBucket(bucketName: String, userProject: Option[GoogleProjectId])(implicit executionContext: ExecutionContext): Future[Either[String, Bucket]] =
    throw new NotImplementedError("getBucket method is not implemented for Azure.")
  override def getRegionForRegionalBucket(bucketName: String, userProject: Option[GoogleProjectId]): Future[Option[String]] =
    throw new NotImplementedError("getRegionForRegionalBucket method is not implemented for Azure.")
  override def getComputeZonesForRegion(googleProject: GoogleProjectId, region: String): Future[List[String]] =
    throw new NotImplementedError("getComputeZonesForRegion method is not implemented for Azure.")
  override def addEmailToGoogleGroup(groupEmail: String, emailToAdd: String): Future[Unit] =
    throw new NotImplementedError("addEmailToGoogleGroup method is not implemented for Azure.")
  override def removeEmailFromGoogleGroup(groupEmail: String, emailToRemove: String): Future[Unit] =
    throw new NotImplementedError("removeEmailFromGoogleGroup method is not implemented for Azure.")
  override def copyFile(sourceBucket: String, sourceObject: String, destinationBucket: String, destinationObject: String,
                        userProject: Option[GoogleProjectId])(implicit executionContext: ExecutionContext): Future[Option[StorageObject]] =
    throw new NotImplementedError("copyFile method is not implemented for Azure.")
  override def listObjectsWithPrefix(bucketName: String, objectNamePrefix: String, userProject: Option[GoogleProjectId]): Future[List[StorageObject]] =
    throw new NotImplementedError("listObjectsWithPrefix method is not implemented for Azure.")
  override def diagnosticBucketRead(userInfo: UserInfo, bucketName: String): Future[Option[ErrorReport]] =
    throw new NotImplementedError("diagnosticBucketRead method is not implemented for Azure.")
  override def testTerraBillingAccountAccess(billingAccountName: RawlsBillingAccountName): Future[Boolean] =
    throw new NotImplementedError("testTerraBillingAccountAccess method is not implemented for Azure.")
  override def testTerraAndUserBillingAccountAccess(billingAccount: RawlsBillingAccountName, userInfo: UserInfo): Future[Boolean] =
    throw new NotImplementedError("testTerraAndUserBillingAccountAccess method is not implemented for Azure.")
  override def testSAGoogleBucketIam(bucketName: GcsBucketName, saKey: String, permissions: Set[IamPermission])(implicit executionContext: ExecutionContext): Future[Set[IamPermission]] =
    throw new NotImplementedError("testSAGoogleBucketIam method is not implemented for Azure.")
  override def testSAGoogleBucketGetLocationOrRequesterPays(googleProject: GoogleProject, bucketName: GcsBucketName, saKey: String)
                                                           (implicit executionContext: ExecutionContext): Future[Boolean] =
    throw new NotImplementedError("testSAGoogleBucketGetLocationOrRequesterPays method is not implemented for Azure.")
  override def testSAGoogleProjectIam(project: GoogleProject, saKey: String, permissions: Set[IamPermission])(implicit executionContext: ExecutionContext): Future[Set[IamPermission]] =
    throw new NotImplementedError("testSAGoogleProjectIam method is not implemented for Azure.")
  override def listBillingAccounts(userInfo: UserInfo, firecloudHasAccess: Option[Boolean] = None): Future[Seq[RawlsBillingAccount]] =
    throw new NotImplementedError("listBillingAccounts method is not implemented for Azure.")
  override def listBillingAccountsUsingServiceCredential(implicit executionContext: ExecutionContext): Future[Seq[RawlsBillingAccount]] =
    throw new NotImplementedError("listBillingAccountsUsingServiceCredential method is not implemented for Azure.")
  override def getBillingInfoForGoogleProject(googleProjectId: GoogleProjectId)(implicit executionContext: ExecutionContext): Future[ProjectBillingInfo] =
    throw new NotImplementedError("getBillingInfoForGoogleProject method is not implemented for Azure.")
  override def getBillingAccountIdForGoogleProject(googleProject: GoogleProject, userInfo: UserInfo)(implicit executionContext: ExecutionContext): Future[Option[String]] =
    throw new NotImplementedError("getBillingAccountIdForGoogleProject method is not implemented for Azure.")
  override def getGenomicsOperation(opId: String): Future[Option[JsObject]] =
    throw new NotImplementedError("getGenomicsOperation method is not implemented for Azure.")
  override def checkGenomicsOperationsHealth(implicit executionContext: ExecutionContext): Future[Boolean] =
    throw new NotImplementedError("checkGenomicsOperationsHealth method is not implemented for Azure.")
  override def getGoogleProject(googleProject: GoogleProjectId): Future[Project] =
    throw new NotImplementedError("getGoogleProject method is not implemented for Azure.")
  override def pollOperation(operationId: OperationId): Future[OperationStatus] =
    throw new NotImplementedError("pollOperation method is not implemented for Azure.")
  override protected def updatePolicyBindings(googleProject: GoogleProjectId)(updatePolicies: Map[String, Set[String]] => Map[String, Set[String]]): Future[Boolean] =
    throw new NotImplementedError("updatePolicyBindings method is not implemented for Azure.")
  override def deleteV1Project(googleProject: GoogleProjectId): Future[Unit] =
    throw new NotImplementedError("deleteV1Project method is not implemented for Azure.")
  override def updateGoogleProject(googleProjectId: GoogleProjectId, googleProjectWithUpdates: Project): Future[Project] =
    throw new NotImplementedError("updateGoogleProject method is not implemented for Azure.")
  override def deleteGoogleProject(googleProject: GoogleProjectId): Future[Unit] =
    throw new NotImplementedError("deleteGoogleProject method is not implemented for Azure.")
  override def getBucketDetails(bucketName: String, project: GoogleProjectId): Future[WorkspaceBucketOptions] =
    throw new NotImplementedError("getBucketDetails method is not implemented for Azure.")
  override def getBucketServiceAccountCredential: Credential =
    throw new NotImplementedError("updatePolicyBindings method is not implemented for Azure.")
  override lazy val getResourceBufferServiceAccountCredential: Credential =
    throw new NotImplementedError("getResourceBufferServiceAccountCredential method is not implemented for Azure.")
  override def toGoogleGroupName(groupName: RawlsGroupName) =
    throw new NotImplementedError("toGoogleGroupName method is not implemented for Azure.")
  override def getAccessTokenUsingJson(saKey: String): Future[String] =
    throw new NotImplementedError("getAccessTokenUsingJson method is not implemented for Azure.")
  override def getUserInfoUsingJson(saKey: String): Future[UserInfo] =
    throw new NotImplementedError("getUserInfoUsingJson method is not implemented for Azure.")
  override def getServiceAccountUserInfo(): Future[UserInfo] =
    throw new NotImplementedError("getServiceAccountUserInfo method is not implemented for Azure.")
  override def addProjectToFolder(googleProject: GoogleProjectId, folderId: String): Future[Unit] =
    throw new NotImplementedError("addProjectToFolder method is not implemented for Azure.")
  override def getFolderId(folderName: String): Future[Option[String]] =
    throw new NotImplementedError("getFolderId method is not implemented for Azure.")
  override val accessContextManagerDAO: AccessContextManagerDAO =
    throw new NotImplementedError("accessContextManagerDAO method is not implemented for Azure.")
  override def setupWorkspace(userInfo: UserInfo, googleProject: GoogleProjectId, policyGroupsByAccessLevel: Map[WorkspaceAccessLevel, WorkbenchEmail], bucketName: GcsBucketName, labels: Map[String, String], requestContext: RawlsRequestContext, bucketLocation: Option[String]): Future[GoogleWorkspaceInfo] =
    throw new NotImplementedError("setupWorkspac method is not implemented for Azure.")
  override def setBillingAccountName(googleProjectId: GoogleProjectId, billingAccountName: RawlsBillingAccountName, tracingContext: RawlsTracingContext): Future[ProjectBillingInfo] =
    throw new NotImplementedError("setBillingAccountName method is not implemented for Azure.")
  override def disableBillingOnGoogleProject(googleProjectId: GoogleProjectId, tracingContext: RawlsTracingContext): Future[ProjectBillingInfo] =
    throw new NotImplementedError("disableBillingOnGoogleProject method is not implemented for Azure.")
}
