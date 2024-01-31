package org.broadinstitute.dsde.rawls.dataaccess

import akka.actor.ActorSystem
import akka.http.scaladsl.{Http, HttpExt}
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.stream.Materializer
import cats.effect.{IO, Temporal}
import com.google.api.client.auth.oauth2.Credential
import com.google.api.client.googleapis.auth.oauth2.{GoogleClientSecrets, GoogleCredential}
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport
import com.google.api.client.http.javanet.NetHttpTransport
import com.google.api.client.json.gson.GsonFactory
import com.google.api.services.cloudbilling.Cloudbilling
import com.google.api.services.cloudbilling.model.{BillingAccount, ListBillingAccountsResponse, ProjectBillingInfo}
import com.google.api.services.cloudresourcemanager.CloudResourceManager
import com.google.api.services.cloudresourcemanager.model._
import com.google.api.services.compute.{Compute, ComputeScopes}
import com.google.api.services.directory.model.Group
import com.google.api.services.directory.DirectoryScopes
import com.google.api.services.genomics.v2alpha1.GenomicsScopes
import com.google.api.services.iam.v1.Iam
import com.google.api.services.iamcredentials.v1.IAMCredentials
import com.google.api.services.lifesciences.v2beta.CloudLifeSciencesScopes
import com.google.api.services.storage.model._
import com.google.api.services.storage.{Storage, StorageScopes}
import com.typesafe.config.Config
import io.opencensus.trace.Span
import org.broadinstitute.dsde.rawls.google.{AccessContextManagerDAO, GoogleUtilities}
import org.broadinstitute.dsde.rawls.metrics.GoogleInstrumented.GoogleCounters
import org.broadinstitute.dsde.rawls.model.WorkspaceAccessLevels._
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.util.ScalaConfig.EnhancedScalaConfig
import org.broadinstitute.dsde.rawls.util.{FutureSupport, HttpClientUtilsStandard}
import org.broadinstitute.dsde.rawls.AppDependencies
import org.broadinstitute.dsde.workbench.google2.GoogleStorageService
import org.broadinstitute.dsde.workbench.model.google.{GcsBucketName, GoogleProject, IamPermission}
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.typelevel.log4cats.SelfAwareStructuredLogger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import spray.json._

import java.io._
import scala.concurrent._

class DisabledHttpGoogleServicesDAO(config:Config,
                            maxPageSize: Int = 200,
                            appDependencies: AppDependencies[IO],
                            override val workbenchMetricBaseName: String,
                            override val accessContextManagerDAO: AccessContextManagerDAO
                           )(implicit override val system: ActorSystem,
                             override val materializer: Materializer,
                             override implicit val executionContext: ExecutionContext,
                             override implicit val timer: Temporal[IO]
                           ) extends HttpGoogleServicesDAO(config, 200,
appDependencies, workbenchMetricBaseName, accessContextManagerDAO) {
  override val http: HttpExt = Http(system)

  override val clientEmail: String = config.getString("serviceClientEmail")
  override val subEmail: String = config.getString("subEmail")
  override val pemFile: String = config.getString("pathToPem")
  override val appsDomain: String = config.getString("appsDomain")
  override val groupsPrefix: String = config.getString("groupsPrefix")
  override val appName: String = config.getString("appName")
  override val serviceProject: String = config.getString("serviceProject")
  override val billingPemEmail: String = config.getString("billingPemEmail")
  override val billingPemFile: String = config.getString("pathToBillingPem")
  override val billingEmail: String = config.getString("billingEmail")
  override val billingGroupEmail: String = config.getString("billingGroupEmail")
  override val googleStorageService: GoogleStorageService[IO] = appDependencies.googleStorageService
  override val proxyNamePrefix: String = config.getStringOr("proxyNamePrefix", "")
  override val terraBucketReaderRole: String = config.getString("terraBucketReaderRole")
  override val terraBucketWriterRole: String = config.getString("terraBucketWriterRole")
  override val resourceBufferJsonFile: String = config.getString("pathToResourceBufferJson")
  override val jsonFactory: GsonFactory = GsonFactory.getDefaultInstance
  override val clientSecrets: GoogleClientSecrets = GoogleClientSecrets.load(jsonFactory, new StringReader(config.getString("secrets")))

  override val httpClientUtils: HttpClientUtilsStandard = HttpClientUtilsStandard()
  override implicit val log4CatsLogger: SelfAwareStructuredLogger[IO] = Slf4jLogger.getLogger[IO]

  override val groupMemberRole =
    "MEMBER" // the Google Group role corresponding to a member (note that this is distinct from the GCS roles defined in WorkspaceAccessLevel)
  override val cloudBillingInfoReadTimeout: Int = 40 * 1000 // socket read timeout when updating billing info

  // modify these if we need more granular access in the future
  override val workbenchLoginScopes: Seq[String] =
    Seq("https://www.googleapis.com/auth/userinfo.email", "https://www.googleapis.com/auth/userinfo.profile")
  override val storageScopes: Seq[String] = Seq(StorageScopes.DEVSTORAGE_FULL_CONTROL, ComputeScopes.COMPUTE) ++ workbenchLoginScopes
  override val directoryScopes: Seq[String] = Seq(DirectoryScopes.ADMIN_DIRECTORY_GROUP)
  override val genomicsScopes: Seq[String] = Seq(
    GenomicsScopes.GENOMICS
  ) // google requires GENOMICS, not just GENOMICS_READONLY, even though we're only doing reads
  override val lifesciencesScopes: Seq[String] = Seq(CloudLifeSciencesScopes.CLOUD_PLATFORM)
  override val billingScopes: Seq[String] = Seq("https://www.googleapis.com/auth/cloud-billing")

  override val httpTransport: NetHttpTransport = GoogleNetHttpTransport.newTrustedTransport
  override val BILLING_ACCOUNT_PERMISSION = "billing.resourceAssociations.create"

  override val SingleRegionLocationType: String = "region"

  override val REQUESTER_PAYS_ERROR_SUBSTRINGS: Seq[String] = Seq("requester pays", "UserProjectMissing")

  override def updateBucketIam(bucketName: GcsBucketName,
                               policyGroupsByAccessLevel: Map[WorkspaceAccessLevel, WorkbenchEmail],
                               userProject: Option[GoogleProjectId],
                               iamPolicyVersion: Int = 1
                              ): Future[Unit] =
    throw new NotImplementedError("updateBucketIam method is not implemented for Azure.")


  //override def setupWorkspace(userInfo: UserInfo,
  //                            googleProject: GoogleProjectId,
  //                            policyGroupsByAccessLevel: Map[WorkspaceAccessLevel, WorkbenchEmail],
  //                            bucketName: GcsBucketName,
  //                            labels: Map[String, String],
  //                            parentSpan: Span = null,
  //                            bucketLocation: Option[String]
  //                           ): Future[GoogleWorkspaceInfo] =
  //  throw new NotImplementedError("setupWorkspace method is not implemented for Azure.")

  override def grantReadAccess(bucketName: String, authBucketReaders: Set[WorkbenchEmail]): Future[String] =
    throw new NotImplementedError("grantReadAccess method is not implemented for Azure.")

  private def newBucketAccessControl(entity: String, accessLevel: String) =
    throw new NotImplementedError("newBucketAccessControl method is not implemented for Azure.")

  private def newObjectAccessControl(entity: String, accessLevel: String) =
    throw new NotImplementedError("newObjectAccessControl method is not implemented for Azure.")

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

  override def getBucketUsage(googleProject: GoogleProjectId,
                              bucketName: String,
                              maxResults: Option[Long]
                             ): Future[BucketUsageResponse] =
    throw new NotImplementedError("getBucketUsage method is not implemented for Azure.")

  override def getBucketACL(bucketName: String): Future[Option[List[BucketAccessControl]]] =
    throw new NotImplementedError("getBucketACL method is not implemented for Azure.")

  override def getBucket(bucketName: String, userProject: Option[GoogleProjectId])(implicit
                                                                                   executionContext: ExecutionContext
  ): Future[Either[String, Bucket]] =
    throw new NotImplementedError("getBucket method is not implemented for Azure.")

  override def getRegionForRegionalBucket(bucketName: String,
                                          userProject: Option[GoogleProjectId]
                                         ): Future[Option[String]] =
    throw new NotImplementedError("getRegionForRegionalBucket method is not implemented for Azure.")

  override def getComputeZonesForRegion(googleProject: GoogleProjectId, region: String): Future[List[String]] =
    throw new NotImplementedError("getComputeZonesForRegion method is not implemented for Azure.")

  override def addEmailToGoogleGroup(groupEmail: String, emailToAdd: String): Future[Unit] =
    throw new NotImplementedError("addEmailToGoogleGroup method is not implemented for Azure.")

  override def removeEmailFromGoogleGroup(groupEmail: String, emailToRemove: String): Future[Unit] =
    throw new NotImplementedError("removeEmailFromGoogleGroup method is not implemented for Azure.")

  override def copyFile(sourceBucket: String,
                        sourceObject: String,
                        destinationBucket: String,
                        destinationObject: String,
                        userProject: Option[GoogleProjectId]
                       )(implicit executionContext: ExecutionContext): Future[Option[StorageObject]] =
    throw new NotImplementedError("copyFile method is not implemented for Azure.")

  override def listObjectsWithPrefix(bucketName: String,
                                     objectNamePrefix: String,
                                     userProject: Option[GoogleProjectId]
                                    ): Future[List[StorageObject]] =
    throw new NotImplementedError("listObjectsWithPrefix method is not implemented for Azure.")

  private def listObjectsRecursive(fetcher: Storage#Objects#List,
                                   accumulated: Option[List[Objects]] = Some(Nil)
                                  ): Future[Option[List[Objects]]] =
    throw new NotImplementedError("listObjectsRecursive method is not implemented for Azure.")

  override def diagnosticBucketRead(userInfo: UserInfo, bucketName: String): Future[Option[ErrorReport]] =
    throw new NotImplementedError("diagnosticBucketRead method is not implemented for Azure.")

  override def testTerraBillingAccountAccess(billingAccountName: RawlsBillingAccountName): Future[Boolean] =
    throw new NotImplementedError("testTerraBillingAccountAccess method is not implemented for Azure.")

  override def testTerraAndUserBillingAccountAccess(billingAccount: RawlsBillingAccountName,
                                                    userInfo: UserInfo
                                                   ): Future[Boolean] =
    throw new NotImplementedError("testTerraAndUserBillingAccountAccess method is not implemented for Azure.")

  override protected def testBillingAccountAccess(billingAccount: RawlsBillingAccountName, credential: Credential)(implicit
                                                                                                          executionContext: ExecutionContext
  ): Future[Boolean] =
    throw new NotImplementedError("testBillingAccountAccess method is not implemented for Azure.")

  override def testSAGoogleBucketIam(bucketName: GcsBucketName, saKey: String, permissions: Set[IamPermission])(implicit
                                                                                                                executionContext: ExecutionContext
  ): Future[Set[IamPermission]] =
    throw new NotImplementedError("testSAGoogleBucketIam method is not implemented for Azure.")

  override def testSAGoogleBucketGetLocationOrRequesterPays(googleProject: GoogleProject,
                                                   bucketName: GcsBucketName,
                                                   saKey: String
                                                  )(implicit
                                                    executionContext: ExecutionContext
                                                  ): Future[Boolean] =
    throw new NotImplementedError("testSAGoogleBucketGetLocationOrRequesterPays method is not implemented for Azure.")

  override def testSAGoogleProjectIam(project: GoogleProject, saKey: String, permissions: Set[IamPermission])(implicit
                                                                                                              executionContext: ExecutionContext
  ): Future[Set[IamPermission]] =
    throw new NotImplementedError("testSAGoogleProjectIam method is not implemented for Azure.")

  override protected def listBillingAccounts(credential: Credential
                                   )(implicit executionContext: ExecutionContext): Future[List[BillingAccount]] =
    throw new NotImplementedError("listBillingAccounts method is not implemented for Azure.")

  override protected def executeGoogleListBillingAccountsRequest(credential: Credential, pageToken: Option[String] = None)(
    implicit counters: GoogleCounters
  ): ListBillingAccountsResponse =
    throw new NotImplementedError("executeGoogleListBillingAccountsRequest method is not implemented for Azure.")

  override def listBillingAccounts(userInfo: UserInfo,
                                   firecloudHasAccess: Option[Boolean] = None
                                  ): Future[Seq[RawlsBillingAccount]] =
    throw new NotImplementedError("listBillingAccounts method is not implemented for Azure.")

  override def listBillingAccountsUsingServiceCredential(implicit
                                                         executionContext: ExecutionContext
                                                        ): Future[Seq[RawlsBillingAccount]] =
    throw new NotImplementedError("listBillingAccountsUsingServiceCredential method is not implemented for Azure.")

  //override def setBillingAccountName(googleProjectId: GoogleProjectId,
  //                                   billingAccountName: RawlsBillingAccountName,
  //                                   span: Span = null
  //                                  ): Future[ProjectBillingInfo] =
  //  throw new NotImplementedError("setBillingAccountName method is not implemented for Azure.")

 // override def disableBillingOnGoogleProject(googleProjectId: GoogleProjectId): Future[ProjectBillingInfo] =
 //   throw new NotImplementedError("disableBillingOnGoogleProject method is not implemented for Azure.")

  private def updateBillingInfo(googleProjectId: GoogleProjectId,
                                projectBillingInfo: ProjectBillingInfo,
                                parentSpan: Span = null
                               ): Future[ProjectBillingInfo] =
    throw new NotImplementedError("updateBillingInfo method is not implemented for Azure.")

  override def getBillingInfoForGoogleProject(
                                               googleProjectId: GoogleProjectId
                                             )(implicit executionContext: ExecutionContext): Future[ProjectBillingInfo] =
    throw new NotImplementedError("getBillingInfoForGoogleProject method is not implemented for Azure.")

  override def getBillingAccountIdForGoogleProject(googleProject: GoogleProject, userInfo: UserInfo)(implicit
                                                                                                     executionContext: ExecutionContext
  ): Future[Option[String]] =
    throw new NotImplementedError("getBillingAccountIdForGoogleProject method is not implemented for Azure.")

  override def getGenomicsOperation(opId: String): Future[Option[JsObject]] =
    throw new NotImplementedError("getGenomicsOperation method is not implemented for Azure.")

  override def checkGenomicsOperationsHealth(implicit executionContext: ExecutionContext): Future[Boolean] =
    throw new NotImplementedError("checkGenomicsOperationsHealth method is not implemented for Azure.")

  override def getGoogleProject(googleProject: GoogleProjectId): Future[Project] =
    throw new NotImplementedError("getGoogleProject method is not implemented for Azure.")

  private def folderNumberOnly(folderId: String) =
    throw new NotImplementedError("folderNumberOnly method is not implemented for Azure.")

  override def pollOperation(operationId: OperationId): Future[OperationStatus] =
    throw new NotImplementedError("pollOperation method is not implemented for Azure.")

  private def toScalaBool(b: java.lang.Boolean) =
    throw new NotImplementedError("toScalaBool( method is not implemented for Azure.")

  private def toErrorMessage(message: String, code: String): String =
    throw new NotImplementedError("toErrorMessage method is not implemented for Azure.")

  private def toErrorMessage(message: String, code: Int): String =
    throw new NotImplementedError("toErrorMessage method is not implemented for Azure.")

  override protected def updatePolicyBindings(
                                               googleProject: GoogleProjectId
                                             )(updatePolicies: Map[String, Set[String]] => Map[String, Set[String]]): Future[Boolean] =
    throw new NotImplementedError("updatePolicyBindings method is not implemented for Azure.")

  override def deleteV1Project(googleProject: GoogleProjectId): Future[Unit] =
    throw new NotImplementedError("deleteV1Project method is not implemented for Azure.")

  override def updateGoogleProject(googleProjectId: GoogleProjectId,
                                   googleProjectWithUpdates: Project
                                  ): Future[Project] =
    throw new NotImplementedError("updateGoogleProject method is not implemented for Azure.")

  override def deleteGoogleProject(googleProject: GoogleProjectId): Future[Unit] =
    throw new NotImplementedError("deleteGoogleProject method is not implemented for Azure.")

  override def projectUsageExportBucketName(googleProject: GoogleProjectId) =
    throw new NotImplementedError("projectUsageExportBucketName method is not implemented for Azure.")

  override def getBucketDetails(bucketName: String, project: GoogleProjectId): Future[WorkspaceBucketOptions] =
    throw new NotImplementedError("getBucketDetails method is not implemented for Azure.")

  override def getComputeManager(credential: Credential): Compute =
    throw new NotImplementedError("getComputeManager method is not implemented for Azure.")

  override def getCloudBillingManager(credential: Credential): Cloudbilling =
    throw new NotImplementedError("getCloudBillingManager method is not implemented for Azure.")

  override def getCloudResourceManager(credential: Credential): CloudResourceManager =
    throw new NotImplementedError("getCloudResourceManager method is not implemented for Azure.")

  override def getIAM(credential: Credential): Iam =
    throw new NotImplementedError("getIAM method is not implemented for Azure.")

  override def getIAMCredentials(credential: Credential): IAMCredentials =
    throw new NotImplementedError("getIAMCredentials method is not implemented for Azure.")

  override def getStorage(credential: Credential) =
    throw new NotImplementedError("getStorage method is not implemented for Azure.")

  override def getGroupDirectory =
    throw new NotImplementedError("getGroupDirectory method is not implemented for Azure.")

  private def getCloudResourceManagerWithBillingServiceAccountCredential =
    throw new NotImplementedError("getCloudResourceManagerWithBillingServiceAccountCredential method is not implemented for Azure.")

  private def getGroupServiceAccountCredential: Credential =
    throw new NotImplementedError("getGroupServiceAccountCredential method is not implemented for Azure.")

  override def getBucketServiceAccountCredential: Credential =
    throw new NotImplementedError("updatePolicyBindings method is not implemented for Azure.")

  override def getGenomicsServiceAccountCredential: Credential =
    throw new NotImplementedError("getGenomicsServiceAccountCredential method is not implemented for Azure.")

  override def getLifeSciencesServiceAccountCredential(): Credential =
    throw new NotImplementedError("getLifeSciencesServiceAccountCredential method is not implemented for Azure.")

  override def getBillingServiceAccountCredential: Credential =
    throw new NotImplementedError("getBillingServiceAccountCredential method is not implemented for Azure.")

  override lazy val getResourceBufferServiceAccountCredential: Credential =
    throw new NotImplementedError("getResourceBufferServiceAccountCredential method is not implemented for Azure.")

  override def toGoogleGroupName(groupName: RawlsGroupName) =
    throw new NotImplementedError("toGoogleGroupName method is not implemented for Azure.")

  override def adminGroupName =
    throw new NotImplementedError("adminGroupName method is not implemented for Azure.")

  override def curatorGroupName =
    throw new NotImplementedError("curatorGroupName method is not implemented for Azure.")

  override def makeGroupEntityString(groupId: String) =
    throw new NotImplementedError("makeGroupEntityString method is not implemented for Azure.")

  private def buildCredentialFromAccessToken(accessToken: String, credentialEmail: String): GoogleCredential =
    throw new NotImplementedError("buildCredentialFromAccessToken method is not implemented for Azure.")

  override def getAccessTokenUsingJson(saKey: String): Future[String] =
    throw new NotImplementedError("getAccessTokenUsingJson method is not implemented for Azure.")

  override def getUserInfoUsingJson(saKey: String): Future[UserInfo] =
    throw new NotImplementedError("getUserInfoUsingJson method is not implemented for Azure.")

  override def getRawlsUserForCreds(creds: Credential): Future[RawlsUser] =
    throw new NotImplementedError("getRawlsUserForCreds method is not implemented for Azure.")

  override def getServiceAccountUserInfo(): Future[UserInfo] =
    throw new NotImplementedError("getServiceAccountUserInfo method is not implemented for Azure.")

  private def streamObject[A](bucketName: String, objectName: String)(f: (InputStream) => A): Future[A] =
    throw new NotImplementedError("streamObject method is not implemented for Azure.")

  override def addProjectToFolder(googleProject: GoogleProjectId, folderId: String): Future[Unit] =
    throw new NotImplementedError("addProjectToFolder method is not implemented for Azure.")

  override def getFolderId(folderName: String): Future[Option[String]] =
    throw new NotImplementedError("getFolderId method is not implemented for Azure.")
}

object DisabledHttpGoogleServicesDAO {
  def handleByOperationIdType[T](opId: String,
                                 papiV1Handler: String => T,
                                 papiV2alpha1Handler: String => T,
                                 lifeSciencesBetaHandler: String => T,
                                 noMatchHandler: String => T
                                ): T =
    throw new NotImplementedError("handleByOperationIdType method is not implemented for Azure.")

  private[dataaccess] def getUserCredential(userInfo: UserInfo): Option[Credential] =
    throw new NotImplementedError("getUserCredential method is not implemented for Azure.")
}

class DisabledGenomicsV1DAO(implicit
                    val system: ActorSystem,
                    val materializer: Materializer,
                    val executionContext: ExecutionContext
                   ) extends DsdeHttpDAO {
  val http = Http(system)
  val httpClientUtils = HttpClientUtilsStandard()

  def getOperation(opId: String, accessToken: OAuth2BearerToken): Future[Option[JsObject]] =
    throw new NotImplementedError("getOperation method is not implemented for Azure.")
}

class DisabledCloudResourceManagerV2DAO(implicit
                                val system: ActorSystem,
                                val materializer: Materializer,
                                val executionContext: ExecutionContext
                               ) extends DsdeHttpDAO {
  val http = Http(system)
  val httpClientUtils = HttpClientUtilsStandard()

  def getFolderId(folderName: String, accessToken: OAuth2BearerToken): Future[Option[String]] =
    throw new NotImplementedError("getFolderId method is not implemented for Azure.")
}

object DisabledCloudResourceManagerV2Model {
  case class Folder(name: String)
  case class FolderSearchResponse(folders: Option[Seq[Folder]])
}
