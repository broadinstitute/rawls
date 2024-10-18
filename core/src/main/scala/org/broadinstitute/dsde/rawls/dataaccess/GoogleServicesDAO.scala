package org.broadinstitute.dsde.rawls.dataaccess

import akka.http.scaladsl.model.StatusCodes
import com.google.api.client.auth.oauth2.Credential
import com.google.api.services.cloudbilling.model.ProjectBillingInfo
import com.google.api.services.cloudresourcemanager.model.Project
import com.google.api.services.directory.model.Group
import com.google.api.services.storage.model.{Bucket, BucketAccessControl, StorageObject}
import com.google.cloud.storage.BucketInfo.LifecycleRule
import com.google.cloud.storage.BucketInfo.SoftDeletePolicy
import org.broadinstitute.dsde.rawls.google.AccessContextManagerDAO
import org.broadinstitute.dsde.rawls.model.WorkspaceAccessLevels._
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.{RawlsException, RawlsExceptionWithErrorReport}
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.{GcsBucketName, GoogleProject, IamPermission}
import spray.json.JsObject

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

object GoogleServicesDAO {
  def getStorageLogsBucketName(googleProject: GoogleProjectId) = s"storage-logs-${googleProject.value}"
}

trait GoogleServicesDAO extends ErrorReportable {
  val errorReportSource = ErrorReportSource("google")

  val accessContextManagerDAO: AccessContextManagerDAO

  val billingEmail: String
  val billingGroupEmail: String
  def adminGroupName: String
  def curatorGroupName: String

  def updateBucketIam(bucketName: GcsBucketName,
                      policyGroupsByAccessLevel: Map[WorkspaceAccessLevel, WorkbenchEmail],
                      userProject: Option[GoogleProjectId] = None,
                      iamPolicyVersion: Int = 1
  ): Future[Unit]

  // returns bucket and group information
  def setupWorkspace(userInfo: UserInfo,
                     googleProject: GoogleProjectId,
                     policyGroupsByAccessLevel: Map[WorkspaceAccessLevel, WorkbenchEmail],
                     bucketName: GcsBucketName,
                     labels: Map[String, String],
                     requestContext: RawlsRequestContext,
                     bucketLocation: Option[String]
  ): Future[GoogleWorkspaceInfo]

  def getGoogleProject(googleProject: GoogleProjectId): Future[Project]

  /** Mark all objects in the bucket for deletion, then attempts to delete the bucket from Google Cloud Storage.
   *
   * The bucket's lifecycle rule is set to delete any objects older than 0 days. This
   * effectively marks all objects in the bucket for deletion the next time GCS inspects the bucket (up to 24 hours
   * later at the time of this writing; see [[https://cloud.google.com/storage/docs/lifecycle#behavior]]).
   * Bucket deletion will not Rawls will periodically retry the bucket deletion until it succeeds.
   *
   * This strategy is inspired by [[http://blog.iangsy.com/2014/04/google-cloud-storage-deleting-full.html]].
   *
   * @param bucketName the name of the bucket to delete
   * @return true if the bucket was deleted, false if not
   */
  def deleteBucket(bucketName: String): Future[Boolean]

  def setBucketLifecycle(bucketName: String, lifecycle: List[LifecycleRule], userProject: GoogleProjectId): Future[Unit]

  def setSoftDeletePolicy(bucketName: String,
                          softDeletePolicy: SoftDeletePolicy,
                          userProject: GoogleProjectId
  ): Future[Unit]

  def setRequesterPays(bucketName: String, requesterPaysEnabled: Boolean, userProject: GoogleProjectId): Future[Unit]

  def isAdmin(userEmail: String): Future[Boolean]

  def isLibraryCurator(userEmail: String): Future[Boolean]

  def addLibraryCurator(userEmail: String): Future[Unit]

  def removeLibraryCurator(userEmail: String): Future[Unit]

  def hasGoogleRole(roleGroupName: String, userEmail: String): Future[Boolean]

  def getGoogleGroup(groupName: String)(implicit executionContext: ExecutionContext): Future[Option[Group]]

  /**
   * Returns the most recent daily storage usage information for a bucket in bytes. The information comes from daily
   * storage logs reported in byte-hours over a 24-hour period, which is divided by 24 to obtain usage in bytes.
   * Queries the objects in a bucket and calculates the total usage (bytes).
   *
   * Note: maxResults is used for integration testing of multi-page queries. While it could potentially be used for
   * performance tuning, it would be better to build that into the service instead of giving the caller a dial to mess
   * with. For that reason, the maxResults parameter should be removed in favor of extracting the creation of Storage
   * objects from the service implementation to enable test doubles to be injected.
   *
   * @param googleProject the name of the project that owns the bucket
   * @param bucketName    the name of the bucket to query
   * @param maxResults    (optional) the page size to use when fetching objects
   * @return the size in bytes of the data stored in the bucket
   */
  def getBucketUsage(googleProject: GoogleProjectId,
                     bucketName: String,
                     maxResults: Option[Long] = None
  ): Future[BucketUsageResponse]

  /**
   * Gets a Google bucket.
   *
   * Note: takes an implicit ExecutionContext to override the class-level ExecutionContext. This
   * is because this method is used for health monitoring, and we want health checks to use a
   * different execution context (thread pool) than user-facing operations.
   *
   * @param bucketName       the bucket name
   * @param executionContext the execution context to use for aysnc operations
   * @param userProject      the project to be billed - optional. If None, defaults to the bucket's project
   * @return optional Google bucket
   */
  def getBucket(bucketName: String, userProject: Option[GoogleProjectId])(implicit
    executionContext: ExecutionContext
  ): Future[Either[String, Bucket]]

  def listObjectsWithPrefix(bucketName: String,
                            objectNamePrefix: String,
                            userProject: Option[GoogleProjectId]
  ): Future[List[StorageObject]]

  def copyFile(sourceBucket: String,
               sourceObject: String,
               destinationBucket: String,
               destinationObject: String,
               userProject: Option[GoogleProjectId]
  )(implicit executionContext: ExecutionContext): Future[Option[StorageObject]]

  def addEmailToGoogleGroup(groupEmail: String, emailToAdd: String): Future[Unit]

  def removeEmailFromGoogleGroup(groupEmail: String, emailToRemove: String): Future[Unit]

  def listBillingAccounts(userInfo: UserInfo,
                          firecloudHasAccess: Option[Boolean] = None
  ): Future[Seq[RawlsBillingAccount]]

  def testTerraBillingAccountAccess(billingAccountName: RawlsBillingAccountName): Future[Boolean]

  /**
   * Lists Google billing accounts using the billing service account.
   *
   * Note: takes an implicit ExecutionContext to override the class-level ExecutionContext. This
   * is because this method is used for health monitoring, and we want health checks to use a
   * different execution context (thread pool) than user-facing operations.
   *
   * @param executionContext the execution context to use for aysnc operations
   * @return sequence of RawlsBillingAccounts
   */
  def listBillingAccountsUsingServiceCredential(implicit
    executionContext: ExecutionContext
  ): Future[Seq[RawlsBillingAccount]]

  def setBillingAccountName(googleProjectId: GoogleProjectId,
                            billingAccountName: RawlsBillingAccountName,
                            tracingContext: RawlsTracingContext
  ): Future[ProjectBillingInfo]

  def disableBillingOnGoogleProject(googleProjectId: GoogleProjectId,
                                    tracingContext: RawlsTracingContext
  ): Future[ProjectBillingInfo]

  def setBillingAccount(googleProjectId: GoogleProjectId,
                        billingAccountName: Option[RawlsBillingAccountName],
                        tracingContext: RawlsTracingContext
  ): Future[ProjectBillingInfo] =
    billingAccountName match {
      case Some(accountName) => setBillingAccountName(googleProjectId, accountName, tracingContext)
      case None              => disableBillingOnGoogleProject(googleProjectId, tracingContext)
    }

  def getBillingInfoForGoogleProject(googleProjectId: GoogleProjectId)(implicit
    executionContext: ExecutionContext
  ): Future[ProjectBillingInfo]

  def getGenomicsOperation(jobId: String): Future[Option[JsObject]]

  /**
   * Checks that a query can be performed against the genomics api.
   *
   * Note: takes an implicit ExecutionContext to override the class-level ExecutionContext. This
   * is because this method is used for health monitoring, and we want health checks to use a
   * different execution context (thread pool) than user-facing operations.
   *
   * @param executionContext the execution context to use for aysnc operations
   * @return sequence of Google operations
   */
  def checkGenomicsOperationsHealth(implicit executionContext: ExecutionContext): Future[Boolean]

  def getResourceBufferServiceAccountCredential: Credential

  def getServiceAccountUserInfo(): Future[UserInfo]

  def getBucketDetails(bucket: String, project: GoogleProjectId): Future[WorkspaceBucketOptions]

  /**
   * Removes the IAM policies from the project's existing policies
   *
   * @return true if the policy was actually changed
   */
  def removePolicyBindings(googleProject: GoogleProjectId,
                           policiesToRemove: Map[String, Set[String]]
  ): Future[Boolean] = updatePolicyBindings(googleProject) { existingPolicies =>
    val updatedKeysWithRemovedPolicies: Map[String, Set[String]] = policiesToRemove.keys.map { k =>
      val existingForKey = existingPolicies.getOrElse(k, Set.empty)
      val updatedForKey = existingForKey diff policiesToRemove(k)
      k -> updatedForKey
    }.toMap

    // Use standard Map ++ instead of semigroup because we want to replace the original values
    existingPolicies ++ updatedKeysWithRemovedPolicies
  }

  /**
   * Adds the IAM policies to the project's existing policies
   *
   * @return true if the policy was actually changed
   */
  def addPolicyBindings(googleProject: GoogleProjectId, policiesToAdd: Map[String, Set[String]]): Future[Boolean] =
    updatePolicyBindings(googleProject) { existingPolicies =>
      // |+| is a semigroup: it combines a map's keys by combining their values' members instead of replacing them
      import cats.implicits._
      existingPolicies |+| policiesToAdd
    }

  /**
   * Internal function to update project IAM bindings.
   *
   * @param googleProject  google project id
   * @param updatePolicies function (existingPolicies => updatedPolicies). May return policies with no members
   *                       which will be handled appropriately when sent to google.
   * @return true if google was called to update policies, false otherwise
   */
  protected def updatePolicyBindings(googleProject: GoogleProjectId)(
    updatePolicies: Map[String, Set[String]] => Map[String, Set[String]]
  ): Future[Boolean]

  def pollOperation(operationId: OperationId): Future[OperationStatus]

  def deleteV1Project(googleProject: GoogleProjectId): Future[Unit]

  def updateGoogleProject(googleProjectId: GoogleProjectId, googleProjectWithUpdates: Project): Future[Project]

  def deleteGoogleProject(googleProject: GoogleProjectId): Future[Unit]

  def getUserInfoUsingJson(saKey: String): Future[UserInfo]

  /**
   * Convert a string to a legal gcp label text, with an optional prefix
   * See: https://cloud.google.com/compute/docs/labeling-resources#restrictions
   *
   * @param s
   * @param prefix defaults to "fc-"
   * @return
   */
  def labelSafeString(s: String, prefix: String = "fc-"): String =
    prefix + s.toLowerCase.replaceAll("[^a-z0-9\\-_]", "-").take(63)

  /**
   * Convert a map of labels to legal gcp label text. Runs [[labelSafeString]] on all keys and values in the map.
   *
   * @param m      Map of label key value pairs
   * @param prefix defaults to "fc-"
   * @return
   */
  def labelSafeMap(m: Map[String, String], prefix: String = "fc-"): Map[String, String] = m.map { case (key, value) =>
    labelSafeString(key, prefix) -> labelSafeString(value, prefix)
  }

  /**
   * Valid text for google project name.
   *
   * "The optional user-assigned display name of the Project. It must be 4 to 30 characters. Allowed
   * characters are: lowercase and uppercase letters, numbers, hyphen, single-quote, double-quote,
   * space, and exclamation point."
   *
   * For more info see: https://cloud.google.com/resource-manager/reference/rest/v1/projects
   *
   * @param name
   * @return
   */
  def googleProjectNameSafeString(name: String): String =
    name.replaceAll("[^a-zA-Z0-9\\-'\" !]", "-").take(30)

  /**
   * Handles getting the google project number from the google [[Project]]
   *
   * @param googleProject
   * @return GoogleProjectNumber
   */
  def getGoogleProjectNumber(googleProject: Project): GoogleProjectNumber = googleProject.getProjectNumber match {
    case null =>
      throw new RawlsExceptionWithErrorReport(
        ErrorReport(StatusCodes.BadGateway,
                    s"Failed to retrieve Google Project Number for Google Project ${googleProject.getProjectId}"
        )
      )
    case googleProjectNumber: java.lang.Long => GoogleProjectNumber(googleProjectNumber.toString)
  }

  def addProjectToFolder(googleProject: GoogleProjectId, folderId: String): Future[Unit]

  def getFolderId(folderName: String): Future[Option[String]]

  def testTerraAndUserBillingAccountAccess(billingAccount: RawlsBillingAccountName, userInfo: UserInfo): Future[Boolean]

  /**
   * Returns location of a regional bucket. If the bucket's location type is `multi-region`, it returns None
   *
   * @param bucketName  the bucket name
   * @param userProject - the project to be billed - optional. If None, defaults to the bucket's project
   * @return optional Google bucket region
   */
  def getRegionForRegionalBucket(bucketName: String, userProject: Option[GoogleProjectId]): Future[Option[String]]

  def getComputeZonesForRegion(googleProject: GoogleProjectId, region: String): Future[List[String]]

  def testSAGoogleBucketIam(bucketName: GcsBucketName, saKey: String, permissions: Set[IamPermission])(implicit
    executionContext: ExecutionContext
  ): Future[Set[IamPermission]]

  /**
    *
    * @param googleProject Google Project
    * @param bucketName Google Bucket
    * @param saKey Pet Service Account Key of the user
    * @param executionContext Execution Context
    * @return A Future Boolean. If true, the SA was able to get the bucket location, or the bucket is requester-pays.
    *         If false, the bucket location could not be retrieved and the bucket is not requester-pays.
    */
  def testSAGoogleBucketGetLocationOrRequesterPays(googleProject: GoogleProject,
                                                   bucketName: GcsBucketName,
                                                   saKey: String
  )(implicit
    executionContext: ExecutionContext
  ): Future[Boolean]

  def testSAGoogleProjectIam(project: GoogleProject, saKey: String, permissions: Set[IamPermission])(implicit
    executionContext: ExecutionContext
  ): Future[Set[IamPermission]]
}

object GoogleApiTypes {
  val allGoogleApiTypes = List(AccessContextManagerApi)

  sealed trait GoogleApiType extends RawlsEnumeration[GoogleApiType] {
    override def toString = GoogleApiTypes.toString(this)
    override def withName(name: String) = GoogleApiTypes.withName(name)
  }

  def withName(name: String): GoogleApiType =
    name match {
      case "AccessContextManager" => AccessContextManagerApi
      case _ =>
        throw new RawlsException(
          s"Invalid GoogleApiType [${name}]. Possible values: ${allGoogleApiTypes.mkString(", ")}"
        )
    }

  def withNameOpt(name: Option[String]): Option[GoogleApiType] =
    name.flatMap(n => Try(withName(n)).toOption)

  def toString(googleApiType: GoogleApiType): String =
    googleApiType match {
      case AccessContextManagerApi => "AccessContextManager"
      case _ =>
        throw new RawlsException(
          s"Invalid GoogleApiType [${googleApiType}]. Possible values: ${allGoogleApiTypes.mkString(", ")}"
        )
    }

  case object AccessContextManagerApi extends GoogleApiType
}

object GoogleOperationNames {
  val allGoogleOperationNames = List(AddProjectToPerimeter)

  sealed trait GoogleOperationName extends RawlsEnumeration[GoogleOperationName] {
    override def toString = GoogleOperationNames.toString(this)
    override def withName(name: String) = GoogleOperationNames.withName(name)
  }

  def withName(name: String): GoogleOperationName =
    name match {
      case "add_project_to_perimeter" => AddProjectToPerimeter
      case _ =>
        throw new RawlsException(
          s"Invalid GoogleOperationName [${name}]. Possible values: ${allGoogleOperationNames.mkString(", ")}"
        )
    }

  def withNameOpt(name: Option[String]): Option[GoogleOperationName] =
    name.flatMap(n => Try(withName(n)).toOption)

  def toString(googleApiType: GoogleOperationName): String =
    googleApiType match {
      case AddProjectToPerimeter => "add_project_to_perimeter"
      case _ =>
        throw new RawlsException(
          s"Invalid GoogleOperationName [${googleApiType}]. Possible values: ${allGoogleOperationNames.mkString(", ")}"
        )
    }

  case object AddProjectToPerimeter extends GoogleOperationName
}

case class OperationId(apiType: GoogleApiTypes.GoogleApiType, operationId: String)
case class OperationStatus(done: Boolean, errorMessage: Option[String])
case class GoogleWorkspaceInfo(bucketName: String, policyGroupsByAccessLevel: Map[WorkspaceAccessLevel, WorkbenchEmail])
