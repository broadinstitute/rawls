package org.broadinstitute.dsde.rawls.dataaccess

import cats.effect.IO
import com.google.api.client.auth.oauth2.Credential
import com.google.api.services.admin.directory.model.Group
import com.google.api.services.cloudresourcemanager.model.Project
import com.google.api.services.storage.model.{Bucket, BucketAccessControl, StorageObject}
import com.google.pubsub.v1.ProjectTopicName
import com.typesafe.config.Config
import fs2.Stream
import io.opencensus.trace.Span
import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.dataaccess.slick.RawlsBillingProjectOperationRecord
import org.broadinstitute.dsde.rawls.google.AccessContextManagerDAO
import org.broadinstitute.dsde.rawls.model.WorkspaceAccessLevels._
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.workbench.google2.GcsBlobName
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.model.google.GcsBucketName
import org.joda.time.DateTime
import spray.json.JsObject

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

abstract class GoogleServicesDAO(groupsPrefix: String) extends ErrorReportable {
  val errorReportSource = ErrorReportSource("google")

  val accessContextManagerDAO: AccessContextManagerDAO

  val billingEmail: String
  val billingGroupEmail: String

  // returns bucket and group information
  def setupWorkspace(userInfo: UserInfo, projectName: RawlsBillingProjectName, policyGroupsByAccessLevel: Map[WorkspaceAccessLevel, WorkbenchEmail], bucketName: String, labels: Map[String, String], parentSpan: Span = null): Future[GoogleWorkspaceInfo]

  def getGoogleProject(projectName: RawlsBillingProjectName): Future[Project]

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

  def getCromwellAuthBucketName(billingProject: RawlsBillingProjectName) = s"cromwell-auth-${billingProject.value}"

  def getStorageLogsBucketName(billingProject: RawlsBillingProjectName) = s"storage-logs-${billingProject.value}"

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
    * @param projectName  the name of the project that owns the bucket
    * @param bucketName the name of the bucket to query
    * @param maxResults (optional) the page size to use when fetching objects
    * @return the size in bytes of the data stored in the bucket
    */
  def getBucketUsage(projectName: RawlsBillingProjectName, bucketName: String, maxResults: Option[Long] = None): Future[BigInt]

  /**
    * Gets a Google bucket.
    *
    * Note: takes an implicit ExecutionContext to override the class-level ExecutionContext. This
    * is because this method is used for health monitoring, and we want health checks to use a
    * different execution context (thread pool) than user-facing operations.
    *
    * @param bucketName the bucket name
    * @param executionContext the execution context to use for aysnc operations
    * @return optional Google bucket
    */
  def getBucket(bucketName: String)(implicit executionContext: ExecutionContext): Future[Option[Bucket]]

  def getBucketACL(bucketName: String): Future[Option[List[BucketAccessControl]]]

  def diagnosticBucketRead(userInfo: UserInfo, bucketName: String): Future[Option[ErrorReport]]

  def listObjectsWithPrefix(bucketName: String, objectNamePrefix: String): Future[List[StorageObject]]

  def storeCromwellMetadata(objectName: GcsBlobName, body: fs2.Stream[fs2.Pure, Byte]): Stream[IO, Unit]

  def copyFile(sourceBucket: String, sourceObject: String, destinationBucket: String, destinationObject: String): Future[Option[StorageObject]]

  def addEmailToGoogleGroup(groupEmail: String, emailToAdd: String): Future[Unit]

  def removeEmailFromGoogleGroup(groupEmail: String, emailToRemove: String): Future[Unit]

  def listBillingAccounts(userInfo: UserInfo): Future[Seq[RawlsBillingAccount]]

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
  def listBillingAccountsUsingServiceCredential(implicit executionContext: ExecutionContext): Future[Seq[RawlsBillingAccount]]
  def storeToken(userInfo: UserInfo, refreshToken: String): Future[Unit]
  def getToken(rawlsUserRef: RawlsUserRef): Future[Option[String]]
  def getTokenDate(rawlsUserRef: RawlsUserRef): Future[Option[DateTime]]
  def deleteToken(rawlsUserRef: RawlsUserRef): Future[Unit]
  def revokeToken(rawlsUserRef: RawlsUserRef): Future[Unit]

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

  def toGoogleGroupName(groupName: RawlsGroupName): String
  def toBillingProjectGroupName(billingProjectName: RawlsBillingProjectName, role: ProjectRoles.ProjectRole) = s"PROJECT_${billingProjectName.value}-${role.toString}"

  def getUserCredentials(rawlsUserRef: RawlsUserRef): Future[Option[Credential]]
  def getBucketServiceAccountCredential: Credential
  def getServiceAccountRawlsUser(): Future[RawlsUser]
  def getServiceAccountUserInfo(): Future[UserInfo]

  def getBucketDetails(bucket: String, project: RawlsBillingProjectName): Future[WorkspaceBucketOptions]

  /**
   * The project creation process is now mostly handled by Deployment Manager.
   *
   * - First, we call Deployment Manager, telling it to kick off its template and create the new project. This gives us back
   * an operation that needs to be polled.
   *
   * - Polling is handled by CreatingBillingProjectMonitor. Once the deployment is completed, CBPM deletes the deployment, as
   * there is a per-project limit on number of deployments, and then marks the project as fully created.
   */
  def createProject(projectName: RawlsBillingProjectName, billingAccount: RawlsBillingAccount, dmTemplatePath: String, highSecurityNetwork: Boolean, enableFlowLogs: Boolean, privateIpGoogleAccess: Boolean, requesterPaysRole: String, ownerGroupEmail: WorkbenchEmail, computeUserGroupEmail: WorkbenchEmail, projectTemplate: ProjectTemplate, parentFolderId: Option[String]): Future[RawlsBillingProjectOperationRecord]

  /**
    *
    */
  def cleanupDMProject(projectName: RawlsBillingProjectName): Future[Unit]

  /**
    * Removes the IAM policies from the project's existing policies
    * @return true if the policy was actually changed
    */
  def removePolicyBindings(projectName: RawlsBillingProjectName, policiesToRemove: Map[String, Set[String]]): Future[Boolean] = updatePolicyBindings(projectName) { existingPolicies =>
    val updatedKeysWithRemovedPolicies: Map[String, Set[String]] = policiesToRemove.keys.map { k =>
      val existingForKey = existingPolicies.get(k).getOrElse(Set.empty)
      val updatedForKey = existingForKey diff policiesToRemove(k)
      k -> updatedForKey
    }.toMap

    // Use standard Map ++ instead of semigroup because we want to replace the original values
    existingPolicies ++ updatedKeysWithRemovedPolicies
  }

  /**
    * Adds the IAM policies to the project's existing policies
    * @return true if the policy was actually changed
    */
  def addPolicyBindings(projectName: RawlsBillingProjectName, policiesToAdd: Map[String, Set[String]]): Future[Boolean] = updatePolicyBindings(projectName) { existingPolicies =>
    // |+| is a semigroup: it combines a map's keys by combining their values' members instead of replacing them
    import cats.implicits._
    existingPolicies |+| policiesToAdd
  }

  /**
    * Internal function to update project IAM bindings.
    * @param projectName google project name
    * @param updatePolicies function (existingPolicies => updatedPolicies). May return policies with no members
    *                       which will be handled appropriately when sent to google.
    * @return true if google was called to update policies, false otherwise
    */
  protected def updatePolicyBindings(projectName: RawlsBillingProjectName)(updatePolicies: Map[String, Set[String]] => Map[String, Set[String]]): Future[Boolean]

  /**
    *
    * @param billingProject
    * @param bucketName
    * @param readers emails of users to be granted read access
    * @return bucket name
    */
  def grantReadAccess(billingProject: RawlsBillingProjectName,
                      bucketName: String,
                      readers: Set[WorkbenchEmail]): Future[String]

  def pollOperation(operationId: OperationId): Future[OperationStatus]
  def deleteProject(projectName: RawlsBillingProjectName): Future[Unit]

  def addRoleToGroup(projectName: RawlsBillingProjectName, groupEmail: WorkbenchEmail, role: String): Future[Boolean]
  def removeRoleFromGroup(projectName: RawlsBillingProjectName, groupEmail: WorkbenchEmail, role: String): Future[Boolean]

  def getAccessTokenUsingJson(saKey: String) : Future[String]
  def getUserInfoUsingJson(saKey: String): Future[UserInfo]

  def labelSafeString(s: String, prefix: String = "fc-"): String = {
    // https://cloud.google.com/compute/docs/labeling-resources#restrictions
    prefix + s.toLowerCase.replaceAll("[^a-z0-9\\-_]", "-").take(63)
  }

  def addProjectToFolder(projectName: RawlsBillingProjectName, folderId: String): Future[Unit]
  def getFolderId(folderName: String): Future[Option[String]]
}

object GoogleApiTypes {
  val allGoogleApiTypes = List(DeploymentManagerApi, AccessContextManagerApi)

  sealed trait GoogleApiType extends RawlsEnumeration[GoogleApiType] {
    override def toString = GoogleApiTypes.toString(this)
    override def withName(name: String) = GoogleApiTypes.withName(name)
  }

  def withName(name: String): GoogleApiType = {
    name match {
      case "DeploymentManager" => DeploymentManagerApi
      case "AccessContextManager" => AccessContextManagerApi
      case _ => throw new RawlsException(s"Invalid GoogleApiType [${name}]. Possible values: ${allGoogleApiTypes.mkString(", ")}")
    }
  }

  def withNameOpt(name: Option[String]): Option[GoogleApiType] = {
    name.flatMap(n => Try(withName(n)).toOption)
  }

  def toString(googleApiType: GoogleApiType): String = {
    googleApiType match {
      case DeploymentManagerApi => "DeploymentManager"
      case AccessContextManagerApi => "AccessContextManager"
      case _ => throw new RawlsException(s"Invalid GoogleApiType [${googleApiType}]. Possible values: ${allGoogleApiTypes.mkString(", ")}")
    }
  }

  case object DeploymentManagerApi extends GoogleApiType
  case object AccessContextManagerApi extends GoogleApiType
}

object GoogleOperationNames {
  val allGoogleOperationNames = List(DeploymentManagerCreateProject, AddProjectToPerimeter)

  sealed trait GoogleOperationName extends RawlsEnumeration[GoogleOperationName] {
    override def toString = GoogleOperationNames.toString(this)
    override def withName(name: String) = GoogleOperationNames.withName(name)
  }

  def withName(name: String): GoogleOperationName = {
    name match {
      case "dm_create_project" => DeploymentManagerCreateProject
      case "add_project_to_perimeter" => AddProjectToPerimeter
      case _ => throw new RawlsException(s"Invalid GoogleOperationName [${name}]. Possible values: ${allGoogleOperationNames.mkString(", ")}")
    }
  }

  def withNameOpt(name: Option[String]): Option[GoogleOperationName] = {
    name.flatMap(n => Try(withName(n)).toOption)
  }

  def toString(googleApiType: GoogleOperationName): String = {
    googleApiType match {
      case DeploymentManagerCreateProject => "dm_create_project"
      case AddProjectToPerimeter => "add_project_to_perimeter"
      case _ => throw new RawlsException(s"Invalid GoogleOperationName [${googleApiType}]. Possible values: ${allGoogleOperationNames.mkString(", ")}")
    }
  }

  case object DeploymentManagerCreateProject extends GoogleOperationName
  case object AddProjectToPerimeter extends GoogleOperationName
}

case class OperationId(apiType: GoogleApiTypes.GoogleApiType, operationId: String)
case class OperationStatus(done: Boolean, errorMessage: Option[String])
case class GoogleWorkspaceInfo(bucketName: String, policyGroupsByAccessLevel: Map[WorkspaceAccessLevel, WorkbenchEmail])
case class ProjectTemplate(owners: Seq[String], editors: Seq[String])
final case class HammCromwellMetadata(bucketName: GcsBucketName, topicName: ProjectTopicName)

case object ProjectTemplate {
  def from(projectTemplateConfig: Config): ProjectTemplate = {
    val projectOwners = projectTemplateConfig.getStringList("owners")
    val projectEditors = projectTemplateConfig.getStringList("editors")
    ProjectTemplate(projectOwners.asScala, projectEditors.asScala)
  }
}
