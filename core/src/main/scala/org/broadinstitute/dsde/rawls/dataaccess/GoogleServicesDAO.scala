package org.broadinstitute.dsde.rawls.dataaccess

import akka.actor.ActorRef
import com.google.api.client.auth.oauth2.Credential
import com.google.api.services.admin.directory.model.Group
import com.google.api.services.storage.model.{BucketAccessControl, Bucket}
import org.broadinstitute.dsde.rawls.dataaccess.slick.RawlsBillingProjectOperationRecord
import org.broadinstitute.dsde.rawls.model.WorkspaceAccessLevels._
import org.broadinstitute.dsde.rawls.model._
import org.joda.time.DateTime
import spray.json.JsObject
import scala.concurrent.Future
import scala.util.Try

abstract class GoogleServicesDAO(groupsPrefix: String) extends ErrorReportable {
  val errorReportSource = ErrorReportSource("google")

  val CREATE_PROJECT_OPERATION = "create_project"

  val billingEmail: String

  // returns bucket and group information
  def setupWorkspace(userInfo: UserInfo, project: RawlsBillingProject, workspaceId: String, workspaceName: WorkspaceName, realm: Option[ManagedGroupRef], realmProjectOwnerIntersection: Option[Set[RawlsUserRef]]): Future[GoogleWorkspaceInfo]

  def createCromwellAuthBucket(billingProject: RawlsBillingProjectName): Future[String]

  /** Deletes a bucket from Google Cloud Storage. If the bucket is not empty, all objects in the bucket will be marked
    * for deletion (see below).
    *
    * Warning: Direct calls to this method may cause deletion to not happen if it has to be deferred and rawls is
    * restarted at an inopportune time! The preferred way to delete a bucket from rawls code is to send a
    * [[org.broadinstitute.dsde.rawls.monitor.BucketDeletionMonitor.DeleteBucket DeleteBucket]] message to the
    * [[org.broadinstitute.dsde.rawls.monitor.BucketDeletionMonitor]] which itself calls this method. The monitor does a
    * better job of retrying deletion if it cannot be done immediately.
    *
    * If the bucket is not empty, the bucket's lifecycle rule is set to delete any objects older than 0 days. This
    * effectively marks all objects in the bucket for deletion the next time GCS inspects the bucket (up to 24 hours
    * later at the time of this writing; see [[https://cloud.google.com/storage/docs/lifecycle#behavior]]).
    * Rawls will periodically retry the bucket deletion until it succeeds.
    *
    * This strategy is inspired by [[http://blog.iangsy.com/2014/04/google-cloud-storage-deleting-full.html]].
    *
    * @param bucketName the name of the bucket to delete
    * @param monitorRef a [[org.broadinstitute.dsde.rawls.monitor.BucketDeletionMonitor]] to handle deferred actions
    * @return a [[scala.concurrent.Future]] for whatever return value is appropriate for the implementation
    */
  def deleteBucket(bucketName: String, monitorRef: ActorRef): Future[Any]

  def getCromwellAuthBucketName(billingProject: RawlsBillingProjectName) = s"cromwell-auth-${billingProject.value}"

  def getStorageLogsBucketName(billingProject: RawlsBillingProjectName) = s"storage-logs-${billingProject.value}"

  def isAdmin(userEmail: String): Future[Boolean]

  def isLibraryCurator(userEmail: String): Future[Boolean]

  def addLibraryCurator(userEmail: String): Future[Unit]

  def removeLibraryCurator(userEmail: String): Future[Unit]

  def hasGoogleRole(roleGroupName: String, userEmail: String): Future[Boolean]

  /**
   *
   * @param group
   * @return None if the google group does not exist, Some(Map.empty) if there are no members, key is the actual
   *         email address in the google group, value is the rawls user or group reference or None if neither
   */
  def listGroupMembers(group: RawlsGroup): Future[Option[Map[String, Option[Either[RawlsUserRef, RawlsGroupRef]]]]]

  def createProxyGroup(user: RawlsUser): Future[Unit]

  def deleteProxyGroup(user: RawlsUser): Future[Unit]

  def addUserToProxyGroup(user: RawlsUser): Future[Unit]

  def removeUserFromProxyGroup(user: RawlsUser): Future[Unit]

  def isUserInProxyGroup(user: RawlsUser): Future[Boolean]

  def createGoogleGroup(groupRef: RawlsGroupRef): Future[RawlsGroup]

  def isEmailInGoogleGroup(email: String, groupName: String): Future[Boolean]

  def getGoogleGroup(groupName: String): Future[Option[Group]]

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

  def getBucket(bucketName: String): Future[Option[Bucket]]

  def getBucketACL(bucketName: String): Future[Option[List[BucketAccessControl]]]

  def diagnosticBucketWrite(user: RawlsUser, bucketName: String): Future[Option[ErrorReport]]

  def diagnosticBucketRead(userInfo: UserInfo, bucketName: String): Future[Option[ErrorReport]]

  def addMemberToGoogleGroup(group: RawlsGroup, member: Either[RawlsUser, RawlsGroup]): Future[Unit]

  def addEmailToGoogleGroup(groupEmail: String, emailToAdd: String): Future[Unit]

  def removeMemberFromGoogleGroup(group: RawlsGroup, member: Either[RawlsUser, RawlsGroup]): Future[Unit]

  def removeEmailFromGoogleGroup(groupEmail: String, emailToRemove: String): Future[Unit]

  def deleteGoogleGroup(group: RawlsGroup): Future[Unit]

  def listBillingAccounts(userInfo: UserInfo): Future[Seq[RawlsBillingAccount]]

  def storeToken(userInfo: UserInfo, refreshToken: String): Future[Unit]
  def getToken(rawlsUserRef: RawlsUserRef): Future[Option[String]]
  def getTokenDate(rawlsUserRef: RawlsUserRef): Future[Option[DateTime]]
  def deleteToken(rawlsUserRef: RawlsUserRef): Future[Unit]
  def revokeToken(rawlsUserRef: RawlsUserRef): Future[Unit]

  def getGenomicsOperation(userInfo: UserInfo, jobId: String): Future[Option[JsObject]]

  def toProxyFromUser(userSubjectId: RawlsUserSubjectId): String
  def toUserFromProxy(proxy: String): String
  def toGoogleGroupName(groupName: RawlsGroupName): String
  def toBillingProjectGroupName(billingProjectName: RawlsBillingProjectName, role: ProjectRoles.ProjectRole) = s"PROJECT_${billingProjectName.value}-${role.toString}"

  def getUserCredentials(rawlsUserRef: RawlsUserRef): Future[Option[Credential]]
  def getBucketServiceAccountCredential: Credential
  def getServiceAccountRawlsUser(): Future[RawlsUser]
  def getServiceAccountUserInfo(): Future[UserInfo]

  /**
   * The project creation process has 3 steps of which this function is the first:
   *
   * - createProject creates the project in google, reserving the name if it does not exist or throwing an exception (usually) if it does.
   * This returns an asynchronous operation that creates the project which may fail. This function should do nothing else,
   * it should be fast and just get the process started.
   *
   * - beginProjectSetup runs once a project is successfully created. It sets up the billing and security then enables appropriate services.
   * Enabling a service is another asynchronous operation. There will be an asynchronous operation for each service enabled
   * but it seems Google is smrt and will group some operations together
   * so the operation ids may not be unique. All google calls that do NOT require enabled services should go in this function.
   *
   * - completeProjectSetup once all the services are enabled (specifically compute and storage) we can create buckets and set the
   * compute usage export bucket. All google calls that DO require enabled APIs should go in this function.
   *
   * @param projectName
   * @param billingAccount used for a label on the project
   * @return an operation for creating the project
   */
  def createProject(projectName: RawlsBillingProjectName, billingAccount: RawlsBillingAccount): Future[RawlsBillingProjectOperationRecord]

  /**
   * Second step of project creation. See createProject for more details.
   *
   * @param project
   * @param projectTemplate
   * @param groupEmailsByRef emails of any subgroups of the project owner or user groups
   *                         (note that this is not required for users because we can infer their proxy group from subject id)
   * @return an operation for each service api specified in projectTemplate
   */
  def beginProjectSetup(project: RawlsBillingProject, projectTemplate: ProjectTemplate, groupEmailsByRef: Map[RawlsGroupRef, RawlsGroupEmail]): Future[Try[Seq[RawlsBillingProjectOperationRecord]]]

  /**
   * Last step of project creation. See createProject for more details.
   * @param project
   * @return
   */
  def completeProjectSetup(project: RawlsBillingProject): Future[Try[Unit]]

  def pollOperation(rawlsBillingProjectOperation: RawlsBillingProjectOperationRecord): Future[RawlsBillingProjectOperationRecord]
  def deleteProject(projectName: RawlsBillingProjectName): Future[Unit]
}

case class GoogleWorkspaceInfo(bucketName: String, accessGroupsByLevel: Map[WorkspaceAccessLevel, RawlsGroup], intersectionGroupsByLevel: Option[Map[WorkspaceAccessLevel, RawlsGroup]])
case class ProjectTemplate(policies: Map[String, Seq[String]], services: Seq[String])
