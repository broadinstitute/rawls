package org.broadinstitute.dsde.rawls.dataaccess

import akka.actor.ActorRef
import com.google.api.client.auth.oauth2.Credential
import com.google.api.services.admin.directory.model.Group
import com.google.api.services.storage.model.{BucketAccessControl, Bucket}
import org.broadinstitute.dsde.rawls.model.WorkspaceAccessLevels._
import org.broadinstitute.dsde.rawls.model._
import org.joda.time.DateTime
import spray.json.JsObject
import scala.concurrent.Future
import scala.util.Try

abstract class GoogleServicesDAO(groupsPrefix: String) extends ErrorReportable {
  override val errorReportSource = "google"

  val billingEmail: String

  // returns bucket and group information
  def setupWorkspace(userInfo: UserInfo, project: RawlsBillingProject, workspaceId: String, workspaceName: WorkspaceName, realm: Option[RawlsGroupRef]): Future[GoogleWorkspaceInfo]

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

  def isAdmin(userEmail: String): Future[Boolean]

  def isLibraryCurator(userEmail: String): Future[Boolean]

  def addLibraryCurator(userEmail: String): Future[Unit]

  def removeLibraryCurator(userEmail: String): Future[Unit]

  def hasGoogleRole(roleGroupName: String, userEmail: String): Future[Boolean]

  /**
   *
   * @param group
   * @return None if the google group does not exist, Some(Seq.empty) if there are no members
   */
  def listGroupMembers(group: RawlsGroup): Future[Option[Set[Either[RawlsUserRef, RawlsGroupRef]]]]

  def createProxyGroup(user: RawlsUser): Future[Unit]

  def deleteProxyGroup(user: RawlsUser): Future[Unit]

  def addUserToProxyGroup(user: RawlsUser): Future[Unit]

  def removeUserFromProxyGroup(user: RawlsUser): Future[Unit]

  def isUserInProxyGroup(user: RawlsUser): Future[Boolean]

  def createGoogleGroup(groupRef: RawlsGroupRef): Future[RawlsGroup]

  def isEmailInGoogleGroup(email: String, groupName: String): Future[Boolean]

  def getGoogleGroup(groupName: String): Future[Option[Group]]

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

  def getGenomicsOperation(jobId: String): Future[Option[JsObject]]

  def toProxyFromUser(userSubjectId: RawlsUserSubjectId): String
  def toUserFromProxy(proxy: String): String
  def toGoogleGroupName(groupName: RawlsGroupName): String
  def toBillingProjectGroupName(billingProjectName: RawlsBillingProjectName, role: ProjectRoles.ProjectRole) = s"PROJECT_${billingProjectName.value}-${role.toString}"

  def getUserCredentials(rawlsUserRef: RawlsUserRef): Future[Option[Credential]]
  def getBucketServiceAccountCredential: Credential
  def getServiceAccountRawlsUser(): Future[RawlsUser]
  def getServiceAccountUserInfo(): Future[UserInfo]

  /**
   * creates a google project under the specified billing account
   * @param projectName
   * @param billingAccount
   * @return
   */
  def createProject(projectName: RawlsBillingProjectName, billingAccount: RawlsBillingAccount): Future[Unit]
  def deleteProject(projectName: RawlsBillingProjectName): Future[Unit]

  /**
   * does all the things to make the specified project usable (security, enabling apis, etc.)
   * @param project
   * @param projectTemplate
   * @param groupEmailsByRef emails of any subgroups of the project owner or user groups
   *                         (note that this is not required for users because we can infer their proxy group from subject id)
   * @return
   */
  def setupProject(project: RawlsBillingProject, projectTemplate: ProjectTemplate, groupEmailsByRef: Map[RawlsGroupRef, RawlsGroupEmail]): Future[Try[Unit]]
}

case class GoogleWorkspaceInfo(bucketName: String, accessGroupsByLevel: Map[WorkspaceAccessLevel, RawlsGroup], intersectionGroupsByLevel: Option[Map[WorkspaceAccessLevel, RawlsGroup]])
case class ProjectTemplate(policies: Map[String, Seq[String]], services: Seq[String])
