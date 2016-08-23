package org.broadinstitute.dsde.rawls.dataaccess

import akka.actor.ActorRef
import com.google.api.client.auth.oauth2.Credential
import com.google.api.client.googleapis.json.GoogleJsonResponseException
import com.google.api.services.admin.directory.model.Group
import com.google.api.services.storage.model.{BucketAccessControl, Bucket}
import org.broadinstitute.dsde.rawls.model.WorkspaceAccessLevels._
import org.broadinstitute.dsde.rawls.model._
import org.joda.time.DateTime
import spray.http.StatusCodes
import spray.json.JsObject
import scala.concurrent.Future

abstract class GoogleServicesDAO(groupsPrefix: String) extends ErrorReportable {
  override val errorReportSource = "google"

  val billingEmail: String

  // returns bucket and group information
  def setupWorkspace(userInfo: UserInfo, projectId: String, workspaceId: String, workspaceName: WorkspaceName, realm: Option[RawlsGroupRef]): Future[GoogleWorkspaceInfo]

  def createCromwellAuthBucket(billingProject: RawlsBillingProjectName): Future[String]

  def deleteBucket(bucketName: String, monitorRef: ActorRef): Future[Any]

  def getCromwellAuthBucketName(billingProject: RawlsBillingProjectName) = s"cromwell-auth-${billingProject.value}"

  def isAdmin(userId: String): Future[Boolean]

  def isLibraryCurator(userId: String): Future[Boolean]

  def addLibraryCurator(userEmail: String): Future[Unit]

  def removeLibraryCurator(userEmail: String): Future[Unit]

  def hasGoogleRole(userId: String, roleGroupName: String): Future[Boolean]

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

  def getUserCredentials(rawlsUserRef: RawlsUserRef): Future[Option[Credential]]
  def getBucketServiceAccountCredential: Credential
  def getServiceAccountRawlsUser(): Future[RawlsUser]
  def getServiceAccountUserInfo(): Future[UserInfo]

  def createProject(projectName: RawlsBillingProjectName, billingAccount: RawlsBillingAccountName, projectTemplate: ProjectTemplate): Future[Unit]
}

case class GoogleWorkspaceInfo(bucketName: String, accessGroupsByLevel: Map[WorkspaceAccessLevel, RawlsGroup], intersectionGroupsByLevel: Option[Map[WorkspaceAccessLevel, RawlsGroup]])
case class ProjectTemplate(policies: Map[String, Seq[String]], services: Seq[String])
