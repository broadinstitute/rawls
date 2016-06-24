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
import scala.concurrent.Future

abstract class GoogleServicesDAO(groupsPrefix: String) extends ErrorReportable {
  override val errorReportSource = "google"

  // returns bucket and group information
  def setupWorkspace(userInfo: UserInfo, projectId: String, workspaceId: String, workspaceName: WorkspaceName, realm: Option[RawlsGroupRef]): Future[GoogleWorkspaceInfo]

  def createCromwellAuthBucket(billingProject: RawlsBillingProjectName): Future[String]

  def deleteBucket(bucketName: String, monitorRef: ActorRef): Future[Any]

  def getCromwellAuthBucketName(billingProject: RawlsBillingProjectName) = s"cromwell-auth-${billingProject.value}"

  def isAdmin(userId: String): Future[Boolean]

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

  def addMemberToGoogleGroup(group: RawlsGroup, member: Either[RawlsUser, RawlsGroup]): Future[Unit]

  def removeMemberFromGoogleGroup(group: RawlsGroup, memberToAdd: Either[RawlsUser, RawlsGroup]): Future[Unit]

  def deleteGoogleGroup(group: RawlsGroup): Future[Unit]

  def storeToken(userInfo: UserInfo, refreshToken: String): Future[Unit]
  def getToken(rawlsUserRef: RawlsUserRef): Future[Option[String]]
  def getTokenDate(userInfo: UserInfo): Future[Option[DateTime]]
  def deleteToken(rawlsUserRef: RawlsUserRef): Future[Unit]
  def revokeToken(rawlsUserRef: RawlsUserRef): Future[Unit]

  def toProxyFromUser(userSubjectId: RawlsUserSubjectId): String
  def toUserFromProxy(proxy: String): String
  def toGoogleGroupName(groupName: RawlsGroupName): String

  def getUserCredentials(rawlsUserRef: RawlsUserRef): Future[Option[Credential]]
  def getBucketServiceAccountCredential: Credential
  def getServiceAccountRawlsUser(): Future[RawlsUser]
  def getServiceAccountUserInfo(): Future[UserInfo]
}

case class GoogleWorkspaceInfo(bucketName: String, accessGroupsByLevel: Map[WorkspaceAccessLevel, RawlsGroup], intersectionGroupsByLevel: Option[Map[WorkspaceAccessLevel, RawlsGroup]])