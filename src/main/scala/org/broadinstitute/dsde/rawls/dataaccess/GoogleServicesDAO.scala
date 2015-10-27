package org.broadinstitute.dsde.rawls.dataaccess

import com.google.api.client.googleapis.json.GoogleJsonResponseException
import com.google.api.services.admin.directory.model.Group
import org.broadinstitute.dsde.rawls.model.WorkspaceAccessLevel._
import org.broadinstitute.dsde.rawls.model.{ErrorReport, WorkspacePermissionsPair, UserInfo, WorkspaceACLUpdate, WorkspaceACL, WorkspaceName}
import spray.http.StatusCodes
import scala.concurrent.Future
import scala.util.Try

trait GoogleServicesDAO {

  // returns a workspaceID
  def createBucket(userInfo: UserInfo, projectId: String, workspaceId: String, workspaceName: WorkspaceName): Future[Unit]

  def deleteBucket(userInfo: UserInfo, workspaceId: String): Future[Any]

  def getACL(workspaceId: String): Future[WorkspaceACL]

  def updateACL(userEmail: String, workspaceId: String, aclUpdates: Seq[WorkspaceACLUpdate]): Future[Option[Seq[ErrorReport]]]

  def getOwners(workspaceId: String): Future[Seq[String]]

  def getMaximumAccessLevel(userId: String, workspaceId: String): Future[WorkspaceAccessLevel]

  def getWorkspaces(userId: String): Future[Seq[WorkspacePermissionsPair]]

  def getBucketName(workspaceId: String): String

  def isAdmin(userId: String): Future[Boolean]

  def addAdmin(userId: String): Future[Unit]

  def deleteAdmin(userId: String): Future[Unit]

  def listAdmins(): Future[Seq[String]]

  def createProxyGroup(userInfo: UserInfo): Future[Unit]

  def toErrorReport(throwable: Throwable) = {
    val SOURCE = "google"
    throwable match {
      case gjre: GoogleJsonResponseException =>
        val statusCode = StatusCodes.getForKey(gjre.getStatusCode)
        ErrorReport(SOURCE,ErrorReport.message(gjre),statusCode,ErrorReport.causes(gjre),Seq.empty)
      case _ =>
        ErrorReport(SOURCE,ErrorReport.message(throwable),None,ErrorReport.causes(throwable),throwable.getStackTrace)
    }
  }
}
