package org.broadinstitute.dsde.rawls.snapshot

import java.util.UUID

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import bio.terra.workspace.model.DataReferenceDescription.{CloningInstructionsEnum, ReferenceTypeEnum}
import com.google.api.client.auth.oauth2.Credential
import org.broadinstitute.dsde.rawls.dataaccess.{SamDAO, SlickDataSource}
import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.WorkspaceManagerDAO
import org.broadinstitute.dsde.rawls.model.{DataRepoSnapshot, DataRepoSnapshotList, DataRepoSnapshotReference, SamWorkspaceActions, UserInfo, WorkspaceAttributeSpecs, WorkspaceName}
import org.broadinstitute.dsde.rawls.util.{FutureSupport, WorkspaceSupport}
import spray.json.{JsObject, JsString}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

object SnapshotService {

  def constructor(dataSource: SlickDataSource, samDAO: SamDAO, workspaceManagerDAO: WorkspaceManagerDAO, serviceAccountCreds: Credential, terraDataRepoUrl: String)(userInfo: UserInfo)
                 (implicit executionContext: ExecutionContext): SnapshotService = {
    new SnapshotService(userInfo, dataSource, samDAO, workspaceManagerDAO, serviceAccountCreds, terraDataRepoUrl)
  }

}

class SnapshotService(protected val userInfo: UserInfo, val dataSource: SlickDataSource, val samDAO: SamDAO, workspaceManagerDAO: WorkspaceManagerDAO, serviceAccountCreds: Credential, terraDataRepoUrl: String)(implicit protected val executionContext: ExecutionContext) extends FutureSupport with WorkspaceSupport {

  def CreateSnapshot(workspaceName: WorkspaceName, dataRepoSnapshot: DataRepoSnapshot): Future[DataRepoSnapshotReference] = createSnapshot(workspaceName, dataRepoSnapshot)
  def GetSnapshot(workspaceName: WorkspaceName, snapshotId: String): Future[DataRepoSnapshotReference] = getSnapshot(workspaceName, snapshotId)
  def EnumerateSnapshots(workspaceName: WorkspaceName, offset: Int, limit: Int): Future[DataRepoSnapshotList] = enumerateSnapshots(workspaceName, offset, limit)

  def createSnapshot(workspaceName: WorkspaceName, snapshot: DataRepoSnapshot): Future[DataRepoSnapshotReference] = {
    getWorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.write, Some(WorkspaceAttributeSpecs(all = false))).flatMap { workspaceContext =>
      if(!workspaceStubExists(workspaceContext.workspaceId, userInfo)) {
        workspaceManagerDAO.createWorkspace(workspaceContext.workspaceId, getServiceAccountAccessToken, userInfo.accessToken)
      }

      val dataRepoReference = JsObject.apply(("instance", JsString(terraDataRepoUrl)), ("snapshot", JsString(snapshot.snapshotId)))
      val ref = workspaceManagerDAO.createDataReference(workspaceContext.workspaceId, snapshot.name, ReferenceTypeEnum.DATAREPOSNAPSHOT.getValue, dataRepoReference, CloningInstructionsEnum.NOTHING.getValue, userInfo.accessToken)

      Future.successful(DataRepoSnapshotReference(ref))
    }
  }

  def getSnapshot(workspaceName: WorkspaceName, snapshotId: String): Future[DataRepoSnapshotReference] = {
    getWorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.read, Some(WorkspaceAttributeSpecs(all = false))).flatMap { workspaceContext =>
      val ref = workspaceManagerDAO.getDataReference(workspaceContext.workspaceId, UUID.fromString(snapshotId), userInfo.accessToken)
      Future.successful(DataRepoSnapshotReference(ref))
    }
  }

  def enumerateSnapshots(workspaceName: WorkspaceName, offset: Int, limit: Int): Future[DataRepoSnapshotList] = {
    getWorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.read, Some(WorkspaceAttributeSpecs(all = false))).map { workspaceContext =>
      val ref = workspaceManagerDAO.enumerateDataReferences(workspaceContext.workspaceId, offset, limit, userInfo.accessToken)
      DataRepoSnapshotList.from(ref)
    }
  }

  private def workspaceStubExists(workspaceId: UUID, userInfo: UserInfo): Boolean = {
    Try(workspaceManagerDAO.getWorkspace(workspaceId, userInfo.accessToken)).isSuccess
  }

  private def getServiceAccountAccessToken: OAuth2BearerToken = {
    val expiresInSeconds = Option(serviceAccountCreds.getExpiresInSeconds).map(_.longValue()).getOrElse(0L)
    if (expiresInSeconds < 60*5) {
      serviceAccountCreds.refreshToken()
    }
    OAuth2BearerToken(serviceAccountCreds.getAccessToken)
  }
}