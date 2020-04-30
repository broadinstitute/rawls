package org.broadinstitute.dsde.rawls.snapshot

import java.util.UUID

import akka.http.scaladsl.model.StatusCodes
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.dataaccess.SlickDataSource
import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.WorkspaceManagerDAO
import org.broadinstitute.dsde.rawls.model.workspacemanager._
import org.broadinstitute.dsde.rawls.model.{ErrorReport, UserInfo, WorkspaceName}
import org.broadinstitute.dsde.rawls.util.FutureSupport
import spray.json.{JsObject, JsString}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

object SnapshotService {

  def constructor(dataSource: SlickDataSource, workspaceManagerDAO: WorkspaceManagerDAO, terraDataRepoUrl: String)(userInfo: UserInfo)
                 (implicit executionContext: ExecutionContext): SnapshotService = {
    new SnapshotService(userInfo, dataSource, workspaceManagerDAO, terraDataRepoUrl)
  }

}

class SnapshotService(protected val userInfo: UserInfo, dataSource: SlickDataSource, workspaceManagerDAO: WorkspaceManagerDAO, terraDataRepoUrl: String)(implicit protected val executionContext: ExecutionContext) extends FutureSupport {

  def CreateSnapshot(workspaceName: WorkspaceName, dataRepoSnapshot: DataRepoSnapshot): Future[WMDataReferenceResponse] = createSnapshot(workspaceName, dataRepoSnapshot)
  def GetSnapshot(workspaceName: WorkspaceName, snapshotId: String): Future[WMDataReferenceResponse] = getSnapshot(workspaceName, snapshotId)

  def createSnapshot(workspaceName: WorkspaceName, snapshot: DataRepoSnapshot): Future[WMDataReferenceResponse] = {
    for {
      workspaceIdOpt <- dataSource.inTransaction { dataAccess => dataAccess.workspaceQuery.getWorkspaceId(workspaceName) }
      workspaceId = workspaceIdOpt.getOrElse(throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.NotFound, s"Workspace $workspaceName not found")))
      stubExists <- workspaceStubExists(workspaceId, userInfo)
      _ <- if(!stubExists) { workspaceManagerDAO.createWorkspace(workspaceId, userInfo) } else Future.successful()
      dataRepoReference = JsObject.apply(("instance", JsString(terraDataRepoUrl)), ("snapshot", JsString(snapshot.snapshotId)))
      dataReference = WMCreateDataReferenceRequest(snapshot.name, None, Option(DataReferenceType.DataRepoSnapshot.toString), Option(dataRepoReference), CloningInstructions.COPY_NOTHING.toString, None)
      res <- workspaceManagerDAO.createDataReference(workspaceId, dataReference, userInfo)
    } yield {
      res
    }
  }

  def getSnapshot(workspaceName: WorkspaceName, snapshotId: String): Future[WMDataReferenceResponse] = {
    for {
      workspaceIdOpt <- dataSource.inTransaction { dataAccess => dataAccess.workspaceQuery.getWorkspaceId(workspaceName) }
      workspaceId = workspaceIdOpt.getOrElse(throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.NotFound, s"Workspace $workspaceName not found")))
      res <- workspaceManagerDAO.getDataReference(workspaceId, UUID.fromString(snapshotId), userInfo)
    } yield {
      res
    }
  }

  private def workspaceStubExists(workspaceId: UUID, userInfo: UserInfo): Future[Boolean] = {
    toFutureTry(workspaceManagerDAO.getWorkspace(workspaceId, userInfo)) map {
      case Success(_) => true
      case Failure(_) => false
    }
  }

}