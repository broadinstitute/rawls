package org.broadinstitute.dsde.rawls.snapshot

import java.util.UUID

import akka.http.scaladsl.model.StatusCodes
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.dataaccess.SlickDataSource
import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.WorkspaceManagerDAO
import org.broadinstitute.dsde.rawls.model.{ErrorReport, UserInfo, WorkspaceName}
import org.broadinstitute.dsde.rawls.model.workspacemanager.{CloningInstructions, DataReferenceType, DataRepoSnapshot, WMCreateDataReferenceRequest}
import spray.json.{JsObject, JsString}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

object SnapshotService {

  def constructor(dataSource: SlickDataSource, workspaceManagerDAO: WorkspaceManagerDAO, terraDataRepoUrl: String)(userInfo: UserInfo)
                 (implicit executionContext: ExecutionContext) = {
    new SnapshotService(userInfo, dataSource, workspaceManagerDAO, terraDataRepoUrl)
  }

}

class SnapshotService(protected val userInfo: UserInfo, dataSource: SlickDataSource, workspaceManagerDAO: WorkspaceManagerDAO, terraDataRepoUrl: String)(implicit protected val executionContext: ExecutionContext) {

  def CreateSnapshot(workspaceName: WorkspaceName, dataRepoSnapshot: DataRepoSnapshot) = createSnapshot(workspaceName, dataRepoSnapshot)

  def createSnapshot(workspaceName: WorkspaceName, snapshot: DataRepoSnapshot) = {
    for {
      workspaceIdOpt <- dataSource.inTransaction { dataAccess => dataAccess.workspaceQuery.getWorkspaceId(workspaceName) }
      workspaceId = workspaceIdOpt.getOrElse(throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.NotFound, s"Workspace $workspaceName not found")))
      stubExists <- workspaceStubExists(workspaceId, userInfo)
      _ <- if(!stubExists) { workspaceManagerDAO.createWorkspace(workspaceId, userInfo) } else Future.successful()
    } yield {
      val dataRepoReference = JsObject.apply(("instance", JsString(terraDataRepoUrl)), ("snapshot", JsString(snapshot.snapshotId)))
      val dataReference = WMCreateDataReferenceRequest(snapshot.name, None, Option(DataReferenceType.DataRepoSnapshot.toString), Option(dataRepoReference), CloningInstructions.COPY_NOTHING.toString, None)

      workspaceManagerDAO.createDataReference(workspaceId, dataReference, userInfo)
    }
  }

  private def workspaceStubExists(workspaceId: UUID, userInfo: UserInfo): Future[Boolean] = {
    Try {
      workspaceManagerDAO.getWorkspace(workspaceId, userInfo)
    } match {
      case Success(_) => Future.successful(true)
      case Failure(_) => Future.successful(false)
    }
  }

}