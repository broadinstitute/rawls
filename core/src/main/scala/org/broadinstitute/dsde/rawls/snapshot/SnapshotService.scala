package org.broadinstitute.dsde.rawls.snapshot

import java.util.UUID

import akka.http.scaladsl.model.StatusCodes
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.dataaccess.SlickDataSource
import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.WorkspaceManagerDAO
import org.broadinstitute.dsde.rawls.model.{CloningInstructions, DataReferenceType, DataRepoSnapshot, DataRepoSnapshotReference, ErrorReport, UserInfo, WorkspaceName}
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

  def CreateSnapshot(workspaceName: WorkspaceName, dataRepoSnapshot: DataRepoSnapshot): Future[DataRepoSnapshotReference] = createSnapshot(workspaceName, dataRepoSnapshot)
  def GetSnapshot(workspaceName: WorkspaceName, snapshotId: String): Future[DataRepoSnapshotReference] = getSnapshot(workspaceName, snapshotId)

  def createSnapshot(workspaceName: WorkspaceName, snapshot: DataRepoSnapshot): Future[DataRepoSnapshotReference] = {
    for {
      workspaceIdOpt <- dataSource.inTransaction { dataAccess => dataAccess.workspaceQuery.getWorkspaceId(workspaceName) }
      workspaceId = workspaceIdOpt.getOrElse(throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.NotFound, s"Workspace $workspaceName not found")))
      stubExists <- workspaceStubExists(workspaceId, userInfo)
      _ <- if(!stubExists) { workspaceManagerDAO.createWorkspace(workspaceId, userInfo) } else Future.successful()
      dataRepoReference = JsObject.apply(("instance", JsString(terraDataRepoUrl)), ("snapshot", JsString(snapshot.snapshotId)))
      res <- workspaceManagerDAO.createDataReference(workspaceId, snapshot.name, DataReferenceType.DataRepoSnapshot.toString, dataRepoReference, CloningInstructions.COPY_NOTHING.toString, userInfo)
    } yield {
      DataRepoSnapshotReference(res.getReferenceId.toString, res.getName, res.getWorkspaceId.toString, Option(res.getReferenceType.toString), Option(res.getReference), res.getCloningInstructions.toString)
    }
  }

  def getSnapshot(workspaceName: WorkspaceName, snapshotId: String): Future[DataRepoSnapshotReference] = {
    for {
      workspaceIdOpt <- dataSource.inTransaction { dataAccess => dataAccess.workspaceQuery.getWorkspaceId(workspaceName) }
      workspaceId = workspaceIdOpt.getOrElse(throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.NotFound, s"Workspace $workspaceName not found")))
      res <- workspaceManagerDAO.getDataReference(workspaceId, UUID.fromString(snapshotId), userInfo)
    } yield {
      DataRepoSnapshotReference(res.getReferenceId.toString, res.getName, res.getWorkspaceId.toString, Option(res.getReferenceType.toString), Option(res.getReference), res.getCloningInstructions.toString)
    }
  }

  private def workspaceStubExists(workspaceId: UUID, userInfo: UserInfo): Future[Boolean] = {
    toFutureTry(workspaceManagerDAO.getWorkspace(workspaceId, userInfo)) map {
      case Success(_) => true
      case Failure(_) => false
    }
  }

}