package org.broadinstitute.dsde.rawls.workspace

import akka.http.scaladsl.model.StatusCodes
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.dataaccess.SlickDataSource
import org.broadinstitute.dsde.rawls.model.{ErrorReport, Workspace, WorkspaceName}
import org.broadinstitute.dsde.rawls.model.WorkspaceState.WorkspaceState
import org.joda.time.DateTime

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

/**
  * Data access for rawls workspaces
  *
  * The intention of this class is to hide direct dependencies on Slick behind a relatively clean interface
  * to ease testability of higher level business logic.
  */
class WorkspaceRepository(dataSource: SlickDataSource) {

  def getWorkspace(workspaceId: UUID): Future[Option[Workspace]] =
    dataSource.inTransaction { access =>
      access.workspaceQuery.findById(workspaceId.toString)
    }

  def createWorkspace(workspace: Workspace)(implicit ec: ExecutionContext): Future[Workspace] =
    dataSource.inTransaction { access =>
      access.workspaceQuery.getWorkspaceId(workspace.toWorkspaceName).map { workspaceId =>
        if (workspaceId.isDefined)
          throw RawlsExceptionWithErrorReport(
            ErrorReport(StatusCodes.Conflict, s"Workspace '${workspace.toWorkspaceName}' already exists")
          )
      }
      access.workspaceQuery.createOrUpdate(workspace)
    }


  def updateState(workspaceId: UUID, state: WorkspaceState): Future[Int] =
    dataSource.inTransaction { access =>
      access.workspaceQuery.updateState(workspaceId, state)
    }

  def setFailedState(workspaceId: UUID, state: WorkspaceState, message: String): Future[Int] =
    dataSource.inTransaction { access =>
      access.workspaceQuery.updateStateWithErrorMessage(workspaceId, state, message)
    }

  def deleteWorkspaceRecord(workspace: Workspace): Future[Boolean] =
    dataSource.inTransaction { access =>
      access.workspaceQuery.delete(workspace.toWorkspaceName)
    }

  def deleteWorkspaceRecord(workspaceName: WorkspaceName): Future[Boolean] =
    dataSource.inTransaction { access =>
      access.workspaceQuery.delete(workspaceName)
    }

  def updateCompletedCloneWorkspaceFileTransfer(wsId: UUID, finishTime: DateTime): Future[Int] = {
    dataSource.inTransaction(_.workspaceQuery.updateCompletedCloneWorkspaceFileTransfer(wsId, finishTime.toDate))
  }


/*
private def createNewWorkspaceRecord(workspaceId: UUID,
                                     request: WorkspaceRequest,
                                     parentContext: RawlsRequestContext,
                                     state: WorkspaceState = WorkspaceState.Ready
): Future[Workspace] =
  dataSource.inTransaction { access =>
    for {
      _ <- failIfWorkspaceExists(request.toWorkspaceName)
      newWorkspace <- createMultiCloudWorkspaceInDatabase(
        workspaceId.toString,
        request.toWorkspaceName,
        request.attributes,
        access,
        parentContext,
        state
      )
    } yield newWorkspace
  }

private def createMultiCloudWorkspaceInDatabase(workspaceId: String,
                                                workspaceName: WorkspaceName,
                                                attributes: AttributeMap,
                                                dataAccess: DataAccess,
                                                parentContext: RawlsRequestContext,
                                                state: WorkspaceState
): ReadWriteAction[Workspace] = {
  val currentDate = DateTime.now
  val workspace = Workspace.buildMcWorkspace(
    namespace = workspaceName.namespace,
    name = workspaceName.name,
    workspaceId = workspaceId,
    createdDate = currentDate,
    lastModified = currentDate,
    createdBy = ctx.userInfo.userEmail.value,
    attributes = attributes,
    state
  )
  traceDBIOWithParent("saveMultiCloudWorkspace", parentContext)(_ =>
    dataAccess.workspaceQuery.createOrUpdate(workspace)
  )
}
 */
}
