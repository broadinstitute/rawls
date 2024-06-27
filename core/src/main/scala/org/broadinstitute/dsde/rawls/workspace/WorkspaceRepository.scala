package org.broadinstitute.dsde.rawls.workspace

import akka.http.scaladsl.model.StatusCodes
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.dataaccess.SlickDataSource
import org.broadinstitute.dsde.rawls.model.Attributable.AttributeMap
import org.broadinstitute.dsde.rawls.model.{
  ErrorReport,
  RawlsRequestContext,
  Workspace,
  WorkspaceAttributeSpecs,
  WorkspaceName,
  WorkspaceState
}
import org.broadinstitute.dsde.rawls.model.WorkspaceState.WorkspaceState
import org.broadinstitute.dsde.rawls.util.TracingUtils.traceDBIOWithParent
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

  def getWorkspace(workspaceId: UUID): Future[Option[Workspace]] = getWorkspace(workspaceId, None)

  def getWorkspace(workspaceId: UUID, attributeSpecs: Option[WorkspaceAttributeSpecs]): Future[Option[Workspace]] =
    dataSource.inTransaction { access =>
      access.workspaceQuery.findV2WorkspaceById(workspaceId, attributeSpecs)
    }

  def getWorkspace(workspaceName: WorkspaceName): Future[Option[Workspace]] = getWorkspace(workspaceName, None)

  def getWorkspace(workspaceName: WorkspaceName,
                   attributeSpecs: Option[WorkspaceAttributeSpecs]
  ): Future[Option[Workspace]] =
    dataSource.inTransaction { dataAccess =>
      dataAccess.workspaceQuery.findV2WorkspaceByName(workspaceName, attributeSpecs)
    }

  def createWorkspace(workspace: Workspace): Future[Workspace] =
    dataSource.inTransaction { access =>
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

  def deleteWorkspace(workspace: Workspace): Future[Boolean] =
    dataSource.inTransaction { access =>
      access.workspaceQuery.delete(workspace.toWorkspaceName)
    }

  def deleteWorkspace(workspaceName: WorkspaceName): Future[Boolean] =
    dataSource.inTransaction { access =>
      access.workspaceQuery.delete(workspaceName)
    }

  def updateCompletedCloneWorkspaceFileTransfer(wsId: UUID, finishTime: DateTime): Future[Int] =
    dataSource.inTransaction(_.workspaceQuery.updateCompletedCloneWorkspaceFileTransfer(wsId, finishTime.toDate))

  def createMCWorkspace(workspaceId: UUID,
                        workspaceName: WorkspaceName,
                        attributes: AttributeMap,
                        parentContext: RawlsRequestContext,
                        state: WorkspaceState = WorkspaceState.Ready
  )(implicit ex: ExecutionContext): Future[Workspace] =
    dataSource.inTransaction { access =>
      for {
        _ <- access.workspaceQuery.getWorkspaceId(workspaceName).map { workspaceId =>
          if (workspaceId.isDefined)
            throw RawlsExceptionWithErrorReport(
              ErrorReport(StatusCodes.Conflict, s"Workspace '$workspaceName' already exists")
            )
        }
        currentDate = DateTime.now
        workspace = Workspace.buildMcWorkspace(
          namespace = workspaceName.namespace,
          name = workspaceName.name,
          workspaceId = workspaceId.toString,
          createdDate = currentDate,
          lastModified = currentDate,
          createdBy = parentContext.userInfo.userEmail.value,
          attributes = attributes,
          state
        )
        newWorkspace <- traceDBIOWithParent("saveMultiCloudWorkspace", parentContext)(_ =>
          access.workspaceQuery.createOrUpdate(workspace)
        )
      } yield newWorkspace
    }

}
