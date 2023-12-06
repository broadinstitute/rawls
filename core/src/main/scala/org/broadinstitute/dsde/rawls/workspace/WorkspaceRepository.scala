package org.broadinstitute.dsde.rawls.workspace

import org.broadinstitute.dsde.rawls.dataaccess.SlickDataSource
import org.broadinstitute.dsde.rawls.model.Workspace
import org.broadinstitute.dsde.rawls.model.WorkspaceState.WorkspaceState

import java.util.UUID
import scala.concurrent.Future

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

  def deleteWorkspaceRecord(workspace: Workspace): Future[Boolean] =
    dataSource.inTransaction { access =>
      access.workspaceQuery.delete(workspace.toWorkspaceName)
    }
}
