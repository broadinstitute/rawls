package org.broadinstitute.dsde.rawls.workspace

import org.broadinstitute.dsde.rawls.dataaccess.SlickDataSource
import org.broadinstitute.dsde.rawls.model.Workspace
import org.broadinstitute.dsde.rawls.model.WorkspaceState.WorkspaceState

import java.sql.Timestamp
import java.util.{Date, UUID}
import scala.concurrent.Future

class WorkspaceRepository(dataSource: SlickDataSource) {

  def getWorkspace(workspaceId: UUID): Future[Option[Workspace]] = ???

  def findDeletion(workspace: Workspace) =
    dataSource.inTransaction { access =>
      access.WorkspaceDeletionQuery.findDeletionRecord(workspace.workspaceIdAsUUID)
    }

  def setAppDeletionStarted(workspace: Workspace) =
    dataSource.inTransaction { access =>
      access.WorkspaceDeletionQuery.setAppDeletionStarted(workspace.workspaceIdAsUUID,
                                                          new Timestamp(new Date().getTime)
      )
    }

  def setAppDeletionFinished(workspace: Workspace) =
    dataSource.inTransaction { access =>
      access.WorkspaceDeletionQuery.setAppDeletionFinished(workspace.workspaceIdAsUUID,
                                                           new Timestamp(new Date().getTime)
      )
    }

  def setRuntimeDeletionStarted(workspace: Workspace) =
    dataSource.inTransaction(
      _.WorkspaceDeletionQuery.setRuntimeDeletionStarted(workspace.workspaceIdAsUUID, new Timestamp(new Date().getTime))
    )

  def setRuntimeDeletionFinished(workspace: Workspace) =
    dataSource.inTransaction(
      _.WorkspaceDeletionQuery.setRuntimeDeletionFinished(workspace.workspaceIdAsUUID,
                                                          new Timestamp(new Date().getTime)
      )
    )

  def setWsmDeletionStarted(workspace: Workspace) =
    dataSource.inTransaction(
      _.WorkspaceDeletionQuery.setWsmDeletionStarted(workspace.workspaceIdAsUUID, new Timestamp(new Date().getTime))
    )

  def setWsmDeletionFinished(workspace: Workspace) =
    dataSource.inTransaction(
      _.WorkspaceDeletionQuery.setWsmDeletionFinished(workspace.workspaceIdAsUUID, new Timestamp(new Date().getTime))
    )

  def updateState(workspace: Workspace, state: WorkspaceState) =
    dataSource.inTransaction { access =>
      access.workspaceQuery.updateState(workspace.workspaceIdAsUUID, state)
    }

  def deleteWorkspaceDeletionStateRecord(workspace: Workspace): Future[Int] =
    dataSource.inTransaction(_.WorkspaceDeletionQuery.delete(workspace.workspaceIdAsUUID))

  def deleteWorkspaceRecord(workspace: Workspace) =
    dataSource.inTransaction { access =>
      access.workspaceQuery.delete(workspace.toWorkspaceName)
    }
}
