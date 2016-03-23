package org.broadinstitute.dsde.rawls.dataaccess

import java.util.UUID

import org.broadinstitute.dsde.rawls.dataaccess.slick.{WorkspaceRecord, ReadAction}
import org.broadinstitute.dsde.rawls.model.{WorkspaceName, Workspace}

/**
 * Holds information about a workspace inside a DB transaction.
 *
 * @param workspace
 */
case class SlickWorkspaceContext(workspace: Workspace) {
  val workspaceId = UUID.fromString(workspace.workspaceId)
}