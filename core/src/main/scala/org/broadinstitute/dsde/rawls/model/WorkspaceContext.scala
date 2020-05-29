package org.broadinstitute.dsde.rawls.model

/**
 * Holds information about a workspace inside a DB transaction.
 *
 * @param workspace
 */
case class SlickWorkspaceContext(workspace: Workspace) {
  val workspaceIdAsUUID = workspace.workspaceIdAsUUID
}