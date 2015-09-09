package org.broadinstitute.dsde.rawls.dataaccess

import com.tinkerpop.blueprints.{Graph, Vertex}
import org.broadinstitute.dsde.rawls.model.{WorkspaceName, Workspace}

/**
 * Holds information about a workspace inside a DB transaction.
 *
 * @param workspace
 * @param workspaceVertex
 */
case class WorkspaceContext(workspace: Workspace, workspaceVertex: Vertex) {
  override def toString = workspace.briefName // used in error messages
}