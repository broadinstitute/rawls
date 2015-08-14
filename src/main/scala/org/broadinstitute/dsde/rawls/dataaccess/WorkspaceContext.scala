package org.broadinstitute.dsde.rawls.dataaccess

import com.tinkerpop.blueprints.{Graph, Vertex}
import org.broadinstitute.dsde.rawls.model.{WorkspaceName, Workspace}

/**
 * Holds information about a workspace inside a DB transaction.
 *
 * @param workspaceName
 * @param bucketName
 * @param workspaceVertex
 */
case class WorkspaceContext(workspaceName: WorkspaceName, bucketName: String, workspaceVertex: Vertex) {
  override def toString = workspaceName.toString // used in error messages
}