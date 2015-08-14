package org.broadinstitute.dsde.rawls.dataaccess

import org.broadinstitute.dsde.rawls.model.{WorkspaceName, Workspace}

/**
 * Created by dvoet on 4/24/15.
 */
trait WorkspaceDAO {
  def save(workspace: Workspace, txn: RawlsTransaction): Workspace
  def load(workspaceName: WorkspaceName, txn: RawlsTransaction): Option[Workspace]
  def loadContext(workspaceName: WorkspaceName, txn: RawlsTransaction): Option[WorkspaceContext]
  def list(txn: RawlsTransaction): TraversableOnce[Workspace]
}
