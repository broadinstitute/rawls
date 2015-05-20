package org.broadinstitute.dsde.rawls.dataaccess

import org.broadinstitute.dsde.rawls.model.{Workspace, WorkspaceShort}

/**
 * Created by dvoet on 4/24/15.
 */
trait WorkspaceDAO {
  def save(workspace: Workspace, txn: RawlsTransaction)
  def load(namespace: String, name: String, txn: RawlsTransaction): Option[Workspace]
  def list(txn: RawlsTransaction): Seq[WorkspaceShort]
  def loadShort(namespace: String, name: String, txn: RawlsTransaction): Option[WorkspaceShort]
}
