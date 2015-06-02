package org.broadinstitute.dsde.rawls.dataaccess

import org.broadinstitute.dsde.rawls.model.Workspace

/**
 * Created by dvoet on 4/24/15.
 */
trait WorkspaceDAO {
  def save(workspace: Workspace, txn: RawlsTransaction): Workspace
  def load(namespace: String, name: String, txn: RawlsTransaction): Option[Workspace]
  def list(txn: RawlsTransaction): Seq[Workspace]
}
