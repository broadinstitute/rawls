package org.broadinstitute.dsde.rawls.dataaccess

import org.broadinstitute.dsde.rawls.model.{Workspace, WorkspaceShort}

/**
 * Created by dvoet on 4/24/15.
 */
trait WorkspaceDAO {
  def save(workspace: Workspace)
  def load(namespace: String, name: String): Option[Workspace]
  def list(): Seq[WorkspaceShort]
  def loadShort(namespace: String, name: String): Option[WorkspaceShort]
}
