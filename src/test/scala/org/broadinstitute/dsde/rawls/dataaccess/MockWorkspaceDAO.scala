package org.broadinstitute.dsde.rawls.dataaccess

import org.broadinstitute.dsde.rawls.model.Workspace

import scala.collection.mutable

/**
 * Created by dvoet on 5/10/15.
 */
object MockWorkspaceDAO extends WorkspaceDAO {
  val store = new mutable.HashMap[Tuple2[String, String], Workspace]()
  def save(workspace: Workspace, txn: RawlsTransaction): Workspace = {
    store.put((workspace.namespace, workspace.name), workspace)
    workspace
  }
  def load(namespace: String, name: String, txn: RawlsTransaction): Option[Workspace] = {
    try {
      Option( store((namespace, name)) )
    } catch {
      case t: NoSuchElementException => None
    }
  }

  override def list(txn: RawlsTransaction): Seq[Workspace] = store.values.toSeq
}
