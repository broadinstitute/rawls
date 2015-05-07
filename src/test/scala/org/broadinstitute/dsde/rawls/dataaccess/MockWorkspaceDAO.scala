package org.broadinstitute.dsde.rawls.dataaccess

import org.broadinstitute.dsde.rawls.model.{Workspace, WorkspaceShort}

import scala.collection.mutable

/**
 * Created by dvoet on 5/10/15.
 */
object MockWorkspaceDAO extends WorkspaceDAO {
  val store = new mutable.HashMap[Tuple2[String, String], Workspace]()
  def save(workspace: Workspace): Unit = {
    store.put((workspace.namespace, workspace.name), workspace)
  }
  def load(namespace: String, name: String): Option[Workspace] = {
    try {
      Option( store((namespace, name)) )
    } catch {
      case t: NoSuchElementException => None
    }
  }

  override def loadShort(namespace: String, name: String): Option[WorkspaceShort] = load(namespace, name).map(workspace => WorkspaceShort(workspace.namespace, workspace.name, workspace.createdDate, workspace.createdBy))

  override def list(): Seq[WorkspaceShort] = store.values.map(w => WorkspaceShort(w.namespace, w.name, w.createdDate, w.createdBy)).toSeq
}
