package org.broadinstitute.dsde.rawls.dataaccess

import org.broadinstitute.dsde.rawls.model.Entity

/**
 * Created by dvoet on 5/6/15.
 */
trait EntityDAO {
  /** gets the given entity */
  def get(workspaceNamespace: String, workspaceName: String, entityType: String, entityName: String): Option[Entity]

  /** creates or replaces an entity */
  def save(workspaceNamespace: String, workspaceName: String, entity: Entity): Entity

  /**
   * deletes an entity
   */
  def delete(workspaceNamespace: String, workspaceName: String, entityType: String, entityName: String)

  /** list all entities of the given type in the workspace */
  def list(workspaceNamespace: String, workspaceName: String, entityType: String): TraversableOnce[Entity]

  def rename(workspaceNamespace: String, workspaceName: String, entityType: String, entityName: String, newName: String)
}
