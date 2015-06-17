package org.broadinstitute.dsde.rawls.dataaccess

import com.tinkerpop.blueprints.Vertex
import org.broadinstitute.dsde.rawls.model.{WorkspaceName, Entity}

/**
 * Created by dvoet on 5/6/15.
 */
trait EntityDAO {
  /** gets the given entity */
  def get(workspaceNamespace: String, workspaceName: String, entityType: String, entityName: String, txn: RawlsTransaction): Option[Entity]

  /** creates or replaces an entity */
  def save(workspaceNamespace: String, workspaceName: String, entity: Entity, txn: RawlsTransaction): Entity

  /**
   * deletes an entity
   */
  def delete(workspaceNamespace: String, workspaceName: String, entityType: String, entityName: String, txn: RawlsTransaction)

  /** list all entities of the given type in the workspace */
  def list(workspaceNamespace: String, workspaceName: String, entityType: String, txn: RawlsTransaction): TraversableOnce[Entity]

  def rename(workspaceNamespace: String, workspaceName: String, entityType: String, entityName: String, newName: String, txn: RawlsTransaction)

  def getEntityTypes(workspaceNamespace: String, workspaceName: String, txn: RawlsTransaction): Seq[String]

  def listEntitiesAllTypes(workspaceNamespace: String, workspaceName: String, txn: RawlsTransaction): TraversableOnce[Entity]

  def cloneAllEntities(workspaceNamespace: String, newWorkspaceNamespace: String, workspaceName: String, newWorkspaceName: String, txn: RawlsTransaction): Unit

  def cloneTheseEntities( entities: Seq[Entity], newWorkspaceNamespace: String, newWorkspaceName: String, txn: RawlsTransaction ): Unit

  def getEntitySubtrees(workspaceNamespace: String, workspaceName: String, entityType: String, entityNames: Seq[String], txn: RawlsTransaction): TraversableOnce[Entity]
}
