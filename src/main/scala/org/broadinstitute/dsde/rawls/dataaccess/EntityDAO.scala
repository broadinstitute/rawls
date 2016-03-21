package org.broadinstitute.dsde.rawls.dataaccess

import org.broadinstitute.dsde.rawls.model._
import com.tinkerpop.blueprints.Vertex
import AttributeUpdateOperations.AttributeUpdateOperation

/**
 * Created by dvoet on 5/6/15.
 */
trait EntityDAO {
  /** gets the given entity */
  def get(workspaceContext: WorkspaceContext, entityType: String, entityName: String, txn: RawlsTransaction): Option[Entity]

  /** creates or replaces an entity */
  def save(workspaceContext: WorkspaceContext, entity: Entity, txn: RawlsTransaction): Entity

  /** deletes an entity */
  def delete(workspaceContext: WorkspaceContext, entityType: String, entityName: String, txn: RawlsTransaction): Boolean

  /** list all entities of the given type in the workspace */
  def list(workspaceContext: WorkspaceContext, entityType: String, txn: RawlsTransaction): TraversableOnce[Entity]

  def rename(workspaceContext: WorkspaceContext, entityType: String, oldName: String, newName: String, txn: RawlsTransaction): Unit

  def getEntityTypes(workspaceContext: WorkspaceContext, txn: RawlsTransaction): TraversableOnce[String]

  def getEntityTypeCount(workspaceContext: WorkspaceContext, entityType: String, txn: RawlsTransaction): Long

  def listEntitiesAllTypes(workspaceContext: WorkspaceContext, txn: RawlsTransaction): TraversableOnce[Entity]

  def cloneAllEntities(sourceWorkspaceContext: WorkspaceContext, destWorkspaceContext: WorkspaceContext, txn: RawlsTransaction): Unit

  def cloneEntities(destWorkspaceContext: WorkspaceContext, entities: Seq[Entity], txn: RawlsTransaction): Unit

  def getEntitySubtrees(workspaceContext: WorkspaceContext, entityType: String, entityNames: Seq[String], txn: RawlsTransaction): TraversableOnce[Entity]

  def copyEntities(sourceWorkspaceContext: WorkspaceContext, destWorkspaceContext: WorkspaceContext, entityType: String, entityNames: Seq[String], txn: RawlsTransaction): TraversableOnce[Entity]

  def getCopyConflicts(destWorkspaceContext: WorkspaceContext, entitiesToCopy: Seq[Entity], txn: RawlsTransaction): TraversableOnce[Entity]
}
