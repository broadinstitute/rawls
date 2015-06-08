package org.broadinstitute.dsde.rawls.dataaccess

import org.broadinstitute.dsde.rawls.model.Entity

import scala.collection.mutable

/**
 * Created by dvoet on 5/10/15.
 */
object MockEntityDAO extends EntityDAO {
  val store = new mutable.HashMap[(String, String), mutable.HashMap[(String, String), Entity]]()

  override def get(workspaceNamespace: String, workspaceName: String, entityType: String, entityName: String, txn: RawlsTransaction): Option[Entity] = {
    store.get(workspaceNamespace, workspaceName).flatMap(_.get(entityType, entityName))
  }

  override def rename(workspaceNamespace: String, workspaceName: String, entityType: String, entityName: String, newName: String, txn: RawlsTransaction): Unit = {
    get(workspaceNamespace, workspaceName, entityType, entityName, txn).foreach { entity =>
      delete(workspaceNamespace, workspaceName, entityType, entityName, txn)
      save(workspaceNamespace, workspaceName, entity.copy(name = newName), txn)
    }
  }

  override def delete(workspaceNamespace: String, workspaceName: String, entityType: String, entityName: String, txn: RawlsTransaction): Unit = {
    store.get(workspaceNamespace, workspaceName).flatMap { workspace =>
      workspace.remove(entityType, entityName)
    }
  }

  override def list(workspaceNamespace: String, workspaceName: String, entityType: String, txn: RawlsTransaction): TraversableOnce[Entity] = {
    store.get(workspaceNamespace, workspaceName).map { workspace =>
      workspace.filterKeys(x => x._1 == entityType).values
    }
  }.getOrElse(Seq.empty)

  /** creates or replaces an entity */
  override def save(workspaceNamespace: String, workspaceName: String, entity: Entity, txn: RawlsTransaction): Entity = {
    store.get(workspaceNamespace, workspaceName).getOrElse({
      store.put((workspaceNamespace, workspaceName), new mutable.HashMap())
      store(workspaceNamespace, workspaceName)
    }).put((entity.entityType, entity.name), entity)
    entity
  }

  override def getEntityTypes(workspaceNamespace: String, workspaceName: String, txn: RawlsTransaction): Seq[String] = {
    store.get((workspaceNamespace, workspaceName)).map(workspace => workspace.keySet.map(_._1).toSet).getOrElse(Seq.empty).toSeq
  }

  override def listEntitiesAllTypes(workspaceNamespace: String, workspaceName: String, txn: RawlsTransaction): TraversableOnce[Entity] = {
    store.get((workspaceNamespace, workspaceName)).map(workspace => workspace.values).getOrElse(Seq.empty)
  }

  override def cloneVertex(workspaceNamespace: String, workspaceName: String, entity: Entity, txn: RawlsTransaction): Entity = {
    store.get(workspaceNamespace, workspaceName).getOrElse({
      store.put((workspaceNamespace, workspaceName), new mutable.HashMap())
      store(workspaceNamespace, workspaceName)
    }).put((entity.entityType, entity.name), entity)
    entity
  }
}
