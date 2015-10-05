package org.broadinstitute.dsde.rawls.dataaccess

import com.tinkerpop.blueprints.{Graph, Edge, Vertex}
import com.tinkerpop.gremlin.java.GremlinPipeline
import com.tinkerpop.pipes.PipeFunction
import com.tinkerpop.pipes.branch.LoopPipe
import com.tinkerpop.pipes.branch.LoopPipe.LoopBundle
import org.broadinstitute.dsde.rawls.model._

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

import org.broadinstitute.dsde.rawls.model.AttributeConversions._

class GraphEntityDAO extends EntityDAO with GraphDAO {

  def loadEntity(entity: Vertex, txn: RawlsTransaction) = {
    loadObject[Entity](entity, txn)
  }

  override def save(workspaceContext: WorkspaceContext, entity: Entity, txn: RawlsTransaction): Entity = txn withGraph { db =>
    // check for illegal dot characters
    validateUserDefinedString(entity.entityType) // do we need to check this here if we're already validating all edges?
    validateUserDefinedString(entity.name)
    entity.attributes.keys.foreach(validateUserDefinedString)
    entity.attributes.keys.foreach(validateAttributeName)

    saveSubObject[Entity](entity.entityType, entity, workspaceContext.workspaceVertex, workspaceContext, db, txn )
    entity
  }

  override def get(workspaceContext: WorkspaceContext, entityType: String, entityName: String, txn: RawlsTransaction): Option[Entity] = txn withGraph { db =>
    getEntityVertex(workspaceContext, entityType, entityName).map(loadEntity(_, txn))
  }

  override def delete(workspaceContext: WorkspaceContext, entityType: String, entityName: String, txn: RawlsTransaction) = txn withGraph { db =>
    getEntityVertex(workspaceContext, entityType, entityName) match {
      case Some(v) => {
        removeObject(v, db, txn)
        true
      }
      case None => false
    }
  }

  override def list(workspaceContext: WorkspaceContext, entityType: String, txn: RawlsTransaction): TraversableOnce[Entity] = txn withGraph { db =>
    workspacePipeline(workspaceContext).out(EdgeSchema.Own.toLabel(entityType)).iterator().map(loadEntity(_, txn))
  }

  override def rename(workspaceContext: WorkspaceContext, entityType: String, entityName: String, newName: String, txn: RawlsTransaction) = txn withGraph { db =>
    getEntityVertex(workspaceContext, entityType, entityName).foreach(_.setProperty("name", newName))
  }

  override def getEntityTypes(workspaceContext: WorkspaceContext, txn: RawlsTransaction): Seq[String] = txn withGraph { graph =>
    workspacePipeline(workspaceContext).out().filter(isVertexOfClass(VertexSchema.Entity)).filter(hasProperty("entityType")).property("entityType").dedup().toList.toSeq.map(_.toString)
  }

  override def listEntitiesAllTypes(workspaceContext: WorkspaceContext, txn: RawlsTransaction): TraversableOnce[Entity] = txn withGraph { db =>
    val entityTypes = getEntityTypes(workspaceContext, txn).map(et => EdgeSchema.Own.toLabel(et))

    //if entityTypes is empty, then the pipeline will freak out. if there are no entity types, there are no entities, so don't do anything else
    entityTypes.size match {
      case 0 => Seq.empty
      case _ => {
        workspacePipeline(workspaceContext).out(entityTypes:_*).iterator().map(loadEntity(_, txn))
      }
    }
  }

  override def getEntitySubtrees(workspaceContext: WorkspaceContext, entityType: String, entityNames: Seq[String], txn: RawlsTransaction): Seq[Entity] = txn withGraph { db =>
    def nameFilter = new PipeFunction[Vertex, java.lang.Boolean] {
      override def compute(v: Vertex) = entityNames.contains(v.getProperty("name"))
    }

    def emitAll = new PipeFunction[LoopPipe.LoopBundle[Vertex], java.lang.Boolean] {
      override def compute(bundle: LoopPipe.LoopBundle[Vertex]): java.lang.Boolean = { true }
    }

    val topLevelEntities = workspacePipeline(workspaceContext).out(EdgeSchema.Own.toLabel(entityType)).filter(nameFilter).iterator().map(loadEntity(_, txn)).toList
    val remainingEntities = workspacePipeline(workspaceContext).out(EdgeSchema.Own.toLabel(entityType)).filter(nameFilter).as("outLoop").out().dedup().loop("outLoop", emitAll, emitAll).filter(isVertexOfClass(VertexSchema.Entity)).iterator().map(loadEntity(_, txn)).toList
    (topLevelEntities:::remainingEntities).distinct
  }

  override def getCopyConflicts(destWorkspaceContext: WorkspaceContext, entitiesToCopy: Seq[Entity], txn: RawlsTransaction): Seq[Entity] = {
    val copyMap = entitiesToCopy.map { entity => (entity.entityType, entity.name) -> entity }.toMap
    listEntitiesAllTypes(destWorkspaceContext, txn).toSeq.filter(entity => copyMap.contains(entity.entityType, entity.name))
  }

  override def copyEntities(sourceWorkspaceContext: WorkspaceContext, destWorkspaceContext: WorkspaceContext, entityType: String, entityNames: Seq[String], txn: RawlsTransaction): Seq[Entity] = {
    val entities = getEntitySubtrees(sourceWorkspaceContext, entityType, entityNames, txn)
    val conflicts = getCopyConflicts(destWorkspaceContext, entities, txn)

    conflicts.size match {
      case 0 => {
        cloneEntities(destWorkspaceContext, entities, txn)
        Seq.empty
      }
      case _ => {
        conflicts
      }
    }
  }

  override def cloneAllEntities(sourceWorkspaceContext: WorkspaceContext, destWorkspaceContext: WorkspaceContext, txn: RawlsTransaction) = txn withGraph { db =>
    val entities = listEntitiesAllTypes(sourceWorkspaceContext, txn).toList
    cloneEntities(destWorkspaceContext, entities, txn)
  }

  override def cloneEntities(destWorkspaceContext: WorkspaceContext, entities: Seq[Entity], txn: RawlsTransaction) = txn withGraph { db =>
    //1. First save JUST the vertex for the entity without any attributes (references)
    //References are preserved in saveSubObject, so we'll be able to be able to correctly save entities in reference cycles
    entities.map(e => save(destWorkspaceContext, e.copy(attributes = Map.empty), txn))
    //2. Re-save the entities, but this time with the attributes that were previously omitted
    //Since all entities were already saved in step 1, we can now save the entities again with their references to other entities
    entities.map(e => save(destWorkspaceContext, e, txn))
  }
}
