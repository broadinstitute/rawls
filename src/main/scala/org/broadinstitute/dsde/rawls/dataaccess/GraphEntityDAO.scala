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

  def loadEntity(entity: Vertex, workspaceNamespace: String, workspaceName: String) = {
    loadFromVertex[Entity](entity, Option(WorkspaceName(workspaceNamespace, workspaceName)))
  }

  def cloneAllEntities(workspaceNamespace: String, newWorkspaceNamespace: String, workspaceName: String, newWorkspaceName: String, txn: RawlsTransaction) = txn withGraph { db =>
    val entities = listEntitiesAllTypes(workspaceNamespace, workspaceName, txn).toList
    val workspace = getWorkspaceVertex(db, workspaceNamespace, workspaceName)
      .getOrElse(throw new IllegalArgumentException("Cannot clone entity from nonexistent workspace " + workspaceNamespace + "::" + workspaceName))

    cloneTheseEntities(entities,newWorkspaceNamespace,newWorkspaceName,txn)
  }

  override def cloneTheseEntities( entities: Seq[Entity], newWorkspaceNamespace: String, newWorkspaceName: String, txn: RawlsTransaction ) = txn withGraph { db =>
    val newWorkspace = getWorkspaceVertex(db, newWorkspaceNamespace, newWorkspaceName)
      .getOrElse(throw new IllegalArgumentException("Cannot clone entity into nonexistent workspace " + newWorkspaceNamespace + "::" + newWorkspaceName))

    //map the entities to ((entity type, entity name), vertex)
    val entityToVertexMap = entities.map { entity => (entity.entityType, entity.name) -> createVertex(newWorkspace, newWorkspaceNamespace, newWorkspaceName, entity, txn)}.toMap
    entities.foreach(entity => {
      val vertex = entityToVertexMap((entity.entityType, entity.name))
      saveToVertex[Entity](entity, vertex, db, WorkspaceName(newWorkspaceNamespace, newWorkspaceName))
    })
  }

  private def createVertex(workspace: Vertex, workspaceNamespace: String, workspaceName: String, entity: Entity, txn: RawlsTransaction): Vertex = txn withGraph { db =>
    // get the entity, creating if it doesn't already exist
    val entityVertex = getEntityVertex(db, workspaceNamespace, workspaceName, entity.entityType, entity.name).getOrElse({
      val newVertex = addVertex(db, VertexSchema.Entity)
      // need to explicitly set name and type so other pipelines can find this vertex,
      // notably in the case of handling cycles
      newVertex.setProperty("name", entity.name)
      newVertex.setProperty("entityType", entity.entityType)

      workspace.addEdge(entity.entityType, newVertex)
      newVertex
    })
    entityVertex
  }

  def save(workspaceNamespace: String, workspaceName: String, entity: Entity, txn: RawlsTransaction): Entity = txn withGraph { db =>
    val workspace = getWorkspaceVertex(db, workspaceNamespace, workspaceName)
      .getOrElse(throw new IllegalArgumentException("Cannot save entity to nonexistent workspace " + workspaceNamespace + "::" + workspaceName))
    val entityVertex = createVertex(workspace, workspaceNamespace, workspaceName, entity, txn)
    saveToVertex[Entity](entity, entityVertex, db, WorkspaceName(workspaceNamespace, workspaceName))
    entity
  }

  def get(workspaceNamespace: String, workspaceName: String, entityType: String, entityName: String, txn: RawlsTransaction): Option[Entity] = txn withGraph { db =>
    getEntityVertex(db, workspaceNamespace, workspaceName, entityType, entityName).map(loadEntity(_, workspaceNamespace, workspaceName))
  }

  def delete(workspaceNamespace: String, workspaceName: String, entityType: String, entityName: String, txn: RawlsTransaction) = txn withGraph { db =>
    getEntityVertex(db, workspaceNamespace, workspaceName, entityType, entityName) match {
      case Some(v) => v.remove()
      case None => Unit
    }
  }

  def list(workspaceNamespace: String, workspaceName: String, entityType: String, txn: RawlsTransaction): TraversableOnce[Entity] = txn withGraph { db =>
    getWorkspaceVertex(db, workspaceNamespace, workspaceName) match {
      case Some(w) => new GremlinPipeline(w).out(entityType).iterator().map(loadEntity(_, workspaceNamespace, workspaceName))
      case None => List.empty
    }
  }

  def rename(workspaceNamespace: String, workspaceName: String, entityType: String, entityName: String, newName: String, txn: RawlsTransaction) = txn withGraph { db =>
    getEntityVertex(db, workspaceNamespace, workspaceName, entityType, entityName).foreach(_.setProperty("name", newName))
  }

  def getEntityTypes(workspaceNamespace: String, workspaceName: String, txn: RawlsTransaction): Seq[String] = txn withGraph { graph =>
    workspacePipeline(graph, workspaceNamespace, workspaceName).out().filter(isVertexOfClass(VertexSchema.Entity)).filter(hasProperty("entityType")).property("entityType").dedup().toList.toSeq.map(_.toString)
  }

  def listEntitiesAllTypes(workspaceNamespace: String, workspaceName: String, txn: RawlsTransaction): TraversableOnce[Entity] = txn withGraph { db =>
    val entityTypes = getEntityTypes(workspaceNamespace, workspaceName, txn)
    getWorkspaceVertex(db, workspaceNamespace, workspaceName) match {
      case Some(w) => new GremlinPipeline(w).out(entityTypes:_*).iterator().map(loadEntity(_, workspaceNamespace, workspaceName))
      case None => List.empty
    }
  }

  def getEntitySubtrees(workspaceNamespace: String, workspaceName: String, entityType: String, entityNames: Seq[String], txn: RawlsTransaction): Seq[Entity] = txn withGraph { db =>
    def nameFilter = new PipeFunction[Vertex, java.lang.Boolean] {
      override def compute(v: Vertex) = entityNames.contains(v.getProperty("name"))
    }

    def emitAll = new PipeFunction[LoopPipe.LoopBundle[Vertex], java.lang.Boolean] {
      override def compute(bundle: LoopPipe.LoopBundle[Vertex]): java.lang.Boolean = { true }
    }

    val subtreeEntities = getWorkspaceVertex(db, workspaceNamespace, workspaceName) match {
      case Some(w) => {
        val topLevelEntities = new GremlinPipeline(w).out(entityType).filter(nameFilter).iterator().map(loadEntity(_, workspaceNamespace, workspaceName)).toList
        val remainingEntities = new GremlinPipeline(w).out(entityType).filter(nameFilter).as("outLoop").out().dedup().loop("outLoop", emitAll, emitAll).filter(isVertexOfClass(VertexSchema.Entity)).iterator().map(loadEntity(_, workspaceNamespace, workspaceName)).toList
        (topLevelEntities:::remainingEntities).distinct
      }
      case None => Seq.empty
    }
    subtreeEntities
  }

  def getCopyConflicts(destNamespace: String, destWorkspace: String, entitiesToCopy: Seq[Entity], txn: RawlsTransaction): Seq[Entity] = {
    val copyMap = entitiesToCopy.map { entity => (entity.entityType, entity.name) -> entity }.toMap
    listEntitiesAllTypes(destNamespace, destWorkspace, txn).toSeq.filter(entity => copyMap.contains(entity.entityType, entity.name))
  }

  def copyEntities(destNamespace: String, destWorkspace: String, sourceNamespace: String, sourceWorkspace: String, entityType: String, entityNames: Seq[String], txn: RawlsTransaction): Seq[Entity] = {
    val entities = getEntitySubtrees(sourceNamespace, sourceWorkspace, entityType, entityNames, txn).toSeq
    val conflicts = getCopyConflicts(destNamespace, destWorkspace, entities, txn)

    conflicts.size match {
      case 0 => {
        cloneTheseEntities(entities, destNamespace, destWorkspace, txn)
        Seq.empty
      }
      case _ => {
        conflicts
      }
    }
  }
}
