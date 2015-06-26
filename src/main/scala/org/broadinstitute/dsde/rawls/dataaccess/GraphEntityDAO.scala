package org.broadinstitute.dsde.rawls.dataaccess

import com.tinkerpop.blueprints.{Edge, Vertex}
import com.tinkerpop.gremlin.java.GremlinPipeline
import org.broadinstitute.dsde.rawls.model._

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

import org.broadinstitute.dsde.rawls.model.AttributeConversions._

class GraphEntityDAO extends EntityDAO with GraphDAO {

  //NOTE: attributeToProperty and propertyToAttrbute have now moved to WorkspaceModel.AttributeConversions.

  def loadEntity(entity: Vertex, workspaceNamespace: String, workspaceName: String) = {
    // NB: when starting from an edge, inV() returns the DESTINATION vertices, not the source vertices, and vice versa.
    def edgeOutgoingReferenceFunc(e: Edge) = {
      val kv = e.getLabel -> new GremlinPipeline(e).inV().transform(
        (v: Vertex) => AttributeReferenceSingle(v.getProperty("_entityType"), v.getProperty("_name"))
      ).head
      // we need some way of distinguishing single references from multiple references, so we use left/right
      if (e.getProperty[Boolean]("_unique")) Left(kv) else Right(kv)
    }

    val attributeVals = entity.getPropertyKeys.filter(!isReservedProperty(_)).map(k => k -> propertyToAttribute(entity.getProperty(k))).toMap

    // TODO make this less horrible
    val (singles, multiples) = new GremlinPipeline(entity).outE().transform((e: Edge) => edgeOutgoingReferenceFunc(e))
      .toList.asScala.partition(_.isLeft)
    val singleRefs = singles.map(_.left.get)
    val multipleRefs = multiples.map(_.right.get).groupBy(_._1).map(kv => kv._1 -> AttributeReferenceList(kv._2.map(_._2)))

    fromVertex[Entity](entity, Map("attributes" -> (attributeVals ++ singleRefs ++ multipleRefs), "workspaceName" -> WorkspaceName(workspaceNamespace, workspaceName)))
  }

  private def addEdge(workspace: Vertex, sourceEntity: Vertex, ref: AttributeReferenceSingle, label: String) = {
    val targetVertex = new GremlinPipeline(workspace).out(ref.entityType).filter(hasProperty("_name", ref.entityName)).headOption
      .getOrElse(throw new IllegalArgumentException(sourceEntity.getProperty[String]("_name") + " references nonexistent entity " + ref.entityName))

    sourceEntity.addEdge(label, targetVertex)
  }

  def cloneAllEntities(workspaceNamespace: String, newWorkspaceNamespace: String, workspaceName: String, newWorkspaceName: String, txn: RawlsTransaction): Unit = txn withGraph { db =>
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
      addEdges(newWorkspace, newWorkspaceNamespace, newWorkspaceName, entity, vertex, txn)
    })
  }

  private def createVertex(workspace: Vertex, workspaceNamespace: String, workspaceName: String, entity: Entity, txn: RawlsTransaction): Vertex = txn withGraph { db =>
    // get the entity, creating if it doesn't already exist
    val entityVertex = getEntityVertex(db, workspaceNamespace, workspaceName, entity.entityType, entity.name).getOrElse({
      val newVertex = setVertexProperties(entity, db.addVertex(null))
      workspace.addEdge(entity.entityType, newVertex)
      newVertex
    })
    entityVertex
  }

  private def addEdges(workspace: Vertex, workspaceNamespace: String, workspaceName: String, entity: Entity, entityVertex: Vertex, txn: RawlsTransaction): Entity = txn withGraph { db =>
    new GremlinPipeline(entityVertex).outE().iterator().foreach(_.remove())

    for (key <- entityVertex.getPropertyKeys; if !isReservedProperty(key)) {
      entityVertex.removeProperty(key).asInstanceOf[Object] //cast to Object required to prevent cast to Nothing which fails at runtime
    }

    entity.attributes.foreach {
      _ match {
        case (label, ref: AttributeReferenceSingle) => addEdge(workspace, entityVertex, ref, label).setProperty("_unique", true)
        case (label, AttributeReferenceList(refList)) => refList.foreach(addEdge(workspace, entityVertex, _, label).setProperty("_unique", false))
        case (name, value: AttributeValue) =>
          if (isReservedProperty(name)) throw new IllegalArgumentException("Illegal property name: " + name)
          entityVertex.setProperty(name, attributeToProperty(value))
      }
    }
    entity
  }

  def save(workspaceNamespace: String, workspaceName: String, entity: Entity, txn: RawlsTransaction): Entity = txn withGraph { db =>
    val workspace = getWorkspaceVertex(db, workspaceNamespace, workspaceName)
      .getOrElse(throw new IllegalArgumentException("Cannot save entity to nonexistent workspace " + workspaceNamespace + "::" + workspaceName))
    val entityVertex = createVertex(workspace, workspaceNamespace, workspaceName, entity, txn)
    addEdges(workspace, workspaceNamespace, workspaceName, entity, entityVertex, txn)
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
    getEntityVertex(db, workspaceNamespace, workspaceName, entityType, entityName).foreach(_.setProperty("_name", newName))
  }

  def getEntityTypes(workspaceNamespace: String, workspaceName: String, txn: RawlsTransaction): Seq[String] = txn withGraph { graph =>
    workspacePipeline(graph, workspaceNamespace, workspaceName).outE().label().dedup().toList.filterNot(_ == MethodConfigEdgeType)
  }

  def listEntitiesAllTypes(workspaceNamespace: String, workspaceName: String, txn: RawlsTransaction): TraversableOnce[Entity] = txn withGraph { db =>
    val entityTypes = getEntityTypes(workspaceNamespace, workspaceName, txn)
    getWorkspaceVertex(db, workspaceNamespace, workspaceName) match {
      case Some(w) => new GremlinPipeline(w).out(entityTypes:_*).iterator().map(loadEntity(_, workspaceNamespace, workspaceName))
      case None => List.empty
    }
  }
}
