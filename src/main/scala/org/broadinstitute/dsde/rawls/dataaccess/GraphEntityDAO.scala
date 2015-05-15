package org.broadinstitute.dsde.rawls.dataaccess

import com.tinkerpop.blueprints.{Edge, Vertex, Graph}
import com.tinkerpop.gremlin.java.GremlinPipeline
import org.broadinstitute.dsde.rawls.model._

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

class GraphEntityDAO(backingDatabase: Graph) extends EntityDAO with GraphDAO {
  val db = backingDatabase

  private def loadEntity(entity: Vertex, workspaceNamespace: String, workspaceName: String) = {
    // NB: when starting from an edge, inV() returns the DESTINATION vertices, not the source vertices, and vice versa.
    def edgeOutgoingReferenceFunc(e: Edge) = {
      val kv = e.getLabel -> new GremlinPipeline(e).inV().transform(
        (v: Vertex) => AttributeReferenceSingle(v.getProperty("_clazz"), v.getProperty("_name"))
      ).head
      // we need some way of distinguishing single references from multiple references, so we use left/right
      if (e.getProperty[Boolean]("_unique")) Left(kv) else Right(kv)
    }

    // TODO figure out how to deserialize attributes properly
    val attributeVals = entity.getPropertyKeys.filter(!isReservedProperty(_)).map(k => k -> entity.getProperty[Attribute](k)).toMap

    // TODO make this less horrible
    val (singles, multiples) = new GremlinPipeline(entity).outE().transform((e: Edge) => edgeOutgoingReferenceFunc(e))
      .toList.asScala.partition(_.isLeft)
    val singleRefs = singles.map(_.left.get)
    val multipleRefs = multiples.map(_.right.get).groupBy(_._1).map(kv => kv._1 -> AttributeReferenceList(kv._2.map(_._2)))

    Entity(entity.getProperty("_name"), entity.getProperty("_clazz"), attributeVals ++ singleRefs ++ multipleRefs,
      WorkspaceName(workspaceNamespace, workspaceName))
  }

  private def addEdge(workspace: Vertex, sourceEntity: Vertex, ref: AttributeReferenceSingle, label: String) = {
    val targetVertex = new GremlinPipeline(workspace).out(ref.entityType).filter(hasProperty("_name", ref.entityName)).headOption
      .getOrElse(throw new IllegalArgumentException(sourceEntity.getProperty[String]("_name") + " references nonexistent entity " + ref.entityName))

    sourceEntity.addEdge(label, targetVertex)
  }

  def get(workspaceNamespace: String, workspaceName: String, entityType: String, entityName: String): Option[Entity] = {
    getEntityVertex(workspaceNamespace, workspaceName, entityType, entityName).map(loadEntity(_, workspaceNamespace, workspaceName))
  }

  def save(workspaceNamespace: String, workspaceName: String, entity: Entity): Entity = {
    val entityName = entity.name
    val entityType = entity.entityType

    val workspace = getWorkspaceVertex(workspaceNamespace, workspaceName)
      .getOrElse(throw new IllegalArgumentException("Cannot save entity to nonexistent workspace " + workspaceNamespace + "::" + workspaceName))

    // get the entity, creating if it doesn't already exist
    val entityVertex = getEntityVertex(workspaceNamespace, workspaceName, entityType, entityName).getOrElse({
      val newVertex = backingDatabase.addVertex(null)
      newVertex.setProperty("_name", entityName)
      newVertex.setProperty("_clazz", entityType)
      workspace.addEdge(entityType, newVertex)
      newVertex
    })

    // split the attributes into references (which will get turned into edges) and non-references (which get turned into properties)
    val (references, properties) = entity.attributes.partition(_._2.isInstanceOf[AttributeReference])

    // TODO should we pre-serialize properties somehow instead of inserting them as raw Objects?
    properties.foreach(p => {
      if (isReservedProperty(p._1)) throw new IllegalArgumentException("Illegal property name: " + p._1)
      entityVertex.setProperty(p._1, p._2)
    })

    // TODO is there a better solution than deleting all the existing edges and re-creating them?
    new GremlinPipeline(entityVertex).outE().iterator().foreach(_.remove())

    references.foreach(r => {
      val label = r._1
      r._2 match {
        case ref: AttributeReferenceSingle => addEdge(workspace, entityVertex, ref, label).setProperty("_unique", true)
        case AttributeReferenceList(refList) => refList.foreach(addEdge(workspace, entityVertex, _, label).setProperty("_unique", false))
      }
    })

    entity
  }

  def delete(workspaceNamespace: String, workspaceName: String, entityType: String, entityName: String) = {
    val entityVertex = getEntityVertex(workspaceNamespace, workspaceName, entityType, entityName)
    entityVertex match {
      case Some(v) => v.remove()
      case None => Unit
    }
  }

  def list(workspaceNamespace: String, workspaceName: String, entityType: String): TraversableOnce[Entity] = {
    val workspaceVertex = getWorkspaceVertex(workspaceNamespace, workspaceName)
    workspaceVertex match {
      case Some(w) => new GremlinPipeline(w).out(entityType).iterator().map(loadEntity(_, workspaceNamespace, workspaceName))
      case None => List.empty
    }
  }

  def rename(workspaceNamespace: String, workspaceName: String, entityType: String, entityName: String, newName: String) = {
    val entityVertex = getEntityVertex(workspaceNamespace, workspaceName, entityType, entityName)
    entityVertex.foreach(_.setProperty("_name", newName))
  }
}
