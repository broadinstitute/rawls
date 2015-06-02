package org.broadinstitute.dsde.rawls.dataaccess

import com.tinkerpop.blueprints.{Edge, Vertex}
import com.tinkerpop.gremlin.java.GremlinPipeline
import org.broadinstitute.dsde.rawls.model._

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

class GraphEntityDAO extends EntityDAO with GraphDAO {

  // need to do some casting to conform to this list: http://orientdb.com/docs/last/Types.html
  private def attributeToProperty(att: AttributeValue): Any = att match {
    case AttributeBoolean(b) => b
    case AttributeNumber(n) => n.bigDecimal
    case AttributeString(s) => s
    case AttributeValueList(l) => l.map(attributeToProperty(_)).asJava
    case _ => throw new IllegalArgumentException("Cannot serialize " + att + " as a property")
  }

  private def propertyToAttribute(prop: Any): AttributeValue = prop match {
    case b: Boolean => AttributeBoolean(b)
    case n: java.math.BigDecimal => AttributeNumber(n)
    case s: String => AttributeString(s)
    case l: java.util.List[Any] => AttributeValueList(l.map(propertyToAttribute(_)))
    case _ => throw new IllegalArgumentException("Cannot deserialize " + prop + " as an attribute")
  }

  private def loadEntity(entity: Vertex, workspaceNamespace: String, workspaceName: String) = {
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

  def get(workspaceNamespace: String, workspaceName: String, entityType: String, entityName: String, txn: RawlsTransaction): Option[Entity] = txn withGraph { db =>
    getEntityVertex(db, workspaceNamespace, workspaceName, entityType, entityName).map(loadEntity(_, workspaceNamespace, workspaceName))
  }

  def save(workspaceNamespace: String, workspaceName: String, entity: Entity, txn: RawlsTransaction): Entity = txn withGraph { db =>
    val workspace = getWorkspaceVertex(db, workspaceNamespace, workspaceName)
      .getOrElse(throw new IllegalArgumentException("Cannot save entity to nonexistent workspace " + workspaceNamespace + "::" + workspaceName))

    // get the entity, creating if it doesn't already exist
    val entityVertex = getEntityVertex(db, workspaceNamespace, workspaceName, entity.entityType, entity.name).getOrElse({
      val newVertex = setVertexProperties(entity, db.addVertex(null))
      workspace.addEdge(entity.entityType, newVertex)
      newVertex
    })
    entityVertex.setProperty("_vaultId", entity.vaultId)

    // TODO is there a better solution than deleting all the existing edges and re-creating them?
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
