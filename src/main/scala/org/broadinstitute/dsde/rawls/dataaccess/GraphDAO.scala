package org.broadinstitute.dsde.rawls.dataaccess

import com.tinkerpop.blueprints.{Graph, Vertex}
import com.tinkerpop.gremlin.java.GremlinPipeline
import com.tinkerpop.pipes.PipeFunction

import scala.collection.JavaConversions._

trait GraphDAO {
  val db: Graph

  def hasProperty[T](key: String, value: T) = new PipeFunction[Vertex, java.lang.Boolean] {
    override def compute(v: Vertex) = v.getProperty[T](key).equals(value)
  }

  // TODO be able to understand different types?
  def hasProperties(props: Map[String, Object]) = new PipeFunction[Vertex, java.lang.Boolean] {
    override def compute(v: Vertex) = props.map(p => v.getProperty(p._1).equals(p._2)).reduce(_&&_)
  }

  implicit def toPipeFunction[A, B](f: A => B) = new PipeFunction[A, B] {
    override def compute(a: A): B = f(a)
  }

  def isReservedProperty(key: String) = key.startsWith("_")

  def getWorkspaceVertex(namespace: String, name: String): Option[Vertex] = {
    def workspaceFunc = hasProperties(Map("_namespace" -> namespace, "_name" -> name))
    val verticesRemaining = new GremlinPipeline(db).V("_clazz", "workspace").filter(workspaceFunc)
    // NB: since GremlinPipelines are like iterators, count() is destructive. So first we pop off the head,
    // NB: then if anything is left over it's a duplicate.
    val workspaceVertex = verticesRemaining.headOption
    if (verticesRemaining.count() > 0) {
      throw new IllegalStateException("Found more than one workspace at " + namespace + "::" + name)
    }
    workspaceVertex
  }

  def getEntityVertex(workspaceNamespace: String, workspaceName: String, entityType: String, entityName: String): Option[Vertex] = {
    def entityFunc = hasProperty("_name", entityName)
    getWorkspaceVertex(workspaceNamespace, workspaceName).flatMap(w => {
      val verticesRemaining = new GremlinPipeline(w).out(entityType).filter(entityFunc)
      val entityVertex = verticesRemaining.headOption
      if (verticesRemaining.count() > 0) {
        throw new IllegalStateException("Found more than one entity with type " + entityType + " and name " + entityName)
      }
      entityVertex
    })
  }
}
