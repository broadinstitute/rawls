package org.broadinstitute.dsde.rawls.dataaccess

import com.tinkerpop.blueprints.{Graph, Vertex}
import com.tinkerpop.gremlin.java.GremlinPipeline
import com.tinkerpop.pipes.PipeFunction

import scala.collection.JavaConversions._

trait GraphDAO {

  implicit def toPipeFunction[A, B](f: A => B) = new PipeFunction[A, B] {
    override def compute(a: A): B = f(a)
  }

  def isReservedProperty(key: String) = key.startsWith("_")

  def getSinglePipelineResult[T](pipeline: GremlinPipeline[_, T]): Option[T] = {
    // Calling count() is destructive, so we first pop off the head, then check for more
    val first = pipeline.headOption
    if (pipeline.count() > 0) throw new IllegalStateException("Expected at most one result, but got multiple")
    first
  }

  // named PipeFunctions

  def hasProperty[T](key: String, value: T) = new PipeFunction[Vertex, java.lang.Boolean] {
    override def compute(v: Vertex) = v.getProperty[T](key).equals(value)
  }

  // TODO be able to understand different types?
  def hasProperties(props: Map[String, Object]) = new PipeFunction[Vertex, java.lang.Boolean] {
    override def compute(v: Vertex) = props.map(p => v.getProperty(p._1).equals(p._2)).reduce(_&&_)
  }

  // named GremlinPipelines

  def workspacePipeline(db: Graph, workspaceNamespace: String, workspaceName: String) = {
    new GremlinPipeline(db).V("_clazz", "workspace").filter(hasProperties(Map("_namespace" -> workspaceNamespace, "_name" -> workspaceName)))
  }

  def entityPipeline(db: Graph, workspaceNamespace: String, workspaceName: String, entityType: String, entityName: String) = {
    workspacePipeline(db, workspaceNamespace, workspaceName).out(entityType).filter(hasProperty("_name", entityName))
  }

  // convenience getters

  def getWorkspaceVertex(db: Graph, workspaceNamespace: String, workspaceName: String) = {
    getSinglePipelineResult(workspacePipeline(db, workspaceNamespace, workspaceName))
  }

  def getEntityVertex(db: Graph, workspaceNamespace: String, workspaceName: String, entityType: String, entityName: String) = {
    getSinglePipelineResult(entityPipeline(db, workspaceNamespace, workspaceName, entityType, entityName))
  }
}
