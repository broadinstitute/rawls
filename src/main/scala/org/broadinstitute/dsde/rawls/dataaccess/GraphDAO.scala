package org.broadinstitute.dsde.rawls.dataaccess

import com.tinkerpop.blueprints.{Graph, Vertex}
import com.tinkerpop.gremlin.java.GremlinPipeline
import com.tinkerpop.pipes.PipeFunction

import scala.collection.JavaConversions._

trait GraphDAO {

  val MethodConfigEdgeType = "_MethodConfig"

  implicit def toPipeFunction[A, B](f: A => B) = new PipeFunction[A, B] {
    override def compute(a: A): B = f(a)
  }

  def isReservedProperty(key: String) = key.startsWith("_")

  /**
   * Gets a single value result of a pipeline.
   *
   * @param pipeline
   * @tparam T the type of the result
   * @return result of the pipeline or None if the pipeline result is empty
   * @throws IllegalStateException if pipeline returns more than one result
   */
  def getSinglePipelineResult[T](pipeline: GremlinPipeline[_, T]): Option[T] = {
    // Calling count() is destructive, so we first pop off the head, then check for more
    val first = pipeline.headOption
    if (pipeline.count() > 0) throw new IllegalStateException("Expected at most one result, but got multiple")
    first
  }

  /**
   * Gets the properties of a vertex.
   *
   * @param pipeline ending in a vertex
   * @tparam T the type of the values of the properties, may be AnyRef
   * @return properties of the resulting vertex or None if the pipeline result is empty
   * @throws IllegalStateException if pipeline returns more than one result
   */
  def getVertexProperties[T](pipeline: GremlinPipeline[_, Vertex]): Option[Map[String, T]] = {
    // Calling count() is destructive, so we first pop off the head, then check for more
    val first = pipeline.map().headOption
    if (pipeline.count() > 0) throw new IllegalStateException("Expected at most one result, but got multiple")
    first.map(_.asInstanceOf[java.util.Map[String, T]].toMap)
  }

  /**
   * Gets the properties of all vertices that are results of the pipeline.
   *
   * @param pipeline ending in a vertex
   * @tparam T the type of the values of the properties, may be AnyRef
   * @return properties of the resulting vertices
   */
  def getPropertiesOfVertices[T](pipeline: GremlinPipeline[_, Vertex]): TraversableOnce[Map[String, T]] = {
    // note use of stream to allow pipeline to pull records into memory as it sees fit
    pipeline.map().iterator().toStream.map(_.asInstanceOf[java.util.Map[String, T]].toMap)
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

  def methodConfigPipeline(db: Graph, workspaceNamespace: String, workspaceName: String, methodConfigNamespace: String, methodConfigName: String) = {
    workspacePipeline(db, workspaceNamespace, workspaceName).out(MethodConfigEdgeType).filter(hasProperties(Map("namespace" -> methodConfigNamespace, "name" -> methodConfigName)))
  }

  // convenience getters

  def getWorkspaceVertex(db: Graph, workspaceNamespace: String, workspaceName: String) = {
    getSinglePipelineResult(workspacePipeline(db, workspaceNamespace, workspaceName))
  }

  def getEntityVertex(db: Graph, workspaceNamespace: String, workspaceName: String, entityType: String, entityName: String) = {
    getSinglePipelineResult(entityPipeline(db, workspaceNamespace, workspaceName, entityType, entityName))
  }
}
