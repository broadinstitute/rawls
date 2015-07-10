package org.broadinstitute.dsde.rawls.dataaccess

import java.util.Date

import com.tinkerpop.blueprints.impls.orient.OrientVertex
import com.tinkerpop.blueprints.{Graph, Vertex}
import com.tinkerpop.pipes.PipeFunction
import com.tinkerpop.gremlin.java.GremlinPipeline
import com.tinkerpop.pipes.branch.LoopPipe.LoopBundle
import org.broadinstitute.dsde.rawls.model.Workspace
import org.broadinstitute.dsde.rawls.{RawlsException, VertexProperty}
import org.joda.time.DateTime

import scala.collection.JavaConversions._
import scala.reflect.ClassTag
import scala.language.implicitConversions

trait GraphDAO {

  val MethodConfigEdgeType = "_MethodConfig"

  implicit def toPipeFunction[A, B](f: A => B) = new PipeFunction[A, B] {
    override def compute(a: A): B = f(a)
  }

  def isReservedProperty(key: String) = key.startsWith("_")

  def addVertex(graph: Graph, o: java.lang.Object ) : Vertex = {
    val v = graph.addVertex(o)
    v.asInstanceOf[OrientVertex].getRecord.setAllowChainedAccess(false)
    v
  }

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
    val first = pipeline.headOption
    if (pipeline.count() > 0) throw new IllegalStateException("Expected at most one result, but got multiple")

    first.map( vert => {
      vert.asInstanceOf[OrientVertex].getRecord.setAllowChainedAccess(false)
      vert.getPropertyKeys map { key => (key, vert.getProperty[T](key)) } toMap
    } )
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
    override def compute(v: Vertex) = v.getProperty[T](key) == value
  }

  // TODO be able to understand different types?
  def hasProperties(props: Map[String, Object]) = new PipeFunction[Vertex, java.lang.Boolean] {
    override def compute(v: Vertex) = props.map(p => v.getProperty(p._1).equals(p._2)).reduce(_&&_)
  }

  // named GremlinPipelines

  def workspacePipeline(db: Graph, workspaceNamespace: String, workspaceName: String) = {
    new GremlinPipeline(db).V("_clazz", classOf[Workspace].getSimpleName).filter(hasProperties(Map("_namespace" -> workspaceNamespace, "_name" -> workspaceName)))
  }

  def entityPipeline(db: Graph, workspaceNamespace: String, workspaceName: String, entityType: String, entityName: String) = {
    workspacePipeline(db, workspaceNamespace, workspaceName).out(entityType).filter(hasProperty("_name", entityName))
  }

  def methodConfigPipeline(db: Graph, workspaceNamespace: String, workspaceName: String, methodConfigNamespace: String, methodConfigName: String) = {
    workspacePipeline(db, workspaceNamespace, workspaceName).out(MethodConfigEdgeType).filter(hasProperties(Map("_namespace" -> methodConfigNamespace, "_name" -> methodConfigName)))
  }

  // convenience getters

  def getWorkspaceVertex(db: Graph, workspaceNamespace: String, workspaceName: String) = {
    getSinglePipelineResult(workspacePipeline(db, workspaceNamespace, workspaceName))
  }

  def getEntityVertex(db: Graph, workspaceNamespace: String, workspaceName: String, entityType: String, entityName: String) = {
    getSinglePipelineResult(entityPipeline(db, workspaceNamespace, workspaceName, entityType, entityName))
  }

  import scala.reflect.runtime.universe._
  import scala.reflect.runtime.{universe=>ru}
  import scala.annotation.meta.field

  /**
   * Takes the fields of obj annotated with VertexProperty and sets them on vertex.
   * Vertex property keys are prefixed with an underscore.
   *
   * @param obj source of data with which to update the vertex
   * @param vertex the vertex to update
   * @tparam T the type of obj
   * @return the updated vertex
   */
  def setVertexProperties[T: TypeTag: ClassTag](obj: T, vertex: Vertex): Vertex = {

    def getProperties(obj: T, prefix: String): Iterable[(String, Option[Any])] = {
      val mirror = ru.runtimeMirror(obj.getClass.getClassLoader)

      for (member <- ru.typeTag[T].tpe.members
           if (member.asTerm.isVal || member.asTerm.isVar) && member.annotations.exists(_.tree.tpe.typeSymbol == ru.typeOf[VertexProperty@field].typeSymbol)
      ) yield {
        val fieldMirror = mirror.reflect(obj).reflectField(member.asTerm)
        prefix + member.name.decodedName.toString.trim -> (fieldMirror.get match {
          case v: Option[Any] => v
          case v => Option(v)
        })
      }
    }

    getProperties(obj, "_").foreach(_ match {
      case (key, None) => vertex.removeProperty(key)
      case (key, Some(value: DateTime)) => vertex.setProperty(key, value.toDate)
      case (key, Some(value)) => vertex.setProperty(key, value)
    })

    vertex.setProperty("_clazz", obj.getClass.getSimpleName)
    vertex
  }

  /**
   * Convert a graph vertex to an instance of T
   * @param vertex
   * @param otherProperties properties required to construct T but are not in the vertex. Keys must match the
   *                        names of the fields on T to set
   * @tparam T
   * @return an instance of T populated from the vertex and otherProperties
   */
  def fromVertex[T: TypeTag](vertex: Vertex, otherProperties: Map[String, Any]): T = {
    val vertexProperties: Map[String, Any] = vertex.getPropertyKeys.map { key => key -> vertex.getProperty(key) }.toMap
    fromPropertyMap(otherProperties ++ vertexProperties)
  }

  /**
   * Constructs and instance of T given a map of properties where the keys correspond to the name of the fields in T
   * (with or without a leading underscore)
   *
   * @param vertexProperties
   * @tparam T
   * @return an instance of T populated from vertexProperties
   */
  def fromPropertyMap[T: TypeTag](vertexProperties: Map[String, Any]): T = {
    val classT = ru.typeOf[T].typeSymbol.asClass
    val classMirror = ru.runtimeMirror(getClass.getClassLoader).reflectClass(classT)
    val ctor = ru.typeOf[T].decl(ru.termNames.CONSTRUCTOR).asMethod
    val ctorMirror = classMirror.reflectConstructor(ctor)

    val parameters = ctor.asMethod.paramLists.head.map { paramSymbol =>
      val paramName = paramSymbol.name.decodedName.toString.trim
      val prop = vertexProperties.get("_" + paramName).orElse(vertexProperties.get(paramName)) map { value =>
        if (paramSymbol.typeSignature =:= typeOf[DateTime]) value match {
          case date: Date => new DateTime(date)
          case _ => throw new RawlsException(s"org.joda.time.DateTime property [${paramName}] of class [${classT.fullName}] does not have a java.util.Date value in ${vertexProperties}")
        }
        else value
      }

      if (paramSymbol.typeSignature <:< typeOf[Option[_]]) {
        prop
      } else {
        prop.getOrElse(throw new RawlsException(s"required property [${paramName}] of class [${classT.fullName}] does not have a value in ${vertexProperties}"))
      }
    }

    ctorMirror(parameters: _*).asInstanceOf[T]
  }
}

