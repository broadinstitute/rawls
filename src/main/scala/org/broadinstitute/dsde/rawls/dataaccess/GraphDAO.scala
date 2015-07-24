package org.broadinstitute.dsde.rawls.dataaccess

import java.util.Date

import com.tinkerpop.blueprints.impls.orient.{OrientGraph, OrientVertex}
import com.tinkerpop.blueprints.{Graph, Vertex}
import com.tinkerpop.blueprints.impls.orient.OrientVertex
import com.tinkerpop.blueprints.{Direction, Graph, Vertex}
import com.tinkerpop.pipes.PipeFunction
import com.tinkerpop.gremlin.java.GremlinPipeline
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.RawlsException
import org.joda.time.DateTime

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.SortedMap
import scala.reflect.ClassTag
import scala.language.implicitConversions
import scala.reflect.runtime.universe._
import scala.reflect.runtime.{universe=>ru}


object VertexSchema {
  // model classes
  val Workspace = vertexClassOf[org.broadinstitute.dsde.rawls.model.Workspace]
  val Entity = vertexClassOf[org.broadinstitute.dsde.rawls.model.Entity]
  val MethodConfig = vertexClassOf[org.broadinstitute.dsde.rawls.model.MethodConfiguration]
  val MethodRepoConfig = vertexClassOf[org.broadinstitute.dsde.rawls.model.MethodRepoConfiguration]
  val MethodRepoMethod = vertexClassOf[org.broadinstitute.dsde.rawls.model.MethodRepoMethod]
  val Submission = vertexClassOf[org.broadinstitute.dsde.rawls.model.Submission]
  val Workflow = vertexClassOf[org.broadinstitute.dsde.rawls.model.Workflow]
  val WorkflowFailure = vertexClassOf[org.broadinstitute.dsde.rawls.model.WorkflowFailure]

  // container types
  val Map = vertexClassOf[scala.collection.Map[String,Attribute]]

  val allClasses = Seq(Workspace, Entity, MethodConfig, MethodRepoConfig, MethodRepoMethod, Submission, Workflow, WorkflowFailure, Map)

  def vertexClassOf[T: TypeTag]: String = typeOf[T].typeSymbol.name.decodedName.toString

  /**
   * creates all the vertex classes
   * @param graph
   * @return a Try for each class, failures will occur where the class already exists
   */
  def createVertexClasses(graph: OrientGraph) = {
    allClasses.map { c => scala.util.Try(graph.createVertexType(c)) }
  }
}

trait GraphDAO {
  val MethodConfigEdgeType = "_MethodConfig"

  val methodRepoMethodEdge: String = "methodRepoMethodEdge"
  val methodRepoConfigEdge: String = "methodRepoConfigEdge"
  val workflowEdge: String = "workflowEdge"
  val workflowFailureEdge: String = "workflowFailureEdge"
  val submissionEdge: String = "submissionEdge"

  implicit def toPipeFunction[A, B](f: A => B) = new PipeFunction[A, B] {
    override def compute(a: A): B = f(a)
  }

  def addVertex(graph: Graph, className: String): Vertex = {
    // must specify class:<CLASSNAME> up front to make sure vertex is placed in the right cluster
    // also, must specify empty Java map due to overloading
    val v = graph.asInstanceOf[OrientGraph].addVertex(s"class:$className", Map.empty[String, Object].asJava)
    v.getRecord.setAllowChainedAccess(false)
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
   * Gets the properties of a vertex.
   *
   * @param vertex
   * @return properties of the vertex as a Map
   */
  def getVertexProperties(vertex: Vertex): Map[String, Any] = {
    vertex.getPropertyKeys map { key => (key, vertex.getProperty[Any](key)) } toMap
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

  def isWorkspace = isVertexOfClass(VertexSchema.Workspace)

  def isVertexOfClass(clazz: String) = new PipeFunction[Vertex, java.lang.Boolean] {
    override def compute(v: Vertex) = v.asInstanceOf[OrientVertex].getRecord.getClassName.equalsIgnoreCase(clazz)
  }

  def hasProperty[T](key: String) = new PipeFunction[Vertex, java.lang.Boolean] {
    override def compute(v: Vertex) = v.getProperty[T](key) != null
  }

  def hasPropertyValue[T](key: String, value: T) = new PipeFunction[Vertex, java.lang.Boolean] {
    override def compute(v: Vertex) = v.getProperty[T](key) == value
  }

  // TODO be able to understand different types?
  def hasProperties(props: Map[String, Object]) = new PipeFunction[Vertex, java.lang.Boolean] {
    override def compute(v: Vertex) = props.map(p => v.getProperty(p._1).equals(p._2)).reduce(_&&_)
  }

  // named GremlinPipelines

  def workspacePipeline(db: Graph, workspaceNamespace: String, workspaceName: String) = {
    new GremlinPipeline(db.asInstanceOf[OrientGraph].getVerticesOfClass(VertexSchema.Workspace)).filter(hasProperties(Map("namespace" -> workspaceNamespace, "name" -> workspaceName)))
  }

  def entityPipeline(db: Graph, workspaceNamespace: String, workspaceName: String, entityType: String, entityName: String) = {
    workspacePipeline(db, workspaceNamespace, workspaceName).out(entityType).filter(hasPropertyValue("name", entityName))
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

  /**
   * Serializes an object to a vertex. This supports all properties extending AnyVal, String, joda DateTime
   * Map[String, Attribute], AttributeReference, Seq[Workflow], Seq[WorkflowFailure], MethodRepoConfiguration,
   * MethodRepoMethod, WorkspaceName. Maps are stored in a sub vertex. Values of AttributeValue as properties on the
   * sub vertex. Values of AttributeReference are stored as edges from the sub-vertex. Seq[Attribute] are stored as
   * maps where the keys are the index.
   *
   * @param obj to save
   * @param vertex to save it to
   * @param graph
   * @param workspaceName used to populate workspace name fields
   * @tparam T type of object being saved
   * @return resulting vertex
   */
  def saveToVertex[T: TypeTag: ClassTag](obj: T, vertex: Vertex, graph: Graph, workspaceName: WorkspaceName): Vertex = {

    def getProperties(obj: T): Iterable[(String, Option[Any])] = {
      val mirror = ru.runtimeMirror(obj.getClass.getClassLoader)

      for (member <- ru.typeTag[T].tpe.members if (member.asTerm.isVal || member.asTerm.isVar)
      ) yield {
        val fieldMirror = mirror.reflect(obj).reflectField(member.asTerm)
        member.name.decodedName.toString.trim -> (fieldMirror.get match {
          case v: Option[Any] => v
          case v => Option(v)
        })
      }
    }

    getProperties(obj).foreach(_ match {
      // each entry we are iterating over has a key and an Option(value). Match on the type of values we
      // support and serialize to the vertex appropriately
      case (key, None) =>
        // no value for the key so remove anything that is there
        // can't tell if this is a value or a map anymore so just remove both (there should only be one)
        vertex.removeProperty(key)
        vertex.getVertices(Direction.OUT, key).foreach(removeMapVertex)
      case (key, Some(value: DateTime)) => vertex.setProperty(key, value.toDate)
      case (key, Some(value: Map[_,_])) => serializeAttributeMap(vertex, key, value.asInstanceOf[Map[String, Attribute]], graph, workspaceName)
      case (key, Some(value: AttributeEntityReference)) => serializeReference(vertex, key, value, graph, workspaceName)
      case (key, Some(seq: Seq[_])) => seq.headOption match {
        case None => // empty, do nothing
        case Some(x: Workflow) =>
          serializeDomainObjects(vertex, seq.asInstanceOf[Seq[Workflow]], workflowEdge, (w: Workflow) => w.workflowId, graph, workspaceName)
          val workspaceVertex = getSinglePipelineResult(workspacePipeline(graph, workspaceName.namespace, workspaceName.name)).get
          new GremlinPipeline(vertex).out(workflowEdge).linkIn(workflowEdge, workspaceVertex).iterate()
        case Some(x: WorkflowFailure) => serializeDomainObjects(vertex, seq.asInstanceOf[Seq[WorkflowFailure]], workflowFailureEdge, (wf: WorkflowFailure) => (wf.entityType, wf.entityName), graph, workspaceName)
        case x => throw new RawlsException(s"Unexpected object of type [${x.getClass}] in Seq for attribute [${key}]: [${x}]")
      }
      case (key, Some(mrConfig: MethodRepoConfiguration)) => serializeDomainObjects(vertex, Seq(mrConfig), methodRepoConfigEdge, thereShouldOnlyBeOne, graph, workspaceName)
      case (key, Some(mrMethod: MethodRepoMethod)) => serializeDomainObjects(vertex, Seq(mrMethod), methodRepoMethodEdge, thereShouldOnlyBeOne, graph, workspaceName)
      case (key, Some(ws: WorkspaceName)) => // don't serialize workspace name objects

      // catchall, treat it as something that can be directly set on the vertex, this works for AnyVal and String
      // but other things may cause orientdb exceptions
      case (key, Some(value)) => vertex.setProperty(key, value)
    })

    vertex
  }

  /**
   * special function passed into idFxn of serializeDomainObjects when there should only be
   * one object and thus any existing should be overwritten
   */
  private def thereShouldOnlyBeOne(x: Any) = 1

  /**
   * Serializes rawls domain objects to sub vertices. Will overwrite corresponding existing vertices, correspondence
   * determined by idFxn.
   *
   * @param vertex the top level vertex
   * @param objs the nested objects
   * @param edgeLabel the label to give to the edges to the sub vertices
   * @param idFxn function mapping an object of type T to an identifier, used to determine if vertices already in the
   *              graph correspond to any of the nested objects
   * @param graph used to create new vertices
   * @param workspaceName used to construct domain objects that require a WorkspaceName
   * @tparam T the type of the object being serializes
   */
  private def serializeDomainObjects[T: TypeTag: ClassTag](vertex: Vertex, objs: Seq[T], edgeLabel: String, idFxn: T => Any, graph: Graph, workspaceName: WorkspaceName): Seq[Vertex] = {
    val existingObjVertexesById = vertex.getVertices(Direction.OUT, edgeLabel).map { objVertex =>
      val obj = loadFromVertex[T](objVertex, Option(workspaceName))
      idFxn(obj) -> objVertex
    } toMap

    // remove any existing that are no longer in the map
    (existingObjVertexesById -- objs.map(idFxn)).foreach(_._2.remove())

    objs.map { obj =>
      val objVertex = existingObjVertexesById.getOrElse(idFxn(obj), {
        val newVertex = addVertex(graph, VertexSchema.vertexClassOf[T])
        vertex.addEdge(edgeLabel, newVertex)
        newVertex
      })

      saveToVertex(obj, objVertex, graph, workspaceName)
    }
  }

  private def serializeAttributeMap(vertex: Vertex, propName: String, map: Map[String, Attribute], graph: Graph, workspaceName: WorkspaceName): Unit = {
    // remove existing map then repopulate
    vertex.getVertices(Direction.OUT, propName).headOption.foreach(removeMapVertex)

    val mapVertex = addVertex(graph, VertexSchema.Map)
    vertex.addEdge(propName, mapVertex)

    map.foreach { case (key, attribute) =>
      attribute match {
        case v: AttributeValue => mapVertex.setProperty(key, AttributeConversions.attributeToProperty(v))
        case ref: AttributeEntityReference => serializeReference(mapVertex, key, ref, graph, workspaceName)
        case AttributeValueList(values) => serializeAttributeMap(mapVertex, key, values.zipWithIndex.map{case (value, index) => index.toString -> value}.toMap, graph, workspaceName)
        case AttributeEntityReferenceList(references) => serializeAttributeMap(mapVertex, key, references.zipWithIndex.map{case (ref, index) => index.toString -> ref}.toMap, graph, workspaceName)
        case AttributeEmptyList => serializeAttributeMap(mapVertex, key, Map.empty, graph, workspaceName)
      }
    }
  }

  private def getVertexClass(vertex: Vertex): String = vertex.asInstanceOf[OrientVertex].getRecord.getClassName

  private def removeMapVertex(mapVertex: Vertex): Unit = {
    mapVertex.asInstanceOf[OrientVertex].getVertices(Direction.OUT).filter(getVertexClass(_) == VertexSchema.Map).foreach(removeMapVertex)
    mapVertex.remove()
  }

  private def serializeReference(vertex: Vertex, key: String, ref: AttributeEntityReference, graph: Graph, workspaceName: WorkspaceName): Unit = {
    val entityVertex = getSinglePipelineResult(entityPipeline(graph, workspaceName.namespace, workspaceName.name, ref.entityType, ref.entityName)).getOrElse {
      throw new RawlsException(s"${workspaceName.namespace}/${workspaceName.name}/${ref.entityType}/${ref.entityName} does not exist")
    }
    vertex.addEdge(key, entityVertex)
  }

  /**
   * Loads an object from a vertex as saved by saveToVertex (see that function for how things are saved)
   * @param vertex to load
   * @param workspaceName
   * @tparam T
   * @return object loaded
   */
  def loadFromVertex[T: TypeTag](vertex: Vertex, workspaceName: Option[WorkspaceName]): T = {
    val classT = ru.typeOf[T].typeSymbol.asClass
    val classMirror = ru.runtimeMirror(getClass.getClassLoader).reflectClass(classT)
    val ctor = ru.typeOf[T].decl(ru.termNames.CONSTRUCTOR).asMethod
    val ctorMirror = classMirror.reflectConstructor(ctor)

    val parameters = ctor.asMethod.paramLists.head.map { paramSymbol =>
      val paramName = paramSymbol.name.decodedName.toString.trim
      val vertexProperty: Option[Any] = Option(vertex.getProperty(paramName))
      paramSymbol.typeSignature match {

        case dateSymbol if dateSymbol =:= typeOf[DateTime] =>
          val date = vertexProperty.getOrElse(throw new RawlsException(s"required property [${paramName}] of class [${classT.fullName}] does not have a value in ${vertex}"))
          date match {
            case date: Date => new DateTime(date)
            case _ => throw new RawlsException(s"org.joda.time.DateTime property [${paramName}] of class [${classT.fullName}] does not have a java.util.Date value in ${vertex}")
          }

        case mapSymbol if mapSymbol <:< typeOf[Map[String, Attribute]] =>
          vertex.getVertices(Direction.OUT, paramName).headOption match {
            case None => Map.empty
            case Some(mapVertex) => deserializeMap(mapVertex)
          }

        case entityRef if entityRef =:= typeOf[AttributeEntityReference] =>
          vertex.getVertices(Direction.OUT, paramName).headOption match {
            case None => throw new RawlsException(s"required property [${paramName}] of class [${classT.fullName}] does not have a reference in ${vertex}")
            case Some(entityVertex) if getVertexClass(entityVertex) == VertexSchema.vertexClassOf[Entity] =>
              AttributeEntityReference(entityVertex.getProperty("entityType"), entityVertex.getProperty("name"))
            case Some(nonEntityVertex) => throw new RawlsException(s"property [${paramName}] of class [${classT.fullName}] does not reference non entity vertex ${nonEntityVertex}")
          }

        case workflowSymbol if workflowSymbol =:= typeOf[Seq[Workflow]] => deserializeDomainObjects[Workflow](vertex, workflowEdge, workspaceName)
        case workflowFailureSymbol if workflowFailureSymbol =:= typeOf[Seq[WorkflowFailure]] => deserializeDomainObjects[WorkflowFailure](vertex, workflowFailureEdge, workspaceName)
        case msConfigSymbol if msConfigSymbol =:= typeOf[MethodRepoConfiguration] => deserializeDomainObjects[MethodRepoConfiguration](vertex, methodRepoConfigEdge, workspaceName).headOption.getOrElse {
          throw new RawlsException(s"required property [${paramName}] of class [${classT.fullName}] does not have a value in ${vertex}")
        }
        case msMethodSymbol if msMethodSymbol =:= typeOf[MethodRepoMethod] => deserializeDomainObjects[MethodRepoMethod](vertex, methodRepoMethodEdge, workspaceName).headOption.getOrElse {
          throw new RawlsException(s"required property [${paramName}] of class [${classT.fullName}] does not have a value in ${vertex}")
        }

        case optionSymbol if optionSymbol <:< typeOf[Option[_]] => vertexProperty

        case workspaceNameSymbol if workspaceNameSymbol =:= typeOf[WorkspaceName] => workspaceName.getOrElse(throw new RawlsException("workspace name not supplied but is required"))

        case _ => vertexProperty.getOrElse(throw new RawlsException(s"required property [${paramName}] of class [${classT.fullName}] does not have a value in ${vertex}"))
      }
    }

    ctorMirror(parameters: _*).asInstanceOf[T]
  }

  private def deserializeDomainObjects[T: TypeTag](vertex: Vertex, edgeLabel: String, workspaceName: Option[WorkspaceName]): Seq[T] = {
    vertex.getVertices(Direction.OUT, edgeLabel).map { v =>
      loadFromVertex[T](v, workspaceName)
    }.toSeq
  }

  private def deserializeMap(vertex: Vertex): Map[String, Attribute] = {
    vertex.asInstanceOf[OrientVertex].getRecord.setAllowChainedAccess(false)

    val scalarProps: Map[String, Attribute] = getVertexProperties(vertex).map(entry => entry._1 -> AttributeConversions.propertyToAttribute(entry._2))

    val refProps = vertex.getEdges(Direction.OUT).map { edge =>
      val subV = edge.getVertex(Direction.IN)
      getVertexClass(subV) match {
        case VertexSchema.Map =>
          // nested maps are sequences where the keys are the indicies
          val attributes: Seq[Attribute] = deserializeSeq(subV)
          val typedList = attributes.headOption match {
            case Some(v: AttributeValue) => AttributeValueList(attributes.map(_.asInstanceOf[AttributeValue]))
            case Some(r: AttributeEntityReference) => AttributeEntityReferenceList(attributes.map(_.asInstanceOf[AttributeEntityReference]))
            case None => AttributeEmptyList
            case x => throw new RawlsException(s"Unexpected member type in attributes Seq [${x.getClass}]: ${x}")
          }
          edge.getLabel -> typedList
        case VertexSchema.Entity => edge.getLabel -> AttributeEntityReference(subV.getProperty("entityType"), subV.getProperty("name"))
        case _ => throw new RawlsException("unexpected vertex class: " + getVertexClass(subV))
      }
    }

    scalarProps ++ refProps
  }

  private def deserializeSeq(subV: Vertex): Seq[Attribute] = {
    // Seqs are stored as maps with the index as key
    // for some reason, orient converts our numeric edge labels to negatives so use Math.abs
    SortedMap(deserializeMap(subV).map(entry => Math.abs(entry._1.toInt) -> entry._2).toSeq: _*).values.toSeq
  }
}

