package org.broadinstitute.dsde.rawls.dataaccess

import java.util.Date

import com.tinkerpop.blueprints.impls.orient.{OrientGraph, OrientVertex}
import com.tinkerpop.blueprints.{Edge, Direction, Graph, Vertex}
import com.tinkerpop.pipes.PipeFunction
import com.tinkerpop.gremlin.java.GremlinPipeline
import org.broadinstitute.dsde.rawls.model.RawlsEnumeration
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.{RawlsExceptionWithStatusCode, RawlsException}
import org.joda.time.DateTime
import spray.http.StatusCodes

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.Map
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
  def vertexClassOf(tpe: Type): String  = tpe.typeSymbol.name.decodedName.toString
  //def vertexClassToType(str: String): Type = ru.runtimeMirror(getClass.getClassLoader).staticClass(str.replace("^", ".")).selfType

  /**
   * creates all the vertex classes
   * @param graph
   * @return a Try for each class, failures will occur where the class already exists
   */
  def createVertexClasses(graph: OrientGraph) = {
    allClasses.map { c => scala.util.Try(graph.createVertexType(c)) }
  }
}

object EdgeSchema {
  val sep = "_"
  sealed trait EdgeRelationType {
    def prefix: String
    def toLabel(suffix:String) = prefix + sep + suffix
  }
  case object Own extends EdgeRelationType {
    override val prefix = "OWN"
  }
  case object Ref extends EdgeRelationType {
    override val prefix = "REF"
  }
  val allEdgeRelations = Seq(Own, Ref)

  def getEdgeRelation(label: String): EdgeRelationType = {
    toEdgeRelation(label.split(sep).head)
  }

  def toEdgeRelation(prefix: String): EdgeRelationType = {
    allEdgeRelations.filter( er => prefix == er.prefix ).head
  }
  def stripEdgeRelation(str: String): String = str.split(sep, 2).last
}

trait GraphDAO {
  val methodConfigEdge: String = "methodConfigEdge"
  val methodRepoMethodEdge: String = "methodRepoMethodEdge"
  val methodRepoConfigEdge: String = "methodRepoConfigEdge"
  val submissionEdge: String = "submissionEdge"

  implicit def toPipeFunction[A, B](f: A => B) = new PipeFunction[A, B] {
    override def compute(a: A): B = f(a)
  }

  def validateUserDefinedString(s: String) = {
    // due to Orient's chained access "feature", we should avoid dots in certain user-defined strings
    if (s.contains('.')) throw new RawlsExceptionWithStatusCode(message = s"User-defined string $s should not contain dot characters", statusCode = StatusCodes.BadRequest)
  }

  def addEdge(source: Vertex, edgeType: EdgeSchema.EdgeRelationType, label: String, dest: Vertex) = {
    // fail-safe check to ensure no edge labels have dots
    if (label.contains('.')) throw new RawlsException(message = s"Edge label $label should not contain dot characters")
    source.addEdge(edgeType.toLabel(label), dest)
  }

  def getEdges(vertex: Vertex, direction: Direction, edgeType: EdgeSchema.EdgeRelationType, labels: String*): Iterable[Edge] = {
      vertex.getEdges(direction, labels.map(l => edgeType.toLabel(l)):_* )
  }

  def getVertices(vertex: Vertex, direction: Direction, edgeType: EdgeSchema.EdgeRelationType, labels: String*): Iterable[Vertex] = {
    vertex.getVertices(direction, labels.map(l => edgeType.toLabel(l)):_* )
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
   * Returns a tuple of vertex property keys and edge keys. */
  def getVertexKeysTuple(vertex: Vertex): (Seq[String], Seq[String]) =
    (vertex.getPropertyKeys.toSeq, vertex.getEdges(Direction.OUT).map( e => EdgeSchema.stripEdgeRelation(e.getLabel) ).toSeq)

  /**
   * Gets all fields associated with a vertex: its properties and its outbound edge names. */
  def getVertexKeys(vertex: Vertex): Seq[String] = {
    getVertexKeysTuple(vertex).productIterator.toList.asInstanceOf[Seq[Seq[String]]].flatten
  }

  /**
   * Returns the class of a vertex. */
  private def getVertexClass(vertex: Vertex): String = vertex.asInstanceOf[OrientVertex].getRecord.getClassName

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
    override def compute(v: Vertex) = getVertexClass(v).equalsIgnoreCase(clazz)
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

  def workspacePipeline(db: Graph, workspaceName: WorkspaceName) = {
    new GremlinPipeline(db.asInstanceOf[OrientGraph].getVerticesOfClass(VertexSchema.Workspace)).filter(hasProperties(Map("namespace" -> workspaceName.namespace, "name" -> workspaceName.name)))
  }

  def workspacePipeline(workspaceContext: WorkspaceContext) = {
    new GremlinPipeline(workspaceContext.workspaceVertex)
  }

  def entityPipeline(workspaceContext: WorkspaceContext, entityType: String, entityName: String) = {
    workspacePipeline(workspaceContext).out(EdgeSchema.Own.toLabel(entityType)).filter(hasPropertyValue("name", entityName))
  }

  def methodConfigPipeline(workspaceContext: WorkspaceContext, methodConfigNamespace: String, methodConfigName: String) = {
    workspacePipeline(workspaceContext).out(EdgeSchema.Own.toLabel(methodConfigEdge)).filter(hasProperties(Map("namespace" -> methodConfigNamespace, "name" -> methodConfigName)))
  }

  def submissionPipeline(workspaceContext: WorkspaceContext, submissionId: String) = {
    workspacePipeline(workspaceContext).out(EdgeSchema.Own.toLabel(submissionEdge)).filter(hasPropertyValue("submissionId", submissionId))
  }

  // vertex getters

  def getWorkspaceVertex(db: Graph, workspaceName: WorkspaceName) = {
    getSinglePipelineResult(workspacePipeline(db, workspaceName))
  }

  def getEntityVertex(workspaceContext: WorkspaceContext, entityType: String, entityName: String) = {
    getSinglePipelineResult(entityPipeline(workspaceContext, entityType, entityName))
  }

  def getMethodConfigVertex(workspaceContext: WorkspaceContext, methodConfigNamespace: String, methodConfigName: String) = {
    getSinglePipelineResult(methodConfigPipeline(workspaceContext, methodConfigNamespace, methodConfigName))
  }

  def getSubmissionVertex(workspaceContext: WorkspaceContext, submissionId: String) = {
    getSinglePipelineResult(submissionPipeline(workspaceContext, submissionId))
  }

  def getCtorProperties(tpe: Type): Iterable[(Type, String)] = {
    val ctor = tpe.decl(ru.termNames.CONSTRUCTOR).asMethod

    for( paramSymbol <- ctor.paramLists.head ) yield {
      (paramSymbol.typeSignature, paramSymbol.name.decodedName.toString.trim)
    }
  }

  //NOTE: We return Iterable[(tpe:Type, propName:String, value:Any)], but value will always be of type tpe.
  //We use getTypeParams to unpack collection types so we don't actually lose info but it makes matching more painful.
  def getPropertiesAndValues(tpe: Type, obj: DomainObject): Iterable[( Type, String, Any )] = {
    val mirror = ru.runtimeMirror(obj.getClass.getClassLoader)

    for (member <- tpe.members if member.asTerm.isVal || member.asTerm.isVar
    ) yield {
      val fieldMirror = mirror.reflect(obj).reflectField(member.asTerm)
      (member.typeSignature, member.name.decodedName.toString.trim, fieldMirror.get)
    }
  }

  def getTypeParams(tpe: Type) = tpe.asInstanceOf[TypeRefApi].args

  //SAVE METHODS
  def saveObject[T <: DomainObject :TypeTag :ClassTag](obj: T, vertex: Vertex, wsc: WorkspaceContext, graph: Graph): Unit = {
    saveObject(typeOf[T], obj, vertex, wsc, graph)
  }

  def saveObject(tpe: Type, obj: DomainObject, vertex: Vertex, wsc: WorkspaceContext, graph: Graph): Unit = {
    //Serialize out each of the case class properties.
    getPropertiesAndValues(tpe, obj).foreach({
      case (tp, prop, value) => saveProperty(tp, prop, value, vertex, wsc, graph)
    })
  }

  private def removeDomainObjectFilterFn(tpe: Type, obj: DomainObject)(vertex: Vertex): Boolean = {
    val mirror = ru.runtimeMirror(obj.getClass.getClassLoader)
    val idFieldSym = tpe.decl(ru.TermName(obj.idField)).asMethod
    vertex.getProperty(obj.idField) == mirror.reflect(obj).reflectField(idFieldSym).get
  }

  def saveSubObject[T <: DomainObject :TypeTag :ClassTag](propName: String, obj: T, vertex: Vertex, wsc: WorkspaceContext, graph: Graph): Vertex = {
    saveSubObject(typeOf[T], propName, obj, vertex, wsc, graph)
  }

  def saveSubObject(tpe: Type, propName: String, obj: DomainObject, vertex: Vertex, wsc: WorkspaceContext, graph: Graph): Vertex = {
    removeProperty(propName, vertex, graph, removeDomainObjectFilterFn(tpe, obj))
    val objVert = addVertex(graph, VertexSchema.vertexClassOf(tpe))
    addEdge(vertex, EdgeSchema.Own, propName, objVert)
    saveObject(tpe, obj, objVert, wsc, graph)
    objVert
  }

  private def saveMap( valuesType: Type, propName: String, map: Map[String, _], vertex: Vertex, wsc: WorkspaceContext, graph: Graph): Unit = {
    val mapDummy = addVertex(graph, VertexSchema.Map)
    addEdge(vertex, EdgeSchema.Own, propName, mapDummy)

    map.map {case (key, value) =>
      saveProperty( valuesType, key, value, mapDummy, wsc, graph )
    }
  }

  private def saveOpt(containedType: Type, propName: String, opt: Option[Any], vertex: Vertex, wsc:WorkspaceContext, graph:Graph): Unit = {
    opt match {
      case Some(v) => saveProperty(containedType, propName, v, vertex, wsc, graph)
      case None => //done, already removed
    }
  }

  private def saveAttributeRef(ref: AttributeEntityReference, propName: String, vertex: Vertex, wsc: WorkspaceContext, graph:Graph): Unit = {
    val entityVertex = getEntityVertex(wsc, ref.entityType, ref.entityName).getOrElse {
      throw new RawlsException(s"${wsc.workspaceName.namespace}/${wsc.workspaceName.name}/${ref.entityType}/${ref.entityName} does not exist")
    }
    addEdge(vertex, EdgeSchema.Ref, propName, entityVertex)
  }

  private def saveProperty(tpe: Type, propName: String, valToSave: Any, vertex: Vertex, wsc: WorkspaceContext, graph: Graph): Unit = {
    removeProperty(propName, vertex, graph) //remove any previously defined value
    (tpe, valToSave) match {

      //Basic types. TODO: More of these?
      case (_, value:String)  => vertex.setProperty(propName, value)
      case (_, value:Int)     => vertex.setProperty(propName, value)
      case (_, value:Boolean) => vertex.setProperty(propName, value)
      case (_, value:DateTime)=> vertex.setProperty(propName, value.toDate)

      //Attributes.

      //Throw an exception if we attempt to save AttributeNull, since a nonexistent property and a null-valued property are indistinguishable
      case (_, AttributeNull) => throw new RawlsException(s"Attempting to save $propName as AttributeNull; this is illegal")
      case (_, value:AttributeValue) => vertex.setProperty(propName, AttributeConversions.attributeToProperty(value))
      case (_, value:AttributeEntityReference) => saveAttributeRef(value, propName, vertex, wsc, graph)
      case (_, value:AttributeValueList) => saveProperty(typeOf[Seq[AttributeValue]], propName, value.list, vertex, wsc, graph)
      case (_, value:AttributeEntityReferenceList) => saveProperty(typeOf[Seq[AttributeEntityReference]], propName, value.list, vertex, wsc, graph)

      //set the values type to Any, but it's irrelevant as the map is empty so no values will be serialized
      case (_, AttributeEmptyList) => saveMap( typeOf[Any], propName, Map.empty[String,Any], vertex, wsc, graph)

      //Enums.
      case (_, value:RawlsEnumeration[_]) => vertex.setProperty(propName, value.toString)

      //Collections. Note that a Seq is treated as a map with the keys as indices.
      //Watch out for type erasure! Ha ha ha ha ha. Ugh.
      case (_, seq:Seq[_]) => saveMap( getTypeParams(tpe).head, propName, seq.zipWithIndex.map({case (elem, idx) => idx.toString -> elem}).toMap, vertex, wsc, graph)
      case (tp, _) if tp <:< typeOf[Seq[_]] => saveMap( getTypeParams(tpe).head, propName, Map.empty, vertex, wsc, graph)

      case (_, map:Map[String,_])  => saveMap( getTypeParams(tpe).last, propName, map, vertex, wsc, graph)
      case (tp, _) if tp <:< typeOf[Map[String,_]] => saveMap( getTypeParams(tpe).last, propName, Map.empty, vertex, wsc, graph)

      case (_, value:Option[_]) => saveOpt(getTypeParams(tpe).head, propName, value, vertex, wsc, graph)
      case (tp, None) if tp <:< typeOf[Option[Any]] => saveOpt(getTypeParams(tp).head, propName, None, vertex, wsc, graph)

      //Everything else (including enum types).
      case (tp, value:DomainObject) => saveSubObject(tp, propName, value, vertex, wsc, graph)

      //The Forbidden Zone
      case (_, value:WorkspaceName) => throw new RawlsException("WorkspaceName is not a supported attribute type.")

      //What the hell is this? Run awaaaaay!
      case (tp, value) => throw new RawlsException(s"Error saving property $propName with value ${value.toString}: unknown type ${tpe.toString}")
    }
  }

  //LOAD METHODS
  def loadObject[T: TypeTag](vertex: Vertex): T = {
    loadObject(typeOf[T], vertex).asInstanceOf[T]
  }

  def loadObject(tpe: Type, vertex: Vertex): Any = {
    val classT = tpe.typeSymbol.asClass
    val classMirror = ru.runtimeMirror(getClass.getClassLoader).reflectClass(classT)
    val ctor = tpe.decl(ru.termNames.CONSTRUCTOR).asMethod
    val ctorMirror = classMirror.reflectConstructor(ctor)

    val parameters = getCtorProperties(tpe).map({
      case (tp, prop) =>
        loadProperty(tp, prop, vertex)
    }).toSeq

    ctorMirror(parameters: _*)
  }

  def loadSubObject(tpe: Type, propName: String, vertex: Vertex): Any = {
    val subVert = getVertices(vertex, Direction.OUT, EdgeSchema.Own, propName).head
    subVert.asInstanceOf[OrientVertex].getRecord.setAllowChainedAccess(false)
    loadObject(tpe, subVert)
  }

  private def loadMap(valuesType: Type, propName: String, vertex: Vertex): Map[String, Any] = {
    val mapDummy = getVertices(vertex, Direction.OUT, EdgeSchema.Own, propName).head
    mapDummy.asInstanceOf[OrientVertex].getRecord.setAllowChainedAccess(false)

    getVertexKeys(mapDummy).map({ key =>
      (key, loadProperty(valuesType, key, mapDummy))
    }).toMap
  }

  private def loadOpt(containedType: Type, propName: String, vertex: Vertex): Option[Any] = {
    getVertexKeys(vertex).contains(propName) match {
      case true => Option(loadProperty(containedType, propName, vertex))
      case false => None
    }
  }

  //We've determined that propName is one of AttributeEntityReferenceList, AttributeValueList, AttributeEmptyList
  //Figure out which one and load it!
  private def loadMysteriouslyTypedAttributeList(propName: String, myVertex: Vertex, mapVertex: Vertex): Attribute = {
    //Peek at the map vertex for the list type.
    val (propKeys, edgeKeys) = getVertexKeysTuple(mapVertex)

    if ( propKeys.size + edgeKeys.size == 0 ) {
      AttributeEmptyList
    } else if ( propKeys.size > 0 ) {
      assert(edgeKeys.size == 0, "Mysteriously typed AttributeList vertex has both property and edge keys!")
      AttributeValueList(
        loadProperty(typeOf[Seq[AttributeValue]], propName, myVertex)
          .asInstanceOf[Seq[AttributeValue]] )
    } else {
      AttributeEntityReferenceList(
        loadProperty( typeOf[Seq[AttributeEntityReference]], propName, myVertex )
          .asInstanceOf[Seq[AttributeEntityReference]] )
    }
  }

  /**
   * Loads an attribute property from the graph.
   * @param propName A property of type Attribute, most likely a value in a Map[String, Attribute].
   *                 The class definition doesn't tell us any more than it's an attribute so we have to
   *                 do some silly inference from the structure of the graph.
   * @param vertex The map dummy vertex this property lives on.
   * @return A nicely typed Attribute.
   */
  private def loadMysteriouslyTypedAttribute(propName: String, vertex: Vertex): Attribute = {
    val vertexProp = Option(vertex.getProperty(propName))
    vertexProp match {
      case Some(value) => loadProperty(ru.typeOf[AttributeValue], propName, vertex).asInstanceOf[AttributeValue]
      case None =>
        val refEdges = vertex.getEdges(Direction.OUT, EdgeSchema.Ref.toLabel(propName)).toSeq
        val ownEdges = vertex.getEdges(Direction.OUT, EdgeSchema.Own.toLabel(propName)).toSeq

        //The second assert here will need to be removed if we ever implement Set types.
        assert( refEdges.size + ownEdges.size > 0, s"Attribute in map with property name $propName seems to be missing" )
        assert( refEdges.size + ownEdges.size <= 1, s"Attribute in map with property name $propName is duplicated" )

        if( refEdges.size == 1 ) {
          //The only place we currently have references is AttributeEntityRefs.
          assert(getVertexClass(refEdges.head.getVertex(Direction.IN)).equalsIgnoreCase(VertexSchema.Entity))
          loadProperty(ru.typeOf[AttributeEntityReference], propName, vertex).asInstanceOf[AttributeEntityReference]
        } else {
          //It's either an AttributeEntityReferenceList, an AttributeValueList, or an AttributeEmptyList
          loadMysteriouslyTypedAttributeList(propName, vertex, ownEdges.head.getVertex(Direction.IN))
        }
    }
  }

  private def loadAttributeRef(propName: String, vertex: Vertex): AttributeEntityReference = {
    val refVtx = vertex.getVertices(Direction.OUT, EdgeSchema.Ref.toLabel(propName)).head
    AttributeEntityReference(refVtx.getProperty("entityType"), refVtx.getProperty("name"))
  }

  private def loadAttributeList[T](propName: String, vertex: Vertex): Seq[T] = {
    loadProperty(typeOf[Seq[AttributeValue]], propName, vertex).asInstanceOf[Seq[T]]
  }

  //Sneaky. The withName() method is defined in a sealed trait, so no way to instance it to call it.
  //Instead, we find a subclass of the enum type, instance that, and call withName() on it.
  private def enumWithName(enumType: Type, enumStr: String): RawlsEnumeration[_] = {
    val m = ru.runtimeMirror(getClass.getClassLoader)
    val anEnumElemClass = enumType.typeSymbol.asClass.knownDirectSubclasses.head.asClass.toType
    val enumModuleMirror = m.staticModule(anEnumElemClass.typeSymbol.asClass.fullName)
    m.reflectModule(enumModuleMirror).instance.asInstanceOf[RawlsEnumeration[_]].withName(enumStr).asInstanceOf[RawlsEnumeration[_]]
  }

  private def loadProperty(tpe: Type, propName: String, vertex: Vertex): Any = {
    tpe match {
      //Basic types. TODO: More of these?
      case tp if tp =:= typeOf[String]  => vertex.getProperty(propName)
      case tp if tp =:= typeOf[Int]     => vertex.getProperty(propName)
      case tp if tp =:= typeOf[Boolean] => vertex.getProperty(propName)
      //serializing joda datetime across the network breaks, though it works fine in-memory
      case tp if tp =:= typeOf[DateTime]=> new DateTime(vertex.getProperty(propName).asInstanceOf[Date])

      //Attributes.
      case tp if tp <:< typeOf[AttributeValue] => AttributeConversions.propertyToAttribute(vertex.getProperty(propName))
      case tp if tp <:< typeOf[AttributeEntityReference] => loadAttributeRef(propName, vertex)
      case tp if tp <:< typeOf[AttributeValueList] => AttributeValueList(loadAttributeList[AttributeValue](propName, vertex))
      case tp if tp <:< typeOf[AttributeEntityReferenceList] => AttributeEntityReferenceList(loadAttributeList[AttributeEntityReference](propName, vertex))
      case tp if tp <:< typeOf[Attribute] => loadMysteriouslyTypedAttribute(propName, vertex)

      //Enums.
      case tp if tp <:< typeOf[RawlsEnumeration[_]] => enumWithName(tp, vertex.getProperty(propName))

      //Collections. Note that a Seq is treated as a map with the keys as indices.
      case tp if tp <:< typeOf[Seq[Any]] => loadMap(getTypeParams(tp).head, propName, vertex).toSeq.sortBy(_._1.toInt).map(_._2)
      case tp if tp <:< typeOf[Map[String,Any]] => loadMap(getTypeParams(tp).last, propName, vertex)
      case tp if tp <:< typeOf[Option[Any]] => loadOpt(tp, propName, vertex)

      //Everything else.
      case tp if tp <:< typeOf[DomainObject] => loadSubObject(tp, propName, vertex)

      //The Forbidden Zone
      case tp if tp <:< typeOf[WorkspaceName] => throw new RawlsException("WorkspaceName is not a supported attribute type.")

      //Aaaaah! Freak out! (le freak, c'est chic)
      case tp => throw new RawlsException(s"Error loading property $propName from $vertex: unknown type ${tp.toString}")
    }
  }

  //REMOVE METHODS
  def removeObject(vertex: Vertex, graph: Graph): Unit = {
    removeOwnedChildVertices(vertex, graph)
    vertex.remove()
  }

  //Removes the named property from the vertex.
  //The vertexFilterFn provides an additional check on target vertices to see if they should be deleted.
  private def removeProperty(propName: String, vertex: Vertex, graph: Graph, vertexFilterFn: (Vertex => Boolean) = {v => true} ) = {
    //Remove the property set directly on the vertex...
    vertex.removeProperty(propName)

    //...and also properties represented by edges.
    vertex.getEdges(Direction.OUT)
      .filter( e => EdgeSchema.stripEdgeRelation(e.getLabel) == propName && vertexFilterFn(e.getVertex(Direction.IN)) )
      .map { e =>
      EdgeSchema.getEdgeRelation(e.getLabel) match {
        //We need to recursively delete sub-objects that we own.
        case EdgeSchema.Own =>
          val childVertex = e.getVertex(Direction.IN)
          removeOwnedChildVertices(childVertex, graph)
          graph.removeVertex(childVertex)
          //No need to delete the edge as deleting the vertex makes it vanish.
        case EdgeSchema.Ref =>
          graph.removeEdge(e)
      }
    }
  }

  //Recursively wipes all linked vertices following ownership chain.
  private def removeOwnedChildVertices( vertex: Vertex, graph: Graph ): Unit = {
    vertex.getEdges(Direction.OUT).map { edge =>
      EdgeSchema.getEdgeRelation(edge.getLabel) match {
        case EdgeSchema.Own =>
          val childVertex = edge.getVertex(Direction.IN)
          removeOwnedChildVertices(childVertex, graph)
          graph.removeVertex(childVertex)
        case EdgeSchema.Ref => //no-op
      }
    }
  }
}

