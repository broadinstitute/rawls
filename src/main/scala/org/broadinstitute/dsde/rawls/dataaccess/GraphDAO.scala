package org.broadinstitute.dsde.rawls.dataaccess

import java.util.Date

import com.tinkerpop.blueprints.impls.orient.{OrientGraph, OrientVertex}
import com.tinkerpop.blueprints.{Edge, Direction, Graph, Vertex}
import com.tinkerpop.pipes.PipeFunction
import com.tinkerpop.gremlin.java.GremlinPipeline
import com.tinkerpop.pipes.branch.LoopPipe
import org.broadinstitute.dsde.rawls.model.RawlsEnumeration
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.{RawlsExceptionWithStatusCode, RawlsException}
import org.joda.time.DateTime
import spray.http.StatusCodes

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.Map
import scala.language.implicitConversions
import scala.reflect.runtime.universe._
import scala.reflect.runtime.{universe=>ru}

object VertexSchema {
  // model classes
  val Workspace = vertexClassOf[org.broadinstitute.dsde.rawls.model.Workspace]
  val Entity = vertexClassOf[org.broadinstitute.dsde.rawls.model.Entity]
  val MethodConfig = vertexClassOf[org.broadinstitute.dsde.rawls.model.MethodConfiguration]
  val MethodRepoMethod = vertexClassOf[org.broadinstitute.dsde.rawls.model.MethodRepoMethod]
  val Submission = vertexClassOf[org.broadinstitute.dsde.rawls.model.Submission]
  val Workflow = vertexClassOf[org.broadinstitute.dsde.rawls.model.Workflow]
  val WorkflowFailure = vertexClassOf[org.broadinstitute.dsde.rawls.model.WorkflowFailure]
  val User = vertexClassOf[org.broadinstitute.dsde.rawls.model.RawlsUser]
  val Group = vertexClassOf[org.broadinstitute.dsde.rawls.model.RawlsGroup]
  val BillingProject = vertexClassOf[org.broadinstitute.dsde.rawls.model.RawlsBillingProject]
  val PendingBucketDeletions = vertexClassOf[org.broadinstitute.dsde.rawls.model.PendingBucketDeletions]

  // container types
  val Map = vertexClassOf[scala.collection.Map[String,Attribute]]

  val allClasses = Seq(Workspace, Entity, MethodConfig, MethodRepoMethod, Submission, Workflow, WorkflowFailure, User, Group, BillingProject, PendingBucketDeletions, Map)

  def vertexClassOf[T :TypeTag]: String = typeOf[T].typeSymbol.name.decodedName.toString
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


import CachedTypes._

trait GraphDAO {

  val methodConfigEdge: String = "methodConfigEdge"
  val methodRepoMethodEdge: String = "methodRepoMethodEdge"
  val methodRepoConfigEdge: String = "methodRepoConfigEdge"
  val submissionEdge: String = "submissionEdge"

  implicit def toPipeFunction[A, B](f: A => B) = new PipeFunction[A, B] {
    override def compute(a: A): B = f(a)
  }

  //in general, we only support alphanumeric, spaces, _, and - for user-input
  def validateUserDefinedString(s: String) = {
    if(!s.matches("[A-z0-9_-]+")) throw new RawlsExceptionWithStatusCode(message = s"""Invalid input: "$s". Input may only contain alphanumeric characters, underscores, and dashes.""", statusCode = StatusCodes.BadRequest)
  }

  def validateAttributeName(name: String) = {
    if (Attributable.reservedAttributeNames.exists(_.equalsIgnoreCase(name))) {
      throw new RawlsExceptionWithStatusCode(message = s"Attribute name $name is reserved", statusCode = StatusCodes.BadRequest)
    }
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
  def getVertexClass(vertex: Vertex): String = vertex.asInstanceOf[OrientVertex].getRecord.getClassName

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

  //turns a PipeFunction into one that takes a LoopBundle
  implicit def pipeToLoopBundle[A,B](f: PipeFunction[A,B]) = new PipeFunction[LoopPipe.LoopBundle[A], B] {
    override def compute(bundle: LoopPipe.LoopBundle[A]) : B = f.compute(bundle.getObject)
  }

  def invert[A](f: PipeFunction[A, java.lang.Boolean]) = new PipeFunction[A, java.lang.Boolean] {
    override def compute(v: A) = !f.compute(v)
  }

  def or[A](f1: PipeFunction[A, java.lang.Boolean], f2: PipeFunction[A, java.lang.Boolean]) = new PipeFunction[A, java.lang.Boolean] {
    override def compute(v: A) = f1.compute(v) || f2.compute(v)
  }

  // named GremlinPipelines

  def workspacePipeline(db: Graph, workspaceName: WorkspaceName) = {
    new GremlinPipeline(db.asInstanceOf[OrientGraph].getVerticesOfClass(VertexSchema.Workspace)).filter(hasProperties(Map("namespace" -> workspaceName.namespace, "name" -> workspaceName.name)))
  }

  def workspacePipeline(db: Graph, workspaceId: String) = {
    new GremlinPipeline(db.asInstanceOf[OrientGraph].getVerticesOfClass(VertexSchema.Workspace)).filter(hasPropertyValue("workspaceId",workspaceId))
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

  def allActiveSubmissionsPipeline(db: Graph) = {
    new GremlinPipeline(db.asInstanceOf[OrientGraph].getVerticesOfClass(VertexSchema.Submission)).filter(hasPropertyValue("status",SubmissionStatuses.Submitted.toString))
  }

  def userPipeline(db: Graph, user: RawlsUserRef) = {
    new GremlinPipeline(db.asInstanceOf[OrientGraph].getVerticesOfClass(VertexSchema.User)).filter(hasPropertyValue("userSubjectId",user.userSubjectId.value))
  }

  def allUsersPipeline(db: Graph) = {
    new GremlinPipeline(db.asInstanceOf[OrientGraph].getVerticesOfClass(VertexSchema.User))
  }

  def userPipelineByEmail(db: Graph, email: String) = {
    new GremlinPipeline(db.asInstanceOf[OrientGraph].getVerticesOfClass(VertexSchema.User)).filter(hasPropertyValue("userEmail",email))
  }

  def groupPipeline(db: Graph, group: RawlsGroupRef) = {
    new GremlinPipeline(db.asInstanceOf[OrientGraph].getVerticesOfClass(VertexSchema.Group)).filter(hasPropertyValue("groupName",group.groupName.value))
  }

  def groupPipelineByEmail(db: Graph, email: String) = {
    new GremlinPipeline(db.asInstanceOf[OrientGraph].getVerticesOfClass(VertexSchema.Group)).filter(hasPropertyValue("groupEmail",email))
  }

  def billingProjectPipeline(db: Graph, projectName: RawlsBillingProjectName) = {
    new GremlinPipeline(db.asInstanceOf[OrientGraph].getVerticesOfClass(VertexSchema.BillingProject)).filter(hasPropertyValue("projectName",projectName.value))
  }

  def pendingBucketDeletionsPipeline(db: Graph): GremlinPipeline[_, Vertex] = {
    new GremlinPipeline(db.asInstanceOf[OrientGraph].getVerticesOfClass(VertexSchema.PendingBucketDeletions))
  }

  // vertex getters

  def getWorkspaceVertex(db: Graph, workspaceName: WorkspaceName) = {
    getSinglePipelineResult(workspacePipeline(db, workspaceName))
  }

  def getWorkspaceVertex(db: Graph, workspaceId: String) = {
    getSinglePipelineResult(workspacePipeline(db, workspaceId))
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

  def getAllActiveSubmissions(db: Graph) = {
    allActiveSubmissionsPipeline(db).iterator.map { submissionVertex =>
      val workspaceVertex = submissionVertex.getVertices(Direction.IN,EdgeSchema.Own.toLabel(submissionEdge)).head
      val workspaceName = WorkspaceName(workspaceVertex.getProperty[String]("namespace"), workspaceVertex.getProperty[String]("name"))
      (workspaceName, submissionVertex)
    }
  }

  def getUserVertex(db: Graph, user: RawlsUserRef) = {
    getSinglePipelineResult(userPipeline(db, user))
  }

  def getAllUserVertices(db: Graph) = {
    allUsersPipeline(db)
  }

  def getUserVertexByEmail(db: Graph, email: String) = {
    getSinglePipelineResult(userPipelineByEmail(db, email))
  }

  def getGroupVertex(db: Graph, group: RawlsGroupRef) = {
    getSinglePipelineResult(groupPipeline(db, group))
  }

  def getGroupVertexByEmail(db: Graph, email: String) = {
    getSinglePipelineResult(groupPipelineByEmail(db, email))
  }

  def getBillingProjectVertex(db: Graph, projectName: RawlsBillingProjectName) = {
    getSinglePipelineResult(billingProjectPipeline(db, projectName))
  }

  def getPendingBucketDeletionsVertex(db: Graph) = {
    getSinglePipelineResult(pendingBucketDeletionsPipeline(db))
  }

  //NOTE: We return Iterable[(tpe:Type, propName:String, value:Any)], but value will always be of type tpe.
  //We use getTypeParams to unpack collection types so we don't actually lose info but it makes matching more painful.
  def getPropertiesAndValues(tpe: AnyCachedType, obj: DomainObject): Iterable[( AnyCachedType, String, Any )] = {
    val mirror = ru.runtimeMirror(obj.getClass.getClassLoader)

    for (member <- tpe.members if member.asTerm.isVal || member.asTerm.isVar
    ) yield {
      val fieldMirror = mirror.reflect(obj).reflectField(member.asTerm)
      (member.typeSignature, member.name.decodedName.toString.trim, fieldMirror.get)
    }
  }

  def getTypeParams(tpe: Type) = tpe.asInstanceOf[TypeRefApi].args

  //SAVE METHODS
  def saveObject[T <: DomainObject :CachedType](obj: T, vertex: Vertex, wsc: Option[WorkspaceContext], graph: Graph): Unit = {
    saveObject(cachedTypeOf[T], obj, vertex, wsc, graph)
  }

  def saveObject(tpe: AnyCachedType, obj: DomainObject, vertex: Vertex, wsc: Option[WorkspaceContext], graph: Graph): Unit = {
    //Serialize out each of the case class properties.
    getPropertiesAndValues(tpe, obj).foreach({
      case (tp, prop, value) => saveProperty(tp, prop, value, vertex, wsc, graph)
    })

    if( wsc.isDefined ) {
      saveProperty(cachedTypeOf[DateTime], "lastModified", DateTime.now, wsc.get.workspaceVertex, wsc, graph)
    }
  }

  // a function to uniquely identify a graph object relative to siblings, by checking the idFields for equality
  private def domainObjectFilterFn(tpe: AnyCachedType, obj: DomainObject): Vertex => Boolean = {
    // performance improvement: evaluate id fields once and use the results in the returned function
    // instead of evaluating the fields (via reflection) within the returned function
    val idFieldValues = obj.idFields.map(field => (field, obj.getFieldValue(tpe.tpe, field)))
    (vertex: Vertex) => idFieldValues.forall { case (field, value) => value == vertex.getProperty(field) }
  }

  def saveSubObject[T <: DomainObject :CachedType](propName: String, obj: T, vertex: Vertex, wsc: Option[WorkspaceContext], graph: Graph): Vertex = {
    saveSubObject(cachedTypeOf[T], propName, obj, vertex, wsc, graph)
  }

  def saveSubObject(tpe: AnyCachedType, propName: String, obj: DomainObject, vertex: Vertex, wsc: Option[WorkspaceContext], graph: Graph): Vertex = {
    //Preserve references into this vertex. List of (vertex, edgeLabel).
    val referencers = getVertices(vertex, Direction.OUT, EdgeSchema.Own, propName)
      .filter( domainObjectFilterFn(tpe, obj) ) //only our actual object
      .map({ subObj =>
      subObj.getEdges(Direction.IN)
        .filter( e => EdgeSchema.getEdgeRelation(e.getLabel) == EdgeSchema.Ref )
        .map( e => ( e.getVertex(Direction.OUT), EdgeSchema.stripEdgeRelation(e.getLabel) ) ).toList
    }).flatten

    removeProperty(propName, vertex, graph, domainObjectFilterFn(tpe, obj))
    val objVert = addVertex(graph, tpe.vertexClass)

    //Restore vertex references.
    referencers.map { case (refVtx, label) =>
      addEdge(refVtx, EdgeSchema.Ref, label, objVert )
    }

    addEdge(vertex, EdgeSchema.Own, propName, objVert)
    saveObject(tpe, obj, objVert, wsc, graph)
    objVert
  }

  private def saveStringMap( valuesType: AnyCachedType, propName: String, map: Map[_, _], vertex: Vertex, wsc: Option[WorkspaceContext], graph: Graph): Unit = {
    val mapDummy = addVertex(graph, VertexSchema.Map)
    addEdge(vertex, EdgeSchema.Own, propName, mapDummy)

    map.map {case (key, value) =>
      saveProperty( valuesType, key.asInstanceOf[String], value, mapDummy, wsc, graph )
    }
  }

  private def saveRawlsEnumMap( valuesType: AnyCachedType, propName: String, map: Map[_, _], vertex: Vertex, wsc: Option[WorkspaceContext], graph: Graph): Unit = {
    val mapDummy = addVertex(graph, VertexSchema.Map)
    addEdge(vertex, EdgeSchema.Own, propName, mapDummy)

    map.map {case (key, value) =>
      saveProperty( valuesType, key.asInstanceOf[RawlsEnumeration[_]].toString, value, mapDummy, wsc, graph )
    }
  }

  private def saveOpt(containedType: AnyCachedType, propName: String, opt: Option[Any], vertex: Vertex, wsc:Option[WorkspaceContext], graph:Graph): Unit = {
    opt match {
      case Some(v) => saveProperty(containedType, propName, v, vertex, wsc, graph)
      case None => //done, already removed
    }
  }

  private def saveAttributeRef(ref: AttributeEntityReference, propName: String, vertex: Vertex, wsc: Option[WorkspaceContext], graph:Graph): Unit = {
    assert( wsc.isDefined, s"Can't saveAttributeRef $ref with no WorkspaceContext!" )
    val entityVertex = getEntityVertex(wsc.get, ref.entityType, ref.entityName).getOrElse {
      throw new RawlsException(s"${wsc.get.workspace.namespace}/${wsc.get.workspace.name}/${ref.entityType}/${ref.entityName} does not exist")
    }
    addEdge(vertex, EdgeSchema.Ref, propName, entityVertex)
  }

  private def saveUserRef(ref: RawlsUserRef, propName: String, vertex: Vertex, db: Graph): Unit =
    getUserVertex(db, ref) match {
      case Some(refVertex) => addEdge(vertex, EdgeSchema.Ref, propName, refVertex)
      case None => throw new RawlsException("Cannot find User corresponding to " + ref.userSubjectId)
    }

  private def saveGroupRef(ref: RawlsGroupRef, propName: String, vertex: Vertex, db: Graph): Unit =
    getGroupVertex(db, ref) match {
      case Some(refVertex) => addEdge(vertex, EdgeSchema.Ref, propName, refVertex)
      case None => throw new RawlsException("Cannot find Group corresponding to " + ref.groupName)
    }

  private def saveProperty(tpe: AnyCachedType, propName: String, valToSave: Any, vertex: Vertex, wsc: Option[WorkspaceContext], graph: Graph): Unit = {
    removeProperty(propName, vertex, graph) //remove any previously defined value
    (tpe, valToSave) match {

      //Basic types. TODO: More of these?
      case (_, value:String)  => vertex.setProperty(propName, value)
      case (_, value:Int)     => vertex.setProperty(propName, value)
      case (_, value:Boolean) => vertex.setProperty(propName, value)
      case (_, value:DateTime)=> vertex.setProperty(propName, value.toDate)

      // RawlsUser and RawlsGroup field types.
      case (_, value:UserAuthType)  => vertex.setProperty(propName, value.value)

      //Attributes.

      //Throw an exception if we attempt to save AttributeNull, since a nonexistent property and a null-valued property are indistinguishable
      case (_, AttributeNull) => throw new RawlsException(s"Attempting to save $propName as AttributeNull; this is illegal")
      case (_, value:AttributeValue) => vertex.setProperty(propName, AttributeConversions.attributeToProperty(value))
      case (_, value:AttributeEntityReference) => saveAttributeRef(value, propName, vertex, wsc, graph)
      case (_, value:AttributeValueList) => saveProperty(cachedTypeOf[Seq[AttributeValue]], propName, value.list, vertex, wsc, graph)
      case (_, value:AttributeEntityReferenceList) => saveProperty(cachedTypeOf[Seq[AttributeEntityReference]], propName, value.list, vertex, wsc, graph)

      //set the values type to Any, but it's irrelevant as the map is empty so no values will be serialized
      case (_, AttributeEmptyList) => saveStringMap( cachedTypeOf[Any], propName, Map.empty[String,Any], vertex, wsc, graph)

      //UserAuthRef types.
      case (_, value:RawlsUserRef) => saveUserRef(value, propName, vertex, graph)
      case (_, value:RawlsGroupRef) => saveGroupRef(value, propName, vertex, graph)

      //Enums.
      case (_, value:RawlsEnumeration[_]) => vertex.setProperty(propName, value.toString)

      //Collections. Note that a Seq is treated as a map with the keys as indices.
      //Watch out for type erasure! Ha ha ha ha ha. Ugh.
      case (_, seq:Seq[_]) => saveStringMap( tpe.typeParams.head, propName, seq.zipWithIndex.map({case (elem, idx) => idx.toString -> elem}).toMap, vertex, wsc, graph)
      case (tp, _) if tp <:< cachedTypeOf[Seq[_]] => saveStringMap( tpe.typeParams.head, propName, Map.empty, vertex, wsc, graph)

      case (_, set:Set[_]) => saveStringMap( tpe.typeParams.head, propName, set.zipWithIndex.map({case (elem, idx) => idx.toString -> elem}).toMap, vertex, wsc, graph)
      case (tp, _) if tp <:< cachedTypeOf[Set[_]] => saveStringMap( tpe.typeParams.head, propName, Map.empty, vertex, wsc, graph)

      case (tp, map) if tp <:< cachedTypeOf[Map[String,_]] => saveStringMap( tpe.typeParams.last, propName, map.asInstanceOf[Map[String, _]], vertex, wsc, graph)
      case (tp, map) if tp <:< cachedTypeOf[Map[_,_]] && tpe.typeParams.head.isRawlsEnum => saveRawlsEnumMap( tpe.typeParams.last, propName, map.asInstanceOf[Map[RawlsEnumeration[_],_]], vertex, wsc, graph)

      case (_, value:Option[_]) => saveOpt(tpe.typeParams.head, propName, value, vertex, wsc, graph)
      case (tp, None) if tp <:< cachedTypeOf[Option[Any]] => saveOpt(tpe.typeParams.head, propName, None, vertex, wsc, graph)

      //Everything else (including enum types).
      case (tp, value:DomainObject) => saveSubObject(tp, propName, value, vertex, wsc, graph)

      //The Forbidden Zone
      case (_, value:WorkspaceName) => throw new RawlsException("WorkspaceName is not a supported attribute type.")

      //What the hell is this? Run awaaaaay!
      case (tp, value) => throw new RawlsException(s"Error saving property $propName with value ${value.toString}: unknown type ${tpe.toString}")
    }
  }

  //LOAD METHODS
  def loadObject[T :CachedType](vertex: Vertex): T = {
    loadObject(cachedTypeOf[T], vertex).asInstanceOf[T]
  }

  def loadObject(tpe: AnyCachedType, vertex: Vertex): Any = {
    val parameters = tpe.ctorProperties.map({
      case (tp, prop) =>
        loadProperty(tp, prop, vertex)
    }).toSeq

    tpe.ctorMirror(parameters: _*)
  }

  def loadSubObject(tpe: AnyCachedType, propName: String, vertex: Vertex): Any = {
    val subVert = getVertices(vertex, Direction.OUT, EdgeSchema.Own, propName).head
    subVert.asInstanceOf[OrientVertex].getRecord.setAllowChainedAccess(false)
    loadObject(tpe, subVert)
  }

  def loadUserAuth(tpe: AnyCachedType, propName: String, vertex: Vertex): Any = {
    tpe.ctorMirror(vertex.getProperty(propName))
  }

  private def loadStringMap(valuesType: AnyCachedType, propName: String, vertex: Vertex): Map[String, Any] = {
    val mapDummy = getVertices(vertex, Direction.OUT, EdgeSchema.Own, propName).head
    mapDummy.asInstanceOf[OrientVertex].getRecord.setAllowChainedAccess(false)

    getVertexKeys(mapDummy).map({ key =>
      (key, loadProperty(valuesType, key, mapDummy))
    }).toMap
  }

  private def loadRawlsEnumMap[R <: RawlsEnumeration[R]](keyType: AnyCachedType, valuesType: AnyCachedType, propName: String, vertex: Vertex): Map[R, Any] = {
    val mapDummy = getVertices(vertex, Direction.OUT, EdgeSchema.Own, propName).head
    mapDummy.asInstanceOf[OrientVertex].getRecord.setAllowChainedAccess(false)

    getVertexKeys(mapDummy).map({ key =>
      (keyType.enumWithName(key), loadProperty(valuesType, key, mapDummy))
    }).toMap
  }

  private def loadOpt(containedType: AnyCachedType, propName: String, vertex: Vertex): Option[Any] = {
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
        loadProperty(cachedTypeOf[Seq[AttributeValue]], propName, myVertex)
          .asInstanceOf[Seq[AttributeValue]] )
    } else {
      AttributeEntityReferenceList(
        loadProperty( cachedTypeOf[Seq[AttributeEntityReference]], propName, myVertex )
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
      case Some(value) => loadProperty(cachedTypeOf[AttributeValue], propName, vertex).asInstanceOf[AttributeValue]
      case None =>
        val refEdges = vertex.getEdges(Direction.OUT, EdgeSchema.Ref.toLabel(propName)).toSeq
        val ownEdges = vertex.getEdges(Direction.OUT, EdgeSchema.Own.toLabel(propName)).toSeq

        //The second assert here will need to be removed if we ever implement Set types.
        assert( refEdges.size + ownEdges.size > 0, s"Attribute in map with property name $propName seems to be missing" )
        assert( refEdges.size + ownEdges.size <= 1, s"Attribute in map with property name $propName is duplicated" )

        if( refEdges.size == 1 ) {
          //The only place we currently have references is AttributeEntityRefs.
          assert(getVertexClass(refEdges.head.getVertex(Direction.IN)).equalsIgnoreCase(VertexSchema.Entity))
          loadProperty(cachedTypeOf[AttributeEntityReference], propName, vertex).asInstanceOf[AttributeEntityReference]
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

  private def loadUserRef(propName: String, vertex: Vertex): RawlsUserRef = {
    val refVtx = vertex.getVertices(Direction.OUT, EdgeSchema.Ref.toLabel(propName)).head
    RawlsUserRef(RawlsUserSubjectId(refVtx.getProperty("userSubjectId")))
  }

  private def loadGroupRef(propName: String, vertex: Vertex): RawlsGroupRef = {
    val refVtx = vertex.getVertices(Direction.OUT, EdgeSchema.Ref.toLabel(propName)).head
    RawlsGroupRef(RawlsGroupName(refVtx.getProperty("groupName")))
  }

  private def loadAttributeList[T](propName: String, vertex: Vertex): Seq[T] = {
    loadProperty(cachedTypeOf[Seq[AttributeValue]], propName, vertex).asInstanceOf[Seq[T]]
  }

  private def loadProperty(tpe: AnyCachedType, propName: String, vertex: Vertex): Any = {
    tpe match {
      //Basic types. TODO: More of these?
      case tp if tp =:= cachedTypeOf[String]  => vertex.getProperty(propName)
      case tp if tp =:= cachedTypeOf[Int]     => vertex.getProperty(propName)
      case tp if tp =:= cachedTypeOf[Boolean] => vertex.getProperty(propName)
      //serializing joda datetime across the network breaks, though it works fine in-memory
      case tp if tp =:= cachedTypeOf[DateTime]=> new DateTime(vertex.getProperty(propName).asInstanceOf[Date])

      // RawlsUser and RawlsGroup field types.
      case tp if tp <:< cachedTypeOf[UserAuthType]  => loadUserAuth(tp, propName, vertex)

      //Attributes.
      case tp if tp <:< cachedTypeOf[AttributeValue] => AttributeConversions.propertyToAttribute(vertex.getProperty(propName))
      case tp if tp <:< cachedTypeOf[AttributeEntityReference] => loadAttributeRef(propName, vertex)
      case tp if tp <:< cachedTypeOf[AttributeValueList] => AttributeValueList(loadAttributeList[AttributeValue](propName, vertex))
      case tp if tp <:< cachedTypeOf[AttributeEntityReferenceList] => AttributeEntityReferenceList(loadAttributeList[AttributeEntityReference](propName, vertex))
      case tp if tp <:< cachedTypeOf[Attribute] => loadMysteriouslyTypedAttribute(propName, vertex)

      //UserAuthRef types.
      case tp if tp <:< cachedTypeOf[RawlsUserRef] => loadUserRef(propName, vertex)
      case tp if tp <:< cachedTypeOf[RawlsGroupRef] => loadGroupRef(propName, vertex)

      //Enums.
      case tp if tp.isRawlsEnum => tp.enumWithName(vertex.getProperty(propName))

      //Collections. Note that a Seq is treated as a map with the keys as indices.
      case tp if tp <:< cachedTypeOf[Seq[_]] => loadStringMap(tp.typeParams.head, propName, vertex).toSeq.sortBy(_._1.toInt).map(_._2)
      case tp if tp <:< cachedTypeOf[Set[_]] => loadStringMap(tp.typeParams.head, propName, vertex).toSeq.sortBy(_._1.toInt).map(_._2).toSet
      case tp if tp <:< cachedTypeOf[Map[String,_]] => loadStringMap(tp.typeParams.last, propName, vertex)

      case tp if tp <:< cachedTypeOf[Map[_,_]] && tp.typeParams.head.isRawlsEnum => loadRawlsEnumMap(tp.typeParams.head, tp.typeParams.last, propName, vertex)

      case tp if tp <:< cachedTypeOf[Option[_]] => loadOpt(tp.typeParams.head, propName, vertex)

      //Everything else.
      case tp if tp <:< cachedTypeOf[DomainObject] => loadSubObject(tp, propName, vertex)

      //The Forbidden Zone
      case tp if tp <:< cachedTypeOf[WorkspaceName] => throw new RawlsException("WorkspaceName is not a supported attribute type.")

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

