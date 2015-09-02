package org.broadinstitute.dsde.rawls.model

import com.tinkerpop.blueprints.{Edge, Direction, Vertex}
import com.tinkerpop.blueprints.impls.orient.{OrientVertex, OrientGraph}
import com.tinkerpop.gremlin.java.GremlinPipeline
import com.tinkerpop.pipes.PipeFunction
import com.tinkerpop.pipes.branch.LoopPipe
import scala.collection.JavaConversions._

import org.joda.time.DateTime

trait Attributable {
  def attributes: Map[String, Attribute]
  def briefName: String
}

/**
 * Created by dvoet on 4/24/15.
 */
case class WorkspaceName(
                      namespace: String,
                      name: String) {
  override def toString = namespace + "/" + name // used in error messages
  def path = s"/workspaces/${namespace}/${name}"
}

case class WorkspaceRequest (
                      namespace: String,
                      name: String,
                      attributes: Map[String, Attribute]
                      ) extends Attributable {
  def toWorkspaceName = WorkspaceName(namespace,name)
  def briefName = toWorkspaceName.toString
}

case class Workspace (
                      namespace: String,
                      name: String,
                      bucketName: String,
                      createdDate: DateTime,
                      createdBy: String,
                      attributes: Map[String, Attribute]
                      ) extends Attributable {
  def toWorkspaceName = WorkspaceName(namespace,name)
  def briefName = toWorkspaceName.toString
}

case class EntityName(
                   name: String)

case class Entity(
                   name: String,
                   entityType: String,
                   attributes: Map[String, Attribute]
                   ) extends Attributable {
  def briefName = name
  def path( workspaceName: WorkspaceName ) = workspaceName.path+s"/entities/${name}"
}

case class MethodConfigurationName(
                   name: String,
                   namespace: String,
                   workspaceName: WorkspaceName
                   )

case class MethodConfigurationNamePair(
                   source: MethodConfigurationName,
                   destination: MethodConfigurationName
                   )

case class EntityCopyDefinition(
                   sourceWorkspace: WorkspaceName,
                   destinationWorkspace: WorkspaceName,
                   entityType: String,
                   entityNames: Seq[String]
                   )

case class MethodRepoMethod(
                   methodNamespace: String,
                   methodName: String,
                   methodVersion: String
                   )

case class MethodRepoConfiguration(
                   methodConfigNamespace: String,
                   methodConfigName: String,
                   methodConfigVersion: String
                   )

case class MethodConfiguration(
                   namespace: String,
                   name: String,
                   rootEntityType: String,
                   prerequisites: Map[String, AttributeString],
                   inputs: Map[String, AttributeString],
                   outputs: Map[String, AttributeString],
                   methodRepoConfig:MethodRepoConfiguration,
                   methodRepoMethod:MethodRepoMethod
                   ) {
  def toShort : MethodConfigurationShort = MethodConfigurationShort(name, rootEntityType, methodRepoConfig, methodRepoMethod, namespace)
  def path( workspaceName: WorkspaceName ) = workspaceName.path+s"/methodConfigs/${namespace}/${name}"
}

case class MethodConfigurationShort(
                                name: String,
                                rootEntityType: String,
                                methodStoreConfig:MethodRepoConfiguration,
                                methodStoreMethod:MethodRepoMethod,
                                namespace: String)

case class MethodRepoConfigurationQuery(
                                         methodRepoNamespace: String,
                                         methodRepoName: String,
                                         methodRepoSnapshotId: String,
                                         destination: MethodConfigurationName
                                         )

case class ConflictingEntities(conflicts: Seq[String])

sealed trait Attribute
sealed trait AttributeValue extends Attribute

case object AttributeNull extends AttributeValue
case class AttributeString(val value: String) extends AttributeValue
case class AttributeNumber(val value: BigDecimal) extends AttributeValue
case class AttributeBoolean(val value: Boolean) extends AttributeValue
case object AttributeEmptyList extends Attribute
case class AttributeValueList(val list: Seq[AttributeValue]) extends Attribute
case class AttributeEntityReferenceList(val list: Seq[AttributeEntityReference]) extends Attribute
case class AttributeEntityReference(val entityType: String, val entityName: String) extends Attribute

object AttributeConversions {
  // need to do some casting to conform to this list: http://orientdb.com/docs/last/Types.html
  def attributeToProperty(att: AttributeValue): Any = att match {
    case AttributeBoolean(b) => b
    case AttributeNumber(n) => n.bigDecimal
    case AttributeString(s) => s
    case AttributeNull => null
    case _ => throw new IllegalArgumentException("Cannot serialize " + att + " as a property")
  }

  def propertyToAttribute(prop: Any): AttributeValue = prop match {
    case b: Boolean => AttributeBoolean(b)
    case n: java.math.BigDecimal => AttributeNumber(n)
    case s: String => AttributeString(s)
    case null => AttributeNull
    case _ => throw new IllegalArgumentException("Cannot deserialize " + prop + " as an attribute")
  }
}

case class GraphVizObject(data: GraphVizData, group: String,
                          removed: Boolean = false, selected: Boolean = false,
                          selectable: Boolean = true, locked: Boolean = false,
                          grabbed: Boolean = false, grabbable: Boolean = true)
case class GraphVizData(id: String, name: String, clazz: String, attributes:Map[String, String], source: Option[String] = None, target: Option[String] = None)


object GraphVizDataTransform {
  def createData(): Seq[GraphVizObject] = {
    val graph = new OrientGraph("memory:foou")
    val v:OrientVertex = graph.addVertex("v1", null)
    v.setProperty("name", "rootVertex")
    v.setProperty("foo", "bar")

    val v2:OrientVertex = graph.addVertex("v2", null)
    v2.setProperty("name", "nextVertex")
    v2.setProperty("foo2", "bar2")

    val e = v.addEdge("edge1", v2)
    e.setProperty("edge_foo", "edge_bar")

    createData(Seq(v))
  }
  def createData(vertexes: Seq[Vertex]): Seq[GraphVizObject] = {



    /*val nodeSchema = Seq(Map("name"->"label", "type"->"string"))
    val edgesSchema = Seq(Map("name"->"label", "type"->"string"))

    val nodesData  = vertexes map {v => Map("id"->v.getId.toString, "label"-> (Seq("class="+v.getClass.getSimpleName) ++ (v.getPropertyKeys map {k => (k+"="+v.getProperty(k))})).mkString(","))} toSeq
    val edges = vertexes map {v => (v.getEdges(Direction.OUT)) toSeq} flatten
    val edgesData  = edges map {e => Map("id"->e.getId.toString, "label"->e.getLabel, "source"->e.getVertex(Direction.IN).getId.toString, "target"->e.getVertex(Direction.OUT).getId.toString)} toSeq

    val dataSchema = new GraphVizVizDataSchema(nodeSchema, edgesSchema)
    val data = new GraphVizVizData(nodesData, edgesData)
    import GraphVizJsonSupport._
    println(new GraphVizData(dataSchema, data).toJson)
    new GraphVizData(dataSchema, data)*/

    def vertexMapFunction(v: Vertex) = {
      val nameVal = Option(v.getProperty("name")).getOrElse {
        val inEdges = Option(v.getEdges(Direction.IN))
        inEdges match {
          case Some(es) => es.headOption match {
            case Some(e) => "for: " + e.getVertex(Direction.OUT).getProperty("name")
            case _ => "unknown"
          }
          case _ => "unknown"
        }}
      GraphVizObject(GraphVizData(id=v.getId.toString,
                                  clazz=v.asInstanceOf[OrientVertex].getRecord.getClassName,
                                  attributes=v.getPropertyKeys.map(k => (k -> v.getProperty(k).toString)) toMap,
                                  name=(v.asInstanceOf[OrientVertex].getRecord.getClassName + "-" + nameVal)),//(v.getPropertyKeys map {(k:String) => (k+"="+v.getProperty(k))}).mkString(",")))),
        group = "nodes")
    }
    val nodesData = vertexes map(vertexMapFunction) toSeq

    val edges = vertexes map {v => (v.getEdges(Direction.OUT)) toSeq} flatten
    def edgeMapFunction(e: Edge) = {
      GraphVizObject(GraphVizData(id = e.getId.toString, name = e.getLabel, clazz="Edge",
        attributes=e.getPropertyKeys.map(k => (k -> e.getProperty(k).toString)) toMap,
        source = Some(e.getVertex(Direction.OUT).getId.toString), target = Some(e.getVertex(Direction.IN).getId.toString)),
        group = "edges")
    }
    val edgeData = edges map(edgeMapFunction) toSeq

    (nodesData ++ edgeData)
    //    {"data":{"id":"605755","name":"PCNAxxx"},"group":"nodes","removed":false,"selected":false,"selectable":true,"locked":false,"grabbed":false,"grabbable":true},
    //
    //    {"data":{"id":"611408","name":"FEN1"},"group":"nodes","removed":false,"selected":false,"selectable":true,"locked":false,"grabbed":false,"grabbable":true},
    //    {"data":{"id":"611409","name":"FEN2"},"group":"nodes","removed":false,"selected":false,"selectable":true,"locked":false,"grabbed":false,"grabbable":true},
    //
    //
    //
    //    {"data":{"source":"611408","target":"611409","group":"spd","id":"e593", "label":"FOO!"},"group":"edges","removed":false,"selected":false,"selectable":false,"locked":false,"grabbed":false,"grabbable":true},
    //    {"data":{"source":"611408","target":"605755","group":"spd","id":"e592", "label":"FOO!"},"group":"edges","removed":false,"selected":false,"selectable":false,"locked":false,"grabbed":false,"grabbable":true}
  }
}

object WorkspaceJsonSupport extends JsonSupport {

  implicit val WorkspaceNameFormat = jsonFormat2(WorkspaceName)

  implicit val EntityFormat = jsonFormat3(Entity)

  implicit val WorkspaceRequestFormat = jsonFormat3(WorkspaceRequest)

  implicit val WorkspaceFormat = jsonFormat6(Workspace)

  implicit val EntityNameFormat = jsonFormat1(EntityName)

  implicit val MethodConfigurationNameFormat = jsonFormat3(MethodConfigurationName)

  implicit val MethodConfigurationNamePairFormat = jsonFormat2(MethodConfigurationNamePair)

  implicit val EntityCopyDefinitionFormat = jsonFormat4(EntityCopyDefinition)

  implicit val MethodStoreMethodFormat = jsonFormat3(MethodRepoMethod)

  implicit val MethodStoreConfigurationFormat = jsonFormat3(MethodRepoConfiguration)

  implicit val MethodConfigurationFormat = jsonFormat8(MethodConfiguration)

  implicit val MethodConfigurationShortFormat = jsonFormat5(MethodConfigurationShort)

  implicit val MethodRepoConfigurationQueryFormat = jsonFormat4(MethodRepoConfigurationQuery)

  implicit val ConflictingEntitiesFormat = jsonFormat1(ConflictingEntities)

  implicit val GraphVizDataFormat = jsonFormat6(GraphVizData)
  implicit val GraphVizObjectFormat = jsonFormat8(GraphVizObject)
}
