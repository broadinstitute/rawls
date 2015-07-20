package org.broadinstitute.dsde.rawls.dataaccess

import com.tinkerpop.blueprints.{Graph, Vertex, Direction}
import com.tinkerpop.gremlin.java.GremlinPipeline
import org.broadinstitute.dsde.rawls.model._
import scala.collection.JavaConversions._

/**
 * Graph implementation of method config dao. Method configs are stored as a
 * top level vertex and 3 subordinate vertices for each of inputs, outputs and
 * prerequisites. Which sub-vertex is which is recorded on the connecting edge.
 */
class GraphMethodConfigurationDAO extends MethodConfigurationDAO with GraphDAO {
  private val InputsEdge: String = "inputs"
  private val OutputsEdge: String = "outputs"
  private val PrerequisitesEdge: String = "prerequisites"

  private def getMethodStoreConfig(props: Map[String, String]): MethodStoreConfiguration = {
    props.get("methodConfigNamespace") match {
      case Some(ns) => MethodStoreConfiguration(ns, props.get("methodConfigName").get, props.get("methodConfigVersion").get)
      case None => null
    }
  }
  private def getMethodStoreMethod(props: Map[String, String]): MethodStoreMethod = {
    props.get("methodNamespace") match {
      case Some(ns) => MethodStoreMethod(ns, props.get("methodName").get, props.get("methodVersion").get)
      case None => null
    }
  }
  /** gets by method config name */
  override def get(workspaceNamespace: String, workspaceName: String, methodConfigurationNamespace: String, methodConfigurationName: String, txn: RawlsTransaction): Option[MethodConfiguration] = txn withGraph { graph =>
    // notice that methodConfigPipe is a def, not a val so we get a new pipeline every time
    def methodConfigPipe = methodConfigPipeline(graph, workspaceNamespace, workspaceName, methodConfigurationNamespace, methodConfigurationName)

    getVertexProperties[String](methodConfigPipe) map { methodConfigProps =>
      val inputs = getVertexProperties[String](methodConfigPipe.out(InputsEdge))
      val outputs = getVertexProperties[String](methodConfigPipe.out(OutputsEdge))
      val prerequisites = getVertexProperties[String](methodConfigPipe.out(PrerequisitesEdge))



      fromPropertyMap[MethodConfiguration](methodConfigProps ++
        Map(
          "inputs" -> inputs.getOrElse(Map.empty),
          "outputs" -> outputs.getOrElse(Map.empty),
          "prerequisites" -> prerequisites.getOrElse(Map.empty),
          "workspaceName" -> WorkspaceName(workspaceNamespace, workspaceName),
          "methodStoreConfig" -> getMethodStoreConfig(methodConfigProps),
          "methodStoreMethod" -> getMethodStoreMethod(methodConfigProps)
        ))
    }
  }

  /** rename method configuration */
  override def rename(workspaceNamespace: String, workspaceName: String, methodConfigurationNamespace: String, methodConfigurationName: String, newName: String, txn: RawlsTransaction): Unit = txn withGraph { graph =>
    getSinglePipelineResult(methodConfigPipeline(graph, workspaceNamespace, workspaceName, methodConfigurationNamespace, methodConfigurationName)).foreach {
      _.setProperty("_name", newName)
    }
  }

  /** delete a method configuration */
  override def delete(workspaceNamespace: String, workspaceName: String, methodConfigurationNamespace: String, methodConfigurationName: String, txn: RawlsTransaction): Unit = txn withGraph { graph =>
    def methodConfigPipe = methodConfigPipeline(graph, workspaceNamespace, workspaceName, methodConfigurationNamespace, methodConfigurationName)
    methodConfigPipe.out().remove()
    methodConfigPipe.remove()
  }

  /** list all method configurations in the workspace */
  override def list(workspaceNamespace: String, workspaceName: String, txn: RawlsTransaction): TraversableOnce[MethodConfigurationShort] = txn withGraph { graph =>
    getPropertiesOfVertices[String](workspacePipeline(graph, workspaceNamespace, workspaceName).out(MethodConfigEdgeType)) map { methodConfigProps =>
      fromPropertyMap[MethodConfigurationShort](methodConfigProps ++
        Map("workspaceName" -> WorkspaceName(workspaceNamespace, workspaceName),
            "methodStoreConfig" -> getMethodStoreConfig(methodConfigProps),
            "methodStoreMethod" -> getMethodStoreMethod(methodConfigProps)))
    }
  }

  /** creates or replaces a method configuration */
  override def save(workspaceNamespace: String, workspaceName: String, methodConfiguration: MethodConfiguration, txn: RawlsTransaction): MethodConfiguration = txn withGraph { graph =>
    val workspace = getWorkspaceVertex(graph, workspaceNamespace, workspaceName)
      .getOrElse(throw new IllegalArgumentException("Cannot save entity to nonexistent workspace " + workspaceNamespace + "::" + workspaceName))

    def methodConfigPipe = methodConfigPipeline(graph, workspaceNamespace, workspaceName, methodConfiguration.namespace, methodConfiguration.name)

    // get the methodConfigVertex, creating if it doesn't already exist
    val (configVertex, inputsVertex, outputsVertex, prerequisitesVertex) =
      getSinglePipelineResult(methodConfigPipe) match {
        case None => {
          val configVertex = addVertex(graph, null)
          workspace.addEdge(MethodConfigEdgeType, configVertex)

          val (inputsVertex, outputsVertex, prerequisitesVertex) = createSubVertices(graph, configVertex)

          (configVertex, inputsVertex, outputsVertex, prerequisitesVertex)
        }

        case Some(configVertex) => {
          configVertex.getVertices(Direction.OUT).foreach(_.remove())
          val (inputsVertex, outputsVertex, prerequisitesVertex) = createSubVertices(graph, configVertex)
          (configVertex, inputsVertex, outputsVertex, prerequisitesVertex)
        }
      }

    setVertexProperties(methodConfiguration, configVertex)


    Option(methodConfiguration.methodStoreConfig).map(_ => {
        configVertex.setProperty("methodConfigNamespace", methodConfiguration.methodStoreConfig.methodConfigNamespace)
        configVertex.setProperty("methodConfigName", methodConfiguration.methodStoreConfig.methodConfigName)
        configVertex.setProperty("methodConfigVersion", methodConfiguration.methodStoreConfig.methodConfigVersion)
      })
    configVertex.setProperty("methodNamespace", methodConfiguration.methodStoreMethod.methodNamespace)
    configVertex.setProperty("methodName", methodConfiguration.methodStoreMethod.methodName)
    configVertex.setProperty("methodVersion", methodConfiguration.methodStoreMethod.methodVersion)

    methodConfiguration.inputs.foreach { entry => inputsVertex.setProperty(entry._1, entry._2) }
    methodConfiguration.outputs.foreach { entry => outputsVertex.setProperty(entry._1, entry._2) }
    methodConfiguration.prerequisites.foreach { entry => prerequisitesVertex.setProperty(entry._1, entry._2) }

    methodConfiguration
  }

  private def createSubVertices(graph: Graph, configVertex: Vertex): (Vertex, Vertex, Vertex) = {
    val inputsVertex = addVertex(graph, null)
    configVertex.addEdge(InputsEdge, inputsVertex)
    val outputsVertex = addVertex(graph, null)
    configVertex.addEdge(OutputsEdge, outputsVertex)
    val prerequisitesVertex = addVertex(graph, null)
    configVertex.addEdge(PrerequisitesEdge, prerequisitesVertex)
    (inputsVertex, outputsVertex, prerequisitesVertex)
  }
}
