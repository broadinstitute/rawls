package org.broadinstitute.dsde.rawls.dataaccess

import com.tinkerpop.blueprints.{Graph, Vertex, Direction}
import com.tinkerpop.gremlin.java.GremlinPipeline
import org.broadinstitute.dsde.rawls.model.{MethodConfigurationShort, Method, WorkspaceName, MethodConfiguration}
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

  /** gets by method config name */
  override def get(workspaceNamespace: String, workspaceName: String, methodConfigurationNamespace: String, methodConfigurationName: String, txn: RawlsTransaction): Option[MethodConfiguration] = txn withGraph { graph =>
    // notice that methodConfigPipe is a def, not a val so we get a new pipeline every time
    def methodConfigPipe = methodConfigPipeline(graph, workspaceNamespace, workspaceName, methodConfigurationNamespace, methodConfigurationName)

    getVertexProperties[String](methodConfigPipe) map { methodConfigProps =>
      val inputs = getVertexProperties[String](methodConfigPipe.out(InputsEdge))
      val outputs = getVertexProperties[String](methodConfigPipe.out(OutputsEdge))
      val prerequisites = getVertexProperties[String](methodConfigPipe.out(PrerequisitesEdge))

      MethodConfiguration(
        namespace = methodConfigProps("namespace"),
        name = methodConfigProps("name"),
        rootEntityType = methodConfigProps("rootEntityType"),
        workspaceName = WorkspaceName(workspaceNamespace, workspaceName),
        inputs = inputs.getOrElse(Map.empty),
        outputs = outputs.getOrElse(Map.empty),
        prerequisites = prerequisites.getOrElse(Map.empty),
        method = Method(methodConfigProps("methodName"), methodConfigProps("methodNamespace"), methodConfigProps("methodVersion"))
      )
    }
  }

  /** rename method configuration */
  override def rename(workspaceNamespace: String, workspaceName: String, methodConfigurationNamespace: String, methodConfigurationName: String, newName: String, txn: RawlsTransaction): Unit = txn withGraph { graph =>
    getSinglePipelineResult(methodConfigPipeline(graph, workspaceNamespace, workspaceName, methodConfigurationNamespace, methodConfigurationName)).foreach {
      _.setProperty("name", newName)
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
      MethodConfigurationShort(
        namespace = methodConfigProps("namespace"),
        name = methodConfigProps("name"),
        rootEntityType = methodConfigProps("rootEntityType"),
        workspaceName = WorkspaceName(workspaceNamespace, workspaceName),
        method = Method(methodConfigProps("methodName"), methodConfigProps("methodNamespace"), methodConfigProps("methodVersion"))
      )
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
          val configVertex = graph.addVertex(null)
          configVertex.setProperty("namespace", methodConfiguration.namespace)
          configVertex.setProperty("name", methodConfiguration.name)
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

    configVertex.setProperty("rootEntityType", methodConfiguration.rootEntityType)
    configVertex.setProperty("methodNamespace", methodConfiguration.method.namespace)
    configVertex.setProperty("methodName", methodConfiguration.method.name)
    configVertex.setProperty("methodVersion", methodConfiguration.method.version)

    methodConfiguration.inputs.foreach { entry => inputsVertex.setProperty(entry._1, entry._2) }
    methodConfiguration.outputs.foreach { entry => outputsVertex.setProperty(entry._1, entry._2) }
    methodConfiguration.prerequisites.foreach { entry => prerequisitesVertex.setProperty(entry._1, entry._2) }

    methodConfiguration
  }

  private def createSubVertices(graph: Graph, configVertex: Vertex): (Vertex, Vertex, Vertex) = {
    val inputsVertex = graph.addVertex(null)
    configVertex.addEdge(InputsEdge, inputsVertex)
    val outputsVertex = graph.addVertex(null)
    configVertex.addEdge(OutputsEdge, outputsVertex)
    val prerequisitesVertex = graph.addVertex(null)
    configVertex.addEdge(PrerequisitesEdge, prerequisitesVertex)
    (inputsVertex, outputsVertex, prerequisitesVertex)
  }
}
