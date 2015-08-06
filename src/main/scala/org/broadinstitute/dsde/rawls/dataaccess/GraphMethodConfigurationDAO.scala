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
  /** gets by method config name */
  override def get(workspaceNamespace: String, workspaceName: String, methodConfigurationNamespace: String, methodConfigurationName: String, txn: RawlsTransaction): Option[MethodConfiguration] = txn withGraph { graph =>
    // notice that methodConfigPipe is a def, not a val so we get a new pipeline every time
    def methodConfigPipe = methodConfigPipeline(graph, workspaceNamespace, workspaceName, methodConfigurationNamespace, methodConfigurationName)
    getSinglePipelineResult(methodConfigPipe) map { loadFromVertex[MethodConfiguration](_, Option(WorkspaceName(workspaceNamespace, workspaceName))) }
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
    workspacePipeline(graph, workspaceNamespace, workspaceName).out(methodConfigEdge).toList map (loadFromVertex[MethodConfigurationShort](_, Option(WorkspaceName(workspaceNamespace, workspaceName))))
  }

  /** creates or replaces a method configuration */
  override def save(workspaceNamespace: String, workspaceName: String, methodConfiguration: MethodConfiguration, txn: RawlsTransaction): MethodConfiguration = txn withGraph { graph =>
    val workspace = getWorkspaceVertex(graph, workspaceNamespace, workspaceName)
      .getOrElse(throw new IllegalArgumentException("Cannot save entity to nonexistent workspace " + workspaceNamespace + "::" + workspaceName))

    def methodConfigPipe = methodConfigPipeline(graph, workspaceNamespace, workspaceName, methodConfiguration.namespace, methodConfiguration.name)
    val configVertex = getSinglePipelineResult(methodConfigPipe) match {
      case None =>
        val configVertex = addVertex(graph, VertexSchema.MethodConfig)
        addEdge(workspace, methodConfigEdge, configVertex)
        configVertex
      case Some(configVertex) => configVertex
    }
    saveToVertex(methodConfiguration, configVertex, graph, WorkspaceName(workspaceNamespace, workspaceName))

    methodConfiguration
  }
}
