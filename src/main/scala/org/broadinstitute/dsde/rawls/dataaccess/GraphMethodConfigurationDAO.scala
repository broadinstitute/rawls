package org.broadinstitute.dsde.rawls.dataaccess

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
  override def get(workspaceContext: WorkspaceContext, methodConfigurationNamespace: String, methodConfigurationName: String, txn: RawlsTransaction): Option[MethodConfiguration] = txn withGraph { graph =>
    getMethodConfigVertex(workspaceContext, methodConfigurationNamespace, methodConfigurationName) map { loadObject[MethodConfiguration] }
  }

  /** rename method configuration */
  override def rename(workspaceContext: WorkspaceContext, methodConfigurationNamespace: String, methodConfigurationName: String, newName: String, txn: RawlsTransaction): Unit = txn withGraph { graph =>
    getMethodConfigVertex(workspaceContext, methodConfigurationNamespace, methodConfigurationName) foreach { _.setProperty("name", newName) }
  }

  /** delete a method configuration */
  override def delete(workspaceContext: WorkspaceContext, methodConfigurationNamespace: String, methodConfigurationName: String, txn: RawlsTransaction): Boolean = txn withGraph { graph =>
    getMethodConfigVertex(workspaceContext, methodConfigurationNamespace, methodConfigurationName) match {
      case Some(vertex) => {
        removeObject(vertex, graph)
        true
      }
      case None => false
    }
  }

  /** list all method configurations in the workspace */
  override def list(workspaceContext: WorkspaceContext, txn: RawlsTransaction): TraversableOnce[MethodConfigurationShort] = txn withGraph { graph =>
    workspacePipeline(workspaceContext).out(EdgeSchema.Own.toLabel(methodConfigEdge)).toList.map(loadObject[MethodConfigurationShort])
  }

  /** creates or replaces a method configuration */
  override def save(workspaceContext: WorkspaceContext, methodConfiguration: MethodConfiguration, txn: RawlsTransaction): MethodConfiguration = txn withGraph { graph =>
    saveSubObject[MethodConfiguration](methodConfigEdge, methodConfiguration, workspaceContext.workspaceVertex, Some(workspaceContext), graph )
    methodConfiguration
  }
}
