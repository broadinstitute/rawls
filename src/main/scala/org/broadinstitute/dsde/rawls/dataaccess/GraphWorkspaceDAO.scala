package org.broadinstitute.dsde.rawls.dataaccess

import com.tinkerpop.blueprints.Vertex
import com.tinkerpop.gremlin.java.GremlinPipeline
import org.broadinstitute.dsde.rawls.model.Workspace

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.language.implicitConversions

class GraphWorkspaceDAO() extends WorkspaceDAO with GraphDAO {

  def save(workspace: Workspace, txn: RawlsTransaction) = txn withGraph { db =>
    val workspaceVertex = getWorkspaceVertex(db, workspace.namespace, workspace.name).getOrElse({
      setVertexProperties(workspace, addVertex(db, null))
    })

    // remove all attributes in the vertex that are not in workspace.attributes
    val removeProps = workspaceVertex.getPropertyKeys.filterNot(isReservedProperty(_)) -- workspace.attributes.keys
    def removeFunc = (key:String) => {workspaceVertex.removeProperty(key); null}
    removeProps.foreach(removeFunc)

    workspace.attributes.foreach(entry => workspaceVertex.setProperty(entry._1, entry._2))
    workspace
  }

  def load(namespace: String, name: String, txn: RawlsTransaction): Option[Workspace] = txn withGraph { db =>
    getWorkspaceVertex(db, namespace, name).map(vertexToWorkspace(_))
  }

  def vertexToWorkspace(workspaceVertex: Vertex): Workspace = {
    val attributes = workspaceVertex.getPropertyKeys.filterNot(isReservedProperty(_)).map(key => key -> workspaceVertex.getProperty(key)).toMap
    fromVertex[Workspace](workspaceVertex, Map("attributes" -> attributes))
  }

  def list(txn: RawlsTransaction): Seq[Workspace] = txn withGraph { db =>
    new GremlinPipeline(db).V("_clazz", classOf[Workspace].getSimpleName).transform((v: Vertex) => vertexToWorkspace(v)).toList.asScala
  }
}
