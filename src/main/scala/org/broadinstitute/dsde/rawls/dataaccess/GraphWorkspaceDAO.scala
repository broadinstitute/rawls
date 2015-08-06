package org.broadinstitute.dsde.rawls.dataaccess

import com.tinkerpop.blueprints.Vertex
import com.tinkerpop.gremlin.java.GremlinPipeline
import org.broadinstitute.dsde.rawls.model.{WorkspaceName, Workspace}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.language.implicitConversions

class GraphWorkspaceDAO() extends WorkspaceDAO with GraphDAO {

  override def save(workspace: Workspace, txn: RawlsTransaction) = txn withGraph { db =>
    // check for illegal dot characters
    validateUserDefinedString(workspace.namespace)
    validateUserDefinedString(workspace.name)
    workspace.attributes.keys.foreach(validateUserDefinedString)

    // get the workspace, creating if it doesn't already exist
    val workspaceVertex = getWorkspaceVertex(db, workspace.namespace, workspace.name).getOrElse(addVertex(db, VertexSchema.Workspace))
    saveToVertex[Workspace](workspace, workspaceVertex, db, workspace.toWorkspaceName)
    workspace
  }

  override def load(namespace: String, name: String, txn: RawlsTransaction): Option[Workspace] = txn withGraph { db =>
    getWorkspaceVertex(db, namespace, name).map(loadFromVertex[Workspace](_, None))
  }

  override def list(txn: RawlsTransaction): Seq[Workspace] = txn withGraph { db =>
    new GremlinPipeline(db).V().filter(isWorkspace).transform((v: Vertex) => loadFromVertex[Workspace](v, None)).toList.asScala
  }
}
