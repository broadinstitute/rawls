package org.broadinstitute.dsde.rawls.dataaccess

import com.tinkerpop.blueprints.{Graph, Vertex}
import com.tinkerpop.gremlin.java.GremlinPipeline
import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.model.{WorkspaceName, Workspace}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.language.implicitConversions

class GraphWorkspaceDAO extends WorkspaceDAO with GraphDAO {

  override def save(workspace: Workspace, txn: RawlsTransaction): Workspace = txn withGraph { db =>
    // check for illegal dot characters
    validateUserDefinedString(workspace.namespace)
    validateUserDefinedString(workspace.name)
    workspace.attributes.keys.foreach(validateUserDefinedString)

    // get the workspace, creating if it doesn't already exist
    val vertex = getWorkspaceVertex(db, workspace.toWorkspaceName).getOrElse(addVertex(db, VertexSchema.Workspace))
    val context = WorkspaceContext(workspace.toWorkspaceName, workspace.bucketName, vertex)
    // note that the vertex gets passed in twice (directly and through WorkspaceContext)
    saveToVertex[Workspace](db, context, workspace, vertex)
    workspace
  }

  override def load(workspaceName: WorkspaceName, txn: RawlsTransaction): Option[Workspace] = {
    loadContext(workspaceName, txn) map { context => loadFromVertex[Workspace](context.workspaceVertex, Some(workspaceName)) }
  }

  /**
   * Gets a WorkspaceContext, without fully loading the Workspace and its associated sub-vertices.
   */
  override def loadContext(workspaceName: WorkspaceName, txn: RawlsTransaction): Option[WorkspaceContext] = txn withGraph { db =>
    getWorkspaceVertex(db, workspaceName) map { vertex =>
      val bucketName = Option(vertex.getProperty[String]("bucketName")).getOrElse(throw new RawlsException(s"Workspace ${workspaceName} is missing a bucketName"))
      WorkspaceContext(workspaceName, bucketName, vertex)
    }
  }

  override def list(txn: RawlsTransaction): Seq[Workspace] = txn withGraph { db =>
    new GremlinPipeline(db).V().filter(isWorkspace).transform((v: Vertex) => loadFromVertex[Workspace](v, None)).toList.asScala
  }

  override def delete(workspaceName: WorkspaceName, txn: RawlsTransaction) : Boolean = txn withGraph { db =>
    getWorkspaceVertex(db, workspaceName) match {
      case Some(v) => {
        val ws = loadFromVertex[Workspace](v, Some(workspaceName))
        deleteVertex(ws, v, WorkspaceContext(ws.toWorkspaceName, ws.bucketName, v))
        true
      }
      case None => false
    }
  }
}
