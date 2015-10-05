package org.broadinstitute.dsde.rawls.dataaccess

import com.tinkerpop.blueprints.{Graph, Vertex}
import com.tinkerpop.gremlin.java.GremlinPipeline
import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.model.{WorkspaceName, Workspace}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.language.implicitConversions

class GraphWorkspaceDAO extends WorkspaceDAO with GraphDAO {

  override def save(workspace: Workspace, txn: RawlsTransaction): WorkspaceContext = txn withGraph { db =>
    // check for illegal dot characters
    validateUserDefinedString(workspace.namespace)
    validateUserDefinedString(workspace.name)
    workspace.attributes.keys.foreach(validateUserDefinedString)
    workspace.attributes.keys.foreach(validateAttributeName)

    // get the workspace, creating if it doesn't already exist
    val vertex = getWorkspaceVertex(db, workspace.toWorkspaceName).getOrElse(addVertex(db, VertexSchema.Workspace))
    // note that the vertex gets passed in twice (directly and through WorkspaceContext)
    val workspaceContext = WorkspaceContext(workspace, vertex)
    saveObject[Workspace](workspace, vertex, workspaceContext, db, txn)
    workspaceContext
  }

  /**
   * Gets a WorkspaceContext, without fully loading the Workspace and its associated sub-vertices.
   */
  override def loadContext(workspaceName: WorkspaceName, txn: RawlsTransaction): Option[WorkspaceContext] = txn withGraph { db =>
    getWorkspaceVertex(db, workspaceName) map { vertex =>
      WorkspaceContext(loadObject[Workspace](vertex, txn), vertex)
    }
  }

  override def findById(workspaceId: String, txn: RawlsTransaction): Option[WorkspaceContext] =
    txn withGraph { db =>
      getWorkspaceVertex(db, workspaceId) map { vertex =>
        WorkspaceContext(loadObject[Workspace](vertex, txn), vertex)
    }
  }

  override def list(txn: RawlsTransaction): Seq[Workspace] = txn withGraph { db =>
    new GremlinPipeline(db).V().filter(isWorkspace).transform((v:Vertex) => loadObject[Workspace](v, txn)).toList.asScala
  }

  override def delete(workspaceName: WorkspaceName, txn: RawlsTransaction) : Boolean = txn withGraph { db =>
    getWorkspaceVertex(db, workspaceName) match {
      case Some(v) => {
        removeObject(v, db, txn)
        true
      }
      case None => false
    }
  }
}
