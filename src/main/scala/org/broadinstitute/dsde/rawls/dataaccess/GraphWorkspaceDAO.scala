package org.broadinstitute.dsde.rawls.dataaccess

import com.tinkerpop.blueprints.{Vertex, Direction, Graph}
import com.tinkerpop.gremlin.java.GremlinPipeline
import org.broadinstitute.dsde.rawls.model.{WorkspaceShort, Workspace}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

class GraphWorkspaceDAO(backingDatabase: Graph) extends WorkspaceDAO with GraphDAO {
  val db = backingDatabase

  def save(workspace: Workspace) = {
    val workspaceVertex = getWorkspaceVertex(workspace.namespace, workspace.name).getOrElse({
      val created = backingDatabase.addVertex(null)
      created.setProperty("_name", workspace.name)
      created.setProperty("_namespace", workspace.namespace)
      created.setProperty("_clazz", "workspace")
      created.setProperty("_createdDate", workspace.createdDate)
      created.setProperty("_createdBy", workspace.createdBy)
      created
    })

    val entityDAO = new GraphEntityDAO(backingDatabase)
    workspace.entities.foreach(e => e._2.foreach(f => entityDAO.save(workspace.namespace, workspace.name, f._2)))
  }

  def load(namespace: String, name: String): Option[Workspace] = {
    val workspaceVertex = getWorkspaceVertex(namespace, name)
    // for now, assume that all edges coming out of workspace vertex are entity types.
    val entityDAO = new GraphEntityDAO(backingDatabase)
    workspaceVertex.map(v => Workspace(
      v.getProperty("_namespace"),
      v.getProperty("_name"),
      v.getProperty("_createdDate"),
      v.getProperty("_createdBy"),
      // here we go...
      v.getEdges(Direction.OUT).map(
        entityTypeEdge =>
          entityTypeEdge.getLabel ->
            entityDAO
              .list(namespace, name, entityTypeEdge.getLabel)
              .map(entity => entity.name -> entity)
              .toMap
      ).toMap
    ))
  }

  def loadShort(namespace: String, name: String): Option[WorkspaceShort] = {
    val workspaceVertex = getWorkspaceVertex(namespace, name)
    workspaceVertex.map(v => WorkspaceShort(
      v.getProperty("_namespace"),
      v.getProperty("_name"),
      v.getProperty("_createdDate"),
      v.getProperty("_createdBy")
    ))
  }

  def list(): Seq[WorkspaceShort] = {
    def shortFunc(v: Vertex) = WorkspaceShort(
      v.getProperty("_namespace"),
      v.getProperty("_name"),
      v.getProperty("_createdDate"),
      v.getProperty("_createdBy")
    )
    new GremlinPipeline(backingDatabase).V("_clazz", "workspace").transform((v: Vertex) => shortFunc(v)).toList.asScala
  }
}
