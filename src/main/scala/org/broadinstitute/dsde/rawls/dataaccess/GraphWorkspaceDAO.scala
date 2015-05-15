package org.broadinstitute.dsde.rawls.dataaccess

import java.util.Date

import com.tinkerpop.blueprints.{Direction, Graph, Vertex}
import com.tinkerpop.gremlin.java.GremlinPipeline
import org.broadinstitute.dsde.rawls.model.{Entity, Workspace, WorkspaceShort}
import org.joda.time.DateTime

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

class GraphWorkspaceDAO(db: Graph) extends WorkspaceDAO with GraphDAO {

  implicit def toJodaTime(d: Date): DateTime = new DateTime(d)

  private def toWorkspaceShort(v: Vertex) = {
    WorkspaceShort(
      v.getProperty("_namespace"),
      v.getProperty("_name"),
      v.getProperty[Date]("_createdDate"),
      v.getProperty("_createdBy")
    )
  }

  private def workspaceFromShort(ws: WorkspaceShort, entities: Map[String, Map[String, Entity]]) = {
    Workspace(ws.namespace, ws.name, ws.createdDate, ws.createdBy, entities)
  }

  def save(workspace: Workspace) = {
    val workspaceVertex = getWorkspaceVertex(db, workspace.namespace, workspace.name).getOrElse({
      val created = db.addVertex(null)
      created.setProperty("_name", workspace.name)
      created.setProperty("_namespace", workspace.namespace)
      created.setProperty("_clazz", "workspace")
      created.setProperty("_createdDate", workspace.createdDate.toDate) // OrientDB only supports java.util.Date
      created.setProperty("_createdBy", workspace.createdBy)
      created
    })

    val entityDAO = new GraphEntityDAO(db)
    workspace.entities.foreach(e => e._2.foreach(f => entityDAO.save(workspace.namespace, workspace.name, f._2)))
  }

  def load(namespace: String, name: String): Option[Workspace] = {
    val workspaceVertex = getWorkspaceVertex(db, namespace, name)
    // for now, assume that all edges coming out of workspace vertex are entity types.
    val entityDAO = new GraphEntityDAO(db)
    workspaceVertex.map(v => workspaceFromShort(
      toWorkspaceShort(v),
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
    getWorkspaceVertex(db, namespace, name).map(toWorkspaceShort(_))
  }

  def list(): Seq[WorkspaceShort] = {
    new GremlinPipeline(db).V("_clazz", "workspace").transform((v: Vertex) => toWorkspaceShort(v)).toList.asScala
  }
}
