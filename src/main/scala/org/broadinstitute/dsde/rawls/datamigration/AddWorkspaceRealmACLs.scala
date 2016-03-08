package org.broadinstitute.dsde.rawls.datamigration

import com.tinkerpop.blueprints.Vertex
import com.tinkerpop.gremlin.java.GremlinPipeline
import com.typesafe.config.Config
import org.broadinstitute.dsde.rawls.dataaccess._
import scala.collection.JavaConversions._

/**
 * Adds realm ACLs to workspaces that existed pre-realm
 */
object AddWorkspaceRealmACLs extends DataMigration with GraphDAO {

  override val migrationId: String = "AddWorkspaceRealmACLs"

  override def migrate(config: Config, containerDAO: DbContainerDAO, txn: RawlsTransaction): Unit = txn withGraph { db =>
    val workspaceVertices = new GremlinPipeline(db).V().filter(isWorkspace).toList

    /*
     * Find all workspace vertices, and add an empty realmACL map to each
     */

    workspaceVertices.foreach { ws =>
      val aclMap = addVertex(db, VertexSchema.Map)
      val aclMapEdge = addEdge(ws, EdgeSchema.Own, "realmACLs", aclMap)
    }

    /*
     * Each workspace now has a realmACL map, so they can be loaded.
     * Since none of these workspaces will have a realm, re-save the workspaces
     * with their realmACLs pointing to their accessGroups
     */

    val workspaces = containerDAO.workspaceDAO.list(txn).toSeq

    workspaces.foreach { ws =>
      containerDAO.workspaceDAO.save(ws.copy(realmACLs = ws.accessLevels), txn)
    }

  }

}
