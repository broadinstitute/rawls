package org.broadinstitute.dsde.rawls.dataaccess

import java.util

import com.tinkerpop.blueprints.{Direction, Vertex}
import com.tinkerpop.pipes.PipeFunction
import com.tinkerpop.pipes.branch.LoopPipe

import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.model.WorkspaceAccessLevels.WorkspaceAccessLevel

import scala.collection.JavaConversions._

class GraphAuthDAO extends AuthDAO with GraphDAO {
  override def saveUser(rawlsUser: RawlsUser, txn: RawlsTransaction): RawlsUser = txn withGraph { db =>
    val vertex = getUserVertex(db, rawlsUser.userSubjectId).getOrElse(addVertex(db, VertexSchema.User))
    saveObject[RawlsUser](rawlsUser, vertex, None, db)
    rawlsUser
  }

  override def saveGroup(rawlsGroup: RawlsGroup, txn: RawlsTransaction): RawlsGroup = txn withGraph { db =>
    val vertex = getGroupVertex(db, rawlsGroup.groupName).getOrElse(addVertex(db, VertexSchema.Group))
    saveObject[RawlsGroup](rawlsGroup, vertex, None, db)
    rawlsGroup
  }

  private def createGroup(rawlsGroup: RawlsGroup, txn: RawlsTransaction): RawlsGroup = txn withGraph { db =>
    getGroupVertex(db, rawlsGroup.groupName) match {
      case Some(_) => throw new RawlsException("Cannot create group %s in database because it already exists".format(rawlsGroup.groupName))
      case None =>
        saveObject[RawlsGroup](rawlsGroup, addVertex(db, VertexSchema.Group), None, db)
        rawlsGroup
    }
  }

  override def createWorkspaceAccessGroups(workspaceName: WorkspaceName, userInfo: UserInfo, txn: RawlsTransaction): Map[WorkspaceAccessLevel, RawlsGroupRef] = {
    val user = RawlsUser(userInfo.userSubjectId)
    saveUser(user, txn)

    // add user to Owner group only
    val oGroup = RawlsGroup(UserAuth.toWorkspaceAccessGroupName(workspaceName, WorkspaceAccessLevels.Owner), Set(user), Set.empty)
    val wGroup = RawlsGroup(UserAuth.toWorkspaceAccessGroupName(workspaceName, WorkspaceAccessLevels.Write), Set.empty, Set.empty)
    val rGroup = RawlsGroup(UserAuth.toWorkspaceAccessGroupName(workspaceName, WorkspaceAccessLevels.Read), Set.empty, Set.empty)
    createGroup(oGroup, txn)
    createGroup(wGroup, txn)
    createGroup(rGroup, txn)

    Map(
      WorkspaceAccessLevels.Owner -> oGroup,
      WorkspaceAccessLevels.Write -> wGroup,
      WorkspaceAccessLevels.Read -> rGroup)
  }

  //turns a PipeFunction into one that takes a LoopBundle
  implicit def pipeToLoopBundle[A,B](f: PipeFunction[A,B]) = new PipeFunction[LoopPipe.LoopBundle[A], B] {
    override def compute(bundle: LoopPipe.LoopBundle[A]) : B = f.compute(bundle.getObject)
  }

  private def invert[A](f: PipeFunction[A, java.lang.Boolean]) = new PipeFunction[A, java.lang.Boolean] {
    override def compute(v: A) = !f.compute(v)
  }

  private def isConnectedToTargetWorkspace(wsId:String) = new PipeFunction[Vertex, java.lang.Boolean] {
    override def compute(v: Vertex) = v.getVertices(Direction.IN).iterator.exists({ case ws =>
      isVertexOfClass(VertexSchema.Workspace).compute(ws) &&
        hasPropertyValue("workspaceId", wsId).compute(ws) })
  }

  override def getMaximumAccessLevel(userSubjectId: String, workspaceId: String, txn: RawlsTransaction): WorkspaceAccessLevel = {
    txn withGraph { db =>

      //Returns a list of paths starting with a group the user belongs in and ending with the workspace's access map.
      // user -> group -> ...some other groups... -> group -> workspace access map
      //Multiple paths implies that the user belongs to multiple groups associated with the workspace.
      val accessMapPaths =
        userPipeline(db, userSubjectId).as("vtx").in()
          .loop(
            "vtx", //start point of loop
            invert(isVertexOfClass(VertexSchema.Workspace)), //loop condition: stop walking the path once we've found a workspace node
            isConnectedToTargetWorkspace(workspaceId)) //loop body: only emit paths to access maps connected to the target workspace
          .enablePath.path()

      //The last two elements of each path are the group and then the workspace access map.
      //The edge label between them corresponds to the access level for the group.
      //This foldl is equivalent to doing a max() on the list of those access levels.
      accessMapPaths.iterator.foldLeft(WorkspaceAccessLevels.NoAccess:WorkspaceAccessLevel)({ (maxAccessLevel, path) =>
        val reversePath = path.reverseIterator
        val accessMap = reversePath.next().asInstanceOf[Vertex]
        val group = reversePath.next().asInstanceOf[Vertex]

        val accessLabel = accessMap.getEdges(Direction.OUT).filter( _.getVertex(Direction.IN) == group ).head.getLabel
        val accessLevel = WorkspaceAccessLevels.withName(EdgeSchema.stripEdgeRelation(accessLabel))

        WorkspaceAccessLevels.max(accessLevel, maxAccessLevel)
      })
    }
  }
}
