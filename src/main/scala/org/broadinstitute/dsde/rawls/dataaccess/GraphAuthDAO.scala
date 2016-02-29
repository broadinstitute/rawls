package org.broadinstitute.dsde.rawls.dataaccess

import com.tinkerpop.blueprints.{Direction, Vertex}
import com.tinkerpop.pipes.PipeFunction
import com.tinkerpop.pipes.branch.LoopPipe

import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.model.WorkspaceAccessLevels.WorkspaceAccessLevel

import scala.collection.JavaConversions._
import CachedTypes._

class GraphAuthDAO extends AuthDAO with GraphDAO {
  override def loadUser(ref: RawlsUserRef, txn: RawlsTransaction): Option[RawlsUser] = txn withGraph { db =>
    getUserVertex(db, ref).map(loadObject[RawlsUser])
  }

  override def loadAllUsers(txn: RawlsTransaction): Seq[RawlsUser] = txn withGraph { db =>
    getAllUserVertices(db).toList.map(loadObject[RawlsUser]).toSeq
  }

  override def loadUserByEmail(userEmail: String, txn: RawlsTransaction): Option[RawlsUser] = txn withGraph { db =>
    getUserVertexByEmail(db, userEmail).map(loadObject[RawlsUser])
  }

  override def saveUser(rawlsUser: RawlsUser, txn: RawlsTransaction): RawlsUser = txn withGraph { db =>
    val vertex = getUserVertex(db, rawlsUser).getOrElse(addVertex(db, VertexSchema.User))
    saveObject[RawlsUser](rawlsUser, vertex, None, db)
    rawlsUser
  }

  override def saveGroup(rawlsGroup: RawlsGroup, txn: RawlsTransaction): RawlsGroup = txn withGraph { db =>
    val vertex = getGroupVertex(db, rawlsGroup).getOrElse(addVertex(db, VertexSchema.Group))
    saveObject[RawlsGroup](rawlsGroup, vertex, None, db)
    rawlsGroup
  }

  override def deleteGroup(rawlsGroup: RawlsGroupRef, txn: RawlsTransaction) = txn withGraph { db =>
    val vertex = getGroupVertex(db, rawlsGroup) match {
      case None => throw new RawlsException("Cannot delete group %s from database because it does not exist".format(rawlsGroup))
      case Some(vertex) => removeObject(vertex, db)
    }
  }

  override def loadGroup(groupRef: RawlsGroupRef, txn: RawlsTransaction): Option[RawlsGroup] = txn withGraph { db =>
    getGroupVertex(db, groupRef).map(loadObject[RawlsGroup])
  }

  override def loadGroupIfMember(groupRef: RawlsGroupRef, userRef: RawlsUserRef, txn: RawlsTransaction): Option[RawlsGroup] = txn withGraph { db =>
    val groupMemberPipeline = userPipeline(db, userRef).as("vtx").in()
      .loop(
        "vtx", //start point of loop
        or(isVertexOfClass(VertexSchema.Group), isVertexOfClass(VertexSchema.Map)), //loop condition: walk while vertex is on an acl path (group or a list which looks like map)
        isTargetGroup(groupRef))

    getSinglePipelineResult(groupMemberPipeline).map(loadObject[RawlsGroup])
  }

  def loadGroupByEmail(groupEmail: String, txn: RawlsTransaction): Option[RawlsGroup] = txn withGraph { db =>
    getGroupVertexByEmail(db, groupEmail).map(loadObject[RawlsGroup])
  }

  private def isTargetWorkspace(wsId:String) = new PipeFunction[Vertex, java.lang.Boolean] {
    override def compute(v: Vertex) = {
      isVertexOfClass(VertexSchema.Workspace).compute(v) && hasPropertyValue("workspaceId", wsId).compute(v)
    }
  }

  private def isTargetGroup(groupRef: RawlsGroupRef) = new PipeFunction[Vertex, java.lang.Boolean] {
    override def compute(v: Vertex) = {
      isVertexOfClass(VertexSchema.Group).compute(v) && hasPropertyValue("groupName", groupRef.groupName.value).compute(v)
    }
  }

  override def getMaximumAccessLevel(user: RawlsUserRef, workspaceId: String, txn: RawlsTransaction): WorkspaceAccessLevel = {
    val workspaces = getWorkspaceAccessLevels(user, isTargetWorkspace(workspaceId), txn)
    workspaces.find(_.workspaceId == workspaceId).map(_.accessLevel).getOrElse(WorkspaceAccessLevels.NoAccess)
  }

  override def listWorkspaces(user: RawlsUserRef, txn: RawlsTransaction): Seq[WorkspacePermissionsPair] = {
    listWorkspaces(user, isVertexOfClass(VertexSchema.Workspace), txn)
  }

  private def getWorkspaceAccessLevels(user: RawlsUserRef, loopEmitFn: PipeFunction[Vertex, java.lang.Boolean], txn: RawlsTransaction): Seq[WorkspacePermissionsPair] = {
    txn withGraph { db =>

      //Returns a list of paths starting with a group the user belongs in and ending with the workspace's access map.
      // user -> group -> ...some other groups... -> group -> workspace access map
      //Multiple paths implies that the user belongs to multiple groups associated with the workspace.
      val accessMapPaths =
        userPipeline(db, user).as("vtx").in()
          .loop(
            "vtx", //start point of loop
            or(isVertexOfClass(VertexSchema.Group), isVertexOfClass(VertexSchema.Map)), //loop condition: walk while vertex is on an acl path (group or a list which looks like map)
            loopEmitFn)
          .enablePath.path()

      //The last 3 elements of each path are the group then the workspace access map then the workspace.
      //The edge label between the group and the workspace access map corresponds to the access level for the group.
      //It is possible for there to be more than 1 access level for a user for a workspace
      //Group the paths by workspace then take the max access level for each workspace
      val accessByWorkspace = accessMapPaths.toList.groupBy(path => loadObject[Workspace](path.last.asInstanceOf[Vertex]))
      val accessLevels = accessByWorkspace map { case (workspace, paths) =>
        paths.foldLeft(WorkspacePermissionsPair(workspace.workspaceId, WorkspaceAccessLevels.NoAccess))({ (maxAccessLevel, path) =>
          val reversePath = path.reverseIterator
          reversePath.next() // remove workspace vertex
          val accessMap = reversePath.next().asInstanceOf[Vertex]
          val group = reversePath.next().asInstanceOf[Vertex]

          val accessLabel = accessMap.getEdges(Direction.OUT).filter(_.getVertex(Direction.IN) == group).head.getLabel
          val accessLevel = WorkspaceAccessLevels.withName(EdgeSchema.stripEdgeRelation(accessLabel))

          WorkspacePermissionsPair(workspace.workspaceId, WorkspaceAccessLevels.max(accessLevel, maxAccessLevel.accessLevel))
        })
      }
      accessLevels.toSeq
    }
  }

  private def listWorkspaces(user: RawlsUserRef, loopEmitFn: PipeFunction[Vertex, java.lang.Boolean], txn: RawlsTransaction): Seq[WorkspacePermissionsPair] = {
    txn withGraph { db =>

      //Returns a list of paths starting with a group the user belongs in and ending with the workspace's access map.
      // user -> group -> ...some other groups... -> group -> workspace access map
      //Multiple paths implies that the user belongs to multiple groups associated with the workspace.
      val accessMapPaths =
        userPipeline(db, user).as("vtx").in()
          .loop(
            "vtx", //start point of loop
            or(isVertexOfClass(VertexSchema.Group), isVertexOfClass(VertexSchema.Map)), //loop condition: walk while vertex is on an acl path (group or a list which looks like map)
            loopEmitFn)
          .enablePath.path()

      //The last 3 elements of each path are the group then the workspace access map then the workspace.
      //The edge label between the group and the workspace access map corresponds to the access level for the group.
      //It is possible for there to be more than 1 access level for a user for a workspace
      //Group the paths by workspace then take the max access level for each workspace
      val accessByWorkspace = accessMapPaths.toList.groupBy(path => loadObject[Workspace](path.last.asInstanceOf[Vertex]))
      val accessLevels = accessByWorkspace map { case (workspace, paths) =>
        paths.foldLeft(WorkspacePermissionsPair(workspace.workspaceId, WorkspaceAccessLevels.NoAccess))({ (maxAccessLevel, path) =>
          val reversePath = path.reverseIterator
          reversePath.next() // remove workspace vertex
          val accessMap = reversePath.next().asInstanceOf[Vertex]
          val group = reversePath.next().asInstanceOf[Vertex]

          val accessLabel = accessMap.getEdges(Direction.OUT).filter(_.getVertex(Direction.IN) == group).head.getLabel
          val accessLevel = WorkspaceAccessLevels.withName(EdgeSchema.stripEdgeRelation(accessLabel))

          //the logic works as follows: if the workspace has no realm, then we proceed as usual
          //if the workspace has a realm, we check to see if the user is in that realm group. if they aren't return NoAccess
          //if they are in the realm group, return their normal access level
          workspace.realm match {
            case None => WorkspacePermissionsPair(workspace.workspaceId, WorkspaceAccessLevels.max(accessLevel, maxAccessLevel.accessLevel))
            case Some(realmGroup) => {
              loadGroupIfMember(realmGroup, user, txn) match {
                case None => WorkspacePermissionsPair(workspace.workspaceId, WorkspaceAccessLevels.NoAccess)
                case Some(_) => WorkspacePermissionsPair(workspace.workspaceId, WorkspaceAccessLevels.max(accessLevel, maxAccessLevel.accessLevel))
              }
            }
          }
        })
      }
      accessLevels.toSeq
    }
  }
}
