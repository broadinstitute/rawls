package org.broadinstitute.dsde.rawls.dataaccess

import com.tinkerpop.blueprints.{Edge, Direction, Vertex}
import com.tinkerpop.gremlin.java.GremlinPipeline
import com.tinkerpop.pipes.PipeFunction
import com.tinkerpop.pipes.branch.LoopPipe

import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.model.WorkspaceAccessLevels.WorkspaceAccessLevel

import scala.collection.JavaConversions._
import CachedTypes._

class GraphAuthDAO extends AuthDAO with GraphDAO {

  def loadUser(user: Vertex) = {
    loadObject[RawlsUserRef](user)
  }

  def loadGroup(group: Vertex) = {
    loadObject[RawlsGroupRef](group)
  }

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

  //returns a list of workspaces that have groupRef present in any part of its ACL tree
  def findWorkspacesForGroup(groupRef: RawlsGroupRef, txn: RawlsTransaction): Seq[Workspace] = {
    txn withGraph { db =>

      val workspacePaths = groupPipeline(db, groupRef).as("vtx").inE().outV()
        .loop(
          "vtx", //start point of loop
          or(isVertexOfClass(VertexSchema.Group), isVertexOfClass(VertexSchema.Map)), //loop condition: walk while vertex is on an acl path (group or a list which looks like map)
          isVertexOfClass(VertexSchema.Workspace)).enablePath.path().toList.map(path => path.reverse.toList)

      val workspaceVertices = workspacePaths collect {
        case (workspace: Vertex) :: (mapTypeEdge: Edge) :: (accessMap: Vertex) :: tail
          if Seq("accessLevels", "realm").contains(EdgeSchema.stripEdgeRelation(mapTypeEdge.getLabel)) =>
            workspace
      }

      workspaceVertices.map(loadObject[Workspace]).toSeq
    }
  }

  def flattenGroupMembers(groupRef: RawlsGroupRef, txn: RawlsTransaction): Set[RawlsUserRef] = {
    txn withGraph { db =>

      val group = loadGroup(groupRef, txn).getOrElse(throw new RawlsException(s"Unable to load group ${groupRef}"))
      val topLevelUsers = group.users

      val subUsers = groupPipeline(db, group).as("vtx").out()
        .loop(
          "vtx",
          or(or(isVertexOfClass(VertexSchema.User), isVertexOfClass(VertexSchema.Group)), isVertexOfClass(VertexSchema.Map)),
          isVertexOfClass(VertexSchema.User)).iterator.map(loadUser).toSet

      topLevelUsers ++ subUsers
    }
  }

  def intersectGroupMembership(group1: RawlsGroupRef, group2: RawlsGroupRef, txn: RawlsTransaction): Set[RawlsUserRef] = {
    val group1Members = flattenGroupMembers(group1, txn)
    val group2Members = flattenGroupMembers(group2, txn)

    group1Members.intersect(group2Members)
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
        userPipeline(db, user).as("vtx").inE().outV()
          .loop(
            "vtx", //start point of loop
            or(isVertexOfClass(VertexSchema.Group), isVertexOfClass(VertexSchema.Map)), //loop condition: walk while vertex is on an acl path (group or a list which looks like map)
            loopEmitFn)
          .enablePath.path()

      //The last 5 elements of each path are the group then the workspace access map then the workspace with edges in between.
      //The edge label between the group and the workspace access map corresponds to the access level for the group.
      //The edge label between the workspace access map and the workspace corresponds to which map it is and we only care about realmACLs here.
      //It is possible for there to be more than 1 access level for a user for a workspace
      //Group the paths by workspace then take the max access level for each workspace
      val accessByWorkspace = accessMapPaths.toList.groupBy(path => loadObject[Workspace](path.last.asInstanceOf[Vertex]))
      val accessLevels = accessByWorkspace map { case (workspace, paths) =>
        paths.foldLeft(WorkspacePermissionsPair(workspace.workspaceId, WorkspaceAccessLevels.NoAccess))({ (maxAccessLevel, path) =>
          val reversePath = path.reverse.toList
          val accessLevel = reversePath match {
            case (workspace: Vertex) :: (mapTypeEdge: Edge) :: (accessMap: Vertex) :: (accessLevelEdge: Edge) :: (group: Vertex) :: tail
              if EdgeSchema.stripEdgeRelation(mapTypeEdge.getLabel) == "realmACLs" =>

              WorkspaceAccessLevels.withName(EdgeSchema.stripEdgeRelation(accessLevelEdge.getLabel))

            case _ => WorkspaceAccessLevels.NoAccess
          }
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
        userPipeline(db, user).as("vtx").inE().outV()
          .loop(
            "vtx", //start point of loop
            or(isVertexOfClass(VertexSchema.Group), isVertexOfClass(VertexSchema.Map)), //loop condition: walk while vertex is on an acl path (group or a list which looks like map)
            loopEmitFn)
          .enablePath.path()

      //The last 3 elements of each path are the group then the workspace access map then the workspace.
      //The edge label between the group and the workspace access map corresponds to the access level for the group.
      //It is possible for there to be more than 1 access level for a user for a workspace
      //Group the paths by workspace then take the max access level for each workspace

      val workspaceWithAccessLevel = accessMapPaths.toList.map(p => p.reverse.toList).collect {
        case (workspace: Vertex) :: (mapTypeEdge: Edge) :: (accessMap: Vertex) :: (accessLevelEdge: Edge) :: (group: Vertex) :: tail
          if EdgeSchema.stripEdgeRelation(mapTypeEdge.getLabel) == "realmACLs" || EdgeSchema.stripEdgeRelation(mapTypeEdge.getLabel) == "accessLevels" =>
          val accessLevel = WorkspaceAccessLevels.withName(EdgeSchema.stripEdgeRelation(accessLevelEdge.getLabel))
          (workspace, accessLevel)
      }

      val accessByWorkspace = workspaceWithAccessLevel.groupBy(t => loadObject[Workspace](t._1))
      val accessLevels = accessByWorkspace map { case (workspace, paths) =>
        paths.foldLeft(WorkspacePermissionsPair(workspace.workspaceId, WorkspaceAccessLevels.NoAccess))({ (maxAccessLevel, path) =>

          //the logic works as follows: if the workspace has no realm, then we proceed as usual
          //if the workspace has a realm, we check to see if the user is in that realm group. if they aren't return NoAccess
          //if they are in the realm group, return their normal access level
          workspace.realm match {
            case None => WorkspacePermissionsPair(workspace.workspaceId, WorkspaceAccessLevels.max(path._2, maxAccessLevel.accessLevel))
            case Some(realmGroup) => {
              loadGroupIfMember(realmGroup, user, txn) match {
                case None => WorkspacePermissionsPair(workspace.workspaceId, WorkspaceAccessLevels.NoAccess)
                case Some(_) => WorkspacePermissionsPair(workspace.workspaceId, WorkspaceAccessLevels.max(path._2, maxAccessLevel.accessLevel))
              }
            }
          }
        })
      }
      accessLevels.toSeq
    }
  }
}
