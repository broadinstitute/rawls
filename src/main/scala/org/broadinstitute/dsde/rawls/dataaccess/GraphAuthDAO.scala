package org.broadinstitute.dsde.rawls.dataaccess

import com.tinkerpop.blueprints.Direction
import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.model._

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

  override def getGroupUsers(groupName: String, txn: RawlsTransaction): Seq[String] = txn withGraph { db =>
    getGroupVertex(db, groupName) match {
      case None => throw new RawlsException(s"Cannot load group ${groupName}")
      case Some(group) =>
        val userOwnerMap = getVertices(group, Direction.OUT, EdgeSchema.Own, "users").head
        getVertices(userOwnerMap, Direction.OUT, EdgeSchema.Ref, "0").map(v => v.getProperty("userSubjectId").asInstanceOf[String]).toSeq
    }
  }

  override def getGroupSubgroups(groupName: String, txn: RawlsTransaction): Seq[String] = txn withGraph { db =>
    getGroupVertex(db, groupName) match {
      case None => throw new RawlsException(s"Cannot load group ${groupName}")
      case Some(group) =>
        val groupOwnerMap = getVertices(group, Direction.OUT, EdgeSchema.Own, "subGroups").head
        getVertices(groupOwnerMap, Direction.OUT, EdgeSchema.Ref, "0").map(v => v.getProperty("groupName").asInstanceOf[String]).toSeq
    }
  }

  override def getAllGroupMembers(groupName: String, txn: RawlsTransaction): Seq[String] = txn withGraph { db =>
    val users = getGroupUsers(groupName, txn)
    val groups = getGroupSubgroups(groupName, txn)
    users++groups
  }

  override def getWorkspaceOwners(workspaceName: WorkspaceName, txn: RawlsTransaction): Seq[String] = txn withGraph { db =>
    getAllGroupMembers(UserAuth.toWorkspaceAccessGroupName(workspaceName, WorkspaceAccessLevel.Owner), txn)
  }

  override def createWorkspaceAccessGroups(workspaceName: WorkspaceName, userInfo: UserInfo, txn: RawlsTransaction): Map[String, RawlsGroupRef] = {
    val user = RawlsUser(userInfo.userSubjectId)
    saveUser(user, txn)

    // add user to Owner group only
    val oGroup = RawlsGroup(UserAuth.toWorkspaceAccessGroupName(workspaceName, WorkspaceAccessLevel.Owner), Set(user), Set.empty)
    val wGroup = RawlsGroup(UserAuth.toWorkspaceAccessGroupName(workspaceName, WorkspaceAccessLevel.Write), Set.empty, Set.empty)
    val rGroup = RawlsGroup(UserAuth.toWorkspaceAccessGroupName(workspaceName, WorkspaceAccessLevel.Read), Set.empty, Set.empty)
    createGroup(oGroup, txn)
    createGroup(wGroup, txn)
    createGroup(rGroup, txn)

    Map(
      WorkspaceAccessLevel.Owner.toString -> oGroup,
      WorkspaceAccessLevel.Write.toString -> wGroup,
      WorkspaceAccessLevel.Read.toString -> rGroup)
  }
}
