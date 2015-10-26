package org.broadinstitute.dsde.rawls.dataaccess

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
