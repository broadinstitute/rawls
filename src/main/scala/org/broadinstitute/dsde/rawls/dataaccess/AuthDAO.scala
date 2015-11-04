package org.broadinstitute.dsde.rawls.dataaccess

import org.broadinstitute.dsde.rawls.model.WorkspaceAccessLevels.WorkspaceAccessLevel
import org.broadinstitute.dsde.rawls.model._

trait AuthDAO {
  def saveUser(rawlsUser: RawlsUser, txn: RawlsTransaction): RawlsUser

  def saveGroup(rawlsGroup: RawlsGroup, txn: RawlsTransaction): RawlsGroup

  def deleteGroup(rawlsGroup: RawlsGroupRef, txn: RawlsTransaction)

  def createWorkspaceAccessGroups(workspaceName: WorkspaceName, userInfo: UserInfo, txn: RawlsTransaction): Map[WorkspaceAccessLevel, RawlsGroupRef]

  def deleteWorkspaceAccessGroups(workspace: Workspace, txn: RawlsTransaction) = {
    workspace.accessLevels foreach { case (_, group) =>
      deleteGroup(group, txn)
    }
  }

  def getMaximumAccessLevel(userSubjectId: String, workspaceId: String, txn: RawlsTransaction): WorkspaceAccessLevel
}
