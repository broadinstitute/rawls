package org.broadinstitute.dsde.rawls.dataaccess

import org.broadinstitute.dsde.rawls.model._

trait AuthDAO {
  def saveUser(rawlsUser: RawlsUser, txn: RawlsTransaction): RawlsUser

  def saveGroup(rawlsGroup: RawlsGroup, txn: RawlsTransaction): RawlsGroup

  def createWorkspaceAccessGroups(workspaceName: WorkspaceName, userInfo: UserInfo, txn: RawlsTransaction): Map[String, RawlsGroupRef]
}