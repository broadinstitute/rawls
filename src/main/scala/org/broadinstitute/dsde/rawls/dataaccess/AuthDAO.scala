package org.broadinstitute.dsde.rawls.dataaccess

import org.broadinstitute.dsde.rawls.model._

trait AuthDAO {
  def saveUser(rawlsUser: RawlsUser, txn: RawlsTransaction): RawlsUser

  def saveGroup(rawlsGroup: RawlsGroup, txn: RawlsTransaction): RawlsGroup

  def createWorkspaceAccessGroups(workspaceName: WorkspaceName, userInfo: UserInfo, txn: RawlsTransaction): Map[String, RawlsGroupRef]

  def getGroupUsers(groupName: String, txn: RawlsTransaction): Seq[String]

  def getGroupSubgroups(groupName: String, txn: RawlsTransaction): Seq[String]

  def getAllGroupMembers(groupName: String, txn: RawlsTransaction): Seq[String]

  def getWorkspaceOwners(workspaceName: WorkspaceName, txn: RawlsTransaction): Seq[String]
}