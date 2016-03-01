package org.broadinstitute.dsde.rawls.dataaccess

import org.broadinstitute.dsde.rawls.model.WorkspaceAccessLevels.WorkspaceAccessLevel
import org.broadinstitute.dsde.rawls.model._

trait AuthDAO {
  def loadUser(ref: RawlsUserRef, txn: RawlsTransaction): Option[RawlsUser]

  def loadAllUsers(txn: RawlsTransaction): Seq[RawlsUser]

  def loadUserByEmail(userEmail: String, txn: RawlsTransaction): Option[RawlsUser]

  def saveUser(rawlsUser: RawlsUser, txn: RawlsTransaction): RawlsUser

  def loadGroup(groupRef: RawlsGroupRef, txn: RawlsTransaction): Option[RawlsGroup]

  def loadGroupIfMember(groupRef: RawlsGroupRef, userRef: RawlsUserRef, txn: RawlsTransaction): Option[RawlsGroup]

  def loadGroupByEmail(groupEmail: String, txn: RawlsTransaction): Option[RawlsGroup]

  def flattenGroupMembers(groupRef: RawlsGroupRef, txn: RawlsTransaction): Set[RawlsUserRef]

  def intersectGroupMembership(group1: RawlsGroupRef, group2: RawlsGroupRef, txn: RawlsTransaction): Set[RawlsUserRef]

  def saveGroup(rawlsGroup: RawlsGroup, txn: RawlsTransaction): RawlsGroup

  def deleteGroup(rawlsGroup: RawlsGroupRef, txn: RawlsTransaction)

  def loadFromEmail(email: String, txn: RawlsTransaction): Option[Either[RawlsUser, RawlsGroup]] = {
    (loadUserByEmail(email, txn).map(Left(_)) ++ loadGroupByEmail(email, txn).map(Right(_))).headOption
  }

  def getMaximumAccessLevel(user: RawlsUserRef, workspaceId: String, txn: RawlsTransaction): WorkspaceAccessLevel

  def listWorkspaces(user: RawlsUserRef, txn: RawlsTransaction): Seq[WorkspacePermissionsPair]
}
