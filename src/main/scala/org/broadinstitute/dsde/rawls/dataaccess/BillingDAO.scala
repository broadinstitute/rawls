package org.broadinstitute.dsde.rawls.dataaccess

import org.broadinstitute.dsde.rawls.model._

trait BillingDAO {
  def saveProject(rawlsProject: RawlsBillingProject, txn: RawlsTransaction): RawlsBillingProject

  def loadProject(rawlsProjectName: RawlsBillingProjectName, txn: RawlsTransaction): Option[RawlsBillingProject]

  def deleteProject(rawlsProject: RawlsBillingProject, txn: RawlsTransaction): Boolean

  def addUserToProject(rawlsUser: RawlsUserRef, rawlsProject: RawlsBillingProject, txn: RawlsTransaction): RawlsBillingProject = {
    saveProject(rawlsProject.copy(users = rawlsProject.users + rawlsUser), txn)
  }

  def removeUserFromProject(rawlsUser: RawlsUserRef, rawlsProject: RawlsBillingProject, txn: RawlsTransaction): RawlsBillingProject = {
    saveProject(rawlsProject.copy(users = rawlsProject.users - rawlsUser), txn)
  }

  def listUserProjects(rawlsUser: RawlsUserRef, txn: RawlsTransaction): Traversable[RawlsBillingProjectName]
}
