package org.broadinstitute.dsde.rawls.dataaccess

import org.broadinstitute.dsde.rawls.model.{RawlsUserEmail, UserInfo, Workspace}

import scala.concurrent.Future

trait RequesterPaysSetupService {
  def getBondProviderServiceAccountEmails(userInfo: UserInfo): Future[List[BondServiceAccountEmail]]
  def grantRequesterPaysToLinkedSAs(userInfo: UserInfo, workspace: Workspace): Future[List[BondServiceAccountEmail]]
  def revokeUserFromWorkspace(userEmail: RawlsUserEmail, workspace: Workspace): Future[Seq[BondServiceAccountEmail]]
  def deleteAllRecordsForWorkspace(workspace: Workspace): Future[Int]
}
