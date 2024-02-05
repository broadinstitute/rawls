package org.broadinstitute.dsde.rawls.dataaccess

import org.broadinstitute.dsde.rawls.model.{RawlsUserEmail, UserInfo, Workspace}
import scala.concurrent.Future

class DisabledRequesterPaysSetupService
  extends RequesterPaysSetup {
  def getBondProviderServiceAccountEmails(userInfo: UserInfo): Future[List[BondServiceAccountEmail]] =
    throw new NotImplementedError("getBondProviderServiceAccountEmails is not implemented for Azure.")
  def grantRequesterPaysToLinkedSAs(userInfo: UserInfo, workspace: Workspace): Future[List[BondServiceAccountEmail]] =
    throw new NotImplementedError("grantRequesterPaysToLinkedSAs is not implemented for Azure.")
  def revokeUserFromWorkspace(userEmail: RawlsUserEmail, workspace: Workspace): Future[Seq[BondServiceAccountEmail]] =
    throw new NotImplementedError("revokeUserFromWorkspace is not implemented for Azure.")
  def deleteAllRecordsForWorkspace(workspace: Workspace): Future[Int] =
    throw new NotImplementedError("deleteAllRecordsForWorkspace is not implemented for Azure.")
}

