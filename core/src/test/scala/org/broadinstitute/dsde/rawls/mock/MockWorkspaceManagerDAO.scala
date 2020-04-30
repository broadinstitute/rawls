package org.broadinstitute.dsde.rawls.mock

import java.util.UUID

import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.WorkspaceManagerDAO
import org.broadinstitute.dsde.rawls.model.UserInfo
import org.broadinstitute.dsde.rawls.model.workspacemanager.{WMCreateDataReferenceRequest, WMDataReferenceResponse, WMCreateWorkspaceResponse, WMDataReferenceResponse, WMGetWorkspaceResponse}

import scala.concurrent.Future

class MockWorkspaceManagerDAO extends WorkspaceManagerDAO {
  override def getWorkspace(workspaceId: UUID, userInfo: UserInfo): Future[WMGetWorkspaceResponse] = ???

  override def createWorkspace(workspaceId: UUID, userInfo: UserInfo): Future[WMCreateWorkspaceResponse] = ???

  override def createDataReference(workspaceId: UUID, createDataReferenceRequest: WMCreateDataReferenceRequest, userInfo: UserInfo): Future[WMDataReferenceResponse] = ???

  override def getDataReference(workspaceId: UUID, referenceId: UUID, userInfo: UserInfo): Future[WMDataReferenceResponse] = ???
}
