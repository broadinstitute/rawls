package org.broadinstitute.dsde.rawls.mock

import java.util.UUID

import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.WorkspaceManagerDAO
import org.broadinstitute.dsde.rawls.model.UserInfo
import org.broadinstitute.dsde.rawls.model.workspacemanager.{WMCreateDataReferenceRequest, WMCreateDataReferenceResponse, WMCreateWorkspaceResponse, WMGetWorkspaceResponse}

import scala.concurrent.Future

class MockWorkspaceManagerDAO extends WorkspaceManagerDAO {
  override def getWorkspace(workspaceId: UUID, userInfo: UserInfo): Future[WMGetWorkspaceResponse] = ???

  override def createWorkspace(workspaceId: UUID, userInfo: UserInfo): Future[WMCreateWorkspaceResponse] = ???

  override def createDataReference(workspaceId: UUID, createDataReferenceRequest: WMCreateDataReferenceRequest, userInfo: UserInfo): Future[WMCreateDataReferenceResponse] = ???
}
