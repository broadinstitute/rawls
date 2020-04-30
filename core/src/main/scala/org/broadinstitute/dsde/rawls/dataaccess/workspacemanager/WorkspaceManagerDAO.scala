package org.broadinstitute.dsde.rawls.dataaccess.workspacemanager

import java.util.UUID

import org.broadinstitute.dsde.rawls.model.UserInfo
import org.broadinstitute.dsde.rawls.model.workspacemanager.{WMCreateDataReferenceRequest, WMCreateDataReferenceResponse, WMCreateWorkspaceResponse, WMGetWorkspaceResponse}
import org.broadinstitute.dsde.workbench.model.ErrorReportSource

import scala.concurrent.Future

trait WorkspaceManagerDAO {
  val errorReportSource = ErrorReportSource("WorkspaceManager")

  def getWorkspace(workspaceId: UUID, userInfo: UserInfo): Future[WMGetWorkspaceResponse]
  def createWorkspace(workspaceId: UUID, userInfo: UserInfo): Future[WMCreateWorkspaceResponse]
  def createDataReference(workspaceId: UUID, createDataReferenceRequest: WMCreateDataReferenceRequest, userInfo: UserInfo): Future[WMCreateDataReferenceResponse]

}
