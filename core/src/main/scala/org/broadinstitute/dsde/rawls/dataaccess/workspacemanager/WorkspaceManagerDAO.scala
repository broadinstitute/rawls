package org.broadinstitute.dsde.rawls.dataaccess.workspacemanager

import java.util.UUID

import bio.terra.workspace.model.{CreatedWorkspace, DataReferenceDescription, WorkspaceDescription}
import org.broadinstitute.dsde.rawls.model.UserInfo
import org.broadinstitute.dsde.workbench.model.ErrorReportSource
import spray.json.JsObject

import scala.concurrent.Future

trait WorkspaceManagerDAO {
  val errorReportSource = ErrorReportSource("WorkspaceManager")

  def getWorkspace(workspaceId: UUID, userInfo: UserInfo): Future[WorkspaceDescription]
  def createWorkspace(workspaceId: UUID, userInfo: UserInfo): Future[CreatedWorkspace]
  def createDataReference(workspaceId: UUID, name: String, referenceType: String, reference: JsObject, cloningInstructions: String, userInfo: UserInfo): Future[DataReferenceDescription]
  def getDataReference(workspaceId: UUID, referenceId: UUID, userInfo: UserInfo): Future[DataReferenceDescription]

}
