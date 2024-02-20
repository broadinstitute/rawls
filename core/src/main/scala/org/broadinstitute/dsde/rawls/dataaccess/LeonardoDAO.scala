package org.broadinstitute.dsde.rawls.dataaccess
import org.broadinstitute.dsde.workbench.client.leonardo.api.AppsApi

import scala.concurrent.{ExecutionContext, Future}
import org.broadinstitute.dsde.rawls.model.{GoogleProjectId, WorkspaceName}
import org.broadinstitute.dsde.workbench.client.leonardo.model.{ListAppResponse, ListRuntimeResponse}

import java.util.UUID

trait LeonardoDAO {

  def createWDSInstance(token: String, workspaceId: UUID, sourceWorkspaceId: Option[UUID]): Unit

  def createApp(token: String,
                workspaceId: UUID,
                appName: String,
                appType: String,
                sourceWorkspaceId: Option[UUID]
  ): Unit

  def deleteApps(token: String, workspaceId: UUID, deleteDisk: Boolean): Unit

  def listApps(token: String, workspaceId: UUID): Seq[ListAppResponse]

  def listAzureRuntimes(token: String, workspaceId: UUID): Seq[ListRuntimeResponse]

  def deleteAzureRuntimes(token: String, workspaceId: UUID, deleteDisk: Boolean): Unit

  def cleanupAllResources(token: String, googleProjectId: GoogleProjectId): Unit
}
