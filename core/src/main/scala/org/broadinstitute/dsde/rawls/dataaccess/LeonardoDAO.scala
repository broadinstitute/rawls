package org.broadinstitute.dsde.rawls.dataaccess

import org.broadinstitute.dsde.rawls.model.GoogleProjectId
import org.broadinstitute.dsde.workbench.client.leonardo.ApiException
import org.broadinstitute.dsde.workbench.client.leonardo.model.{
  ListAppResponse,
  ListPersistentDiskResponse,
  ListRuntimeResponse,
  UpdateAppRequest,
  UpdateDiskRequest,
  UpdateRuntimeRequest
}

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

  def updateAppConfig(token: String, googleProject: String, name: String, updateAppRequest: UpdateAppRequest): Unit

  def listRuntimesByWorkspace(token: String, workspaceId: UUID): Seq[ListRuntimeResponse]

  def updateRuntimeConfig(
    token: String,
    googleProject: String,
    name: String,
    updateRuntimeRequest: UpdateRuntimeRequest
  ): Unit

  def listAzureRuntimes(token: String, workspaceId: UUID): Seq[ListRuntimeResponse]

  def deleteAzureRuntimes(token: String, workspaceId: UUID, deleteDisk: Boolean): Unit

  def listDisksByWorkspaceNamespace(token: String, workspaceNamespace: String): Seq[ListPersistentDiskResponse]

  def updateDiskConfig(
    token: String,
    googleProject: String,
    name: String,
    updateDiskRequest: UpdateDiskRequest
  ): Unit

  @throws(classOf[ApiException])
  def cleanupAllResources(token: String, googleProjectId: GoogleProjectId): Unit
}
