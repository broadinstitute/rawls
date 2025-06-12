package org.broadinstitute.dsde.rawls.dataaccess

import org.broadinstitute.dsde.rawls.model.GoogleProjectId
import org.broadinstitute.dsde.workbench.client.leonardo.model.{
  ListAppResponse,
  ListPersistentDiskResponse,
  ListRuntimeResponse,
  UpdateAppRequest,
  UpdateDiskRequest,
  UpdateRuntimeRequest
}

import java.util.UUID

class MockLeonardoDAO extends LeonardoDAO {

  override def createApp(token: String,
                         workspaceId: UUID,
                         appName: String,
                         appType: String,
                         sourceWorkspaceId: Option[UUID]
  ): Unit = ()

  override def createWDSInstance(token: String, workspaceId: UUID, sourceWorkspaceId: Option[UUID]): Unit = ()

  override def deleteApps(token: String, workspaceId: UUID, deleteDisk: Boolean): Unit = ???

  override def listApps(token: String, workspaceId: UUID): Seq[ListAppResponse] = ???

  override def updateAppConfig(token: String,
                               googleProject: String,
                               name: String,
                               updateAppRequest: UpdateAppRequest
  ): Unit = ???

  override def listRuntimesByWorkspace(token: String, workspaceId: UUID): Seq[ListRuntimeResponse] = ???

  override def updateRuntimeConfig(
    token: String,
    googleProject: String,
    name: String,
    updateRuntimeRequest: UpdateRuntimeRequest
  ): Unit = ???

  override def listAzureRuntimes(token: String, workspaceId: UUID): Seq[ListRuntimeResponse] = ???

  override def deleteAzureRuntimes(token: String, workspaceId: UUID, deleteDisk: Boolean): Unit = ???

  override def listDisksByWorkspaceNamespace(token: String,
                                             workspaceNamespace: String
  ): Seq[ListPersistentDiskResponse] = ???

  override def updateDiskConfig(
    token: String,
    googleProject: String,
    name: String,
    updateDiskRequest: UpdateDiskRequest
  ): Unit = ???

  override def cleanupAllResources(token: String, googleProjectId: GoogleProjectId): Unit = ???
}
