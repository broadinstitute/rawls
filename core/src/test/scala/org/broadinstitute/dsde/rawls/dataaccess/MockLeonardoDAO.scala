package org.broadinstitute.dsde.rawls.dataaccess

import org.broadinstitute.dsde.workbench.client.leonardo.model.{ListAppResponse, ListRuntimeResponse}

import java.util.UUID

class MockLeonardoDAO() extends LeonardoDAO {

  override def createApp(token: String,
                         workspaceId: UUID,
                         appName: String,
                         appType: String,
                         sourceWorkspaceId: Option[UUID]
  ): Unit = ()

  override def createWDSInstance(token: String, workspaceId: UUID, sourceWorkspaceId: Option[UUID]): Unit = ()

  override def deleteApps(token: String, workspaceId: String, deleteDisk: Boolean): Unit = ???

  override def listApps(token: String, workspaceId: String): Seq[ListAppResponse] = ???

  override def listAzureRuntimes(token: String, workspaceId: String): Seq[ListRuntimeResponse] = ???

  override def deleteAzureRuntimes(token: String, workspaceId: String, deleteDisk: Boolean): Unit = ???
}
