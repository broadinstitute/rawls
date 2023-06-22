package org.broadinstitute.dsde.rawls.dataaccess

import org.broadinstitute.dsde.workbench.client.leonardo.model.ListAppResponse

import java.util.UUID
import java.util.List

class MockLeonardoDAO() extends LeonardoDAO {

  override def createApp(token: String,
                         workspaceId: UUID,
                         appName: String,
                         appType: String,
                         sourceWorkspaceId: Option[UUID]
  ): Unit = ()

  override def createWDSInstance(token: String, workspaceId: UUID, sourceWorkspaceId: Option[UUID]): Unit = ()

  override def listAppsV2(token: String, workspaceId: UUID): List[ListAppResponse] = ???

  override def deleteAppV2(token: String, workspaceId: UUID, appName: String): Unit = ???

}
