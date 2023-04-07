package org.broadinstitute.dsde.rawls.dataaccess

import java.util.UUID

class MockLeonardoDAO() extends LeonardoDAO {

  override def createApp(token: String, workspaceId: UUID, appName: String, appType: String): Unit = ()

  override def createWDSInstance(token: String, workspaceId: UUID): Unit = ()

}
