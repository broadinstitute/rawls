package org.broadinstitute.dsde.rawls.dataaccess

import org.broadinstitute.dsde.rawls.model.GoogleProjectId
import org.broadinstitute.dsde.workbench.client.leonardo.model.{ListAppResponse, ListRuntimeResponse}

class MockLeonardoDAO extends LeonardoDAO {

  override def listApps(token: String, googleProjectId: GoogleProjectId): Seq[ListAppResponse] = ???

  override def listRuntimes(token: String, googleProjectId: GoogleProjectId): Seq[ListRuntimeResponse] = ???

  override def cleanupAllResources(token: String, googleProjectId: GoogleProjectId): Unit = ???
}
