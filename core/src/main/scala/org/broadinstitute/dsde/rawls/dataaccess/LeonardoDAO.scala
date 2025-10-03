package org.broadinstitute.dsde.rawls.dataaccess

import org.broadinstitute.dsde.rawls.model.GoogleProjectId
import org.broadinstitute.dsde.workbench.client.leonardo.ApiException
import org.broadinstitute.dsde.workbench.client.leonardo.model.{
  ListAppResponse,
  ListPersistentDiskResponse,
  ListRuntimeResponse
}

trait LeonardoDAO {

  def listApps(token: String, googleProjectId: GoogleProjectId): Seq[ListAppResponse]

  def listDisks(token: String, googleProjectId: GoogleProjectId): Seq[ListPersistentDiskResponse]

  def listRuntimes(token: String, googleProjectId: GoogleProjectId): Seq[ListRuntimeResponse]

  @throws(classOf[ApiException])
  def cleanupAllResources(token: String, googleProjectId: GoogleProjectId): Unit
}
