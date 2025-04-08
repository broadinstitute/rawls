package org.broadinstitute.dsde.rawls.dataaccess.policyservice

import org.broadinstitute.dsde.rawls.model.{RawlsRequestContext, WorkspaceRequest}

import java.util.UUID
import scala.concurrent.Future

trait PolicyServiceDAO {
  def createWorkspacePao(workspaceId: UUID, workspaceRequest: WorkspaceRequest, ctx: RawlsRequestContext): Future[Unit]
}
