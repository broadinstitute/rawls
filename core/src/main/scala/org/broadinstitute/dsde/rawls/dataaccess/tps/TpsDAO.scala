package org.broadinstitute.dsde.rawls.dataaccess.tps

import org.broadinstitute.dsde.rawls.model.{RawlsRequestContext, WorkspaceRequest}

import java.util.UUID
import scala.concurrent.Future

trait TpsDAO {
  def createWorkspacePao(workspaceId: UUID, workspaceRequest: WorkspaceRequest, ctx: RawlsRequestContext): Future[Unit]
}
