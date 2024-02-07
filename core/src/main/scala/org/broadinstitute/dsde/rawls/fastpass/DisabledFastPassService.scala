package org.broadinstitute.dsde.rawls.fastpass

import cats.effect.IO
import org.broadinstitute.dsde.rawls.dataaccess.SlickDataSource
import org.broadinstitute.dsde.rawls.model.{RawlsRequestContext, Workspace}
import org.broadinstitute.dsde.workbench.openTelemetry.OpenTelemetryMetrics

import scala.concurrent.{ExecutionContext, Future}

object DisabledFastPassService {
  def constructor()(ctx: RawlsRequestContext, dataSource: SlickDataSource)(implicit executionContext: ExecutionContext,
                                                                          openTelemetry: OpenTelemetryMetrics[IO]
  ): DisabledFastPassService = new DisabledFastPassService(ctx, dataSource)
}

class DisabledFastPassService(protected val ctx: RawlsRequestContext, protected val dataSource: SlickDataSource
                     )(implicit protected val executionContext: ExecutionContext, val openTelemetry: OpenTelemetryMetrics[IO])
  extends FastPass {
  def setupFastPassForUserInClonedWorkspace(parentWorkspace: Workspace, childWorkspace: Workspace): Future[Unit] =
    throw new NotImplementedError("getBondProviderServiceAccountEmails is not implemented for Azure.")
  def syncFastPassesForUserInWorkspace(workspace: Workspace): Future[Unit] =
    throw new NotImplementedError("getBondProviderServiceAccountEmails is not implemented for Azure.")
  def syncFastPassesForUserInWorkspace(workspace: Workspace, email: String): Future[Unit] =
    throw new NotImplementedError("getBondProviderServiceAccountEmails is not implemented for Azure.")
  def removeFastPassGrantsForWorkspace(workspace: Workspace): Future[Unit] =
    throw new NotImplementedError("getBondProviderServiceAccountEmails is not implemented for Azure.")
}

