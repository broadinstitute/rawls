package org.broadinstitute.dsde.rawls.disabled

import org.broadinstitute.dsde.rawls.dataaccess.SlickDataSource
import org.broadinstitute.dsde.rawls.fastpass.FastPass
import org.broadinstitute.dsde.rawls.model.{RawlsRequestContext, Workspace}

import scala.concurrent.{ExecutionContext, Future}

object DisabledFastPassService {
  def constructor()(ctx: RawlsRequestContext, dataSource: SlickDataSource)(implicit
    executionContext: ExecutionContext
  ): DisabledFastPassService = new DisabledFastPassService(ctx, dataSource)
}

class DisabledFastPassService(protected val ctx: RawlsRequestContext, protected val dataSource: SlickDataSource)(
  implicit protected val executionContext: ExecutionContext
) extends FastPass {
  def setupFastPassForUserInClonedWorkspace(parentWorkspace: Workspace, childWorkspace: Workspace): Future[Unit] =
    throw new NotImplementedError("setupFastPassForUserInClonedWorkspace is not implemented for Azure.")
  def syncFastPassesForUserInWorkspace(workspace: Workspace): Future[Unit] =
    throw new NotImplementedError("syncFastPassesForUserInWorkspace is not implemented for Azure.")
  def syncFastPassesForUserInWorkspace(workspace: Workspace, email: String): Future[Unit] =
    throw new NotImplementedError("syncFastPassesForUserInWorkspace is not implemented for Azure.")
  def removeFastPassGrantsForWorkspace(workspace: Workspace): Future[Unit] =
    throw new NotImplementedError("removeFastPassGrantsForWorkspace is not implemented for Azure.")
}
