package org.broadinstitute.dsde.rawls.fastpass

import org.broadinstitute.dsde.rawls.model.Workspace
import scala.concurrent.Future

trait FastPassService {
  def setupFastPassForUserInClonedWorkspace(parentWorkspace: Workspace, childWorkspace: Workspace): Future[Unit]
  def syncFastPassesForUserInWorkspace(workspace: Workspace): Future[Unit]
  def syncFastPassesForUserInWorkspace(workspace: Workspace, email: String): Future[Unit]
  def removeFastPassGrantsForWorkspace(workspace: Workspace): Future[Unit]
}
