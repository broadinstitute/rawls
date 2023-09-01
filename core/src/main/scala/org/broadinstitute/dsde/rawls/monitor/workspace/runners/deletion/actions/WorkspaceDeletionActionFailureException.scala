package org.broadinstitute.dsde.rawls.monitor.workspace.runners.deletion.actions

import org.broadinstitute.dsde.rawls.RawlsException

case class WorkspaceDeletionActionFailureException(message: String) extends RawlsException(message) {}
