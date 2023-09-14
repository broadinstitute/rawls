package org.broadinstitute.dsde.rawls.monitor.workspace.runners.deletion.actions

import org.broadinstitute.dsde.rawls.RawlsException

class WorkspaceDeletionActionTimeoutException(message: String) extends WorkspaceDeletionActionFailureException(message)
class WorkspaceDeletionActionFailureException(message: String) extends RawlsException(message) {}
