package org.broadinstitute.dsde.rawls.workspace

import org.broadinstitute.dsde.rawls.monitor.workspace.runners.deletion.actions.WorkspaceDeletionActionFailureException

import java.util.UUID

class LeonardoOperationFailureException(message: String, val workspaceId: UUID)
    extends WorkspaceDeletionActionFailureException(message)
