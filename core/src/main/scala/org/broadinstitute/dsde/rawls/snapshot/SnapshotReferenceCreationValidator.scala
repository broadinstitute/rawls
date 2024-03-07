package org.broadinstitute.dsde.rawls.snapshot

import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.model.WorkspaceCloudPlatform.WorkspaceCloudPlatform
import org.broadinstitute.dsde.rawls.model.{Workspace, WorkspaceCloudPlatform}

object SnapshotReferenceCreationValidator {
  def constructor(workspaceContext: Workspace, snapshot: WrappedSnapshot): SnapshotReferenceCreationValidator =
    new SnapshotReferenceCreationValidator(workspaceContext, snapshot)
}

// thrown to disallow creation of a snapshot reference on an unsupported platform
class UnsupportedPlatformException(message: String) extends RawlsException(message)
// thrown to disallow creation of a snapshot reference across cloud boundaries
class PlatformBoundaryException(message: String) extends RawlsException(message)
// thrown to disallow creation of a snapshot reference across a protected data boundary
class ProtectedDataException(message: String) extends RawlsException(message)

class SnapshotReferenceCreationValidator(val workspaceContext: Workspace, val snapshot: WrappedSnapshot) {

  // Ideally this would rely on Terra Policy Service, but until TPS is enabled for GCP
  // We'll have to use this workaround for identifying protected status.
  @throws(classOf[ProtectedDataException])
  def validateProtectedStatus(): Unit =
    if (!isWorkspaceProtected && snapshot.isProtected) {
      throw new ProtectedDataException("Unable to add protected snapshot to unprotected workspace.")
    }

  // Throws an exception when the given snapshot cannot be referenced by the given workspace due to crossing an
  // unsupported platform boundary.
  @throws(classOf[PlatformBoundaryException])
  def validateWorkspacePlatformCompatibility(workspacePlatform: Option[WorkspaceCloudPlatform]): Unit =
    workspacePlatform match {
      case Some(WorkspaceCloudPlatform.Gcp) => // ok
      case Some(WorkspaceCloudPlatform.Azure) =>
        throw new UnsupportedPlatformException("Snapshots by reference are not supported for Azure workspaces.")
      case None =>
        throw new PlatformBoundaryException(
          "Snapshots by reference are not supported into a workspace with no cloud context (" +
            s"snapshot: ${snapshot.platform}, workspace: ${workspacePlatform})."
        )
    }

  // TODO: get this information from a more authoritative source rather than relying on the hardcoded bucket prefix
  private def isWorkspaceProtected: Boolean = workspaceContext.bucketName.startsWith("fc-secure")
}
