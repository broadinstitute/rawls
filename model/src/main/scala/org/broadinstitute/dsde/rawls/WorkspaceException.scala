package org.broadinstitute.dsde.rawls

import akka.http.scaladsl.model.{StatusCode, StatusCodes}
import org.broadinstitute.dsde.rawls.model.{ErrorReport, ErrorReportSource, WorkspaceName}

sealed abstract class WorkspaceException(statusCode: StatusCode, message: String)(implicit source: ErrorReportSource)
    extends RawlsExceptionWithErrorReport(ErrorReport(statusCode, message)) {

  def usingId(workspaceId: String): WorkspaceException = this match {
    case LockedWorkspaceException(_)       => LockedWorkspaceException(workspaceId)
    case NoSuchWorkspaceException(_)       => NoSuchWorkspaceException(workspaceId)
    case WorkspaceAccessDeniedException(_) => WorkspaceAccessDeniedException(workspaceId)
  }

}

case class WorkspaceAccessDeniedException(workspace: String)(implicit source: ErrorReportSource)
    extends WorkspaceException(StatusCodes.Forbidden, s"insufficient permissions to perform operation on $workspace")

object WorkspaceAccessDeniedException {
  def apply(workspaceName: WorkspaceName)(implicit source: ErrorReportSource): WorkspaceAccessDeniedException =
    WorkspaceAccessDeniedException(workspaceName.toString)
}

case class NoSuchWorkspaceException(workspace: String)(implicit source: ErrorReportSource)
    extends WorkspaceException(StatusCodes.NotFound,
                               s"workspace $workspace does not exist or you do not have permission to use it"
    )

object NoSuchWorkspaceException {
  def apply(workspaceName: WorkspaceName)(implicit source: ErrorReportSource): NoSuchWorkspaceException =
    NoSuchWorkspaceException(workspaceName.toString)
}

case class LockedWorkspaceException(workspace: String)(implicit source: ErrorReportSource)
    extends WorkspaceException(StatusCodes.Forbidden, s"The workspace $workspace is locked.")

object LockedWorkspaceException {
  def apply(workspaceName: WorkspaceName)(implicit source: ErrorReportSource): LockedWorkspaceException =
    LockedWorkspaceException(workspaceName.toString)
}
