package org.broadinstitute.dsde.rawls.workspace

import akka.http.scaladsl.model.StatusCodes
import org.broadinstitute.dsde.rawls.model.WorkspaceCloudPlatform.WorkspaceCloudPlatform
import org.broadinstitute.dsde.rawls.model.{
  AzureManagedAppCoordinates,
  ErrorReport,
  GoogleProjectId,
  Workspace,
  WorkspaceCloudPlatform,
  WorkspacePolicy,
  WorkspaceState,
  WorkspaceType
}

/**
  * Represents the aggregation of a "rawls" workspace with any data from
  * external sources (i.e,. workspace manager cloud context, policies, etc.)
  * @param baseWorkspace Source rawls workspace
  * @param googleProjectId Google project ID (if present)
  * @param azureCloudContext Azure cloud context (if present)
  * @param policies Terra policies
  */
case class AggregatedWorkspace(
  baseWorkspace: Workspace,
  googleProjectId: Option[GoogleProjectId],
  azureCloudContext: Option[AzureManagedAppCoordinates],
  policies: List[WorkspacePolicy]
) {

  def getCloudPlatform: WorkspaceCloudPlatform = {
    if (baseWorkspace.workspaceType == WorkspaceType.RawlsWorkspace) {
      return WorkspaceCloudPlatform.Gcp
    }
    (googleProjectId, azureCloudContext) match {
      case (Some(_), None) => WorkspaceCloudPlatform.Gcp
      case (None, Some(_)) => WorkspaceCloudPlatform.Azure
      case (_, _) =>
        throw new InvalidCloudContextException(
          ErrorReport(
            StatusCodes.NotImplemented,
            s"Unexpected state, expected exactly one set of cloud metadata for workspace ${baseWorkspace.workspaceId}"
          )
        )
    }
  }

  def getCloudPlatformHandlingDeleting: Option[WorkspaceCloudPlatform] = {
    if (baseWorkspace.workspaceType == WorkspaceType.RawlsWorkspace) {
      return Some(WorkspaceCloudPlatform.Gcp)
    }

    def handleMissingCloudContext(): Option[WorkspaceCloudPlatform] =
      (googleProjectId, azureCloudContext, baseWorkspace.state) match {
        case (None, None, WorkspaceState.Deleting) => None
        case (_, _, _) =>
          throw new InvalidCloudContextException(
            ErrorReport(
              StatusCodes.NotImplemented,
              s"Expected exactly one set of cloud metadata for workspace ${baseWorkspace.workspaceId} in state ${baseWorkspace.state}"
            )
          )
      }

    (googleProjectId, azureCloudContext) match {
      case (Some(_), None) => Some(WorkspaceCloudPlatform.Gcp)
      case (None, Some(_)) => Some(WorkspaceCloudPlatform.Azure)
      case (_, _)          => handleMissingCloudContext()
    }
  }
}
