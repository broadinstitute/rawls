package org.broadinstitute.dsde.rawls.workspace

import akka.http.scaladsl.model.StatusCodes
import org.broadinstitute.dsde.rawls.model.WorkspaceCloudPlatform.WorkspaceCloudPlatform
import org.broadinstitute.dsde.rawls.model.{
  AzureManagedAppCoordinates,
  ErrorReport,
  Workspace,
  WorkspaceCloudPlatform,
  WorkspacePolicy,
  WorkspaceType
}

/**
  * Represents the aggregation of a "rawls" workspace with any data from
  * external sources (i.e,. workspace manager cloud context, policies, etc.)
  * @param rawlsWorkspace Source rawls worksapce
  * @param azureCloudContext Azure cloud context (if present)
  * @param policies Terra policies
  */
case class AggregatedWorkspace(
  rawlsWorkspace: Workspace,
  azureCloudContext: Option[AzureManagedAppCoordinates],
  policies: List[WorkspacePolicy]
) {

  def getCloudPlatform: WorkspaceCloudPlatform = {
    if (rawlsWorkspace.workspaceType == WorkspaceType.RawlsWorkspace) {
      return WorkspaceCloudPlatform.Gcp
    }
    azureCloudContext match {
      case Some(_) => WorkspaceCloudPlatform.Azure
      case None =>
        throw new InvalidCloudContextException(
          ErrorReport(StatusCodes.NotImplemented,
                      s"Unexpected state, no cloud context found for workspace ${rawlsWorkspace.workspaceId}"
          )
        )
    }
  }
}
