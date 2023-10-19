package org.broadinstitute.dsde.rawls.workspace

import akka.http.scaladsl.model.StatusCodes
import bio.terra.workspace.client.ApiException
import bio.terra.workspace.model.{WorkspaceDescription, WorkspaceStageModel}
import com.typesafe.scalalogging.LazyLogging
import io.opencensus.scala.Tracing.startSpanWithParent
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.WorkspaceManagerDAO
import org.broadinstitute.dsde.rawls.model.{AzureManagedAppCoordinates, ErrorReport, GoogleProjectId, RawlsRequestContext, Workspace, WorkspacePolicy, WorkspaceState, WorkspaceType}

import java.util.UUID
import scala.jdk.CollectionConverters._

/**
  * Responsible for aggregating workspace data together from various sources (i.e, WSM).
  * @param workspaceManagerDAO DAO for talking to WSM
  */
class AggregatedWorkspaceService(workspaceManagerDAO: WorkspaceManagerDAO) extends LazyLogging {

  /**
   * Given a workspace, aggregates any available workspace information from workspace manager (WSM).
   *
   * If the WSM workspace is a legacy Rawls workspace, it is assumed to be GCP, and the googleProjectId from the
   * provided workspace will be echo'd back out.
   *
   * @param workspace The source rawls workspace
   * @param ctx       Rawls request and tracing context.
   * @throws AggregateWorkspaceNotFoundException when the source workspace references a workspace manager
   *                                             record that is not present
   * @throws WorkspaceAggregationException       when an error is encountered pulling data from aggregating upstream systems
   * @throws InvalidCloudContextException        when an aggregated workspace does not have info for exactly one cloud context
   */
  def fetchAggregatedWorkspace(workspace: Workspace, ctx: RawlsRequestContext): AggregatedWorkspace = {
    val span = startSpanWithParent("getWorkspaceFromWorkspaceManager", ctx.tracingSpan.orNull)
    try {
      val wsmInfo = workspaceManagerDAO.getWorkspace(workspace.workspaceIdAsUUID, ctx)
      (wsmInfo.getStage, Option(wsmInfo.getGcpContext), Option(wsmInfo.getAzureContext), workspace.state) match {
        case (WorkspaceStageModel.RAWLS_WORKSPACE, _, _, _) =>
          AggregatedWorkspace(workspace,
                              Some(workspace.googleProjectId),
                              azureCloudContext = None,
                              policies = List.empty
          )
        case (WorkspaceStageModel.MC_WORKSPACE, None, Some(azureContext), _) =>
          AggregatedWorkspace(
            workspace,
            googleProjectId = None,
            Some(
              AzureManagedAppCoordinates(UUID.fromString(azureContext.getTenantId),
                                         UUID.fromString(azureContext.getSubscriptionId),
                                         azureContext.getResourceGroupId
              )
            ),
            convertPolicies(wsmInfo)
          )
        case (WorkspaceStageModel.MC_WORKSPACE, Some(gcpContext), None, _) =>
          AggregatedWorkspace(
            workspace,
            Some(GoogleProjectId(gcpContext.getProjectId)),
            azureCloudContext = None,
            convertPolicies(wsmInfo)
          )
        case (WorkspaceStageModel.MC_WORKSPACE, None, None, WorkspaceState.Deleting) =>
          AggregatedWorkspace(
            workspace,
            googleProjectId = None,
            azureCloudContext = None,
            convertPolicies(wsmInfo)
          )
        case (_, _, _, _) =>
          throw new InvalidCloudContextException(
            ErrorReport(
              StatusCodes.NotImplemented,
              s"Unexpected state, expected exactly one set of cloud metadata for workspace ${workspace.workspaceId}"
            )
          )
      }
    } catch {
      case e: ApiException =>
        if (e.getCode == StatusCodes.NotFound.intValue) {
          throw new AggregateWorkspaceNotFoundException(errorReport = ErrorReport(StatusCodes.NotFound, e))
        } else {
          throw new WorkspaceAggregationException(errorReport = ErrorReport(e.getCode, e))
        }
    } finally
      span.end()
  }

  /**
   * Optimized version of [[fetchAggregatedWorkspace]]
   *
   * If the provided workspace is not of type "MC", returns the provided "rawls" workspace with no WSM information, as
   * it can be assumed to be GCP and does not need to call out to WSM for this.
   */
  def optimizedFetchAggregatedWorkspace(workspace: Workspace, ctx: RawlsRequestContext): AggregatedWorkspace =
    workspace.workspaceType match {
      case WorkspaceType.RawlsWorkspace =>
        AggregatedWorkspace(workspace, Some(workspace.googleProjectId), azureCloudContext = None, policies = List.empty)
      case WorkspaceType.McWorkspace =>
        fetchAggregatedWorkspace(workspace, ctx)
    }

  private def convertPolicies(wsmInfo: WorkspaceDescription): List[WorkspacePolicy] =
    Option(wsmInfo.getPolicies)
      .map(policies =>
        policies.asScala.toList.map(input =>
          WorkspacePolicy(
            input.getName,
            input.getNamespace,
            Option(
              input.getAdditionalData.asScala.toList
                .map(data => Map.apply(data.getKey -> data.getValue))
            )
              .getOrElse(List.empty)
          )
        )
      )
      .getOrElse(List.empty)
}

class InvalidCloudContextException(errorReport: ErrorReport) extends WorkspaceAggregationException(errorReport)
class AggregateWorkspaceNotFoundException(errorReport: ErrorReport) extends WorkspaceAggregationException(errorReport)
class WorkspaceAggregationException(errorReport: ErrorReport) extends RawlsExceptionWithErrorReport(errorReport)
