package org.broadinstitute.dsde.rawls.workspace

import akka.http.scaladsl.model.StatusCodes
import bio.terra.workspace.client.ApiException
import com.typesafe.scalalogging.LazyLogging
import io.opencensus.scala.Tracing.startSpanWithParent
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.WorkspaceManagerDAO
import org.broadinstitute.dsde.rawls.model.{
  AzureManagedAppCoordinates,
  ErrorReport,
  RawlsRequestContext,
  Workspace,
  WorkspacePolicy,
  WorkspaceType
}

import java.util.UUID
import scala.jdk.CollectionConverters._

/**
  * Responsible for aggregating workspace data together from various sources (i.e, WSM).
  * @param workspaceManagerDAO DAO for talking to WSM
  */
class AggregatedWorkspaceService(workspaceManagerDAO: WorkspaceManagerDAO) extends LazyLogging {

  /**
    * Given a workspace, aggregates any available workpsace information form
    * workspace manager (WSM).
    *
    * * If the provided workspace is not of type "MC", returns the provided
    * "rawls" workspace with no WSM information.
    *
    * @param workspace The source rawls workspace
    * @param ctx Rawls request and tracing context. 
    *
    * @throws AggregateWorkspaceNotFoundException when the source workspace references a workspace manager
    *                                             record that is not present
    * @throws WorkspaceAggregationException when an error is encountered pulling data from aggregating upstream systems
    *                                       
    * @throws InvalidCloudContextException when an aggregated workspace does not have an Azure cloud context
    */
  def getAggregatedWorkspace(workspace: Workspace, ctx: RawlsRequestContext): AggregatedWorkspace = {
    val span = startSpanWithParent("getWorkspaceFromWorkspaceManager", ctx.tracingSpan.orNull)

    workspace.workspaceType match {
      case WorkspaceType.RawlsWorkspace => AggregatedWorkspace(workspace, None, List.empty)
      case WorkspaceType.McWorkspace =>
        try {
          val wsmInfo = workspaceManagerDAO.getWorkspace(workspace.workspaceIdAsUUID, ctx)
          Option(wsmInfo.getAzureContext) match {
            case Some(azureContext) =>
              AggregatedWorkspace(
                workspace,
                Some(
                  AzureManagedAppCoordinates(UUID.fromString(azureContext.getTenantId),
                                             UUID.fromString(azureContext.getSubscriptionId),
                                             azureContext.getResourceGroupId
                  )
                ),
                Option(wsmInfo.getPolicies)
                  .map(policies =>
                    policies.asScala.toList.map(input =>
                      WorkspacePolicy(
                        input.getName,
                        input.getNamespace,
                        Option(input.getAdditionalData)
                          .map(data => data.asScala.map(p => p.getKey -> p.getValue).toMap)
                          .getOrElse(Map.empty)
                      )
                    )
                  )
                  .getOrElse(List.empty)
              )
            case None =>
              throw new InvalidCloudContextException(
                ErrorReport(StatusCodes.NotImplemented,
                            s"Unexpected state, no cloud context found for workspace ${workspace.workspaceId}"
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
  }
}

class InvalidCloudContextException(errorReport: ErrorReport) extends WorkspaceAggregationException(errorReport)
class AggregateWorkspaceNotFoundException(errorReport: ErrorReport) extends WorkspaceAggregationException(errorReport)
class WorkspaceAggregationException(errorReport: ErrorReport) extends RawlsExceptionWithErrorReport(errorReport)
