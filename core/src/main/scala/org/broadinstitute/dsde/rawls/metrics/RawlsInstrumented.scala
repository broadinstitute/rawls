package org.broadinstitute.dsde.rawls.metrics

import nl.grons.metrics4.scala.{Counter, Histogram, Timer}
import org.broadinstitute.dsde.rawls.metrics.RawlsExpansion._
import org.broadinstitute.dsde.rawls.model.SubmissionStatuses.SubmissionStatus
import org.broadinstitute.dsde.rawls.model.WorkflowStatuses.WorkflowStatus
import org.broadinstitute.dsde.rawls.model.WorkspaceName
import slick.dbio.{DBIOAction, Effect, NoStream}

import java.util.UUID
import scala.concurrent.ExecutionContext

/**
  * Created by rtitle on 7/13/17.
  */
trait RawlsInstrumented extends WorkbenchInstrumented {

  // Keys for expanded metric fragments
  final val SubmissionMetricKey = "submission"
  final val SubmissionStatusMetricKey = "submissionStatus"
  final val SubsystemMetricKey = "subsystem"
  final val WorkflowStatusMetricKey = "workflowStatus"
  final val WorkspaceMetricKey = "workspace"
  final val WorkspaceDataMetricKey = "workspaceData"
  final val AggregationMetricKey = "aggregation"

  /**
    * An ExpandedMetricBuilder for a WorkspaceName.
    */
  protected def workspaceMetricBuilder(workspaceName: WorkspaceName): ExpandedMetricBuilder =
    ExpandedMetricBuilder.expand(WorkspaceMetricKey, workspaceName)

  /**
    * An ExpandedMetricBuilder for a WorkspaceName and a submission ID.
    */
  protected def workspaceSubmissionMetricBuilder(workspaceName: WorkspaceName,
                                                 submissionId: UUID
  ): ExpandedMetricBuilder =
    workspaceMetricBuilder(workspaceName).expand(SubmissionMetricKey, submissionId)

  /**
    * Provides a counter for a SubmissionStatus.
    * @param builder base builder used to generate the counter
    * @return SubmissionStatus => Counter
    */
  protected def submissionStatusCounter(builder: ExpandedMetricBuilder): SubmissionStatus => Counter =
    status =>
      builder
        .expand(SubmissionStatusMetricKey, status)
        .transient()
        .asCounter("count")

  /**
    * Provides a counter for a WorkflowStatus.
    * @param builder base builder used to generate the counter
    * @return WorkflowStatus => Counter
    */
  protected def workflowStatusCounter(builder: ExpandedMetricBuilder): WorkflowStatus => Counter =
    status =>
      builder
        .expand(WorkflowStatusMetricKey, status)
        .transient()
        .asCounter("count")

  /**
    * A timer for capturing latency between initial Rawls submission and workflow processing in Cromwell.
    */
  protected def workflowToCromwellLatency: Timer =
    ExpandedMetricBuilder
      .expand(WorkspaceMetricKey, "submission_to_cromwell")
      .asTimer("latency")

  /**
    * A counter to track the total number of times that the entity cache was manually updated, as opposed to automatically.
    */
  protected def opportunisticEntityCacheSaveCounter: Counter =
    ExpandedMetricBuilder
      .expand(WorkspaceDataMetricKey, "opportunistic_entity_cache_save")
      .transient()
      .asCounter("count")

  /**
    * A counter to track the total number of entity cache saves, regardless of type.
    */
  protected def entityCacheSaveCounter: Counter =
    ExpandedMetricBuilder
      .expand(WorkspaceDataMetricKey, "entity_cache_save")
      .transient()
      .asCounter("count")

  /**
    * A timer for capturing cache staleness for Rawls entities.
    */
  protected def entityCacheStaleness: Timer =
    ExpandedMetricBuilder
      .expand(AggregationMetricKey, "entity_cache")
      .transient()
      .asTimer("staleness")

  /**
    * A counter to track the total number of created non-multi-cloud workspaces.
    */
  protected def createdWorkspaceCounter: Counter =
    ExpandedMetricBuilder
      .expand(AggregationMetricKey, "created_workspaces")
      .transient()
      .asCounter("count")

  /**
    * A counter to track the total number of created multi-cloud workspaces from Azure billing projects.
    */
  protected def createdMultiCloudWorkspaceCounter: Counter =
    ExpandedMetricBuilder
      .expand(AggregationMetricKey, "created_mc_workspaces")
      .transient()
      .asCounter("count")

  /**
    * A counter to track the total number of cloned workspaces.
    */
  protected def clonedWorkspaceCounter: Counter =
    ExpandedMetricBuilder
      .expand(AggregationMetricKey, "cloned_workspaces")
      .transient()
      .asCounter("count")

  /**
    * Counts the total number of cloned 'Workspace Manager'-managed workspaces.
    */
  protected def clonedMultiCloudWorkspaceCounter: Counter =
    ExpandedMetricBuilder
      .expand(AggregationMetricKey, "cloned_mc_workspaces")
      .transient()
      .asCounter("count")

  /**
    * A histogram to track the number of entities in each cloned workspace.
    */
  protected def clonedWorkspaceEntityHistogram: Histogram =
    ExpandedMetricBuilder
      .expand(AggregationMetricKey, "cloned_ws_entities")
      .transient()
      .asHistogram("count")

  /**
    * A histogram to track the number of attributes in each cloned workspace.
    */
  protected def clonedWorkspaceAttributeHistogram: Histogram =
    ExpandedMetricBuilder
      .expand(AggregationMetricKey, "cloned_ws_attributes")
      .transient()
      .asHistogram("count")

  protected def deletedMultiCloudWorkspaceCounter: Counter =
    ExpandedMetricBuilder
      .expand(AggregationMetricKey, "deleted_mc_workspaces")
      .transient()
      .asCounter("count")
}

object RawlsInstrumented {

  /**
    * Adds a .countDBResult method to Counter which counts the result of a numeric DBIOAction.
    */
  implicit class CounterDBIOActionSupport(counter: Counter) {
    def countDBResult[R, S <: NoStream, E <: Effect](
      action: DBIOAction[R, S, E]
    )(implicit numeric: Numeric[R], executionContext: ExecutionContext): DBIOAction[R, NoStream, E] =
      action.map { count =>
        counter += numeric.toLong(count)
        count
      }
  }
}
