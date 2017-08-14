package org.broadinstitute.dsde.rawls.jobexec

import java.util.UUID

import akka.actor.SupervisorStrategy.Restart
import akka.actor.{Actor, ActorRef, Cancellable, Props, SupervisorStrategy}
import com.google.api.client.auth.oauth2.Credential
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.jobexec.SubmissionSupervisor.{CheckCurrentWorkflowStatusCounts, SaveCurrentWorkflowStatusCounts, SubmissionStarted}
import org.broadinstitute.dsde.rawls.metrics.RawlsExpansion._
import org.broadinstitute.dsde.rawls.metrics.RawlsInstrumented
import org.broadinstitute.dsde.rawls.model.SubmissionStatuses.SubmissionStatus
import org.broadinstitute.dsde.rawls.model.WorkflowStatuses.WorkflowStatus
import org.broadinstitute.dsde.rawls.model.{SubmissionStatuses, WorkflowStatuses, WorkspaceName}
import org.broadinstitute.dsde.rawls.util.{ThresholdOneForOneStrategy, addJitter}

import scala.concurrent.duration._
import scala.util.control.NonFatal

/**
 * Created by dvoet on 6/26/15.
 */
object SubmissionSupervisor {
  sealed trait SubmissionSupervisorMessage

  case class SubmissionStarted(workspaceName: WorkspaceName, submissionId: UUID, credential: Credential)

  /**
    * A periodic collection of all current workflow and submission status counts.
    * This is used for instrumentation.
    */
  case object CheckCurrentWorkflowStatusCounts
  case class SaveCurrentWorkflowStatusCounts(workflowStatusCounts: Map[WorkflowStatus, Int], submissionStatusCounts: Map[SubmissionStatus, Int], reschedule: Boolean)

  def props(executionServiceCluster: ExecutionServiceCluster,
            datasource: SlickDataSource,
            submissionPollInterval: FiniteDuration = 1 minutes,
            workbenchMetricBaseName: String): Props = {
    Props(new SubmissionSupervisor(executionServiceCluster, datasource, submissionPollInterval, workbenchMetricBaseName))
  }
}

/**
 * Supervisor actor that should run for the life of the app. SubmissionStarted messages will start a monitor
 * for the given submission. Errors are logged if that monitor fails.
 * 
 * @param executionServiceCluster
 * @param datasource
 * @param submissionPollInterval
 */
class SubmissionSupervisor(executionServiceCluster: ExecutionServiceCluster,
                           datasource: SlickDataSource,
                           submissionPollInterval: FiniteDuration,
                           override val workbenchMetricBaseName: String) extends Actor with LazyLogging with RawlsInstrumented {
  import context._

  // These fields are marked volatile because it is read by a separate statsd thread.
  // It is only written by this actor.
  @volatile
  private var currentWorkflowStatusCounts: Map[WorkflowStatus, Int] = Map.empty
  @volatile
  private var currentSubmissionStatusCounts: Map[SubmissionStatus, Int] = Map.empty

  override def receive = {
    case SubmissionStarted(workspaceContext, submissionId, credential) =>
      val child = startSubmissionMonitor(workspaceContext, submissionId, credential)
      scheduleNextCheckCurrentWorkflowStatus(child)
      initGauge(workspaceContext, submissionId)

    case SaveCurrentWorkflowStatusCounts(workflowStatusCounts, submissionStatusCounts, reschedule) =>
      this.currentWorkflowStatusCounts = workflowStatusCounts
      this.currentSubmissionStatusCounts = submissionStatusCounts
      if (reschedule) {
        scheduleNextCheckCurrentWorkflowStatus(sender)
      }
  }

  private def startSubmissionMonitor(workspaceName: WorkspaceName, submissionId: UUID, credential: Credential): ActorRef = {
    actorOf(SubmissionMonitorActor.props(workspaceName, submissionId, datasource, executionServiceCluster, credential, submissionPollInterval, workbenchMetricBaseName), submissionId.toString)
  }

  private def scheduleNextCheckCurrentWorkflowStatus(actor: ActorRef): Cancellable = {
    system.scheduler.scheduleOnce(addJitter(submissionPollInterval), actor, CheckCurrentWorkflowStatusCounts)
  }

  // restart the actor on failure (e.g. a DB deadlock or failed transaction)
  // if this actor has failed more than 3 times, log each new failure
  override val supervisorStrategy = {
    val alwaysRestart: SupervisorStrategy.Decider = {
      case _ => Restart
    }

    def thresholdFunc(cause: Throwable, count: Int): Unit = {
      logger.error(s"error monitoring submission after $count times", cause)
    }

    new ThresholdOneForOneStrategy(thresholdLimit = 3)(alwaysRestart)(thresholdFunc)
  }

  private def initGauge(workspaceName: WorkspaceName, submissionId: UUID): Unit = {
    try {
      WorkflowStatuses.allStatuses.foreach { status =>
        workspaceSubmissionMetricBuilder(workspaceName, submissionId).expand(WorkflowStatusMetricKey, status).asGauge("current") {
          currentWorkflowStatusCounts.getOrElse(status, 0)
        }
      }
      SubmissionStatuses.allStatuses.foreach { status =>
        workspaceMetricBuilder(workspaceName).expand(SubmissionStatusMetricKey, status).asGauge("current") {
          currentSubmissionStatusCounts.getOrElse(status, 0)
        }
      }
    } catch {
      case NonFatal(e) => logger.warn(s"Could not initialize gauge metrics for workspace $workspaceName and submission $submissionId", e)
    }
  }
}
