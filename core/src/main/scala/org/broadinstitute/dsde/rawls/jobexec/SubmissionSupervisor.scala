package org.broadinstitute.dsde.rawls.jobexec

import akka.actor.SupervisorStrategy.Restart
import akka.actor.{Actor, ActorRef, Cancellable, Props, SupervisorStrategy}
import akka.pattern._
import com.google.api.client.auth.oauth2.Credential
import com.typesafe.scalalogging.LazyLogging
import nl.grons.metrics4.scala.Counter
import org.broadinstitute.dsde.rawls.coordination.DataSourceAccess
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.jobexec.SubmissionMonitorActor.MonitoredSubmissionException
import org.broadinstitute.dsde.rawls.jobexec.SubmissionSupervisor._
import org.broadinstitute.dsde.rawls.metrics.RawlsExpansion._
import org.broadinstitute.dsde.rawls.metrics.RawlsInstrumented
import org.broadinstitute.dsde.rawls.model.SubmissionStatuses.SubmissionStatus
import org.broadinstitute.dsde.rawls.model.WorkflowStatuses.WorkflowStatus
import org.broadinstitute.dsde.rawls.model.{SubmissionStatuses, WorkflowStatuses, WorkspaceName}
import org.broadinstitute.dsde.rawls.util.ThresholdOneForOneStrategy
import org.broadinstitute.dsde.workbench.dataaccess.NotificationDAO

import java.util.UUID
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.control.NonFatal

/**
 * Created by dvoet on 6/26/15.
 */
object SubmissionSupervisor {
  sealed trait SubmissionSupervisorMessage

  case class SubmissionStarted(workspaceName: WorkspaceName, submissionId: UUID)
  case object StartMonitorPass
  case object SubmissionMonitorPassComplete

  /**
    * A periodic collection of all current workflow and submission status counts.
    * This is used for instrumentation.
    */
  case object CheckCurrentWorkflowStatusCounts
  case class SaveCurrentWorkflowStatusCounts(workspaceName: WorkspaceName,
                                             submissionId: UUID,
                                             workflowStatusCounts: Map[WorkflowStatus, Int],
                                             submissionStatusCounts: Map[SubmissionStatus, Int],
                                             reschedule: Boolean
  )
  case object RefreshGlobalJobExecGauges
  case class SaveGlobalJobExecCounts(submissionStatuses: Map[SubmissionStatus, Int],
                                     workflowStatuses: Map[WorkflowStatus, Int]
  )

  def props(executionServiceCluster: ExecutionServiceCluster,
            datasource: DataSourceAccess,
            samDAO: SamDAO,
            googleServicesDAO: GoogleServicesDAO,
            notificationDAO: NotificationDAO,
            bucketCredential: Credential,
            submissionMonitorConfig: SubmissionMonitorConfig,
            workbenchMetricBaseName: String
  ): Props =
    Props(
      new SubmissionSupervisor(executionServiceCluster,
                               datasource,
                               samDAO,
                               googleServicesDAO,
                               notificationDAO,
                               bucketCredential,
                               submissionMonitorConfig,
                               workbenchMetricBaseName
      )
    )
}

/**
 * Supervisor actor that should run for the life of the app. SubmissionStarted messages will start a monitor
 * for the given submission. Errors are logged if that monitor fails.
 *
 * @param executionServiceCluster
 * @param datasource
 */
//noinspection ScalaDocMissingParameterDescription,ActorMutableStateInspection,TypeAnnotation
class SubmissionSupervisor(executionServiceCluster: ExecutionServiceCluster,
                           datasource: DataSourceAccess,
                           samDAO: SamDAO,
                           googleServicesDAO: GoogleServicesDAO,
                           notificationDAO: NotificationDAO,
                           bucketCredential: Credential,
                           submissionMonitorConfig: SubmissionMonitorConfig,
                           override val workbenchMetricBaseName: String
) extends Actor
    with LazyLogging
    with RawlsInstrumented {
  import context._

  /* A note on metrics:
   *  We instrument four things here:
   *  1. The number of submissions in each state across the whole system.
   *  2. The number of workflows in each state across the whole system.
   *  3. The number of submissions in each state, by workspace, for any workspace with at least one running submission.
   *  4. The number of workflows in each state, by workspace and submission, for any submission with at least one running workflow.
   *
   *  Aggregating over #3 and #4 will give you strange results, as metrics will disappear when submissions or workspaces finish running.
   *  #3 and #4 are meant to be used as a way to diagnose the behaviour of a particular workspace or submission.
   *
   *  Every SubmissionMonitorActor calculates #3 and sends it in the SaveCurrentWorkflowStatusCounts message. Therefore two submissions
   *  running in the same workspace will update activeSubmissionStatusCounts to the same value once per poll interval. This is silly but fine.
   *
   *  The fields below are marked volatile because they are read by a separate statsd thread.
   *  They are only written by this actor.
   */
  @volatile
  private var activeSubmissionStatusCounts: Map[WorkspaceName, Map[SubmissionStatus, Int]] = Map.empty
  @volatile
  private var activeWorkflowStatusCounts: Map[UUID, Map[WorkflowStatus, Int]] = Map.empty
  @volatile
  private var globalSubmissionStatusCounts: Map[SubmissionStatus, Int] = Map.empty
  @volatile
  private var globalWorkflowStatusCounts: Map[WorkflowStatus, Int] = Map.empty

  override def preStart(): Unit = {
    super.preStart()

    scheduleInitialMonitorPass
    registerGlobalJobExecGauges()
    system.scheduler.schedule(0 seconds,
                              submissionMonitorConfig.submissionPollInterval,
                              self,
                              RefreshGlobalJobExecGauges
    )
  }

  override def receive = {
    case StartMonitorPass =>
      startMonitoringNewSubmissions pipeTo self
    case SubmissionStarted(workspaceName, submissionId) =>
      val child = startSubmissionMonitor(workspaceName, submissionId, bucketCredential)
      scheduleNextCheckCurrentWorkflowStatus(child)
      registerDetailedJobExecGauges(workspaceName, submissionId)

    case SubmissionMonitorPassComplete =>
      scheduleNextMonitorPass

    case SaveCurrentWorkflowStatusCounts(workspaceName,
                                         submissionId,
                                         workflowStatusCounts,
                                         submissionStatusCounts,
                                         reschedule
        ) =>
      this.activeWorkflowStatusCounts += submissionId -> workflowStatusCounts
      this.activeSubmissionStatusCounts += workspaceName -> submissionStatusCounts
      if (reschedule) {
        scheduleNextCheckCurrentWorkflowStatus(sender)
      } else {
        // this submission is complete; save some metrics and some memory by removing its workflow status counter gauge
        unregisterWorkflowGauges(workspaceName, submissionId)

        // if all the submissions in this workspace are in terminal statuses, we can unregister the gauge for its submission count too
        if ((submissionStatusCounts.keySet -- SubmissionStatuses.terminalStatuses.toSet).isEmpty) {
          unregisterSubmissionGauges(workspaceName)
        }
      }

    case RefreshGlobalJobExecGauges =>
      refreshGlobalJobExecGauges pipeTo self

    case SaveGlobalJobExecCounts(submissionStatuses, workflowStatuses) =>
      saveGlobalJobExecCounts(submissionStatuses, workflowStatuses)
  }

  private def scheduleInitialMonitorPass: Cancellable =
    system.scheduler.scheduleOnce(submissionMonitorConfig.submissionPollInterval, self, StartMonitorPass)

  private def scheduleNextMonitorPass: Cancellable =
    system.scheduler.scheduleOnce(submissionMonitorConfig.submissionPollInterval, self, StartMonitorPass)

  private def restartCounter(workspaceName: WorkspaceName, submissionId: UUID, cause: Throwable): Counter =
    // Note the restart counter is _not_ marked transient() because restarts are relatively rare and
    // we want to track them over a longer time frame.
    workspaceSubmissionMetricBuilder(workspaceName, submissionId).expand("cause", cause).asCounter("monitorRestarted")

  private def startSubmissionMonitor(workspaceName: WorkspaceName,
                                     submissionId: UUID,
                                     credential: Credential
  ): ActorRef =
    actorOf(
      SubmissionMonitorActor
        .props(
          workspaceName,
          submissionId,
          datasource,
          samDAO,
          googleServicesDAO,
          notificationDAO,
          executionServiceCluster,
          credential,
          submissionMonitorConfig,
          workbenchMetricBaseName
        )
        .withDispatcher("submission-monitor-dispatcher"),
      submissionId.toString
    )

  private def scheduleNextCheckCurrentWorkflowStatus(actor: ActorRef): Cancellable =
    system.scheduler.scheduleOnce(submissionMonitorConfig.submissionPollInterval,
                                  actor,
                                  CheckCurrentWorkflowStatusCounts
    )

  private def refreshGlobalJobExecGauges: Future[SaveGlobalJobExecCounts] =
    datasource.inTransaction { dataAccess =>
      for {
        subStatuses <- dataAccess.submissionQuery.countAllStatuses
        wfStatuses <- dataAccess.workflowQuery.countAllStatuses
      } yield SaveGlobalJobExecCounts(subStatuses.map { case (k, v) => SubmissionStatuses.withName(k) -> v },
                                      wfStatuses.map { case (k, v) => WorkflowStatuses.withName(k) -> v }
      )
    }

  private def startMonitoringNewSubmissions: Future[SubmissionMonitorPassComplete.type] = {
    val monitoredSubmissions = context.children.map(_.path.name).toSet

    datasource.inTransaction { dataAccess =>
      dataAccess.submissionQuery.listActiveSubmissionIdsWithWorkspace(limit = submissionMonitorConfig.submissionPollExpiration) map { activeSubs =>
        val unmonitoredSubmissions = activeSubs.filterNot { case (subId, _) =>
          monitoredSubmissions.contains(subId.toString)
        }

        unmonitoredSubmissions.foreach { case (subId, wsName) =>
          self ! SubmissionStarted(wsName, subId)
        }
        SubmissionMonitorPassComplete
      }
    } recover { case t: Throwable =>
      logger.error("Error starting submission monitor actors for new submissions", t)
      SubmissionMonitorPassComplete
    }
  }

  private def saveGlobalJobExecCounts(submissionStatuses: Map[SubmissionStatus, Int],
                                      workflowStatus: Map[WorkflowStatus, Int]
  ) = {
    this.globalSubmissionStatusCounts = submissionStatuses
    this.globalWorkflowStatusCounts = workflowStatus
  }

  // restart the actor on failure (e.g. a DB deadlock or failed transaction)
  // if this actor has failed more than 3 times, log each new failure
  override val supervisorStrategy = {
    val alwaysRestart: SupervisorStrategy.Decider = { case _ =>
      Restart
    }

    def thresholdFunc(throwable: Throwable, count: Int): Unit = throwable match {
      case MonitoredSubmissionException(workspaceName, submissionId, cause) =>
        // increment smaRestart counter
        restartCounter(workspaceName, submissionId, cause) += 1
        logger.error(s"error monitoring submission $submissionId in workspace $workspaceName after $count times", cause)
      case _ =>
        logger.error(s"error monitoring submission after $count times", throwable)
    }

    new ThresholdOneForOneStrategy(thresholdLimit = 3)(alwaysRestart)(thresholdFunc)
  }

  private def registerGlobalJobExecGauges(): Unit =
    try {
      SubmissionStatuses.allStatuses.foreach { status =>
        ExpandedMetricBuilder.expand(SubmissionStatusMetricKey, status).asGaugeIfAbsent("current") {
          globalSubmissionStatusCounts.getOrElse(status, 0)
        }
      }
      WorkflowStatuses.allStatuses.foreach { status =>
        ExpandedMetricBuilder.expand(WorkflowStatusMetricKey, status).asGaugeIfAbsent("current") {
          globalWorkflowStatusCounts.getOrElse(status, 0)
        }
      }
    } catch {
      case NonFatal(e) => logger.warn(s"Could not initialize gauge metrics for jobexec", e)
    }

  private def registerDetailedJobExecGauges(workspaceName: WorkspaceName, submissionId: UUID): Unit =
    if (submissionMonitorConfig.trackDetailedSubmissionMetrics) {
      try {
        WorkflowStatuses.allStatuses.foreach { status =>
          workspaceSubmissionMetricBuilder(workspaceName, submissionId)
            .expand(WorkflowStatusMetricKey, status)
            .transient()
            .asGaugeIfAbsent("current") {
              activeWorkflowStatusCounts.get(submissionId).map(_.getOrElse(status, 0)).getOrElse(0)
            }
        }
        SubmissionStatuses.allStatuses.foreach { status =>
          workspaceMetricBuilder(workspaceName)
            .expand(SubmissionStatusMetricKey, status)
            .transient()
            .asGaugeIfAbsent("current") {
              activeSubmissionStatusCounts.get(workspaceName).map(_.getOrElse(status, 0)).getOrElse(0)
            }
        }
      } catch {
        case NonFatal(e) =>
          logger.warn(s"Could not initialize gauge metrics for workspace $workspaceName and submission $submissionId",
                      e
          )
      }
    }

  private def unregisterWorkflowGauges(workspaceName: WorkspaceName, submissionId: UUID): Unit =
    if (submissionMonitorConfig.trackDetailedSubmissionMetrics) {
      WorkflowStatuses.allStatuses.foreach { status =>
        workspaceSubmissionMetricBuilder(workspaceName, submissionId)
          .expand(WorkflowStatusMetricKey, status)
          .transient()
          .unregisterMetric("current")
      }
      activeWorkflowStatusCounts -= submissionId
    }

  private def unregisterSubmissionGauges(workspaceName: WorkspaceName): Unit =
    if (submissionMonitorConfig.trackDetailedSubmissionMetrics) {
      SubmissionStatuses.allStatuses.foreach { status =>
        workspaceMetricBuilder(workspaceName)
          .expand(SubmissionStatusMetricKey, status)
          .transient()
          .unregisterMetric("current")
      }
      activeSubmissionStatusCounts -= workspaceName
    }
}
