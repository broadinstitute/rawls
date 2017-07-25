package org.broadinstitute.dsde.rawls.jobexec

import java.util.UUID

import akka.actor.SupervisorStrategy.Restart
import akka.actor.{Actor, Props, SupervisorStrategy}
import com.google.api.client.auth.oauth2.Credential
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.jobexec.SubmissionSupervisor.SubmissionStarted
import org.broadinstitute.dsde.rawls.model.WorkspaceName
import org.broadinstitute.dsde.rawls.util.ThresholdOneForOneStrategy

import scala.concurrent.duration._

/**
 * Created by dvoet on 6/26/15.
 */
object SubmissionSupervisor {
  sealed trait SubmissionSupervisorMessage

  case class SubmissionStarted(workspaceName: WorkspaceName, submissionId: UUID, credential: Credential)

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
                           workbenchMetricsBaseName: String) extends Actor {
  import context._

  override def receive = {
    case SubmissionStarted(workspaceContext, submissionId, credential) => startSubmissionMonitor(workspaceContext, submissionId, credential)
  }

  private def startSubmissionMonitor(workspaceName: WorkspaceName, submissionId: UUID, credential: Credential): Unit = {
    actorOf(SubmissionMonitorActor.props(workspaceName, submissionId, datasource, executionServiceCluster, credential, submissionPollInterval, workbenchMetricsBaseName), submissionId.toString)
  }

  // restart the actor on failure (e.g. a DB deadlock or failed transaction)
  // if this actor has failed more than 3 times, log each new failure
  override val supervisorStrategy = {
    val alwaysRestart: SupervisorStrategy.Decider = {
      case _ => Restart
    }

    def thresholdFunc(cause: Throwable, count: Int): Unit = {
      system.log.error(cause, s"error monitoring submission: SubmissionMonitorActor has been restarted $count times")
    }

    new ThresholdOneForOneStrategy(thresholdLimit = 3)(alwaysRestart)(thresholdFunc)
  }
}
