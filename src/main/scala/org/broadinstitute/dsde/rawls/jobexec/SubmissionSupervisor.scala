package org.broadinstitute.dsde.rawls.jobexec

import akka.actor.SupervisorStrategy.Restart
import akka.actor.{Props, OneForOneStrategy, Actor}
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.jobexec.SubmissionSupervisor.SubmissionStarted
import org.broadinstitute.dsde.rawls.model.{Submission,WorkspaceName}
import spray.http.HttpCookie

import scala.concurrent.duration._

/**
 * Created by dvoet on 6/26/15.
 */
object SubmissionSupervisor {
  sealed trait SubmissionSupervisorMessage

  case class SubmissionStarted(workspaceName: WorkspaceName, submission: Submission, authCookie: HttpCookie)

  def props(containerDAO: GraphContainerDAO,
            executionServiceDAO: ExecutionServiceDAO,
            datasource: DataSource,
            workflowPollInterval: Duration = 1 minutes,
            submissionPollInterval: Duration = 30 minutes): Props = {
    Props(new SubmissionSupervisor(containerDAO, executionServiceDAO, datasource, workflowPollInterval, submissionPollInterval))
  }
}

/**
 * Supervisor actor that should run for the life of the app. SubmissionStarted messages will start a monitor
 * for the given submission. Errors are logged if that monitor fails.
 * 
 * @param containerDAO
 * @param executionServiceDAO
 * @param datasource
 * @param workflowPollInterval
 * @param submissionPollInterval
 */
class SubmissionSupervisor(containerDAO: GraphContainerDAO,
                           executionServiceDAO: ExecutionServiceDAO,
                           datasource: DataSource,
                           workflowPollInterval: Duration,
                           submissionPollInterval: Duration) extends Actor {
  import context._

  override def receive = {
    case SubmissionStarted(workspaceName, submission, authCookie) => startSubmissionMonitor(workspaceName, submission, authCookie)
  }

  private def startSubmissionMonitor(workspaceName: WorkspaceName, submission: Submission, authCookie: HttpCookie): Unit = {
    actorOf(SubmissionMonitor.props(workspaceName, submission, containerDAO, datasource, workflowPollInterval, submissionPollInterval,
      WorkflowMonitor.props(workflowPollInterval, containerDAO, executionServiceDAO, datasource, authCookie)), submission.submissionId)
  }

  override val supervisorStrategy =
    OneForOneStrategy(maxNrOfRetries = 3) {
      case e => {
        system.log.error(e, "error monitoring submission")
        Restart
      }
    }
}
