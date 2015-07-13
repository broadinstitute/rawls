package org.broadinstitute.dsde.rawls.jobexec

import akka.actor.SupervisorStrategy.Restart
import akka.actor.{Props, OneForOneStrategy, Actor}
import org.broadinstitute.dsde.rawls.dataaccess.{DataSource, WorkflowDAO, ExecutionServiceDAO, SubmissionDAO}
import org.broadinstitute.dsde.rawls.jobexec.SubmissionSupervisor.SubmissionStarted
import org.broadinstitute.dsde.rawls.model.Submission
import spray.http.HttpCookie

import scala.concurrent.duration._

/**
 * Created by dvoet on 6/26/15.
 */
object SubmissionSupervisor {
  sealed trait SubmissionSupervisorMessage

  case class SubmissionStarted(submission: Submission, authCookie: HttpCookie)

  def props(submissionDAO: SubmissionDAO,
            executionServiceDAO: ExecutionServiceDAO,
            workflowDAO: WorkflowDAO,
            datasource: DataSource,
            workflowPollInterval: Duration = 1 minutes,
            submissionPollInterval: Duration = 30 minutes): Props = {

    Props(new SubmissionSupervisor(submissionDAO, executionServiceDAO, workflowDAO, datasource, workflowPollInterval, submissionPollInterval))
  }
}

/**
 * Supervisor actor that should run for the life of the app. SubmissionStarted messages will start a monitor
 * for the given submission. Errors are logged if that monitor fails.
 * 
 * @param submissionDAO
 * @param executionServiceDAO
 * @param workflowDAO
 * @param datasource
 * @param workflowPollInterval
 * @param submissionPollInterval
 */
class SubmissionSupervisor(submissionDAO: SubmissionDAO,
                           executionServiceDAO: ExecutionServiceDAO,
                           workflowDAO: WorkflowDAO,
                           datasource: DataSource,
                           workflowPollInterval: Duration,
                           submissionPollInterval: Duration) extends Actor {
  import context._

  override def receive = {
    case SubmissionStarted(submission, authCookie) => startSubmissionMonitor(submission, authCookie)
  }

  private def startSubmissionMonitor(submission: Submission, authCookie: HttpCookie): Unit = {
    actorOf(SubmissionMonitor.props(submission, submissionDAO, workflowDAO, datasource, workflowPollInterval, submissionPollInterval,
      WorkflowMonitor.props(workflowPollInterval, executionServiceDAO, workflowDAO, datasource, authCookie)), submission.id)
  }

  override val supervisorStrategy =
    OneForOneStrategy(maxNrOfRetries = 3) {
      case e => {
        system.log.error(e, "error monitoring submission")
        Restart
      }
    }
}
