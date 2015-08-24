package org.broadinstitute.dsde.rawls.jobexec

import akka.actor.SupervisorStrategy.Restart
import akka.actor.{Props, OneForOneStrategy, Actor}
import org.broadinstitute.dsde.rawls.dataaccess._
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

  def props(containerDAO: AbstractContainerDAO,
            datasource: DataSource,
            workflowPollInterval: Duration = 1 minutes,
            submissionPollInterval: Duration = 30 minutes): Props = {
    Props(new SubmissionSupervisor(containerDAO, datasource, workflowPollInterval, submissionPollInterval))
  }
}

/**
 * Supervisor actor that should run for the life of the app. SubmissionStarted messages will start a monitor
 * for the given submission. Errors are logged if that monitor fails.
 * 
 * @param containerDAO
 * @param datasource
 * @param workflowPollInterval
 * @param submissionPollInterval
 */
class SubmissionSupervisor(containerDAO: AbstractContainerDAO,
                           datasource: DataSource,
                           workflowPollInterval: Duration,
                           submissionPollInterval: Duration) extends Actor {
  import context._

  override def receive = {
    case SubmissionStarted(submission, authCookie) => startSubmissionMonitor(submission, authCookie)
  }

  private def startSubmissionMonitor(submission: Submission, authCookie: HttpCookie): Unit = {
    actorOf(SubmissionMonitor.props(submission, containerDAO.workspaceDAO, containerDAO.submissionDAO, containerDAO.workflowDAO, containerDAO.entityDAO, datasource, workflowPollInterval, submissionPollInterval,
      WorkflowMonitor.props(workflowPollInterval, containerDAO.workspaceDAO, containerDAO.executionServiceDAO, containerDAO.workflowDAO, containerDAO.methodConfigDAO, containerDAO.entityDAO, datasource, authCookie)), submission.submissionId)
  }

  override val supervisorStrategy =
    OneForOneStrategy(maxNrOfRetries = 3) {
      case e => {
        system.log.error(e, "error monitoring submission")
        Restart
      }
    }
}
