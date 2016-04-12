package org.broadinstitute.dsde.rawls.jobexec

import java.util.UUID

import akka.actor.SupervisorStrategy.Restart
import akka.actor.{Props, OneForOneStrategy, Actor}
import com.google.api.client.auth.oauth2.Credential
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.jobexec.SubmissionSupervisor.SubmissionStarted
import org.broadinstitute.dsde.rawls.model.WorkspaceName

import scala.concurrent.duration._

/**
 * Created by dvoet on 6/26/15.
 */
object SubmissionSupervisor {
  sealed trait SubmissionSupervisorMessage

  case class SubmissionStarted(workspaceName: WorkspaceName, submissionId: UUID, credential: Credential)

  def props(executionServiceDAO: ExecutionServiceDAO,
            datasource: SlickDataSource,
            submissionPollInterval: FiniteDuration = 1 minutes): Props = {
    Props(new SubmissionSupervisor(executionServiceDAO, datasource, submissionPollInterval))
  }
}

/**
 * Supervisor actor that should run for the life of the app. SubmissionStarted messages will start a monitor
 * for the given submission. Errors are logged if that monitor fails.
 * 
 * @param executionServiceDAO
 * @param datasource
 * @param submissionPollInterval
 */
class SubmissionSupervisor(executionServiceDAO: ExecutionServiceDAO,
                           datasource: SlickDataSource,
                           submissionPollInterval: FiniteDuration) extends Actor {
  import context._

  override def receive = {
    case SubmissionStarted(workspaceContext, submissionId, credential) => startSubmissionMonitor(workspaceContext, submissionId, credential)
  }

  private def startSubmissionMonitor(workspaceName: WorkspaceName, submissionId: UUID, credential: Credential): Unit = {
    actorOf(SubmissionMonitorActor.props(workspaceName, submissionId, datasource, executionServiceDAO, credential, submissionPollInterval), submissionId.toString)
  }

  override val supervisorStrategy =
    OneForOneStrategy(maxNrOfRetries = 3) {
      case e => {
        system.log.error(e, "error monitoring submission")
        Restart
      }
    }
}
