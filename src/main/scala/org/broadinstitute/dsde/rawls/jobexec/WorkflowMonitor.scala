package org.broadinstitute.dsde.rawls.jobexec

import akka.actor._
import com.google.api.client.auth.oauth2.Credential
import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.jobexec.SubmissionMonitor.WorkflowStatusChange
import org.broadinstitute.dsde.rawls.model.WorkflowStatuses.{Failed, Succeeded}
import org.broadinstitute.dsde.rawls.model._
import spray.http.OAuth2BearerToken

import scala.concurrent.duration.Duration
import scala.util.{Failure, Try}

import akka.pattern.pipe

/**
 * Created by dvoet on 6/29/15.
 */
object WorkflowMonitor {
  def props(pollInterval: Duration,
    containerDAO: DbContainerDAO,
    executionServiceDAO: ExecutionServiceDAO,
    datasource: DataSource,
    credential: Credential)
    (parent: ActorRef, workspaceName: WorkspaceName, submissionId: String, workflow: Workflow): Props = {
    Props(new WorkflowMonitor(parent, pollInterval, workspaceName, submissionId, workflow, containerDAO, executionServiceDAO, datasource, credential))
  }
}

/**
 * Polls executionServiceDAO every pollInterval to get workflow status and reports changes via a SubmissionMonitor.WorkflowStatusChange
 * message to parent.
 * @param parent actor ref to report changes to
 * @param pollInterval
 * @param workflow
 * @param containerDAO
 * @param executionServiceDAO
 * @param datasource
 * @param credential for accessing exec service
 */
class WorkflowMonitor(parent: ActorRef,
                      pollInterval: Duration,
                      workspaceName: WorkspaceName,
                      submissionId: String,
                      workflow: Workflow,
                      containerDAO: DbContainerDAO,
                      executionServiceDAO: ExecutionServiceDAO,
                      datasource: DataSource,
                      credential: Credential) extends Actor {
  import context._

  setReceiveTimeout(pollInterval)

  override def receive = {
    case ReceiveTimeout => pollWorkflowStatus()
    case statusResponse: ExecutionServiceStatus => updateWorkflowStatus(statusResponse)
    case outputsResponse: ExecutionServiceOutputs => attachOutputs(outputsResponse)
    case Status.Failure(t) => throw t
  }

  def pollWorkflowStatus(): Unit = {
    system.log.debug("polling execution service for workflow {}", workflow.workflowId)
    executionServiceDAO.status(workflow.workflowId, getUserInfo) pipeTo self
  }

  def updateWorkflowStatus(statusResponse: ExecutionServiceStatus) = datasource.inTransaction(readLocks=Set(workspaceName)) { txn =>
    val status = WorkflowStatuses.withName(statusResponse.status)

    val refreshedWorkflow = containerDAO.workflowDAO.get(getWorkspaceContext(workspaceName, txn),   submissionId, workflow.workflowId, txn).getOrElse(
      throw new RawlsException(s"workflow ${workflow} could not be found")
    )

    if (refreshedWorkflow.status != status) {
      status match {
        case Succeeded =>
          executionServiceDAO.outputs(workflow.workflowId, getUserInfo) pipeTo self
          // stop(self) will get called in attachOutputs
        case Failed =>
          parent ! WorkflowStatusChange(refreshedWorkflow.copy(status = status, messages = refreshedWorkflow.messages :+ AttributeString("Workflow execution failed, check outputs for details")), None)
          stop(self)
        case _ =>
          parent ! WorkflowStatusChange(refreshedWorkflow.copy(status = status), None)
          if (status.isDone) {
            stop(self)
          }
      }
    }
  }

  def withMethodConfig(workspaceContext: WorkspaceContext, txn: RawlsTransaction)(op: MethodConfiguration => WorkflowStatusChange): WorkflowStatusChange = {
    val submission = containerDAO.submissionDAO.get(getWorkspaceContext(workspaceName, txn), submissionId, txn).getOrElse(
      throw new RawlsException(s"submissions ${submissionId} does not exist")
    )
    containerDAO.methodConfigurationDAO.get(workspaceContext, submission.methodConfigurationNamespace, submission.methodConfigurationName, txn) match {
      case None => WorkflowStatusChange(workflow.copy(status = Failed, messages = workflow.messages :+ AttributeString(s"Could not find method config ${submission.methodConfigurationNamespace}/${submission.methodConfigurationName}, was it deleted?")), None)
      case Some(methodConfig) => op(methodConfig)
    }
  }

  def attachOutputs(outputsResponse: ExecutionServiceOutputs): Unit = datasource.inTransaction(readLocks = Set(workspaceName)) { txn =>
    val workspaceContext = getWorkspaceContext(workspaceName, txn)
    val statusMessage = withMethodConfig(workspaceContext, txn) { methodConfig =>
      val outputs = outputsResponse.outputs

      val attributes = methodConfig.outputs.map { case (outputName, attributeName) =>
        Try {
          attributeName.value -> outputs.getOrElse(outputName, {
            throw new RawlsException(s"output named ${outputName} does not exist")
          })
        }
      }

      if (attributes.forall(_.isSuccess)) {
        WorkflowStatusChange(workflow.copy(status = Succeeded), Option(attributes.map(_.get).toMap))

      } else {
        val errors = attributes.collect { case Failure(t) => AttributeString(t.getMessage) }
        WorkflowStatusChange(workflow.copy(messages = errors.toSeq, status = WorkflowStatuses.Failed), None)
      }
    }
    parent ! statusMessage
    stop(self)
  }

  private def getWorkspaceContext(workspaceName: WorkspaceName, txn: RawlsTransaction): WorkspaceContext = {
    containerDAO.workspaceDAO.loadContext(workspaceName, txn).getOrElse(throw new RawlsException(s"workspace ${workspaceName} does not exist"))
  }

  private def getUserInfo = {
    val expiresInSeconds: Long = Option(credential.getExpiresInSeconds).map(_.toLong).getOrElse(0)
    if (expiresInSeconds <= 5*60) {
      credential.refreshToken()
    }
    UserInfo("", OAuth2BearerToken(credential.getAccessToken), Option(credential.getExpiresInSeconds).map(_.toLong).getOrElse(0), "")
  }
}
