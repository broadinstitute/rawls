package org.broadinstitute.dsde.rawls.jobexec

import akka.actor.{ActorRef, Props, ReceiveTimeout, Actor}
import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.jobexec.SubmissionMonitor.WorkflowStatusChange
import org.broadinstitute.dsde.rawls.model.WorkflowStatuses.{Failed, Succeeded}
import org.broadinstitute.dsde.rawls.model._
import spray.http.HttpCookie

import scala.concurrent.duration.Duration
import scala.util.{Success, Failure, Try}

/**
 * Created by dvoet on 6/29/15.
 */
object WorkflowMonitor {
  def props(pollInterval: Duration,
    workspaceDAO: WorkspaceDAO,
    executionServiceDAO: ExecutionServiceDAO,
    workflowDAO: WorkflowDAO,
    methodConfigurationDAO: MethodConfigurationDAO,
    entityDAO: EntityDAO,
    datasource: DataSource,
    authCookie: HttpCookie)
    (parent: ActorRef, submission: Submission, workflow: Workflow): Props = {
    Props(new WorkflowMonitor(parent, pollInterval, submission, workflow, workspaceDAO, executionServiceDAO, workflowDAO, methodConfigurationDAO, entityDAO, datasource, authCookie))
  }
}

/**
 * Polls executionServiceDAO every pollInterval to get workflow status and reports changes via a SubmissionMonitor.WorkflowStatusChange
 * message to parent.
 * @param parent actor ref to report changes to
 * @param pollInterval
 * @param workflow
 * @param workspaceDAO
 * @param executionServiceDAO
 * @param workflowDAO
 * @param datasource
 * @param authCookie auth cookie required by executionServiceDAO
 */
class WorkflowMonitor(parent: ActorRef,
                      pollInterval: Duration,
                      submission: Submission,
                      workflow: Workflow,
                      workspaceDAO: WorkspaceDAO,
                      executionServiceDAO: ExecutionServiceDAO,
                      workflowDAO: WorkflowDAO,
                      methodConfigurationDAO: MethodConfigurationDAO,
                      entityDAO: EntityDAO,
                      datasource: DataSource,
                      authCookie: HttpCookie) extends Actor {
  import context._

  setReceiveTimeout(pollInterval)

  override def receive = {
    case ReceiveTimeout => checkWorkflowStatus()
  }

  def checkWorkflowStatus(): Unit = datasource inTransaction { txn =>
    system.log.debug("polling execution service for workflow {}", workflow.workflowId)
    val refreshedWorkflow = workflowDAO.get(getWorkspaceContext(workflow.workspaceName, txn), workflow.workflowId, txn).getOrElse(
      throw new RawlsException(s"workflow ${workflow} could not be found")
    )
    val statusResponse = executionServiceDAO.status(workflow.workflowId, authCookie)
    val status = WorkflowStatuses.withName(statusResponse.status)

    if (refreshedWorkflow.status != status) {
      val updates = status match {
        case Succeeded => onWorkflowSuccess(workflow, status, txn)
        case Failed => WorkflowStatusChange(refreshedWorkflow.copy(status = status, messages = refreshedWorkflow.messages :+ AttributeString("Workflow execution failed, check outputs for details")), None)
        case _ => WorkflowStatusChange(refreshedWorkflow.copy(status = status), None)
      }

      parent ! updates
    }

    if (status.isDone) {
      stop(self)
    }
  }

  def withMethodConfig(workspaceContext: WorkspaceContext, txn: RawlsTransaction)(op: MethodConfiguration => WorkflowStatusChange): WorkflowStatusChange = {
    methodConfigurationDAO.get(workspaceContext, submission.methodConfigurationNamespace, submission.methodConfigurationName, txn) match {
      case None => WorkflowStatusChange(workflow.copy(status = Failed, messages = workflow.messages :+ AttributeString(s"Could not find method config ${submission.methodConfigurationNamespace}/${submission.methodConfigurationName}, was it deleted?")), None)
      case Some(methodConfig) => op(methodConfig)
    }
  }

  def onWorkflowSuccess(workflow: Workflow, completionStatus: WorkflowStatuses.WorkflowStatus, txn: RawlsTransaction): WorkflowStatusChange = {
    val workspaceContext = getWorkspaceContext(workflow.workspaceName, txn)
    withMethodConfig(workspaceContext, txn) { methodConfig =>
      val outputs = executionServiceDAO.outputs(workflow.workflowId, authCookie).outputs

      val attributes = methodConfig.outputs.map { case (outputName, attributeName) =>
        Try {
          attributeName.value -> outputs.getOrElse(outputName, {
            throw new RawlsException(s"output named ${outputName} does not exist")
          })
        }
      }

      if (attributes.forall(_.isSuccess)) {
        WorkflowStatusChange(workflow.copy(status = completionStatus), Option(attributes.map(_.get).toMap))

      } else {
        val errors = attributes.collect { case Failure(t) => AttributeString(t.getMessage) }
        WorkflowStatusChange(workflow.copy(messages = errors.toSeq, status = WorkflowStatuses.Failed), None)
      }
    }
  }

  private def getWorkspaceContext(workspaceName: WorkspaceName, txn: RawlsTransaction): WorkspaceContext = {
    workspaceDAO.loadContext(workspaceName, txn).getOrElse(throw new RawlsException(s"workspace ${workspaceName} does not exist"))
  }
}
