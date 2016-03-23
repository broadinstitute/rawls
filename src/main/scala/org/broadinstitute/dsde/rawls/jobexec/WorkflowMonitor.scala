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
import org.broadinstitute.dsde.rawls.dataaccess.slick.ReadWriteAction
import scala.concurrent.Future
import org.broadinstitute.dsde.rawls.dataaccess.slick.DataAccess

/**
 * Created by dvoet on 6/29/15.
 */
object WorkflowMonitor {
  def props(pollInterval: Duration,
    executionServiceDAO: ExecutionServiceDAO,
    datasource: SlickDataSource,
    credential: Credential)
    (parent: ActorRef, workspaceName: WorkspaceName, submissionId: String, workflow: Workflow): Props = {
    Props(new WorkflowMonitor(parent, pollInterval, workspaceName, submissionId, workflow, executionServiceDAO, datasource, credential))
  }
}

/**
 * Polls executionServiceDAO every pollInterval to get workflow status and reports changes via a SubmissionMonitor.WorkflowStatusChange
 * message to parent.
 * @param parent actor ref to report changes to
 * @param pollInterval
 * @param workflow
 * @param executionServiceDAO
 * @param datasource
 * @param credential for accessing exec service
 */
class WorkflowMonitor(parent: ActorRef,
                      pollInterval: Duration,
                      workspaceName: WorkspaceName,
                      submissionId: String,
                      workflow: Workflow,
                      executionServiceDAO: ExecutionServiceDAO,
                      datasource: SlickDataSource,
                      credential: Credential) extends Actor {
  import context._
  import datasource.dataAccess.driver.api._

  setReceiveTimeout(pollInterval)

  override def receive = {
    case ReceiveTimeout => pollWorkflowStatus()
    case statusResponse: ExecutionServiceStatus => updateWorkflowStatus(statusResponse) pipeTo self
    case outputsResponse: ExecutionServiceOutputs => attachOutputs(outputsResponse) pipeTo self

    case Unit => // some future completed successfully, ignore
    case Status.Failure(t) => throw t
  }

  def pollWorkflowStatus(): Unit = {
    system.log.debug("polling execution service for workflow {}", workflow.workflowId)
    executionServiceDAO.status(workflow.workflowId, getUserInfo) pipeTo self
  }

  def updateWorkflowStatus(statusResponse: ExecutionServiceStatus): Future[Unit] = datasource.inTransaction { dataAccess =>
    val status = WorkflowStatuses.withName(statusResponse.status)

    withWorkspaceContext(workspaceName, dataAccess) { workspaceContext =>
      dataAccess.workflowQuery.get(workspaceContext, submissionId, workflow.workflowEntity.get.entityType, workflow.workflowEntity.get.entityName) map {
        case None => throw new RawlsException(s"workflow ${workflow} could not be found")
        case Some(refreshedWorkflow) =>
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
    }
  } map {_ => Unit }

  def withMethodConfig(workspaceContext: SlickWorkspaceContext, dataAccess: DataAccess)(op: MethodConfiguration => ReadWriteAction[WorkflowStatusChange]): ReadWriteAction[WorkflowStatusChange] = {
    dataAccess.submissionQuery.get(workspaceContext, submissionId) flatMap {
      case None => DBIO.failed(new RawlsException(s"submissions ${submissionId} does not exist"))
      case Some(submission) =>
        dataAccess.methodConfigurationQuery.get(workspaceContext, submission.methodConfigurationNamespace, submission.methodConfigurationName) flatMap {
          case None => DBIO.successful(WorkflowStatusChange(workflow.copy(status = Failed, messages = workflow.messages :+ AttributeString(s"Could not find method config ${submission.methodConfigurationNamespace}/${submission.methodConfigurationName}, was it deleted?")), None))
          case Some(methodConfig) => op(methodConfig)
        }
    }
  }

  def attachOutputs(outputsResponse: ExecutionServiceOutputs): Future[Unit] = datasource.inTransaction { dataAccess =>
    withWorkspaceContext(workspaceName, dataAccess) { workspaceContext =>
      withMethodConfig(workspaceContext, dataAccess) { methodConfig =>
        val outputs = outputsResponse.outputs
  
        val attributes = methodConfig.outputs.map { case (outputName, attributeName) =>
          Try {
            attributeName.value -> outputs.getOrElse(outputName, {
              throw new RawlsException(s"output named ${outputName} does not exist")
            })
          }
        }
  
        if (attributes.forall(_.isSuccess)) {
          DBIO.successful(WorkflowStatusChange(workflow.copy(status = Succeeded), Option(attributes.map(_.get).toMap)))
  
        } else {
          val errors = attributes.collect { case Failure(t) => AttributeString(t.getMessage) }
          DBIO.successful(WorkflowStatusChange(workflow.copy(messages = errors.toSeq, status = WorkflowStatuses.Failed), None))
        }
      } map { statusMessage =>
        parent ! statusMessage
        stop(self)
      }
    }
  }

  private def withWorkspaceContext[T](workspaceName: WorkspaceName, dataAccess: DataAccess)(op: (SlickWorkspaceContext) => ReadWriteAction[T] ): ReadWriteAction[T] = {
    dataAccess.workspaceQuery.findByName(workspaceName) flatMap {
      case None => DBIO.failed(new RawlsException(s"workspace ${workspaceName} does not exist"))
      case Some(workspace) => op(SlickWorkspaceContext(workspace))
    }
  }

  private def getUserInfo = {
    val expiresInSeconds: Long = Option(credential.getExpiresInSeconds).map(_.toLong).getOrElse(0)
    if (expiresInSeconds <= 5*60) {
      credential.refreshToken()
    }
    UserInfo("", OAuth2BearerToken(credential.getAccessToken), Option(credential.getExpiresInSeconds).map(_.toLong).getOrElse(0), "")
  }
}
