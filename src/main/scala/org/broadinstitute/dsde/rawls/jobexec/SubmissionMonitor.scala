package org.broadinstitute.dsde.rawls.jobexec

import akka.actor.SupervisorStrategy.Restart
import akka.actor._
import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.jobexec.SubmissionMonitor.WorkflowStatusChange
import org.broadinstitute.dsde.rawls.model.WorkflowStatuses.Failed
import org.broadinstitute.dsde.rawls.model._
import org.joda.time.DateTime
import spray.http

import scala.concurrent.duration.Duration
import scala.util.{Success, Try, Failure}

/**
 * Created by dvoet on 6/26/15.
 */
object SubmissionMonitor {
  def props(workspaceName: WorkspaceName,
            submissionId: String,
            containerDAO: GraphContainerDAO,
            datasource: DataSource,
            workflowPollInterval: Duration,
            submissionPollInterval: Duration,
            workflowMonitorProps: (ActorRef, WorkspaceName, String, Workflow) => Props): Props = {
    Props(new SubmissionMonitor(workspaceName, submissionId, containerDAO, datasource, workflowPollInterval, submissionPollInterval, workflowMonitorProps))
  }

  sealed trait SubmissionMonitorMessage
  case class WorkflowStatusChange(workflow: Workflow, workflowOutputs: Option[Map[String, Attribute]]) extends SubmissionMonitorMessage
}

/**
 * An actor that monitors the status of a submission. Each workflow is monitored by a separate actor instantiated
 * using workflowMonitorProps. Each of those actors will report back via WorkflowStatusChange messages. The submission
 * actor will watch the workflow actor for termination and mark associated workflows as unknown if they are not yet done.
 * When there is no workflow activity this actor will wake up every submissionPollInterval to double check (although
 * this seems unnecessary, it make me feel better).
 *
 * @param submissionId id of submission to monitor
 * @param containerDAO
 * @param datasource
 * @param workflowPollInterval time between polls of execution service for individual workflow status
 * @param submissionPollInterval time between polls of db for all workflow statuses within submission while workflows
 *                               are not finishing, mainly a safety mechanism
 * @param workflowMonitorProps constructor function used to create workflow monitor actors. The function takes
 *                             an actor ref to report back to and the workflow to monitor
 */
class SubmissionMonitor(workspaceName: WorkspaceName,
                        submissionId: String,
                        containerDAO: GraphContainerDAO,
                        datasource: DataSource,
                        workflowPollInterval: Duration,
                        submissionPollInterval: Duration,
                        workflowMonitorProps: (ActorRef, WorkspaceName, String, Workflow) => Props) extends Actor {
  import context._

  setReceiveTimeout(submissionPollInterval)
  startWorkflowMonitorActors()

  override def receive = {
    case WorkflowStatusChange(workflow, workflowOutputs) =>
      system.log.debug("workflow state change, submission {}, workflow {} {}", submissionId, workflow.workflowId, workflow.status)
      handleStatusChange(workflow, workflowOutputs)
    case ReceiveTimeout =>
      system.log.debug("submission monitor timeout, submission {}", submissionId)
      checkSubmissionStatus()
    case Terminated(monitor) =>
      system.log.debug("workflow monitor terminated, submission {}, actor {}", submissionId, monitor)
      handleTerminatedMonitor(submissionId, monitor.path.name)
  }

  private def handleTerminatedMonitor(submissionId: String, workflowId: String): Unit = {
    val workflow = datasource.inTransaction(readLocks=Set(workspaceName)) { txn =>
      withWorkspaceContext(workspaceName, txn) { workspaceContext =>
        containerDAO.workflowDAO.get(workspaceContext, submissionId, workflowId, txn).getOrElse(
          throw new RawlsException(s"Could not find workflow in workspace ${workspaceName} with id ${workflowId}")
        )
      }
    }

    if (!workflow.status.isDone) {
      // the workflow is not done but the monitor has terminated
      handleStatusChange(workflow.copy(status = WorkflowStatuses.Unknown), None)
    }
  }

  private def startWorkflowMonitorActors(): Unit = {
    datasource.inTransaction(readLocks=Set(workspaceName)) { txn =>
      withWorkspaceContext(workspaceName, txn) { workspaceContext =>
        val submission = containerDAO.submissionDAO.get(workspaceContext, submissionId, txn).getOrElse(
          throw new RawlsException(s"submission ${submissionId} does not exist")
        )
        submission.workflows.filterNot(workflow => workflow.status.isDone).foreach(startWorkflowMonitorActor(_))
      }
    }
  }

  private def startWorkflowMonitorActor(workflow: Workflow): Unit = {
    watch(actorOf(workflowMonitorProps(self, workspaceName, submissionId, workflow), workflow.workflowId))
  }

  private def handleStatusChange(workflow: Workflow, workflowOutputsOption: Option[Map[String, Attribute]]): Unit = {
    val savedWorkflow = datasource.inTransaction(writeLocks=Set(workspaceName)) { txn =>
      withWorkspaceContext(workspaceName, txn) { workspaceContext =>
        val saveOutputs = Try {
          workflowOutputsOption foreach { workflowOutputs =>

            //Partition outputs by whether their attributes are entity attributes (begin with "this.") or workspace ones (implicitly; begin with "workspace.")
            //This assumption (that it's either "this." or "workspace.") will be guaranteed by checking of the method config when it's imported; see DSDEEPB-1603.
            //Yes I know this is a var but it's more terse this way.
            var (entityAttributes, workspaceAttributes) = workflowOutputs.partition({ case (k, v) => k.startsWith("this.") })
            entityAttributes = entityAttributes.map({ case (k, v) => (k.stripPrefix("this."), v) })
            workspaceAttributes = workspaceAttributes.map({ case (k, v) => (k.stripPrefix("workspace."), v) })

            val workflowEntity = workflow.workflowEntity.getOrElse(throw new RawlsException("Workflow entity no longer exists, was it deleted?"))
            val entity = containerDAO.entityDAO.get(workspaceContext, workflowEntity.entityType, workflowEntity.entityName, txn).getOrElse {
              throw new RawlsException(s"Could not find ${workflowEntity.entityType} ${workflowEntity.entityName}, was it deleted?")
            }

            //Update entity and workspace with expression values.
            containerDAO.entityDAO.save(workspaceContext, entity.copy(attributes = entity.attributes ++ entityAttributes), txn)
            containerDAO.workspaceDAO.save(workspaceContext.workspace.copy(attributes = workspaceContext.workspace.attributes ++ workspaceAttributes), txn)
          }
        }

        val workflowToSave = saveOutputs match {
          case Success(_) => workflow
          case Failure(t) => workflow.copy(status = Failed, messages = workflow.messages :+ AttributeString(t.getMessage))
        }

        containerDAO.workflowDAO.update(workspaceContext, submissionId, workflowToSave.copy(statusLastChangedDate = DateTime.now), txn)
      }
    }

    if (savedWorkflow.status.isDone) {
      checkSubmissionStatus()
    }
  }

  private def checkSubmissionStatus(): Unit = {
    system.log.debug("polling workflow status, submission {}", submissionId)
    datasource.inTransaction(readLocks=Set(workspaceName)) { txn =>
      withWorkspaceContext(workspaceName, txn) { workspaceContext =>
        val refreshedSubmission = containerDAO.submissionDAO.get(workspaceContext, submissionId, txn).getOrElse(
          throw new RawlsException(s"submissions ${submissionId} does not exist")
        )

        if (refreshedSubmission.workflows.forall(workflow => workflow.status.isDone)) {
          val originalStatus = containerDAO.submissionDAO.get(workspaceContext, refreshedSubmission.submissionId, txn).get.status
          originalStatus match {
            case SubmissionStatuses.Submitted =>
              containerDAO.submissionDAO.update(workspaceContext, refreshedSubmission.copy(status = SubmissionStatuses.Done), txn)
            case SubmissionStatuses.Aborting =>
              containerDAO.submissionDAO.update(workspaceContext, refreshedSubmission.copy(status = SubmissionStatuses.Aborted), txn)
            case _ => //We shouldn't ever reach this case, but match it for completeness. Then stop monitoring.
          }
          stop(self)
        }
      }
    }
  }

  override val supervisorStrategy =
    OneForOneStrategy(maxNrOfRetries = 3, withinTimeRange = workflowPollInterval * 10) {
      case e => {
        system.log.error(e, "error monitoring workflow")
        Restart
      }
    }

  private def withWorkspaceContext[T](workspaceName: WorkspaceName, txn: RawlsTransaction)(op: (WorkspaceContext) => T ) = {
    assert( txn.readLocks.contains(workspaceName) || txn.writeLocks.contains(workspaceName),
      s"Attempting to use context on workspace $workspaceName but it's not read or write locked! Add it to inTransaction or inFutureTransaction")
    containerDAO.workspaceDAO.loadContext(workspaceName, txn) match {
      case None => throw new RawlsException(s"workspace ${workspaceName} does not exist")
      case Some(workspaceContext) => op(workspaceContext)
    }
  }
}
