package org.broadinstitute.dsde.rawls.jobexec

import akka.actor.SupervisorStrategy.Restart
import akka.actor._
import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.jobexec.SubmissionMonitor.WorkflowStatusChange
import org.broadinstitute.dsde.rawls.model.WorkflowStatuses.Failed
import org.broadinstitute.dsde.rawls.model._

import scala.concurrent.duration.Duration
import scala.util.{Success, Try, Failure}

/**
 * Created by dvoet on 6/26/15.
 */
object SubmissionMonitor {
  def props(submission: Submission,
            submissionDAO: SubmissionDAO,
            workflowDAO: WorkflowDAO,
            entityDAO: EntityDAO,
            datasource: DataSource,
            workflowPollInterval: Duration,
            submissionPollInterval: Duration,
            workflowMonitorProps: (ActorRef, Submission, Workflow) => Props): Props = {
    Props(new SubmissionMonitor(submission, submissionDAO, workflowDAO, entityDAO, datasource, workflowPollInterval, submissionPollInterval, workflowMonitorProps))
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
 * @param submission to monitor
 * @param submissionDAO
 * @param workflowDAO
 * @param entityDAO
 * @param datasource
 * @param workflowPollInterval time between polls of execution service for individual workflow status
 * @param submissionPollInterval time between polls of db for all workflow statuses within submission while workflows
 *                               are not finishing, mainly a safety mechanism
 * @param workflowMonitorProps constructor function used to create workflow monitor actors. The function takes
 *                             an actor ref to report back to and the workflow to monitor
 */
class SubmissionMonitor(submission: Submission,
                        submissionDAO: SubmissionDAO,
                        workflowDAO: WorkflowDAO,
                        entityDAO: EntityDAO,
                        datasource: DataSource,
                        workflowPollInterval: Duration,
                        submissionPollInterval: Duration,
                        workflowMonitorProps: (ActorRef, Submission, Workflow) => Props) extends Actor {
  import context._

  setReceiveTimeout(submissionPollInterval)
  startWorkflowMonitorActors()

  override def receive = {
    case WorkflowStatusChange(workflow, workflowOutputs) =>
      system.log.debug("workflow state change, submission {}, workflow {} {}", submission.submissionId, workflow.workflowId, workflow.status)
      handleStatusChange(workflow, workflowOutputs)
    case ReceiveTimeout =>
      system.log.debug("submission monitor timeout, submission {}", submission.submissionId)
      checkSubmissionStatus()
    case Terminated(monitor) =>
      system.log.debug("workflow monitor terminated, submission {}, actor ", submission.submissionId, monitor)
      handleTerminatedMonitor(monitor.path.name)
  }

  private def handleTerminatedMonitor(workflowId: String): Unit = {
    val workflow = datasource inTransaction { txn =>
      workflowDAO.get(submission.workspaceName, workflowId, txn).getOrElse(
        throw new RawlsException(s"Could not find workflow in workspace ${submission.workspaceName} with id ${workflowId}")
      )
    }

    if (!workflow.status.isDone) {
      // the workflow is not done but the monitor has terminated
      handleStatusChange(workflow.copy(status = WorkflowStatuses.Unknown), None)
    }
  }

  private def startWorkflowMonitorActors(): Unit = {
    submission.workflows.filterNot(workflow => workflow.status.isDone).foreach(startWorkflowMonitorActor(_))
  }

  private def startWorkflowMonitorActor(workflow: Workflow): Unit = {
    watch(actorOf(workflowMonitorProps(self, submission, workflow), workflow.workflowId))
  }

  private def handleStatusChange(workflow: Workflow, workflowOutputsOption: Option[Map[String, Attribute]]): Unit = {
    val savedWorkflow = datasource inTransaction { txn =>
      val saveOutputs = Try {
        workflowOutputsOption foreach { workflowOutputs =>
          val entity = entityDAO.get(submission.workspaceName.namespace, submission.workspaceName.name, workflow.workflowEntity.entityType, workflow.workflowEntity.entityName, txn).getOrElse {
            throw new RawlsException(s"Could not find ${workflow.workflowEntity.entityType} ${workflow.workflowEntity.entityName}, was it deleted?")
          }
          entityDAO.save(submission.workspaceName.namespace, submission.workspaceName.name, entity.copy(attributes = entity.attributes ++ workflowOutputs), txn)
        }
      }

      val workflowToSave = saveOutputs match {
        case Success(_) => workflow
        case Failure(t) => workflow.copy(status = Failed, messages = workflow.messages :+ AttributeString(t.getMessage))
      }

      workflowDAO.update(workflowToSave.workspaceName, workflowToSave, txn)
    }

    if (savedWorkflow.status.isDone) {
      checkSubmissionStatus()
    }
  }

  private def checkSubmissionStatus(): Unit = {
    system.log.debug("polling workflow status, submission {}", submission.submissionId)
    datasource inTransaction { txn =>
      val refreshedSubmission = submissionDAO.get(submission.workspaceName.namespace, submission.workspaceName.name, submission.submissionId, txn).getOrElse(
        throw new RawlsException(s"submissions ${submission} does not exist")
      )

      if (refreshedSubmission.workflows.forall(workflow => workflow.status.isDone)) {
        submissionDAO.update(refreshedSubmission.copy(status = SubmissionStatuses.Done), txn)
        stop(self)
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

}
