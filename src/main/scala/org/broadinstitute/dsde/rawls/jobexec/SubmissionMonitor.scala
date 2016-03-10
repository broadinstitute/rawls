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
import org.broadinstitute.dsde.rawls.dataaccess.slick.DataAccess
import org.broadinstitute.dsde.rawls.dataaccess.slick.ReadWriteAction
import scala.concurrent.Future
import akka.pattern._

/**
 * Created by dvoet on 6/26/15.
 */
object SubmissionMonitor {
  def props(workspaceName: WorkspaceName,
            submissionId: String,
            datasource: SlickDataSource,
            workflowPollInterval: Duration,
            submissionPollInterval: Duration,
            workflowMonitorProps: (ActorRef, WorkspaceName, String, Workflow) => Props): Props = {
    Props(new SubmissionMonitor(workspaceName, submissionId, datasource, workflowPollInterval, submissionPollInterval, workflowMonitorProps))
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
                        datasource: SlickDataSource,
                        workflowPollInterval: Duration,
                        submissionPollInterval: Duration,
                        workflowMonitorProps: (ActorRef, WorkspaceName, String, Workflow) => Props) extends Actor {
  import context._
  import datasource.dataAccess.driver.api._

  setReceiveTimeout(submissionPollInterval)
  startWorkflowMonitorActors() pipeTo self

  override def receive = {
    case WorkflowStatusChange(workflow, workflowOutputs) =>
      system.log.debug("workflow state change, submission {}, workflow {} {}", submissionId, workflow.workflowId, workflow.status)
      handleStatusChange(workflow, workflowOutputs) pipeTo self
    case ReceiveTimeout =>
      system.log.debug("submission monitor timeout, submission {}", submissionId)
      checkSubmissionStatus() pipeTo self
    case Terminated(monitor) =>
      system.log.debug("workflow monitor terminated, submission {}, actor {}", submissionId, monitor)
      handleTerminatedMonitor(submissionId, monitor.path.name) pipeTo self

    case Unit => // some future completed successfully, ignore
    case akka.actor.Status.Failure(t) => throw t // an error happened in some future, let the supervisor handle it
  }

  private def handleTerminatedMonitor(submissionId: String, workflowId: String): Future[Unit] = {
    val workflowFuture = datasource.inTransaction { dataAccess =>
      withWorkspaceContext[Workflow](workspaceName, dataAccess) { workspaceContext =>
        dataAccess.workflowQuery.get(workspaceContext, submissionId, workflowId).map(_.getOrElse(
          throw new RawlsException(s"Could not find workflow in workspace ${workspaceName} with id ${workflowId}")
        ))
      }
    }
    
    workflowFuture flatMap {
      case workflow if !workflow.status.isDone => handleStatusChange(workflow.copy(status = WorkflowStatuses.Unknown), None)
      case _ => Future.successful(Unit)
    }
  }

  private def startWorkflowMonitorActors(): Future[Unit] = {
    datasource.inTransaction { dataAccess =>
      withWorkspaceContext(workspaceName, dataAccess) { workspaceContext =>
        dataAccess.submissionQuery.get(workspaceContext, submissionId).map {
          case None => throw new RawlsException(s"submission ${submissionId} does not exist")
          case Some(submission) => submission.workflows.filterNot(workflow => workflow.status.isDone).foreach(startWorkflowMonitorActor(_))
        }
      }
    }
  }

  private def startWorkflowMonitorActor(workflow: Workflow): Unit = {
    watch(actorOf(workflowMonitorProps(self, workspaceName, submissionId, workflow), workflow.workflowId))
  }

  private def handleStatusChange(workflow: Workflow, workflowOutputsOption: Option[Map[String, Attribute]]): Future[Unit] = {
    val savedWorkflowFuture = datasource.inTransaction { dataAccess =>
      withWorkspaceContext(workspaceName, dataAccess) { workspaceContext =>
        val saveOutputs = workflowOutputsOption map { workflowOutputs =>

            //Partition outputs by whether their attributes are entity attributes (begin with "this.") or workspace ones (implicitly; begin with "workspace.")
            //This assumption (that it's either "this." or "workspace.") will be guaranteed by checking of the method config when it's imported; see DSDEEPB-1603.
            //Yes I know this is a var but it's more terse this way.
            var (entityAttributes, workspaceAttributes) = workflowOutputs.partition({ case (k, v) => k.startsWith("this.") })
            entityAttributes = entityAttributes.map({ case (k, v) => (k.stripPrefix("this."), v) })
            workspaceAttributes = workspaceAttributes.map({ case (k, v) => (k.stripPrefix("workspace."), v) })

            val workflowEntity = workflow.workflowEntity.getOrElse(throw new RawlsException("Workflow entity no longer exists, was it deleted?"))

            (dataAccess.entityQuery.get(workspaceContext, workflowEntity.entityType, workflowEntity.entityName) flatMap {
              case None => DBIO.failed(new RawlsException(s"Could not find ${workflowEntity.entityType} ${workflowEntity.entityName}, was it deleted?"))
              case Some(entity) =>
                dataAccess.entityQuery.save(workspaceContext, entity.copy(attributes = entity.attributes ++ entityAttributes)) andThen
                dataAccess.workspaceQuery.save(workspaceContext.workspace.copy(attributes = workspaceContext.workspace.attributes ++ workspaceAttributes))
            }).asTry
        }

        val workflowToSave = saveOutputs map { _.map {
          case Success(_) => workflow
          case Failure(t) => workflow.copy(status = Failed, messages = workflow.messages :+ AttributeString(t.getMessage))
        }}

        workflowToSave match {
          case None => DBIO.successful(workflow)
          case Some(action) => action.flatMap(wf => dataAccess.workflowQuery.update(workspaceContext, submissionId, wf.copy(statusLastChangedDate = DateTime.now))) 
        }
      }
    }

    savedWorkflowFuture.map { savedWorkflow => 
      if (savedWorkflow.status.isDone) {
        checkSubmissionStatus()
      }
    }
  }

  private def checkSubmissionStatus(): Future[Unit] = {
    system.log.debug("polling workflow status, submission {}", submissionId)
    datasource.inTransaction { dataAccess =>
      withWorkspaceContext(workspaceName, dataAccess) { workspaceContext =>
        dataAccess.submissionQuery.get(workspaceContext, submissionId) flatMap {
          case None => DBIO.failed(new RawlsException(s"submissions ${submissionId} does not exist"))
          case Some(refreshedSubmission) if (refreshedSubmission.workflows.forall(workflow => workflow.status.isDone)) =>
            val updateStatusAction = refreshedSubmission.status match {
              case SubmissionStatuses.Submitted =>
                dataAccess.submissionQuery.updateStatus(workspaceContext, refreshedSubmission.submissionId, SubmissionStatuses.Done)
              case SubmissionStatuses.Aborting =>
                dataAccess.submissionQuery.updateStatus(workspaceContext, refreshedSubmission.submissionId, SubmissionStatuses.Aborted)
              case _ => //We shouldn't ever reach this case, but match it for completeness. Then stop monitoring.
                DBIO.successful(0)
            }
            updateStatusAction.map { _ => stop(self) }
          case _ => DBIO.successful(Unit)
        }
      }
    } map { _ => Unit }
  }

  override val supervisorStrategy =
    OneForOneStrategy(maxNrOfRetries = 3, withinTimeRange = workflowPollInterval * 10) {
      case e => {
        system.log.error(e, "error monitoring workflow")
        Restart
      }
    }

  private def withWorkspaceContext[T](workspaceName: WorkspaceName, dataAccess: DataAccess)(op: (SlickWorkspaceContext) => ReadWriteAction[T] ) = {
    dataAccess.workspaceQuery.findByName(workspaceName) flatMap {
      case None => DBIO.failed(new RawlsException(s"workspace ${workspaceName} does not exist"))
      case Some(workspace) => op(SlickWorkspaceContext(workspace))
    }
  }
}
