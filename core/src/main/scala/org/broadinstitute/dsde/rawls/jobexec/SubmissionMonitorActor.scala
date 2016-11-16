package org.broadinstitute.dsde.rawls.jobexec

import akka.actor._
import com.google.api.client.auth.oauth2.Credential
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.jobexec.SubmissionMonitorActor._
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.util.FutureSupport

import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.util.{Failure, Success, Try}
import org.broadinstitute.dsde.rawls.dataaccess.slick.{DataAccess, ReadAction, ReadWriteAction, WorkflowRecord}

import scala.concurrent.{ExecutionContext, Future}
import akka.pattern._
import java.util.UUID

/**
 * Created by dvoet on 6/26/15.
 */
object SubmissionMonitorActor {
  def props(workspaceName: WorkspaceName,
            submissionId: UUID,
            datasource: SlickDataSource,
            executionServiceCluster: ExecutionServiceCluster,
            credential: Credential,
            submissionPollInterval: FiniteDuration): Props = {
    Props(new SubmissionMonitorActor(workspaceName, submissionId, datasource, executionServiceCluster, credential, submissionPollInterval))
  }

  sealed trait SubmissionMonitorMessage
  case object StartMonitorPass extends SubmissionMonitorMessage

  /**
   * The response from querying the exec services.
    *
    * @param statusResponse If a successful response shows an unchanged status there
   * will be a Success(None) entry in the statusResponse Seq. If the status has changed it will be
   * Some(workflowRecord, outputsOption) where workflowRecord will have the updated status. When the workflow
   * has Succeeded and there are outputs, outputsOption will contain the response from the exec service.
   */
  case class ExecutionServiceStatusResponse(statusResponse: Seq[Try[Option[(WorkflowRecord, Option[ExecutionServiceOutputs])]]]) extends SubmissionMonitorMessage
  case class StatusCheckComplete(terminateActor: Boolean) extends SubmissionMonitorMessage
}

/**
 * An actor that monitors the status of a submission. Wakes up every submissionPollInterval and queries
 * the execution service for status of workflows that we don't think are done yet. For any workflows
 * that are successful, query again for outputs. Once all workflows are done mark the submission as done
 * and terminate the actor.
 *
 * @param submissionId id of submission to monitor
 * @param datasource
 * @param submissionPollInterval time between polls of db for all workflow statuses within submission
 */
class SubmissionMonitorActor(val workspaceName: WorkspaceName,
                             val submissionId: UUID,
                             val datasource: SlickDataSource,
                             val executionServiceCluster: ExecutionServiceCluster,
                             val credential: Credential,
                             val submissionPollInterval: FiniteDuration) extends Actor with SubmissionMonitor with LazyLogging {

  import context._

  scheduleNextMonitorPass

  override def receive = {
    case StartMonitorPass =>
      logger.debug(s"polling workflows for submission $submissionId")
      queryExecutionServiceForStatus() pipeTo self
    case response: ExecutionServiceStatusResponse =>
      logger.debug(s"handling execution service response for submission $submissionId")
      handleStatusResponses(response) pipeTo self
    case StatusCheckComplete(terminateActor) =>
      logger.debug(s"done checking status for submission $submissionId, terminateActor = $terminateActor")
      if (terminateActor) stop(self)
      else scheduleNextMonitorPass

    case Status.Failure(t) => throw t // an error happened in some future, let the supervisor handle it
  }

  def scheduleNextMonitorPass: Cancellable = {
    system.scheduler.scheduleOnce(submissionPollInterval, self, StartMonitorPass)
  }

}

trait SubmissionMonitor extends FutureSupport with LazyLogging {
  val workspaceName: WorkspaceName
  val submissionId: UUID
  val datasource: SlickDataSource
  val executionServiceCluster: ExecutionServiceCluster
  val credential: Credential
  val submissionPollInterval: Duration

  import datasource.dataAccess.driver.api._

  /**
   * This function starts a monitoring pass
   *
   * @param executionContext
   * @return
   */
  def queryExecutionServiceForStatus()(implicit executionContext: ExecutionContext): Future[ExecutionServiceStatusResponse] = {
    val activeStatuses = Seq(WorkflowStatuses.Running, WorkflowStatuses.Submitted)

    val submissionFuture = datasource.inTransaction { dataAccess =>
      dataAccess.submissionQuery.loadSubmission(submissionId)
    }

    def abortQueuedWorkflows(submissionId: UUID) = {
      datasource.inTransaction { dataAccess =>
        dataAccess.workflowQuery.batchUpdateWorkflowsOfStatus(submissionId, WorkflowStatuses.Queued, WorkflowStatuses.Aborted)
      }
    }

    def abortActiveWorkflows(submissionId: UUID, workflows: Seq[Workflow]) = {
      datasource.inTransaction { dataAccess =>
        // look up abortable WorkflowRecs for this submission
        val wrquery = dataAccess.workflowQuery.findWorkflowsForAbort(submissionId)
        wrquery.result map { _.map{ wr =>
          Future.successful(wr.externalId).zip(executionServiceCluster.abort(wr, UserInfo.buildFromTokens(credential)))
        }}
      }
    }

    def queryForWorkflowStatuses() = {
      datasource.inTransaction { dataAccess =>
        dataAccess.workflowQuery.listWorkflowRecsForSubmissionAndStatuses(submissionId, WorkflowStatuses.runningStatuses: _*)
      } flatMap { externalWorkflowIds =>
        Future.traverse(externalWorkflowIds) { workflowRec =>
          // for each workflow query the exec service for status and if has Succeeded query again for outputs
          toFutureTry(execServiceStatus(workflowRec) flatMap {
            case Some(updatedWorkflowRec) => execServiceOutputs(updatedWorkflowRec)
            case None => Future.successful(None)
          })
        }
      } map (ExecutionServiceStatusResponse)
    }

    submissionFuture flatMap {
      case Some(submission) =>
        if(submission.status == SubmissionStatuses.Aborting) {
          for {
            abortQueued <- abortQueuedWorkflows(submissionId)
            abortActive <- abortActiveWorkflows(submissionId, submission.workflows.filter(wf => activeStatuses.contains(wf.status)))
            getStatuses <- queryForWorkflowStatuses()
          } yield getStatuses
        }
        else {
          queryForWorkflowStatuses()
        }
      case None => throw new RawlsException(s"Submission ${submissionId} could not be found")
    }
  }

  private def execServiceStatus(workflowRec: WorkflowRecord)(implicit executionContext: ExecutionContext): Future[Option[WorkflowRecord]] = {
    workflowRec.externalId match {
      case Some(externalId) =>     executionServiceCluster.status(workflowRec, UserInfo.buildFromTokens(credential)).map(newStatus => {
        if (newStatus.status != workflowRec.status) Option(workflowRec.copy(status = newStatus.status))
        else None
      })
      case None => Future.successful(None)
    }
  }

  private def execServiceOutputs(workflowRec: WorkflowRecord)(implicit executionContext: ExecutionContext): Future[Option[(WorkflowRecord, Option[ExecutionServiceOutputs])]] = {
    WorkflowStatuses.withName(workflowRec.status) match {
      case WorkflowStatuses.Succeeded =>
        executionServiceCluster.outputs(workflowRec, UserInfo.buildFromTokens(credential)).map(outputs => Option((workflowRec, Option(outputs))))

      case _ => Future.successful(Option((workflowRec, None)))
    }
  }

  /**
   * once all the execution service queries have completed this function is called to handle the responses
    *
    * @param response
   * @param executionContext
   * @return
   */
  def handleStatusResponses(response: ExecutionServiceStatusResponse)(implicit executionContext: ExecutionContext): Future[StatusCheckComplete] = {
    response.statusResponse.collect { case Failure(t) => t }.foreach { t =>
      logger.error(s"Failure monitoring workflow in submission $submissionId", t)
    }

    //Update the workflow statuses in their own transaction
    val workflowsWithOutputsF = datasource.inTransaction { dataAccess =>
      val updatedRecs = response.statusResponse.collect {
        case Success(Some((updatedRec, _))) => updatedRec
      }

      val workflowsWithOutputs = response.statusResponse.collect {
        case Success(Some((workflowRec, Some(outputs)))) => (workflowRec, outputs)
      }

      // to minimize database updates do 1 update per status
      DBIO.seq(updatedRecs.groupBy(_.status).map { case (status, recs) =>
        dataAccess.workflowQuery.batchUpdateStatus(recs, WorkflowStatuses.withName(status))
      }.toSeq: _*) andThen
        DBIO.successful(workflowsWithOutputs)
    }

    //Then in a new transaction, update the submission status.
    //If this transaction throws an exception, it'll roll back, but the workflows will still be in their terminal state.
    //The submission supervisor will restart the submission monitor, which will find this submission (again) in a non-terminal state,
    //query Cromwell again, and restart from the top of this function with the results.
    workflowsWithOutputsF flatMap { workflowsWithOutputs =>
      datasource.inTransaction { dataAccess =>
        handleOutputs(workflowsWithOutputs, dataAccess) flatMap { _ =>
          checkOverallStatus(dataAccess) map {
            shouldStop => StatusCheckComplete(shouldStop)
          }
        }
      }
    }
  }

  /**
   * When there are no workflows with a running or queued status, mark the submission as done or aborted as appropriate.
    *
    * @param dataAccess
   * @param executionContext
   * @return true if the submission is done/aborted
   */
  def checkOverallStatus(dataAccess: DataAccess)(implicit executionContext: ExecutionContext): ReadWriteAction[Boolean] = {
    dataAccess.workflowQuery.listWorkflowRecsForSubmissionAndStatuses(submissionId, (WorkflowStatuses.queuedStatuses ++ WorkflowStatuses.runningStatuses):_*) flatMap { workflowRecs =>
      if (workflowRecs.isEmpty) {
        dataAccess.submissionQuery.findById(submissionId).map(_.status).result.head.map { status =>
          SubmissionStatuses.withName(status) match {
            case SubmissionStatuses.Aborting => SubmissionStatuses.Aborted
            case _ => SubmissionStatuses.Done
          }
        } flatMap { newStatus =>
          logger.debug(s"submission $submissionId terminating to status $newStatus")
          dataAccess.submissionQuery.updateStatus(submissionId, newStatus)
        } map(_ => true)
      } else {
        DBIO.successful(false)
      }
    }
  }

  def handleOutputs(workflowsWithOutputs: Seq[(WorkflowRecord, ExecutionServiceOutputs)], dataAccess: DataAccess)(implicit executionContext: ExecutionContext): ReadWriteAction[Unit] = {
    if (workflowsWithOutputs.isEmpty) {
      DBIO.successful(Unit)
    } else {
      for {
        // load all the starting data
        entitiesById <-      listWorkflowEntitiesById(workflowsWithOutputs, dataAccess)
        outputExpressions <- listMethodConfigOutputsForSubmission(dataAccess)
        workspace <-         getWorkspace(dataAccess).map(_.getOrElse(throw new RawlsException(s"workspace for submission $submissionId not found")))

        // update the appropriate entities and workspace (in memory)
        updatedEntitiesAndWorkspace = attachOutputs(workspace, workflowsWithOutputs, entitiesById, outputExpressions)

        // save everything to the db
        _ <- saveWorkspace(dataAccess, updatedEntitiesAndWorkspace)
        _ <- saveEntities(dataAccess, workspace, updatedEntitiesAndWorkspace)
        _ <- saveErrors(updatedEntitiesAndWorkspace.collect { case Right(errors) => errors }, dataAccess)
      } yield ()
    }
  }

  def getWorkspace(dataAccess: DataAccess): ReadAction[Option[Workspace]] = {
    dataAccess.workspaceQuery.findByName(workspaceName)
  }

  def listMethodConfigOutputsForSubmission(dataAccess: DataAccess): ReadAction[Map[String, String]] = {
    dataAccess.submissionQuery.getMethodConfigOutputExpressions(submissionId)
  }

  def listWorkflowEntitiesById(workflowsWithOutputs: Seq[(WorkflowRecord, ExecutionServiceOutputs)], dataAccess: DataAccess)(implicit executionContext: ExecutionContext): ReadAction[scala.collection.Map[Long, Entity]] = {
    dataAccess.entityQuery.listByIds(workflowsWithOutputs.map { case (workflowRec, outputs) => workflowRec.workflowEntityId }).map(_.toMap)
  }

  def saveWorkspace(dataAccess: DataAccess, updatedEntitiesAndWorkspace: Seq[Either[(Option[Entity], Option[Workspace]), (WorkflowRecord, scala.Seq[AttributeString])]]) = {
    //note there is only 1 workspace (may be None if it is not updated) even though it may be updated multiple times so reduce it into 1 update
    val workspaces = updatedEntitiesAndWorkspace.collect { case Left((_, Some(workspace))) => workspace }
    if (workspaces.isEmpty) DBIO.successful(0)
    else dataAccess.workspaceQuery.save(workspaces.reduce((a, b) => a.copy(attributes = a.attributes ++ b.attributes)))
  }

  def saveEntities(dataAccess: DataAccess, workspace: Workspace, updatedEntitiesAndWorkspace: Seq[Either[(Option[Entity], Option[Workspace]), (WorkflowRecord, scala.Seq[AttributeString])]]) = {
    val entities = updatedEntitiesAndWorkspace.collect { case Left((Some(entity), _)) => entity }
    if (entities.isEmpty) DBIO.successful(0)
    else dataAccess.entityQuery.save(SlickWorkspaceContext(workspace), entities)
  }

  def attachOutputs(workspace: Workspace, workflowsWithOutputs: Seq[(WorkflowRecord, ExecutionServiceOutputs)], entitiesById: scala.collection.Map[Long, Entity], outputExpressions: Map[String, String]): Seq[Either[(Option[Entity], Option[Workspace]), (WorkflowRecord, Seq[AttributeString])]] = {
    workflowsWithOutputs.map { case (workflowRecord, outputsResponse) =>
      val outputs = outputsResponse.outputs

      val attributes = outputExpressions.map { case (outputName, outputExpr) =>
        Try {
          outputs.get(outputName) match {
            case None => throw new RawlsException(s"output named ${outputName} does not exist")
            case Some(Right(uot: UnsupportedOutputType)) => throw new RawlsException(s"output named ${outputName} is not a supported type, received json u${uot.json.compactPrint}")
            case Some(Left(output)) => outputExpr -> output
          }
        }
      }

      if (attributes.forall(_.isSuccess)) {
        Left(updateEntityAndWorkspace(entitiesById(workflowRecord.workflowEntityId), workspace, attributes.map(_.get).toMap))

      } else {
        Right((workflowRecord, attributes.collect { case Failure(t) => AttributeString(t.getMessage) }.toSeq))
      }
    }
  }

  def updateEntityAndWorkspace(entity: Entity, workspace: Workspace, workflowOutputs: Map[String, Attribute]): (Option[Entity], Option[Workspace]) = {
    //Partition outputs by whether their attributes are entity attributes (begin with "this.") or workspace ones (implicitly; begin with "workspace.")
    //This assumption (that it's either "this." or "workspace.") will be guaranteed by checking of the method config when it's imported; see DSDEEPB-1603.
    val (partitionEntity, partitionWorkspace) = workflowOutputs.partition({ case (k, v) => k.startsWith("this.") })
    val entityAttributes = partitionEntity.map({ case (k, v) => (AttributeName.fromDelimitedName(k.stripPrefix("this.")), v) })
    val workspaceAttributes = partitionWorkspace.map({ case (k, v) => (AttributeName.fromDelimitedName(k.stripPrefix("workspace.")), v) })

    val updatedEntity = if (entityAttributes.isEmpty) None else Option(entity.copy(attributes = entity.attributes ++ entityAttributes))
    val updatedWorkspace = if (workspaceAttributes.isEmpty) None else Option(workspace.copy(attributes = workspace.attributes ++ workspaceAttributes))
    
    (updatedEntity, updatedWorkspace)
  }

  def saveErrors(errors: Seq[(WorkflowRecord, Seq[AttributeString])], dataAccess: DataAccess) = {
    DBIO.sequence(errors.map { case (workflowRecord, errorMessages) =>
      dataAccess.workflowQuery.updateStatus(workflowRecord, WorkflowStatuses.Failed) andThen
        dataAccess.workflowQuery.saveMessages(errorMessages, workflowRecord.id)
    })
  }
}
