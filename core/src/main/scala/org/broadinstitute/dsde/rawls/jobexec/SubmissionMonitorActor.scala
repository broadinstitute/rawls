package org.broadinstitute.dsde.rawls.jobexec

import java.util.UUID

import akka.actor._
import akka.pattern._
import com.google.api.client.auth.oauth2.Credential
import com.typesafe.scalalogging.LazyLogging
import nl.grons.metrics.scala.Counter
import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.dataaccess.slick.{DataAccess, ReadAction, ReadWriteAction, WorkflowRecord}
import org.broadinstitute.dsde.rawls.jobexec.SubmissionMonitorActor._
import org.broadinstitute.dsde.rawls.jobexec.SubmissionSupervisor.{CheckCurrentWorkflowStatusCounts, SaveCurrentWorkflowStatusCounts}
import org.broadinstitute.dsde.rawls.metrics.RawlsInstrumented
import org.broadinstitute.dsde.rawls.model.SubmissionStatuses.SubmissionStatus
import org.broadinstitute.dsde.rawls.model.WorkflowStatuses.WorkflowStatus
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.util.{FutureSupport, addJitter}

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

/**
 * Created by dvoet on 6/26/15.
 */
object SubmissionMonitorActor {
  def props(workspaceName: WorkspaceName,
            submissionId: UUID,
            datasource: SlickDataSource,
            executionServiceCluster: ExecutionServiceCluster,
            credential: Credential,
            submissionPollInterval: FiniteDuration,
            workbenchMetricBaseName: String): Props = {
    Props(new SubmissionMonitorActor(workspaceName, submissionId, datasource, executionServiceCluster, credential, submissionPollInterval, workbenchMetricBaseName))
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
                             val submissionPollInterval: FiniteDuration,
                             override val workbenchMetricBaseName: String) extends Actor with SubmissionMonitor with LazyLogging {
  import context._


  override def preStart(): Unit = {
    super.preStart()
    scheduleNextMonitorPass
  }

  override def receive = {
    case StartMonitorPass =>
      logger.debug(s"polling workflows for submission $submissionId")
      queryExecutionServiceForStatus() pipeTo self
    case response: ExecutionServiceStatusResponse =>
      logger.debug(s"handling execution service response for submission $submissionId")
      handleStatusResponses(response) pipeTo self
    case StatusCheckComplete(terminateActor) =>
      logger.debug(s"done checking status for submission $submissionId, terminateActor = $terminateActor")
      // Before terminating this actor, run one more CheckCurrentWorkflowStatusCounts pass to ensure
      // we have accurate metrics at the time of actor termination.
      if (terminateActor) {
        checkCurrentWorkflowStatusCounts(false) pipeTo parent andThen { case _ => stop(self) }
      }
      else scheduleNextMonitorPass
    case CheckCurrentWorkflowStatusCounts =>
      logger.debug(s"check current workflow status counts for submission $submissionId")
      checkCurrentWorkflowStatusCounts(true) pipeTo parent

    case Status.Failure(t) => throw t // an error happened in some future, let the supervisor handle it
  }

  private def scheduleNextMonitorPass: Cancellable = {
    system.scheduler.scheduleOnce(addJitter(submissionPollInterval), self, StartMonitorPass)
  }

}

trait SubmissionMonitor extends FutureSupport with LazyLogging with RawlsInstrumented {
  val workspaceName: WorkspaceName
  val submissionId: UUID
  val datasource: SlickDataSource
  val executionServiceCluster: ExecutionServiceCluster
  val credential: Credential
  val submissionPollInterval: Duration

  // Cache these metric builders since they won't change for this SubmissionMonitor
  protected lazy val workspaceMetricBuilder: ExpandedMetricBuilder =
    workspaceMetricBuilder(workspaceName)

  protected lazy val workspaceSubmissionMetricBuilder: ExpandedMetricBuilder =
    workspaceSubmissionMetricBuilder(workspaceName, submissionId)

  // implicitly passed to WorkflowComponent/SubmissionComponent methods
  private implicit val wfStatusCounter: WorkflowStatus => Counter =
    workflowStatusCounter(workspaceSubmissionMetricBuilder)

  private implicit val subStatusCounter: SubmissionStatus => Counter =
    submissionStatusCounter(workspaceMetricBuilder)

  import datasource.dataAccess.driver.api._

  /**
   * This function starts a monitoring pass
   *
   * @param executionContext
   * @return
   */
  def queryExecutionServiceForStatus()(implicit executionContext: ExecutionContext): Future[ExecutionServiceStatusResponse] = {
    val submissionFuture = datasource.inTransaction { dataAccess =>
      dataAccess.submissionQuery.loadSubmission(submissionId)
    }

    def abortQueuedWorkflows(submissionId: UUID) = {
      datasource.inTransaction { dataAccess =>
        dataAccess.workflowQuery.batchUpdateWorkflowsOfStatus(submissionId, WorkflowStatuses.Queued, WorkflowStatuses.Aborted)
      }
    }

    def abortActiveWorkflows(submissionId: UUID): Future[Seq[(Option[String], Try[ExecutionServiceStatus])]] = {
      datasource.inTransaction { dataAccess =>
        // look up abortable WorkflowRecs for this submission
        dataAccess.workflowQuery.findWorkflowsForAbort(submissionId).result
      }.flatMap { workflowRecs =>
        Future.traverse(workflowRecs) { workflowRec =>
          Future.successful(workflowRec.externalId).zip(executionServiceCluster.abort(workflowRec, UserInfo.buildFromTokens(credential)))
        }
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
        val abortFuture = if(submission.status == SubmissionStatuses.Aborting) {
          for {
            _ <- abortQueuedWorkflows(submissionId)
            _ <- abortActiveWorkflows(submissionId)
          } yield {}
        } else {
          Future.successful(())
        }
        abortFuture flatMap( _ => queryForWorkflowStatuses() )
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
    * the WorkflowRecords in ExecutionServiceStatus response have not been saved to the database but have been updated with their status from Cromwell.
    *
    * @param response
   * @param executionContext
   * @return
   */
  def handleStatusResponses(response: ExecutionServiceStatusResponse)(implicit executionContext: ExecutionContext): Future[StatusCheckComplete] = {
    response.statusResponse.collect { case Failure(t) => t }.foreach { t =>
      logger.error(s"Failure monitoring workflow in submission $submissionId", t)
    }

    //all workflow records in this status response list
    val workflowsWithStatuses = response.statusResponse.collect {
      case Success(Some((aWorkflow, _))) => aWorkflow
    }

    //just the workflow records in this response list which have outputs
    val workflowsWithOutputs = response.statusResponse.collect {
      case Success(Some((workflowRec, Some(outputs)))) =>
        (workflowRec, outputs)
    }

    // Attach the outputs in a txn of their own.
    // If attaching outputs fails for legit reasons (e.g. they're missing), it will mark the workflow as failed. This is correct.
    // If attaching outputs throws an exception (because e.g. deadlock or ConcurrentModificationException), the status will remain un-updated
    // and will be re-processed next time we call queryForWorkflowStatus().
    // This is why it's important to attach the outputs before updating the status -- if you update the status to Successful first, and the attach
    // outputs fails, we'll stop querying for the workflow status and never attach the outputs.
    datasource.inTransaction { dataAccess =>
      handleOutputs(workflowsWithOutputs, dataAccess)
    } flatMap { _ =>
      // NEW TXN! Update statuses for workflows and submission.
      datasource.inTransaction { dataAccess =>

        // Refetch workflows as some may have been marked as Failed by handleOutputs.
        dataAccess.workflowQuery.findWorkflowByIds(workflowsWithStatuses.map(_.id)).result flatMap { updatedRecs =>

          //New statuses according to the execution service.
          val workflowIdToNewStatus = workflowsWithStatuses.map({ workflowRec => workflowRec.id -> workflowRec.status }).toMap

          // No need to update statuses for any workflows that are in terminal statuses.
          // Doing so would potentially overwrite them with the execution service status if they'd been marked as failed by attachOutputs.
          val workflowsToUpdate = updatedRecs.filter(rec => !WorkflowStatuses.terminalStatuses.contains(WorkflowStatuses.withName(rec.status)))
          val workflowsWithNewStatuses = workflowsToUpdate.map(rec => rec.copy(status = workflowIdToNewStatus(rec.id)))

          // to minimize database updates batch 1 update per workflow status
          DBIO.sequence( workflowsWithNewStatuses.groupBy(_.status).map { case (status, recs) =>
              dataAccess.workflowQuery.batchUpdateStatus(recs, WorkflowStatuses.withName(status))
          })

        } flatMap { _ =>
          // update submission after workflows are updated
          updateSubmissionStatus(dataAccess) map { shouldStop: Boolean =>
            //return a message about whether our submission is done entirely
            StatusCheckComplete(shouldStop)
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
  def updateSubmissionStatus(dataAccess: DataAccess)(implicit executionContext: ExecutionContext): ReadWriteAction[Boolean] = {
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
      DBIO.successful(())
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
    dataAccess.entityQuery.getEntities(workflowsWithOutputs.map { case (workflowRec, outputs) => workflowRec.workflowEntityId }).map(_.toMap)
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
      logger.debug(s"attaching outputs for ${submissionId.toString}/${workflowRecord.externalId.getOrElse("MISSING_WORKFLOW")}: ${outputs}")
      logger.debug(s"output expressions for ${submissionId.toString}/${workflowRecord.externalId.getOrElse("MISSING_WORKFLOW")}: ${outputExpressions}")

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
        val updates = updateEntityAndWorkspace(entitiesById(workflowRecord.workflowEntityId), workspace, attributes.map(_.get).toMap)
        val (optEnt, optWs) = updates
        logger.debug(s"updated entityattrs for ${submissionId.toString}/${workflowRecord.externalId.getOrElse("MISSING_WORKFLOW")}: ${optEnt.map(_.attributes)}")
        logger.debug(s"updated wsattrs for ${submissionId.toString}/${workflowRecord.externalId.getOrElse("MISSING_WORKFLOW")}: ${optWs.map(_.attributes)}")
        Left(updates)
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

  def checkCurrentWorkflowStatusCounts(reschedule: Boolean)(implicit executionContext: ExecutionContext): Future[SaveCurrentWorkflowStatusCounts] = {
    datasource.inTransaction { dataAccess =>
      for {
        wfStatuses <- dataAccess.workflowQuery.countWorkflowsForSubmissionByQueueStatus(submissionId)
        workspace <- getWorkspace(dataAccess).map(_.getOrElse(throw new RawlsException(s"workspace for submission $submissionId not found")))
        subStatuses <- dataAccess.submissionQuery.countByStatus(SlickWorkspaceContext(workspace))
      } yield {
        val workflowStatuses = wfStatuses.map { case (k, v) => WorkflowStatuses.withName(k) -> v }
        val submissionStatuses = subStatuses.map { case (k, v) => SubmissionStatuses.withName(k) -> v }
        (workflowStatuses, submissionStatuses)
      }
    }.recover { case NonFatal(e) =>
      // Recover on errors since this just affects metrics and we don't want it to blow up the whole actor if it fails
      logger.error("Error occurred checking current workflow status counts", e)
      (Map.empty[WorkflowStatus, Int], Map.empty[SubmissionStatus, Int])
    }.map { case (wfCounts, subCounts) =>
      SaveCurrentWorkflowStatusCounts(wfCounts, subCounts, reschedule)
    }
  }
}
