package org.broadinstitute.dsde.rawls.jobexec

import java.util.UUID

import akka.actor._
import akka.pattern._
import com.google.api.client.auth.oauth2.Credential
import com.typesafe.scalalogging.LazyLogging
import io.opencensus.scala.Tracing.{trace, traceWithParent}
import io.opencensus.trace.Span
import nl.grons.metrics4.scala.Counter
import org.broadinstitute.dsde.rawls.{RawlsException, RawlsFatalExceptionWithErrorReport, model}
import org.broadinstitute.dsde.rawls.coordination.DataSourceAccess
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.dataaccess.slick._
import org.broadinstitute.dsde.rawls.expressions.{BoundOutputExpression, OutputExpression, ThisEntityTarget, WorkspaceTarget}
import org.broadinstitute.dsde.rawls.jobexec.SubmissionMonitorActor._
import org.broadinstitute.dsde.rawls.jobexec.SubmissionSupervisor.{CheckCurrentWorkflowStatusCounts, SaveCurrentWorkflowStatusCounts}
import org.broadinstitute.dsde.rawls.metrics.RawlsInstrumented
import org.broadinstitute.dsde.rawls.model.Attributable.AttributeMap
import org.broadinstitute.dsde.rawls.model.SubmissionStatuses.SubmissionStatus
import org.broadinstitute.dsde.rawls.model.WorkflowStatuses.WorkflowStatus
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.util.OpenCensusDBIOUtils.traceDBIOWithParent
import org.broadinstitute.dsde.rawls.util.{FutureSupport, addJitter}

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.language.postfixOps
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

/**
 * Created by dvoet on 6/26/15.
 */
object SubmissionMonitorActor {
  def props(workspaceName: WorkspaceName,
            submissionId: UUID,
            datasource: DataSourceAccess,
            samDAO: SamDAO,
            googleServicesDAO: GoogleServicesDAO,
            executionServiceCluster: ExecutionServiceCluster,
            credential: Credential,
            config: SubmissionMonitorConfig,
            workbenchMetricBaseName: String): Props = {
    Props(new SubmissionMonitorActor(workspaceName, submissionId, datasource, samDAO, googleServicesDAO, executionServiceCluster, credential, config, workbenchMetricBaseName))
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

  case object SubmissionDeletedException extends Exception
  case class MonitoredSubmissionException(workspaceName: WorkspaceName, submissionId: UUID, cause: Throwable) extends Exception(cause)
}

/**
 * An actor that monitors the status of a submission. Wakes up every submissionPollInterval and queries
 * the execution service for status of workflows that we don't think are done yet. For any workflows
 * that are successful, query again for outputs. Once all workflows are done mark the submission as done
 * and terminate the actor.
 *
 * @param submissionId id of submission to monitor
 */
//noinspection ScalaDocMissingParameterDescription,TypeAnnotation,NameBooleanParameters
class SubmissionMonitorActor(val workspaceName: WorkspaceName,
                             val submissionId: UUID,
                             val datasource: DataSourceAccess,
                             val samDAO: SamDAO,
                             val googleServicesDAO: GoogleServicesDAO,
                             val executionServiceCluster: ExecutionServiceCluster,
                             val credential: Credential,
                             val config: SubmissionMonitorConfig,
                             override val workbenchMetricBaseName: String) extends Actor with SubmissionMonitor with LazyLogging {
  import context._

  override def preStart(): Unit = {
    super.preStart()
    scheduleInitialMonitorPass
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

    case Status.Failure(SubmissionDeletedException) =>
      logger.debug(s"submission $submissionId has been deleted, terminating disgracefully")
      stop(self)

    case Status.Failure(t) =>
      // an error happened in some future, let the supervisor handle it
      // wrap in MonitoredSubmissionException so the supervisor can log/instrument the submission details
      throw MonitoredSubmissionException(workspaceName, submissionId, t)
  }

  private def scheduleInitialMonitorPass: Cancellable = {
    //Wait anything _up to_ the poll interval for a much wider distribution of submission monitor start times when Rawls starts up
    system.scheduler.scheduleOnce(addJitter(0 seconds, config.submissionPollInterval), self, StartMonitorPass)
  }

  private def scheduleNextMonitorPass: Cancellable = {
    system.scheduler.scheduleOnce(addJitter(config.submissionPollInterval), self, StartMonitorPass)
  }

}

//A map of writebacks to apply to the given entity reference
case class WorkflowEntityUpdate(entityRef: AttributeEntityReference, upserts: AttributeMap)

//noinspection ScalaDocMissingParameterDescription,RedundantBlock,TypeAnnotation,ReplaceWithFlatten,ScalaUnnecessaryParentheses,ScalaUnusedSymbol,DuplicatedCode
trait SubmissionMonitor extends FutureSupport with LazyLogging with RawlsInstrumented {
  val workspaceName: WorkspaceName
  val submissionId: UUID
  val datasource: DataSourceAccess
  val samDAO: SamDAO
  val googleServicesDAO: GoogleServicesDAO
  val executionServiceCluster: ExecutionServiceCluster
  val credential: Credential
  val config: SubmissionMonitorConfig

  // Cache these metric builders since they won't change for this SubmissionMonitor
  protected lazy val workspaceMetricBuilder: ExpandedMetricBuilder =
    workspaceMetricBuilder(workspaceName)

  protected lazy val workspaceSubmissionMetricBuilder: ExpandedMetricBuilder =
    workspaceSubmissionMetricBuilder(workspaceName, submissionId)

  // implicitly passed to WorkflowComponent/SubmissionComponent methods
  // note this returns an Option[Counter] because per-submission metrics can be disabled with the trackDetailedSubmissionMetrics flag.
  private implicit val wfStatusCounter: WorkflowStatus => Option[Counter] = status =>
    if (config.trackDetailedSubmissionMetrics) Option(workflowStatusCounter(workspaceSubmissionMetricBuilder)(status)) else None

  private implicit val subStatusCounter: SubmissionStatus => Counter =
    submissionStatusCounter(workspaceMetricBuilder)

  import datasource.slickDataSource.dataAccess.driver.api._

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

    def getWorkspaceAndSubmitter(dataAccess: DataAccess): ReadWriteAction[(RawlsUserEmail, WorkspaceRecord)] = {
      for {
        submissionRec <- dataAccess.submissionQuery.findById(submissionId).result.map(_.head)
        workspaceRec <- dataAccess.workspaceQuery.findByIdQuery(submissionRec.workspaceId).result.map(_.head)
      } yield {
        (RawlsUserEmail(submissionRec.submitterEmail), workspaceRec)
      }
    }

    def getPetSAUserInfo(googleProjectId: GoogleProjectId, submitterEmail: RawlsUserEmail): Future[UserInfo] = {
      for {
      petSAJson <- samDAO.getPetServiceAccountKeyForUser(googleProjectId, submitterEmail)
      petUserInfo <- googleServicesDAO.getUserInfoUsingJson(petSAJson)
      } yield {
        petUserInfo
      }
    }

    def abortActiveWorkflows(submissionId: UUID): Future[Seq[(Option[String], Try[ExecutionServiceStatus])]] = {
      datasource.inTransaction { dataAccess =>
        for {
          // look up abortable WorkflowRecs for this submission
          wfRecs <- dataAccess.workflowQuery.findWorkflowsForAbort(submissionId).result
          (submitter, workspaceRec) <- getWorkspaceAndSubmitter(dataAccess)
        } yield {
          (wfRecs, submitter, workspaceRec)
        }
      } flatMap { case (workflowRecs, submitter, workspaceRec) =>
          for {
            petUserInfo <- getPetSAUserInfo(GoogleProjectId(workspaceRec.googleProject), submitter)
            abortResults <- Future.traverse(workflowRecs) { workflowRec =>
              Future.successful(workflowRec.externalId).zip(executionServiceCluster.abort(workflowRec, petUserInfo))
            }
          } yield {
            abortResults
          }
      }
    }

    def gatherWorkflowOutputs(externalWorkflowIds: Seq[WorkflowRecord], petUser: UserInfo) = {
      Future.traverse(externalWorkflowIds) { workflowRec =>
        // for each workflow query the exec service for status and if has Succeeded query again for outputs
        toFutureTry(execServiceStatus(workflowRec, petUser) flatMap {
          case Some(updatedWorkflowRec) => execServiceOutputs(updatedWorkflowRec, petUser)
          case None => Future.successful(None)
        })
      }
    }

    def queryForWorkflowStatuses() = {
      datasource.inTransaction { dataAccess =>
        for {
          wfRecs <- dataAccess.workflowQuery.listWorkflowRecsForSubmissionAndStatuses(submissionId, WorkflowStatuses.runningStatuses: _*)
          (submitter, workspaceRec) <- getWorkspaceAndSubmitter(dataAccess)
        } yield {
          (wfRecs, submitter, workspaceRec)
        }
      } flatMap { case (externalWorkflowIds, submitter, workspaceRec) =>
        for {
          petUserInfo <- getPetSAUserInfo(GoogleProjectId(workspaceRec.googleProject), submitter)
          workflowOutputs <- gatherWorkflowOutputs(externalWorkflowIds, petUserInfo)
        } yield {
          workflowOutputs
        }

      } map (ExecutionServiceStatusResponse)
    }

    submissionFuture flatMap {
      case Some(submission) =>
        val abortFuture = if (submission.status == SubmissionStatuses.Aborting) {
          //abort workflows if necessary
          for {
            _ <- abortQueuedWorkflows(submissionId)
            _ <- abortActiveWorkflows(submissionId)
          } yield {}
        } else {
          Future.successful(())
        }
        abortFuture flatMap( _ => queryForWorkflowStatuses() )
      case None =>
        //submission has been deleted, most likely because the owning workspace has been deleted
        // treat this as a failure and let it get caught when we pipe it to ourselves
        throw SubmissionDeletedException
    }
  }

  private def execServiceStatus(workflowRec: WorkflowRecord, petUser: UserInfo)(implicit executionContext: ExecutionContext): Future[Option[WorkflowRecord]] = {
    workflowRec.externalId match {
      case Some(externalId) =>
        executionServiceCluster.status(workflowRec, petUser).map(newStatus => {
          if (newStatus.status != workflowRec.status) Option(workflowRec.copy(status = newStatus.status))
          else None
        })
      case None => Future.successful(None)
    }
  }

  private def execServiceOutputs(workflowRec: WorkflowRecord, petUser: UserInfo)(implicit executionContext: ExecutionContext): Future[Option[(WorkflowRecord, Option[ExecutionServiceOutputs])]] = {
    WorkflowStatuses.withName(workflowRec.status) match {
      case status if (WorkflowStatuses.terminalStatuses.contains(status)) =>
        if(status == WorkflowStatuses.Succeeded)
          executionServiceCluster.outputs(workflowRec, petUser).map(outputs => Option((workflowRec, Option(outputs))))
        else
          Future.successful(Some((workflowRec, None)))
      case _ => Future.successful(Some((workflowRec, None)))
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
    trace("SubmissionMonitorActor.handleStatusResponses") { rootSpan =>

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
      traceWithParent("attach-outputs", rootSpan) ( _ =>
        datasource.inTransactionWithAttrTempTable { dataAccess =>
          handleOutputs(workflowsWithOutputs, dataAccess, rootSpan)
        } recoverWith {
          // If there is something fatally wrong handling outputs, mark the workflows as failed
          case fatal: RawlsFatalExceptionWithErrorReport =>
            datasource.inTransaction { dataAccess =>
              traceDBIOWithParent("fatal-exception-attach-outputs", rootSpan) ( _ =>
                DBIO.sequence(workflowsWithStatuses map { workflowRecord =>
                  traceDBIOWithParent("update-workflow-status", rootSpan) ( _ => dataAccess.workflowQuery.updateStatus(workflowRecord, WorkflowStatuses.Failed)) andThen
                    traceDBIOWithParent("update-workflow-messages", rootSpan) ( _ => dataAccess.workflowQuery.saveMessages(Seq(AttributeString(fatal.toString)), workflowRecord.id))
                })
              )
            }
        } flatMap { _ =>
          // NEW TXN! Update statuses for workflows and submission.
          datasource.inTransaction { dataAccess =>
            traceDBIOWithParent("update-workflows-and-submission-statuses", rootSpan) ( _ =>
              // Refetch workflows as some may have been marked as Failed by handleOutputs.
              dataAccess.workflowQuery.findWorkflowByIds(workflowsWithStatuses.map(_.id)).result flatMap { updatedRecs =>

                //New statuses according to the execution service.
                val workflowIdToNewStatus = workflowsWithStatuses.map({ workflowRec => workflowRec.id -> workflowRec.status }).toMap

                // No need to update statuses for any workflows that are in terminal statuses.
                // Doing so would potentially overwrite them with the execution service status if they'd been marked as failed by attachOutputs.
                val workflowsToUpdate = updatedRecs.filter(rec => !WorkflowStatuses.terminalStatuses.contains(WorkflowStatuses.withName(rec.status)))
                val workflowsWithNewStatuses = workflowsToUpdate.map(rec => rec.copy(status = workflowIdToNewStatus(rec.id)))

                // to minimize database updates batch 1 update per workflow status
                DBIO.sequence(workflowsWithNewStatuses.groupBy(_.status).map { case (status, recs) =>
                  dataAccess.workflowQuery.batchUpdateStatus(recs, WorkflowStatuses.withName(status))
                })

              } flatMap { _ =>
                // update submission after workflows are updated
                updateSubmissionStatus(dataAccess) map { shouldStop: Boolean =>
                  //return a message about whether our submission is done entirely
                  StatusCheckComplete(shouldStop)
                }
              }
            )
          }
        }
      )
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

  def handleOutputs(workflowsWithOutputs: Seq[(WorkflowRecord, ExecutionServiceOutputs)], dataAccess: DataAccess, parentSpan: Span = null)(implicit executionContext: ExecutionContext): ReadWriteAction[Unit] = {
    traceDBIOWithParent("SubmissionMonitorActor.handleOutputs", parentSpan)( _ =>
      if (workflowsWithOutputs.isEmpty) {
        DBIO.successful(())
      } else {
        for {
          // load all the starting data
          entitiesById <-         traceDBIOWithParent("SubmissionMonitorActor.handleOutputs.listWorkflowEntitiesById", parentSpan) ( _ => listWorkflowEntitiesById(workflowsWithOutputs, dataAccess))
          outputExpressionMap <-  traceDBIOWithParent("SubmissionMonitorActor.handleOutputs.listMethodConfigOutputsForSubmission", parentSpan) ( _ => listMethodConfigOutputsForSubmission(dataAccess))
          workspace <-            traceDBIOWithParent("SubmissionMonitorActor.handleOutputs.getWorkspace", parentSpan) ( _ => getWorkspace(dataAccess).map(_.getOrElse(throw new RawlsException(s"workspace for submission $submissionId not found"))))

          // figure out the updates that need to occur to entities and workspaces
          updatedEntitiesAndWorkspace = attachOutputs(workspace, workflowsWithOutputs, entitiesById, outputExpressionMap)

          // for debugging purposes
          workspacesToUpdate = updatedEntitiesAndWorkspace.collect { case Left((_, Some(workspace))) => workspace }
          entityUpdates = updatedEntitiesAndWorkspace.collect { case Left((Some(entityUpdate), _)) if entityUpdate.upserts.nonEmpty => entityUpdate }
          _ = if (workspacesToUpdate.nonEmpty && entityUpdates.nonEmpty)
                logger.info("handleOutputs writing to both workspace and entity attributes")
              else if (workspacesToUpdate.nonEmpty)
                logger.info("handleOutputs writing to workspace attributes only")
              else if (entityUpdates.nonEmpty)
                logger.info("handleOutputs writing to entity attributes only")
              else
                logger.info("handleOutputs writing to neither workspace nor entity attributes; could be errors")

          // save everything to the db
          _ <- saveWorkspace(dataAccess, updatedEntitiesAndWorkspace, parentSpan)
          _ <- saveEntities(dataAccess, workspace, updatedEntitiesAndWorkspace, parentSpan)
          _ <- saveErrors(updatedEntitiesAndWorkspace.collect { case Right(errors) => errors }, dataAccess, parentSpan)
        } yield ()
      }
    )
  }

  def getWorkspace(dataAccess: DataAccess): ReadAction[Option[Workspace]] = {
    dataAccess.workspaceQuery.findByName(workspaceName)
  }

  def listMethodConfigOutputsForSubmission(dataAccess: DataAccess): ReadAction[Map[String, String]] = {
    dataAccess.submissionQuery.getMethodConfigOutputExpressions(submissionId)
  }

  def listWorkflowEntitiesById(workflowsWithOutputs: Seq[(WorkflowRecord, ExecutionServiceOutputs)], dataAccess: DataAccess)
                              (implicit executionContext: ExecutionContext): ReadAction[scala.collection.Map[Long, Entity]] = {
    //Note that we can't look up entities for workflows that didn't run on entities (obviously), so they get dropped here.
    //Those are handled in handle/attachOutputs.
    val workflowsWithEntities = workflowsWithOutputs.filter(wwo => wwo._1.workflowEntityId.isDefined)

    //yank out of Seq[(Long, Option[Entity])] and into Seq[(Long, Entity)]
    val entityIds = workflowsWithEntities.flatMap{ case (workflowRec, outputs) => workflowRec.workflowEntityId }
    dataAccess.entityQuery.getEntities(entityIds).map(_.toMap)
  }

  def saveWorkspace(dataAccess: DataAccess, updatedEntitiesAndWorkspace: Seq[Either[(Option[WorkflowEntityUpdate], Option[Workspace]), (WorkflowRecord, scala.Seq[AttributeString])]], parentSpan: Span = null)(implicit executionContext: ExecutionContext) = {
    traceDBIOWithParent("SubmissionMonitorActor.saveWorkspace", parentSpan) { _ =>
      //note there is only 1 workspace (may be None if it is not updated) even though it may be updated multiple times so reduce it into 1 update
      val workspaces = updatedEntitiesAndWorkspace.collect { case Left((_, Some(workspace))) => workspace }
      if (workspaces.isEmpty) DBIO.successful(0)
      else dataAccess.workspaceQuery.save(workspaces.reduce((a, b) => a.copy(attributes = a.attributes ++ b.attributes)), parentSpan)
    }
  }

  def saveEntities(dataAccess: DataAccess, workspace: Workspace, updatedEntitiesAndWorkspace: Seq[Either[(Option[WorkflowEntityUpdate], Option[Workspace]), (WorkflowRecord, scala.Seq[AttributeString])]], parentSpan: Span = null)(implicit executionContext: ExecutionContext) = {
    traceDBIOWithParent("SubmissionMonitorActor.saveEntities", parentSpan) { _ =>
      val entityUpdates = updatedEntitiesAndWorkspace.collect { case Left((Some(entityUpdate), _)) if entityUpdate.upserts.nonEmpty => entityUpdate }
      if (entityUpdates.isEmpty) {
        DBIO.successful(())
      }
      else {
        DBIO.sequence(entityUpdates map { entityUpd =>
          dataAccess.entityQuery.saveEntityPatch(workspace, entityUpd.entityRef, entityUpd.upserts, Seq(), parentSpan)
        })
      }
    }
  }

  def attachOutputs(workspace: Workspace, workflowsWithOutputs: Seq[(WorkflowRecord, ExecutionServiceOutputs)], entitiesById: scala.collection.Map[Long, Entity], outputExpressionMap: Map[String, String]): Seq[Either[(Option[WorkflowEntityUpdate], Option[Workspace]), (WorkflowRecord, Seq[AttributeString])]] = {
    workflowsWithOutputs.map { case (workflowRecord, outputsResponse) =>
      val outputs = outputsResponse.outputs
      logger.debug(s"attaching outputs for ${submissionId.toString}/${workflowRecord.externalId.getOrElse("MISSING_WORKFLOW")}: ${outputs}")
      logger.debug(s"output expressions for ${submissionId.toString}/${workflowRecord.externalId.getOrElse("MISSING_WORKFLOW")}: ${outputExpressionMap}")

      val parsedExpressions: Seq[Try[OutputExpression]] = outputExpressionMap.map { case (outputName, outputExprStr) =>
        outputs.get(outputName) match {
          case None => Failure(new RawlsException(s"output named ${outputName} does not exist"))
          case Some(Right(uot: UnsupportedOutputType)) => Failure(new RawlsException(s"output named ${outputName} is not a supported type, received json u${uot.json.compactPrint}"))
          case Some(Left(output)) =>
            val entityTypeOpt = workflowRecord.workflowEntityId.flatMap(entitiesById.get).map(_.entityType)
            OutputExpression.build(outputExprStr, output, entityTypeOpt)
        }
      }.toSeq

      if (parsedExpressions.forall(_.isSuccess)) {
        val boundExpressions: Seq[BoundOutputExpression] = parsedExpressions.collect { case Success(boe @ BoundOutputExpression(target, name, attr)) => boe }
        val updates = updateEntityAndWorkspace(workflowRecord.workflowEntityId.map(id => Some(entitiesById(id))).getOrElse(None), workspace, boundExpressions)

        val (optEntityUpdates, optWs) = updates
        optEntityUpdates foreach { update: WorkflowEntityUpdate =>
          logger.debug(s"Updating ${update.upserts.size} attributes for entity ${update.entityRef.entityName} of type ${update.entityRef.entityType} in ${submissionId.toString}/${workflowRecord.externalId.getOrElse("MISSING_WORKFLOW")}. First 100: ${update.upserts.take(100)}")
        }
        optWs foreach { workspace: Workspace =>
          logger.debug(s"Updating ${workspace.attributes.size} workspace attributes in ${submissionId.toString}/${workflowRecord.externalId.getOrElse("MISSING_WORKFLOW")}. First 100: ${workspace.attributes.take(100)}")
        }

        Left(updates)
      } else {
        Right((workflowRecord, parsedExpressions.collect { case Failure(t) => AttributeString(t.getMessage) }))
      }
    }
  }

  private def updateEntityAndWorkspace(entity: Option[Entity], workspace: Workspace, workflowOutputs: Iterable[BoundOutputExpression]): (Option[WorkflowEntityUpdate], Option[Workspace]) = {
    val entityUpsert = workflowOutputs.collect({ case BoundOutputExpression(ThisEntityTarget, attrName, attr) => (attrName, attr) })
    val workspaceAttributes = workflowOutputs.collect({ case BoundOutputExpression(WorkspaceTarget, attrName, attr) => (attrName, attr) })

    if(entity.isEmpty && entityUpsert.nonEmpty) {
      //TODO: we shouldn't ever run into this case, but if we somehow do, it'll make the submission actor restart forever
      throw new RawlsException("how am I supposed to bind expressions to a nonexistent entity??!!")
    }

    val entityAndUpsert = entity.map(e => WorkflowEntityUpdate(e.toReference, entityUpsert.toMap))
    val updatedWorkspace = if (workspaceAttributes.isEmpty) None else Option(workspace.copy(attributes = workspace.attributes ++ workspaceAttributes))

    (entityAndUpsert, updatedWorkspace)
  }

  def saveErrors(errors: Seq[(WorkflowRecord, Seq[AttributeString])], dataAccess: DataAccess, parentSpan: Span = null)(implicit executionContext: ExecutionContext) = {
    traceDBIOWithParent("SubmissionMonitorActor.saveError", parentSpan) ( _ =>
      DBIO.sequence(errors.map { case (workflowRecord, errorMessages) =>
        traceDBIOWithParent("update-workflow-status", parentSpan) ( _ =>dataAccess.workflowQuery.updateStatus(workflowRecord, WorkflowStatuses.Failed)) andThen
          traceDBIOWithParent("save-error-messages", parentSpan) ( _ =>dataAccess.workflowQuery.saveMessages(errorMessages, workflowRecord.id))
    }))
  }

  def checkCurrentWorkflowStatusCounts(reschedule: Boolean)(implicit executionContext: ExecutionContext): Future[SaveCurrentWorkflowStatusCounts] = {
    datasource.inTransaction { dataAccess =>
      for {
        wfStatuses <- dataAccess.workflowQuery.countWorkflowsForSubmissionByQueueStatus(submissionId)
        workspace <- getWorkspace(dataAccess).map(_.getOrElse(throw new RawlsException(s"workspace for submission $submissionId not found")))
        subStatuses <- dataAccess.submissionQuery.countByStatus(workspace)
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
      SaveCurrentWorkflowStatusCounts(workspaceName, submissionId, wfCounts, subCounts, reschedule)
    }
  }
}

final case class SubmissionMonitorConfig(submissionPollInterval: FiniteDuration, trackDetailedSubmissionMetrics: Boolean)
