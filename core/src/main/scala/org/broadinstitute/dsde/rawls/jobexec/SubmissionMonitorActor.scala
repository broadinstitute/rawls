package org.broadinstitute.dsde.rawls.jobexec

import akka.actor._
import akka.pattern._
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.implicits._
import com.google.api.client.auth.oauth2.Credential
import com.typesafe.scalalogging.LazyLogging
import io.opencensus.trace.{AttributeValue => OpenCensusAttributeValue}
import io.opentelemetry.api.common.AttributeKey
import nl.grons.metrics4.scala.Counter
import org.broadinstitute.dsde.rawls.coordination.DataSourceAccess
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.dataaccess.slick._
import org.broadinstitute.dsde.rawls.expressions.{
  BoundOutputExpression,
  OutputExpression,
  ThisEntityTarget,
  WorkspaceTarget
}
import org.broadinstitute.dsde.rawls.jobexec.SubmissionMonitorActor._
import org.broadinstitute.dsde.rawls.jobexec.SubmissionSupervisor.{
  CheckCurrentWorkflowStatusCounts,
  SaveCurrentWorkflowStatusCounts
}
import org.broadinstitute.dsde.rawls.metrics.RawlsInstrumented
import org.broadinstitute.dsde.rawls.model.Attributable.{attributeCount, safePrint, AttributeMap}
import org.broadinstitute.dsde.rawls.model.SubmissionStatuses.SubmissionStatus
import org.broadinstitute.dsde.rawls.model.WorkflowStatuses.WorkflowStatus
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.util.{addJitter, AuthUtil, FutureSupport}
import org.broadinstitute.dsde.rawls.util.TracingUtils.{
  setTraceSpanAttribute,
  traceDBIOWithParent,
  traceFuture,
  traceFutureWithParent
}
import org.broadinstitute.dsde.rawls.{RawlsException, RawlsFatalExceptionWithErrorReport}
import org.broadinstitute.dsde.workbench.dataaccess.NotificationDAO
import org.broadinstitute.dsde.workbench.model.{Notifications, WorkbenchUserId}
import org.broadinstitute.dsde.workbench.model.Notifications.{
  AbortedSubmissionNotification,
  FailedSubmissionNotification,
  Notification,
  SuccessfulSubmissionNotification
}

import java.util.UUID
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.language.postfixOps
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}
import spray.json._

/**
 * Created by dvoet on 6/26/15.
 */
object SubmissionMonitorActor {
  def props(workspaceName: WorkspaceName,
            submissionId: UUID,
            datasource: DataSourceAccess,
            samDAO: SamDAO,
            googleServicesDAO: GoogleServicesDAO,
            notificationDAO: NotificationDAO,
            executionServiceCluster: ExecutionServiceCluster,
            config: SubmissionMonitorConfig,
            queryTimeout: Duration,
            workbenchMetricBaseName: String
  ): Props =
    Props(
      new SubmissionMonitorActor(workspaceName,
                                 submissionId,
                                 datasource,
                                 samDAO,
                                 googleServicesDAO,
                                 notificationDAO,
                                 executionServiceCluster,
                                 config,
                                 queryTimeout,
                                 workbenchMetricBaseName
      )
    )

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
  case class ExecutionServiceStatusResponse(
    statusResponse: Seq[Try[Option[(WorkflowRecord, Option[ExecutionServiceOutputs])]]]
  ) extends SubmissionMonitorMessage
  case class StatusCheckComplete(terminateActor: Boolean) extends SubmissionMonitorMessage

  case object SubmissionDeletedException extends Exception
  case class MonitoredSubmissionException(workspaceName: WorkspaceName, submissionId: UUID, cause: Throwable)
      extends Exception(cause)
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
                             val notificationDAO: NotificationDAO,
                             val executionServiceCluster: ExecutionServiceCluster,
                             val config: SubmissionMonitorConfig,
                             val queryTimeout: Duration,
                             override val workbenchMetricBaseName: String
) extends Actor
    with SubmissionMonitor
    with LazyLogging {
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
      } else scheduleNextMonitorPass
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

  private def scheduleInitialMonitorPass: Cancellable =
    // Wait anything _up to_ the poll interval for a much wider distribution of submission monitor start times when Rawls starts up
    system.scheduler.scheduleOnce(addJitter(0 seconds, config.submissionPollInterval), self, StartMonitorPass)

  private def scheduleNextMonitorPass: Cancellable =
    system.scheduler.scheduleOnce(addJitter(config.submissionPollInterval), self, StartMonitorPass)

}

//A map of writebacks to apply to the given entity reference
case class WorkflowEntityUpdate(entityRef: AttributeEntityReference, upserts: AttributeMap)

//noinspection ScalaDocMissingParameterDescription,RedundantBlock,TypeAnnotation,ReplaceWithFlatten,ScalaUnnecessaryParentheses,ScalaUnusedSymbol,DuplicatedCode
trait SubmissionMonitor extends FutureSupport with LazyLogging with RawlsInstrumented with AuthUtil {
  val workspaceName: WorkspaceName
  val submissionId: UUID
  val datasource: DataSourceAccess
  val samDAO: SamDAO
  val googleServicesDAO: GoogleServicesDAO
  val notificationDAO: NotificationDAO
  val executionServiceCluster: ExecutionServiceCluster
  val config: SubmissionMonitorConfig
  val queryTimeout: Duration

  // Cache these metric builders since they won't change for this SubmissionMonitor
  protected lazy val workspaceMetricBuilder: ExpandedMetricBuilder =
    workspaceMetricBuilder(workspaceName)

  protected lazy val workspaceSubmissionMetricBuilder: ExpandedMetricBuilder =
    workspaceSubmissionMetricBuilder(workspaceName, submissionId)

  // implicitly passed to WorkflowComponent/SubmissionComponent methods
  // note this returns an Option[Counter] because per-submission metrics can be disabled with the trackDetailedSubmissionMetrics flag.
  implicit private val wfStatusCounter: WorkflowStatus => Option[Counter] = status =>
    if (config.trackDetailedSubmissionMetrics) Option(workflowStatusCounter(workspaceSubmissionMetricBuilder)(status))
    else None

  implicit private val subStatusCounter: SubmissionStatus => Counter =
    submissionStatusCounter(workspaceMetricBuilder)

  import datasource.slickDataSource.dataAccess.driver.api._

  /**
   * This function starts a monitoring pass
   *
   * @param executionContext
   * @return
   */
  def queryExecutionServiceForStatus()(implicit
    executionContext: ExecutionContext
  ): Future[ExecutionServiceStatusResponse] = {
    val submissionFuture = datasource.inTransaction { dataAccess =>
      dataAccess.uniqueResult[SubmissionRecord](dataAccess.submissionQuery.findById(submissionId))
    }

    def abortQueuedWorkflows(submissionId: UUID) =
      datasource.inTransaction { dataAccess =>
        dataAccess.workflowQuery.batchUpdateWorkflowsOfStatus(submissionId,
                                                              WorkflowStatuses.Queued,
                                                              WorkflowStatuses.Aborted
        )
      }

    def getWorkspace(dataAccess: DataAccess, workspaceId: UUID): ReadWriteAction[WorkspaceRecord] =
      for {
        workspaceRec <- dataAccess.workspaceQuery.findByIdQuery(workspaceId).result.map(_.head)
      } yield workspaceRec

    def abortActiveWorkflows(
      submissionRec: SubmissionRecord
    ): Future[Seq[(Option[String], Try[ExecutionServiceStatus])]] =
      datasource.inTransaction { dataAccess =>
        for {
          // look up abortable WorkflowRecs for this submission
          wfRecs <- dataAccess.workflowQuery.findWorkflowsForAbort(submissionId).result
          submitter = RawlsUserEmail(submissionRec.submitterEmail)
          workspaceRec <- getWorkspace(dataAccess, submissionRec.workspaceId)
        } yield (wfRecs, submitter, workspaceRec)
      } flatMap { case (workflowRecs, submitter, workspaceRec) =>
        for {
          petUserInfo <- getPetServiceAccountUserInfo(GoogleProjectId(workspaceRec.googleProjectId), submitter)
          abortResults <- Future.traverse(workflowRecs) { workflowRec =>
            Future.successful(workflowRec.externalId).zip(executionServiceCluster.abort(workflowRec, petUserInfo))
          }
          // TODO: should this also mark the workflows as Aborted in the Rawls db???
        } yield abortResults
      }

    def gatherWorkflowOutputs(externalWorkflowIds: Seq[WorkflowRecord], petUser: UserInfo) =
      Future.traverse(externalWorkflowIds) { workflowRec =>
        // for each workflow query the exec service for status and if has Succeeded query again for outputs
        toFutureTry(execServiceStatus(workflowRec, petUser) flatMap {
          case Some(updatedWorkflowRec) => execServiceOutputs(updatedWorkflowRec, petUser)
          case None                     => Future.successful(None)
        })
      }

    def queryForWorkflowStatuses(submissionRec: SubmissionRecord) =
      datasource.inTransaction { dataAccess =>
        for {
          wfRecs <- dataAccess.workflowQuery.listWorkflowRecsForSubmissionAndStatuses(
            submissionId,
            WorkflowStatuses.runningStatuses: _*
          )
          submitter = RawlsUserEmail(submissionRec.submitterEmail)
          workspaceRec <- getWorkspace(dataAccess, submissionRec.workspaceId)
        } yield (wfRecs, submitter, workspaceRec)
      } flatMap { case (externalWorkflowIds, submitter, workspaceRec) =>
        for {
          petUserInfo <- getPetServiceAccountUserInfo(GoogleProjectId(workspaceRec.googleProjectId), submitter)
          workflowOutputs <- gatherWorkflowOutputs(externalWorkflowIds, petUserInfo)
        } yield workflowOutputs

      } map ExecutionServiceStatusResponse

    submissionFuture flatMap {
      case Some(submissionRec) =>
        val abortFuture = if (SubmissionStatuses.withName(submissionRec.status) == SubmissionStatuses.Aborting) {
          // abort workflows if necessary
          for {
            _ <- abortQueuedWorkflows(submissionId)
            _ <- abortActiveWorkflows(submissionRec)
          } yield {}
        } else {
          Future.successful(())
        }
        abortFuture flatMap (_ => queryForWorkflowStatuses(submissionRec))
      case None =>
        // submission has been deleted, most likely because the owning workspace has been deleted
        // treat this as a failure and let it get caught when we pipe it to ourselves
        throw SubmissionDeletedException
    }
  }

  private def execServiceStatus(workflowRec: WorkflowRecord, petUser: UserInfo)(implicit
    executionContext: ExecutionContext
  ): Future[Option[WorkflowRecord]] =
    workflowRec.externalId match {
      case Some(externalId) =>
        executionServiceCluster.status(workflowRec, petUser).map { newStatus =>
          if (newStatus.status != workflowRec.status) Option(workflowRec.copy(status = newStatus.status))
          else None
        }
      case None => Future.successful(None)
    }

  private def execServiceOutputs(workflowRec: WorkflowRecord, petUser: UserInfo)(implicit
    executionContext: ExecutionContext
  ): Future[Option[(WorkflowRecord, Option[ExecutionServiceOutputs])]] =
    WorkflowStatuses.withName(workflowRec.status) match {
      case status if WorkflowStatuses.terminalStatuses.contains(status) =>
        if (status == WorkflowStatuses.Succeeded)
          executionServiceCluster.outputs(workflowRec, petUser).map(outputs => Option((workflowRec, Option(outputs))))
        else
          Future.successful(Some((workflowRec, None)))
      case _ => Future.successful(Some((workflowRec, None)))
    }

  /**
    * once all the execution service queries have completed this function is called to handle the responses
    * the WorkflowRecords in ExecutionServiceStatus response have not been saved to the database but
    * have been updated with their status from Cromwell.
    *
    * @param response all workflows in this submission whose statuses are different in Cromwell than in Rawls. If a
    *                 workflow has outputs in Cromwell, those outputs are included here.
    * @param executionContext
    * @return
    */
  def handleStatusResponses(
    response: ExecutionServiceStatusResponse
  )(implicit executionContext: ExecutionContext): Future[StatusCheckComplete] =
    // start a trace
    traceFuture("SubmissionMonitorActor.handleStatusResponses") { rootContext =>
      setTraceSpanAttribute(rootContext, AttributeKey.stringKey("submissionId"), submissionId.toString)
      setTraceSpanAttribute(rootContext, AttributeKey.stringKey("workspaceNamespace"), workspaceName.namespace)
      setTraceSpanAttribute(rootContext, AttributeKey.stringKey("workspaceName"), workspaceName.name)

      // log any failures.
      response.statusResponse.collect { case Failure(t) => t }.foreach { t =>
        logger.error(s"Failure monitoring workflow in submission $submissionId", t)
      }

      // partition workflows into two variables: those workflows that do have
      // outputs vs. those workflows that do not have outputs. This allows us to process
      // the easier workflows first (those without outputs)
      val (yesOutputs, noOutputs) = response.statusResponse
        .collect { case Success(Some((workflowRec, maybeOutputs))) =>
          (workflowRec, maybeOutputs)
        }
        .partition(_._2.isDefined)

      // extra logic just for logging
      val noOutputsGrouped = noOutputs.groupBy(_._1.status).view.mapValues(v => v.size).toMap
      val yesOutputsGrouped = yesOutputs.groupBy(_._1.status).view.mapValues(v => v.size).toMap

      logger.info(
        s"will process ${noOutputs.size} workflow(s) without outputs ($noOutputsGrouped) and ${yesOutputs.size} workflow(s) with outputs ($yesOutputsGrouped) for submission $submissionId"
      )

      /**
        * Reusable function for looping over a group of workflows, using IO to ensure operations happen sequentially,
        * and calling processWorkflow() for each workflow in the group
        * @param workflowGroup the workflows to process
        * @param tracingLabel human-readable label for tracing purposes
        * @return nothing interesting
        */
      def processMultipleWorkflows(
        workflowGroup: Seq[(WorkflowRecord, Option[ExecutionServiceOutputs])],
        tracingLabel: String
      ) =
        traceFutureWithParent(tracingLabel, rootContext) { innerContext =>
          setTraceSpanAttribute(innerContext,
                                AttributeKey.longKey("numWorkflows"),
                                java.lang.Long.valueOf(workflowGroup.size)
          )
          workflowGroup
            .traverse { case (workflowRec, outputsOption) =>
              IO.fromFuture(IO(processWorkflow(workflowRec, outputsOption, innerContext)))
            }
            .unsafeToFuture()
        }

      for {
        // process all the workflows that do NOT have outputs
        _ <- processMultipleWorkflows(noOutputs, "workflowsWithoutOutputs")
        // then, process all the workflows that DO have outputs
        _ <- processMultipleWorkflows(yesOutputs, "workflowsWithOutputs")
        // finally, after processing all workflows, check if the submission as a whole
        // can be completed
        statusCheckComplete <- datasource.inTransaction { dataAccess =>
          // update submission after workflows are updated
          updateSubmissionStatus(dataAccess) map { shouldStop: Boolean =>
            // return a message about whether our submission is done entirely
            StatusCheckComplete(shouldStop)
          }
        }
      } yield statusCheckComplete
    }

  private def markWorkflowFailed(workflowRecord: WorkflowRecord, fatal: RawlsFatalExceptionWithErrorReport) = {
    logger.error(
      s"Marking workflow ${externalId(workflowRecord)} as failed handling outputs in $submissionId with user-visible reason ${fatal.toString})"
    )
    datasource.inTransaction { dataAccess =>
      dataAccess.workflowQuery.updateStatus(workflowRecord, WorkflowStatuses.Failed) andThen
        dataAccess.workflowQuery.saveMessages(Seq(AttributeString(fatal.toString)), workflowRecord.id)
    }
  }

  private def externalId(workflowRecord: WorkflowRecord): String = workflowRecord.externalId.getOrElse("?")

  private def processWorkflow(workflowRec: WorkflowRecord,
                              execServiceOutputsOption: Option[ExecutionServiceOutputs],
                              tracingContext: RawlsTracingContext
  )(implicit
    executionContext: ExecutionContext
  ) =
    // In an independent transaction,
    //  - attach the outputs for this workflow, if any outputs exist
    //  - update the status of the workflow in Rawls to match what Cromwell says
    // If attaching outputs fails for legit reasons (e.g. they're missing), it will mark the workflow as failed. This is correct.
    // If attaching outputs throws an exception (because e.g. deadlock or ConcurrentModificationException), the status will remain un-updated
    // and will be re-processed next time we call queryForWorkflowStatus().
    // This is why it's important to attach the outputs before updating the status -- if you update the status to Successful first, and the attach
    // outputs fails, we'll stop querying for the workflow status and never attach the outputs.
    datasource
      .inTransactionWithAttrTempTable { dataAccess =>
        (execServiceOutputsOption match {
          case Some(execServiceOutputs) =>
            // this workflow has Cromwell outputs. Persist those outputs.
            handleOutputs(Seq((workflowRec, execServiceOutputs)), dataAccess, tracingContext)
          case None => DBIO.successful(())
        }).flatMap { _ =>
          for {
            // refetch the workflow record; this ensures that 1) its record version is up to date, and 2) that we can
            // check its status to see if handleOutputs marked it as failed
            currentRec <- dataAccess.workflowQuery.findWorkflowById(workflowRec.id).result.head
            // No need to update statuses for any workflows that are in terminal statuses.
            // Doing so would potentially overwrite them with the execution service status if they'd been marked as failed by handleOutputs.
            doRecordUpdate = !WorkflowStatuses.terminalStatuses.contains(WorkflowStatuses.withName(currentRec.status))
            numRowsUpdated <-
              if (doRecordUpdate) {
                for {
                  updateResult <- dataAccess.workflowQuery.updateStatus(currentRec,
                                                                        WorkflowStatuses.withName(workflowRec.status)
                  )
                  _ = logger.info(
                    s"workflow ${externalId(currentRec)} status change ${currentRec.status} -> ${workflowRec.status} in submission ${submissionId}"
                  )
                } yield updateResult
              } else DBIO.successful(0)
          } yield numRowsUpdated
        }
      }
      .recoverWith {
        // fatal error writing outputs for this workflow. Mark it as failed.
        case fatal: RawlsFatalExceptionWithErrorReport =>
          markWorkflowFailed(workflowRec, fatal)
      }

  private def toThurloeNotification(submission: Submission,
                                    workspaceName: WorkspaceName,
                                    finalStatus: SubmissionStatus,
                                    recipientUserId: WorkbenchUserId
  ): Option[Notification] = {
    val methodConfigFullName =
      s"${submission.methodConfigurationNamespace}/${submission.methodConfigurationName}" // Format: myConfigNamespace/myConfigName
    val dataEntity = submission.submissionEntity.fold("N/A")(entity =>
      s"${entity.entityName} (${entity.entityType})"
    ) // Format: my_sample (sample)
    val hasFailedWorkflows = submission.workflows.exists(_.status.equals(WorkflowStatuses.Failed))
    val notificationWorkspaceName = Notifications.WorkspaceName(workspaceName.namespace, workspaceName.name)
    val userComment = submission.userComment.getOrElse("N/A")

    finalStatus match {
      case SubmissionStatuses.Aborted =>
        Some(
          AbortedSubmissionNotification(
            recipientUserId,
            notificationWorkspaceName,
            submissionId.toString,
            submission.submissionDate.toString,
            methodConfigFullName,
            dataEntity,
            submission.workflows.size.toLong,
            userComment
          )
        )
      case SubmissionStatuses.Done if hasFailedWorkflows =>
        Some(
          FailedSubmissionNotification(
            recipientUserId,
            notificationWorkspaceName,
            submissionId.toString,
            submission.submissionDate.toString,
            methodConfigFullName,
            dataEntity,
            submission.workflows.size.toLong,
            userComment
          )
        )
      case SubmissionStatuses.Done if !hasFailedWorkflows =>
        Some(
          SuccessfulSubmissionNotification(
            recipientUserId,
            notificationWorkspaceName,
            submissionId.toString,
            submission.submissionDate.toString,
            methodConfigFullName,
            dataEntity,
            submission.workflows.size.toLong,
            userComment
          )
        )
      case _ =>
        logger.info(
          s"Unable to send terminal submission notification for ${submissionId}. State was unexpected: status: ${finalStatus}, hasFailedWorkflows: ${hasFailedWorkflows}"
        )
        None
    }
  }

  private def sendTerminalSubmissionNotification(submissionId: UUID, finalStatus: SubmissionStatus)(implicit
    executionContext: ExecutionContext
  ): ReadAction[Unit] =
    datasource.slickDataSource.dataAccess.submissionQuery.loadSubmission(submissionId) flatMap {
      case Some(submission) =>
        // This Sam lookup is a bit unfortunate. Rawls only stores the submitter email address for the submission, but Thurloe
        // requires their googleSubjectId in order to look up the contact email (which may differ from their account email).
        // Because all submission monitoring happens asynchronously, we also don't have their subject ID on-hand to use.
        // Additionally, the fact that this is the googleSubjectId and not the userSubjectId will pose some challenges for
        // the multicloud world, where not every user will have a googleSubjectId.
        DBIO.from(samDAO.getUserIdInfoForEmail(submission.submitter)) map { userIdInfo =>
          userIdInfo.googleSubjectId match {
            case Some(googleSubjectId) =>
              toThurloeNotification(submission, workspaceName, finalStatus, WorkbenchUserId(googleSubjectId)).fold()(
                notification => notificationDAO.fireAndForgetNotification(notification)
              )
            case None =>
              logger.info(
                s"Submitter does not have a googleSubjectId. Will not send an email notification for submission ${submissionId}."
              )
          }
        }
      case None =>
        logger.info(s"Unable to send terminal submission notification for ${submissionId}. Submission not found.")
        DBIO.successful()
    }

  /**
   * When there are no workflows with a running or queued status, mark the submission as done or aborted as appropriate.
    *
    * @param dataAccess
   * @param executionContext
   * @return true if the submission is done/aborted
   */
  def updateSubmissionStatus(
    dataAccess: DataAccess
  )(implicit executionContext: ExecutionContext): ReadWriteAction[Boolean] =
    dataAccess.workflowQuery.listWorkflowRecsForSubmissionAndStatuses(
      submissionId,
      (WorkflowStatuses.queuedStatuses ++ WorkflowStatuses.runningStatuses): _*
    ) flatMap { workflowRecs =>
      if (workflowRecs.isEmpty) {
        dataAccess.submissionQuery.findById(submissionId).map(_.status).result.head.flatMap { status =>
          val finalStatus = SubmissionStatuses.withName(status) match {
            case SubmissionStatuses.Aborting => SubmissionStatuses.Aborted
            case _                           => SubmissionStatuses.Done
          }
          if (config.enableEmailNotifications)
            sendTerminalSubmissionNotification(submissionId, finalStatus).map(_ => finalStatus)
          else DBIO.successful(finalStatus)
        } flatMap { newStatus =>
          logger.debug(s"submission $submissionId terminating to status $newStatus")
          dataAccess.submissionQuery.updateStatus(submissionId, newStatus)
        } map (_ => true)
      } else {
        DBIO.successful(false)
      }
    }

  def handleOutputs(workflowsWithOutputs: Seq[(WorkflowRecord, ExecutionServiceOutputs)],
                    dataAccess: DataAccess,
                    tracingContext: RawlsTracingContext
  )(implicit
    executionContext: ExecutionContext
  ): ReadWriteAction[Unit] =
    if (workflowsWithOutputs.isEmpty) {
      DBIO.successful(())
    } else {
      traceDBIOWithParent("handleOutputs", tracingContext) { rootSpan =>
        setTraceSpanAttribute(rootSpan, AttributeKey.stringKey("submissionId"), submissionId.toString)
        setTraceSpanAttribute(rootSpan,
                              AttributeKey.longKey("numWorkflowsWithOutputs"),
                              java.lang.Long.valueOf(workflowsWithOutputs.length)
        )

        for {
          // load all the starting data
          workspace <- traceDBIOWithParent("getWorkspace", rootSpan)(_ => getWorkspace(dataAccess)).map(
            _.getOrElse(throw new RawlsException(s"workspace for submission $submissionId not found"))
          )
          entitiesById <- traceDBIOWithParent("listWorkflowEntitiesById", rootSpan)(_ =>
            listWorkflowEntitiesById(workspace, workflowsWithOutputs, dataAccess)
          )
          outputExpressionMap <- traceDBIOWithParent("listMethodConfigOutputsForSubmission", rootSpan)(_ =>
            listMethodConfigOutputsForSubmission(dataAccess)
          )
          emptyOutputs <- traceDBIOWithParent("getSubmissionEmptyOutputParam", rootSpan)(_ =>
            getSubmissionEmptyOutputParam(dataAccess)
          )

          // figure out the updates that need to occur to entities and workspaces
          updatedEntitiesAndWorkspace = attachOutputs(workspace,
                                                      workflowsWithOutputs,
                                                      entitiesById,
                                                      outputExpressionMap,
                                                      emptyOutputs
          )

          // for debugging purposes
          workspacesToUpdate = updatedEntitiesAndWorkspace.collect { case Left((_, Some(workspace))) => workspace }
          entityUpdates = updatedEntitiesAndWorkspace.collect {
            case Left((Some(entityUpdate), _)) if entityUpdate.upserts.nonEmpty => entityUpdate
          }
          _ =
            if (workspacesToUpdate.nonEmpty && entityUpdates.nonEmpty)
              logger.info("handleOutputs writing to both workspace and entity attributes")
            else if (workspacesToUpdate.nonEmpty)
              logger.info("handleOutputs writing to workspace attributes only")
            else if (entityUpdates.nonEmpty)
              logger.info("handleOutputs writing to entity attributes only")
            else
              logger.info("handleOutputs writing to neither workspace nor entity attributes; could be errors")

          // save everything to the db
          _ <- traceDBIOWithParent("saveWorkspace", rootSpan)(_ =>
            saveWorkspace(dataAccess, updatedEntitiesAndWorkspace)
          )
          _ <- saveEntities(dataAccess, workspace, updatedEntitiesAndWorkspace, rootSpan) // has its own tracing
          _ <- traceDBIOWithParent("saveErrors", rootSpan)(_ =>
            saveErrors(updatedEntitiesAndWorkspace.collect { case Right(errors) => errors }, dataAccess)
          )
        } yield ()
      }
    }

  def getWorkspace(dataAccess: DataAccess): ReadAction[Option[Workspace]] =
    dataAccess.workspaceQuery.findByName(workspaceName)

  def listMethodConfigOutputsForSubmission(dataAccess: DataAccess): ReadAction[Map[String, String]] =
    dataAccess.submissionQuery.getMethodConfigOutputExpressions(submissionId)

  def getSubmissionEmptyOutputParam(dataAccess: DataAccess): ReadAction[Boolean] =
    dataAccess.submissionQuery.getEmptyOutputParam(submissionId)

  def listWorkflowEntitiesById(workspace: Workspace,
                               workflowsWithOutputs: Seq[(WorkflowRecord, ExecutionServiceOutputs)],
                               dataAccess: DataAccess
  )(implicit executionContext: ExecutionContext): ReadAction[scala.collection.Map[Long, Entity]] = {
    // Note that we can't look up entities for workflows that didn't run on entities (obviously), so they get dropped here.
    // Those are handled in handle/attachOutputs.
    val workflowsWithEntities = workflowsWithOutputs.filter(wwo => wwo._1.workflowEntityId.isDefined)

    // yank out of Seq[(Long, Option[Entity])] and into Seq[(Long, Entity)]
    val entityIds = workflowsWithEntities.flatMap { case (workflowRec, outputs) => workflowRec.workflowEntityId }

    dataAccess.entityQuery.getEntities(workspace.workspaceIdAsUUID, entityIds).map(_.toMap)
  }

  def saveWorkspace(
    dataAccess: DataAccess,
    updatedEntitiesAndWorkspace: Seq[
      Either[(Option[WorkflowEntityUpdate], Option[Workspace]), (WorkflowRecord, scala.Seq[AttributeString])]
    ]
  ) = {
    // note there is only 1 workspace (may be None if it is not updated) even though it may be updated multiple times so reduce it into 1 update
    val workspaces = updatedEntitiesAndWorkspace.collect { case Left((_, Some(workspace))) => workspace }
    if (workspaces.isEmpty) DBIO.successful(0)
    else
      dataAccess.workspaceQuery.createOrUpdate(
        workspaces.reduce((a, b) => a.copy(attributes = a.attributes ++ b.attributes))
      )
  }

  def saveEntities(
    dataAccess: DataAccess,
    workspace: Workspace,
    updatedEntitiesAndWorkspace: Seq[
      Either[(Option[WorkflowEntityUpdate], Option[Workspace]), (WorkflowRecord, scala.Seq[AttributeString])]
    ],
    tracingContext: RawlsTracingContext
  )(implicit executionContext: ExecutionContext) =
    traceDBIOWithParent("saveEntities", tracingContext) { span =>
      val entityUpdates = updatedEntitiesAndWorkspace.collect {
        case Left((Some(entityUpdate), _)) if entityUpdate.upserts.nonEmpty => entityUpdate
      }
      setTraceSpanAttribute(span,
                            AttributeKey.longKey("numEntityUpdates"),
                            java.lang.Long.valueOf(entityUpdates.length)
      )
      if (entityUpdates.isEmpty) {
        DBIO.successful(())
      } else {
        traceDBIOWithParent("saveEntityPatchSequence", span) { innerSpan =>
          DBIO.sequence(entityUpdates map { entityUpd =>
            dataAccess.entityQuery
              .saveEntityPatch(workspace, entityUpd.entityRef, entityUpd.upserts, Seq(), innerSpan)
              .withStatementParameters(statementInit = _.setQueryTimeout(queryTimeout.toSeconds.toInt))
          })
        }
      }
    }

  private def attributeIsEmpty(attribute: Attribute): Boolean =
    attribute match {
      case AttributeNull       => true
      case AttributeString("") => true
      case _                   => false
    }

  def attachOutputs(workspace: Workspace,
                    workflowsWithOutputs: Seq[(WorkflowRecord, ExecutionServiceOutputs)],
                    entitiesById: scala.collection.Map[Long, Entity],
                    outputExpressionMap: Map[String, String],
                    ignoreEmptyOutputs: Boolean
  ): Seq[Either[(Option[WorkflowEntityUpdate], Option[Workspace]), (WorkflowRecord, Seq[AttributeString])]] =
    workflowsWithOutputs.map { case (workflowRecord, outputsResponse) =>
      val outputs = outputsResponse.outputs
      logger.debug(
        s"attaching outputs for ${submissionId.toString}/${workflowRecord.externalId
            .getOrElse("MISSING_WORKFLOW")}: ${outputExpressionMap.size} expressions, ${outputs.size} attribute values"
      )

      val parsedExpressions: Seq[Try[OutputExpression]] = outputExpressionMap.map { case (outputName, outputExprStr) =>
        outputs.get(outputName) match {
          case None => Failure(new RawlsException(s"output named ${outputName} does not exist"))
          case Some(Right(uot: UnsupportedOutputType)) =>
            Failure(
              new RawlsException(
                s"output named ${outputName} is not a supported type, received json u${uot.json.compactPrint}"
              )
            )
          case Some(Left(output)) =>
            val entityTypeOpt = workflowRecord.workflowEntityId.flatMap(entitiesById.get).map(_.entityType)
            OutputExpression.build(outputExprStr, output, entityTypeOpt)
        }
      }.toSeq

      if (parsedExpressions.forall(_.isSuccess)) {
        val boundExpressions: Seq[BoundOutputExpression] = parsedExpressions.collect {
          case Success(boe @ BoundOutputExpression(target, name, attr))
              if !(attributeIsEmpty(attr) && ignoreEmptyOutputs) =>
            boe
        }
        val updates =
          updateEntityAndWorkspace(workflowRecord.workflowEntityId.map(id => Some(entitiesById(id))).getOrElse(None),
                                   workspace,
                                   boundExpressions
          )

        val (optEntityUpdates, optWs) = updates

        val entityAttributeCount = optEntityUpdates map { update: WorkflowEntityUpdate =>
          val cnt = attributeCount(update.upserts.values)
          logger.trace(
            s"Updating $cnt attribute values for entity ${update.entityRef.entityName} of type ${update.entityRef.entityType} in ${submissionId.toString}/${workflowRecord.externalId
                .getOrElse("MISSING_WORKFLOW")}. ${safePrint(update.upserts)}"
          )
          cnt
        } getOrElse 0

        val workspaceAttributeCount = optWs map { workspace: Workspace =>
          val cnt = attributeCount(workspace.attributes.values)
          logger.trace(
            s"Updating $cnt attribute values for workspace in ${submissionId.toString}/${workflowRecord.externalId
                .getOrElse("MISSING_WORKFLOW")}. ${safePrint(workspace.attributes)}"
          )
          cnt
        } getOrElse 0

        if (entityAttributeCount > config.attributeUpdatesPerWorkflow) {
          logger.error(
            s"Cancelled update of $entityAttributeCount entity attributes for workflow ${workflowRecord.externalId.getOrElse("MISSING_WORKFLOW")}."
          )
          Right(
            (workflowRecord,
             Seq(
               AttributeString(
                 s"Cannot save outputs to entity because workflow's attribute count of $entityAttributeCount exceeds Terra maximum of ${config.attributeUpdatesPerWorkflow}."
               )
             )
            )
          )
        } else if (workspaceAttributeCount > config.attributeUpdatesPerWorkflow) {
          logger.error(
            s"Cancelled update of $workspaceAttributeCount workspace attributes for workflow ${workflowRecord.externalId
                .getOrElse("MISSING_WORKFLOW")}."
          )
          Right(
            (workflowRecord,
             Seq(
               AttributeString(
                 s"Cannot save outputs to workspace because workflow's attribute count of $workspaceAttributeCount exceeds Terra maximum of ${config.attributeUpdatesPerWorkflow}."
               )
             )
            )
          )
        } else {
          Left(updates)
        }
      } else {
        Right((workflowRecord, parsedExpressions.collect { case Failure(t) => AttributeString(t.getMessage) }))
      }
    }

  private def updateEntityAndWorkspace(entity: Option[Entity],
                                       workspace: Workspace,
                                       workflowOutputs: Iterable[BoundOutputExpression]
  ): (Option[WorkflowEntityUpdate], Option[Workspace]) = {
    val entityUpsert = workflowOutputs.collect { case BoundOutputExpression(ThisEntityTarget, attrName, attr) =>
      (attrName, attr)
    }
    val workspaceAttributes = workflowOutputs.collect { case BoundOutputExpression(WorkspaceTarget, attrName, attr) =>
      (attrName, attr)
    }

    if (entity.isEmpty && entityUpsert.nonEmpty) {
      // TODO: we shouldn't ever run into this case, but if we somehow do, it'll make the submission actor restart forever
      throw new RawlsException("how am I supposed to bind expressions to a nonexistent entity??!!")
    }

    val entityAndUpsert = entity.map(e => WorkflowEntityUpdate(e.toReference, entityUpsert.toMap))
    val updatedWorkspace =
      if (workspaceAttributes.isEmpty) None
      else Option(workspace.copy(attributes = workspace.attributes ++ workspaceAttributes))

    (entityAndUpsert, updatedWorkspace)
  }

  def saveErrors(errors: Seq[(WorkflowRecord, Seq[AttributeString])], dataAccess: DataAccess) =
    DBIO.sequence(errors.map { case (workflowRecord, errorMessages) =>
      dataAccess.workflowQuery.updateStatus(workflowRecord, WorkflowStatuses.Failed) andThen
        dataAccess.workflowQuery.saveMessages(errorMessages, workflowRecord.id)
    })

  def checkCurrentWorkflowStatusCounts(
    reschedule: Boolean
  )(implicit executionContext: ExecutionContext): Future[SaveCurrentWorkflowStatusCounts] =
    datasource
      .inTransaction { dataAccess =>
        for {
          wfStatuses <- dataAccess.workflowQuery.countWorkflowsForSubmissionByQueueStatus(submissionId)
          workspace <- getWorkspace(dataAccess).map(
            _.getOrElse(throw new RawlsException(s"workspace for submission $submissionId not found"))
          )
          subStatuses <- dataAccess.submissionQuery.countByStatus(workspace)
        } yield {
          val workflowStatuses = wfStatuses.map { case (k, v) => WorkflowStatuses.withName(k) -> v }
          val submissionStatuses = subStatuses.map { case (k, v) => SubmissionStatuses.withName(k) -> v }
          (workflowStatuses, submissionStatuses)
        }
      }
      .recover { case NonFatal(e) =>
        // Recover on errors since this just affects metrics and we don't want it to blow up the whole actor if it fails
        logger.error("Error occurred checking current workflow status counts", e)
        (Map.empty[WorkflowStatus, Int], Map.empty[SubmissionStatus, Int])
      }
      .map { case (wfCounts, subCounts) =>
        SaveCurrentWorkflowStatusCounts(workspaceName, submissionId, wfCounts, subCounts, reschedule)
      }
}

final case class SubmissionMonitorConfig(submissionPollInterval: FiniteDuration,
                                         submissionPollExpiration: FiniteDuration,
                                         trackDetailedSubmissionMetrics: Boolean,
                                         attributeUpdatesPerWorkflow: Int,
                                         enableEmailNotifications: Boolean
)
