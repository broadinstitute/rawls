package org.broadinstitute.dsde.rawls.submissions

import akka.http.scaladsl.model.StatusCodes
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.config.WorkspaceServiceConfig
import org.broadinstitute.dsde.rawls.dataaccess.slick.{DataAccess, ReadWriteAction, WorkflowRecord}
import org.broadinstitute.dsde.rawls.{NoSuchWorkspaceException, RawlsExceptionWithErrorReport, StringValidationUtils}
import org.broadinstitute.dsde.rawls.dataaccess.{
  ExecutionServiceCluster,
  ExecutionServiceDAO,
  ExecutionServiceId,
  GoogleServicesDAO,
  MethodRepoDAO,
  SamDAO,
  SlickDataSource,
  SubmissionCostService
}
import org.broadinstitute.dsde.rawls.entities.base.ExpressionEvaluationSupport.LookupExpression
import org.broadinstitute.dsde.rawls.entities.{EntityManager, EntityRequestArguments}
import org.broadinstitute.dsde.rawls.entities.base.{EntityProvider, ExpressionEvaluationContext}
import org.broadinstitute.dsde.rawls.expressions.ExpressionEvaluator
import org.broadinstitute.dsde.rawls.genomics.GenomicsService
import org.broadinstitute.dsde.rawls.jobexec.MethodConfigResolver
import org.broadinstitute.dsde.rawls.jobexec.MethodConfigResolver.GatherInputsResult
import org.broadinstitute.dsde.rawls.methods.MethodConfigurationUtils
import org.broadinstitute.dsde.rawls.metrics.RawlsInstrumented
import org.broadinstitute.dsde.rawls.model.WorkflowFailureModes.WorkflowFailureMode
import org.broadinstitute.dsde.rawls.model.WorkflowStatuses.WorkflowStatus
import org.broadinstitute.dsde.rawls.model.{
  ActiveSubmission,
  AttributeEntityReference,
  AttributeString,
  AttributeValue,
  ErrorReport,
  ErrorReportSource,
  ExecutionServiceLogs,
  ExecutionServiceOutputs,
  ExternalEntityInfo,
  MetadataParams,
  MethodConfiguration,
  PreparedSubmission,
  RawlsBillingProject,
  RawlsBillingProjectName,
  RawlsRequestContext,
  RetriedSubmissionReport,
  SamWorkspaceActions,
  Submission,
  SubmissionListResponse,
  SubmissionReport,
  SubmissionRequest,
  SubmissionRetry,
  SubmissionStatuses,
  SubmissionValidationEntityInputs,
  SubmissionValidationHeader,
  SubmissionValidationInput,
  SubmissionValidationReport,
  TaskOutput,
  UserCommentUpdateOperation,
  Workflow,
  WorkflowCost,
  WorkflowFailureModes,
  WorkflowOutputs,
  WorkflowQueueStatusByUserResponse,
  WorkflowQueueStatusResponse,
  WorkflowStatuses,
  Workspace,
  WorkspaceAttributeSpecs,
  WorkspaceName
}
import org.broadinstitute.dsde.rawls.submissions.SubmissionsService.{
  extractOperationIdsFromCromwellMetadata,
  getTerminalStatusDate,
  submissionRootPath
}
import org.broadinstitute.dsde.rawls.util.{FutureSupport, RoleSupport, WorkspaceSupport}
import org.broadinstitute.dsde.rawls.util.TracingUtils.traceFutureWithParent
import org.broadinstitute.dsde.rawls.workspace.{WorkspaceRepository, WorkspaceService}
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.broadinstitute.dsde.workbench.util.FutureSupport.toFutureTry
import org.joda.time.DateTime
import slick.jdbc.TransactionIsolation
import spray.json.DefaultJsonProtocol._
import spray.json.JsObject

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

object SubmissionsService {
  def constructor(
    dataSource: SlickDataSource,
    entityManager: EntityManager,
    methodRepoDAO: MethodRepoDAO,
    cromiamDAO: ExecutionServiceDAO,
    executionServiceCluster: ExecutionServiceCluster,
    methodConfigResolver: MethodConfigResolver,
    gcsDAO: GoogleServicesDAO,
    samDAO: SamDAO,
    maxActiveWorkflowsTotal: Int,
    maxActiveWorkflowsPerUser: Int,
    workbenchMetricBaseName: String,
    submissionCostService: SubmissionCostService,
    genomicsServiceConstructor: RawlsRequestContext => GenomicsService,
    config: WorkspaceServiceConfig,
    workspaceRepository: WorkspaceRepository
  )(
    ctx: RawlsRequestContext
  )(implicit executionContext: ExecutionContext): SubmissionsService =
    new SubmissionsService(
      ctx,
      dataSource,
      entityManager,
      methodRepoDAO,
      cromiamDAO,
      executionServiceCluster,
      methodConfigResolver,
      gcsDAO,
      samDAO,
      maxActiveWorkflowsTotal,
      maxActiveWorkflowsPerUser,
      workbenchMetricBaseName,
      submissionCostService,
      genomicsServiceConstructor,
      config,
      workspaceRepository
    )

  def extractOperationIdsFromCromwellMetadata(metadataJson: JsObject): Iterable[String] = {
    case class Call(jobId: Option[String])
    case class OpMetadata(calls: Option[Map[String, Seq[Call]]])
    implicit val callFormat = jsonFormat1(Call)
    implicit val opMetadataFormat = jsonFormat1(OpMetadata)

    for {
      calls <- metadataJson
        .convertTo[OpMetadata]
        .calls
        .toList // toList on the Option makes the compiler like the for comp
      call <- calls.values.flatten
      jobId <- call.jobId
    } yield jobId
  }

  def getTerminalStatusDate(submission: Submission, workflowID: Option[String]): Option[DateTime] = {
    // find all workflows that have finished
    val terminalWorkflows =
      submission.workflows.filter(workflow => WorkflowStatuses.terminalStatuses.contains(workflow.status))
    // optionally limit the list to a specific workflowID
    val workflows = workflowID match {
      case Some(_) => terminalWorkflows.filter(_.workflowId == workflowID)
      case None    => terminalWorkflows
    }
    if (workflows.isEmpty) {
      None
    } else {
      // use the latest date the workflow(s) reached a terminal status
      Option(workflows.map(_.statusLastChangedDate).maxBy(_.getMillis))
    }
  }

  def submissionRootPath(workspace: Workspace, id: UUID): String =
    // Intermediate/final output separation: location 1/2 (SU-166, WX-1702)
    // All intermediate files including logs live here.
    // UI links to execution directory point here.
    // Temporarily paused as of 2024-09-05
    s"gs://${workspace.bucketName}/submissions/$id"
}

class SubmissionsService(
  protected val ctx: RawlsRequestContext,
  val dataSource: SlickDataSource,
  val entityManager: EntityManager,
  val methodRepoDAO: MethodRepoDAO,
  val cromiamDAO: ExecutionServiceDAO,
  executionServiceCluster: ExecutionServiceCluster,
  val methodConfigResolver: MethodConfigResolver,
  protected val gcsDAO: GoogleServicesDAO,
  val samDAO: SamDAO,
  maxActiveWorkflowsTotal: Int,
  maxActiveWorkflowsPerUser: Int,
  override val workbenchMetricBaseName: String,
  submissionCostService: SubmissionCostService,
  val genomicsServiceConstructor: RawlsRequestContext => GenomicsService,
  config: WorkspaceServiceConfig,
  val workspaceRepository: WorkspaceRepository
)(implicit protected val executionContext: ExecutionContext)
    extends RoleSupport
    with FutureSupport
    with LazyLogging
    with RawlsInstrumented
    with WorkspaceSupport
    with StringValidationUtils {

  implicit val errorReportSource: ErrorReportSource = ErrorReportSource("rawls")

  import dataSource.dataAccess.driver.api._

  // Note: this limit is also hard-coded in the terra-ui code to allow client-side validation.
  // If it is changed, it must also be updated in that repository.
  private val UserCommentMaxLength: Int = 1000

  def getGenomicsOperationV2(workflowId: String, operationId: List[String]): Future[Option[JsObject]] =
    // note that cromiam should only give back metadata if the user is authorized to see it
    cromiamDAO.callLevelMetadata(workflowId, MetadataParams(includeKeys = Set("jobId")), ctx.userInfo).flatMap {
      metadataJson =>
        val operationIds: Iterable[String] = extractOperationIdsFromCromwellMetadata(metadataJson)

        val operationIdString = operationId.mkString("/")
        // check that the requested operation id actually exists in the workflow
        if (operationIds.toList.contains(operationIdString)) {
          val genomicsServiceRef = genomicsServiceConstructor(ctx)
          genomicsServiceRef.getOperation(operationIdString)
        } else {
          Future.failed(
            new RawlsExceptionWithErrorReport(
              ErrorReport(StatusCodes.NotFound, s"operation id ${operationIdString} not found in workflow $workflowId")
            )
          )
        }
    }

  def workflowMetadata(workspaceName: WorkspaceName,
                       submissionId: String,
                       workflowId: String,
                       metadataParams: MetadataParams
  ): Future[JsObject] = {

    // two possibilities here:
    //
    // (classic case) if the workflow is a top-level workflow of a submission, it has a row in the DB and an
    // association with a specific execution service shard.  Use the DB to verify the submission association and retrieve
    // the execution service identifier.
    //
    // if it's a subworkflow (or sub-sub-workflow, etc) it's not present in the Rawls DB and we don't know which
    // execution service shard has processed it.  Query all* execution service shards for the workflow to learn its
    // submission association and which shard processed it.
    //
    // * in practice, one shard does everything except for some older workflows on shard 2.  Revisit this if that changes!

    // determine which case this is, and close the DB transaction
    val execIdFutOpt: Future[Option[ExecutionServiceId]] =
      getV2WorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.read) flatMap { workspaceContext =>
        dataSource.inTransaction { dataAccess =>
          withSubmissionAndWorkflowExecutionServiceKey(workspaceContext, submissionId, workflowId, dataAccess) {
            optExecKey =>
              DBIO.successful(optExecKey)
          }
        }
      }

    // query the execution service(s) for the metadata
    execIdFutOpt flatMap {
      executionServiceCluster.callLevelMetadata(submissionId, workflowId, metadataParams, _, ctx.userInfo)
    }
  }

  def workflowQueueStatus(): Future[WorkflowQueueStatusResponse] =
    dataSource.inTransaction { dataAccess =>
      dataAccess.workflowQuery.countWorkflowsByQueueStatus.flatMap { statusMap =>
        // determine the current size of the workflow queue
        statusMap.get(WorkflowStatuses.Queued.toString) match {
          case Some(x) if x > 0 =>
            for {
              timeEstimate <- dataAccess.workflowAuditStatusQuery.queueTimeMostRecentSubmittedWorkflow
              workflowsAhead <- dataAccess.workflowQuery.countWorkflowsAheadOfUserInQueue(ctx.userInfo)
            } yield WorkflowQueueStatusResponse(timeEstimate, workflowsAhead, statusMap)
          case _ => DBIO.successful(WorkflowQueueStatusResponse(0, 0, statusMap))
        }
      }
    }

  def getSubmissionMethodConfiguration(workspaceName: WorkspaceName,
                                       submissionId: String
  ): Future[MethodConfiguration] =
    getV2WorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.read) flatMap { workspaceContext =>
      dataSource.inTransaction { dataAccess =>
        dataAccess.submissionQuery
          .getSubmissionMethodConfigId(workspaceContext, UUID.fromString(submissionId))
          .flatMap { id =>
            id match {
              case Some(id) =>
                dataAccess.methodConfigurationQuery.get(id).map {
                  case Some(methodConfig) => methodConfig
                  case None =>
                    throw new RawlsExceptionWithErrorReport(
                      ErrorReport(StatusCodes.NotFound,
                                  s"The method configuration for submission ${submissionId} could not be found."
                      )
                    )
                }
              case None =>
                throw new RawlsExceptionWithErrorReport(
                  ErrorReport(StatusCodes.NotFound,
                              s"The method configuration for submission ${submissionId} could not be found."
                  )
                )
            }
          }
      }
    }

  // Admin endpoint, not limited to V2 workspaces
  def adminWorkflowQueueStatusByUser(): Future[WorkflowQueueStatusByUserResponse] =
    asFCAdmin {
      dataSource.inTransaction(
        dataAccess =>
          for {
            global <- dataAccess.workflowQuery.countWorkflowsByQueueStatus
            perUser <- dataAccess.workflowQuery.countWorkflowsByQueueStatusByUser
          } yield WorkflowQueueStatusByUserResponse(global,
                                                    perUser,
                                                    maxActiveWorkflowsTotal,
                                                    maxActiveWorkflowsPerUser
          ),
        TransactionIsolation.ReadUncommitted
      )
    }

  // Admin endpoint, not limited to V2 workspaces
  def adminListAllActiveSubmissions(): Future[Seq[ActiveSubmission]] =
    asFCAdmin {
      dataSource.inTransaction { dataAccess =>
        dataAccess.submissionQuery.listAllActiveSubmissions()
      }
    }

  // Admin endpoint, not limited to V2 workspaces
  def adminAbortSubmission(workspaceName: WorkspaceName, submissionId: String): Future[Int] =
    asFCAdmin {
      dataSource.inTransaction { dataAccess =>
        withWorkspaceContext(workspaceName, dataAccess) { workspaceContext =>
          abortSubmission(workspaceContext, submissionId, dataAccess)
        }
      }
    }

  private def withWorkspaceContext[T](workspaceName: WorkspaceName,
                                      dataAccess: DataAccess,
                                      attributeSpecs: Option[WorkspaceAttributeSpecs] = None
  )(op: (Workspace) => ReadWriteAction[T]) =
    dataAccess.workspaceQuery.findByName(workspaceName, attributeSpecs) flatMap {
      case None            => throw NoSuchWorkspaceException(workspaceName)
      case Some(workspace) => op(workspace)
    }

  // retrieve the cost of this Workflow from BigQuery, if available
  def workflowCost(workspaceName: WorkspaceName, submissionId: String, workflowId: String): Future[WorkflowCost] = {

    // confirm: the user can Read this Workspace, the Submission is in this Workspace,
    // and the Workflow is in the Submission

    val execIdFutOpt = getV2WorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.read) flatMap {
      workspaceContext =>
        dataSource.inTransaction { dataAccess =>
          withSubmissionAndWorkflowExecutionServiceKey(workspaceContext, submissionId, workflowId, dataAccess) {
            optExecKey =>
              withSubmission(workspaceContext, submissionId, dataAccess) { submission =>
                DBIO.successful((optExecKey, submission, workspaceContext))
              }
          }
        }
    }

    for {
      (optExecId, submission, workspace) <- execIdFutOpt
      tableName <- getSpendReportTableName(RawlsBillingProjectName(workspaceName.namespace))

      // we don't need the Execution Service ID, but we do need to confirm the Workflow is in one for this Submission
      // if we weren't able to do so above
      _ <- executionServiceCluster.findExecService(submissionId, workflowId, ctx.userInfo, optExecId)
      submissionDoneDate = getTerminalStatusDate(submission, Option(workflowId))
      costs <- submissionCostService.getWorkflowCost(workflowId,
                                                     workspace.googleProjectId,
                                                     submission.submissionDate,
                                                     submissionDoneDate,
                                                     tableName
      )
    } yield WorkflowCost(workflowId, costs.get(workflowId))
  }

  def listSubmissions(workspaceName: WorkspaceName,
                      parentContext: RawlsRequestContext
  ): Future[Seq[SubmissionListResponse]] = {
    val costlessSubmissionsFuture =
      getV2WorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.read) flatMap { workspaceContext =>
        dataSource.inTransaction { dataAccess =>
          dataAccess.submissionQuery.listWithSubmitter(workspaceContext)
        }
      }

    // TODO David An 2018-05-30: temporarily disabling cost calculations for submission list due to potential performance hit
    // val costMapFuture = costlessSubmissionsFuture flatMap { submissions =>
    //   submissionCostService.getWorkflowCosts(submissions.flatMap(_.workflowIds).flatten, workspaceName.namespace)
    // }
    val costMapFuture = Future.successful(Map.empty[String, Float])

    toFutureTry(costMapFuture) flatMap { costMapTry =>
      val costMap: Map[String, Float] = costMapTry match {
        case Failure(ex) =>
          logger.error("Unable to get cost data from BigQuery", ex)
          Map()
        case Success(costs) => costs
      }

      traceFutureWithParent("costlessSubmissionsFuture", parentContext) { span =>
        costlessSubmissionsFuture map { costlessSubmissions =>
          val costedSubmissions = costlessSubmissions map { costlessSubmission =>
            // TODO David An 2018-05-30: temporarily disabling cost calculations for submission list due to potential performance hit
            // val summedCost = costlessSubmission.workflowIds.map { workflowIds => workflowIds.flatMap(costMap.get).sum }
            val summedCost = None
            // Clearing workflowIds is a quick fix to prevent SubmissionListResponse from having too much data. Will address in the near future.
            costlessSubmission.copy(cost = summedCost, workflowIds = None)
          }
          costedSubmissions
        }
      }
    }
  }

  def countSubmissions(workspaceName: WorkspaceName): Future[Map[String, Int]] =
    getV2WorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.read) flatMap { workspaceContext =>
      dataSource.inTransaction { dataAccess =>
        dataAccess.submissionQuery.countByStatus(workspaceContext)
      }
    }

  def getSubmissionStatus(workspaceName: WorkspaceName,
                          submissionId: String,
                          parentContext: RawlsRequestContext
  ): Future[Submission] = {
    val submissionWithoutCostsAndWorkspace =
      getV2WorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.read) flatMap { workspaceContext =>
        dataSource.inTransaction { dataAccess =>
          withSubmission(workspaceContext, submissionId, dataAccess) { submission =>
            DBIO.successful((submission, workspaceContext))
          }
        }
      }

    traceFutureWithParent("submissionWithoutCostsAndWorkspace", parentContext) { span =>
      submissionWithoutCostsAndWorkspace flatMap { case (submission, workspace) =>
        val allWorkflowIds: Seq[String] = submission.workflows.flatMap(_.workflowId)
        val submissionDoneDate: Option[DateTime] = getTerminalStatusDate(submission, None)

        getSpendReportTableName(RawlsBillingProjectName(workspaceName.namespace)) flatMap { tableName =>
          toFutureTry(
            submissionCostService.getSubmissionCosts(submissionId,
                                                     allWorkflowIds,
                                                     workspace.googleProjectId,
                                                     submission.submissionDate,
                                                     submissionDoneDate,
                                                     tableName
            )
          ) map {
            case Failure(ex) =>
              logger.error(s"Unable to get workflow costs for submission $submissionId", ex)
              submission
            case Success(costMap) =>
              val costedWorkflows = submission.workflows.map { workflow =>
                workflow.workflowId match {
                  case Some(wfId) => workflow.copy(cost = costMap.get(wfId))
                  case None       => workflow
                }
              }
              val costedSubmission = submission.copy(cost = Some(costMap.values.sum), workflows = costedWorkflows)
              costedSubmission
          }
        }
      }
    }
  }

  def retrySubmission(workspaceName: WorkspaceName,
                      submissionRetry: SubmissionRetry,
                      submissionId: String
  ): Future[RetriedSubmissionReport] =
    getV2WorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.write) flatMap { workspaceContext =>
      dataSource.inTransaction { dataAccess =>
        withSubmission(workspaceContext, submissionId, dataAccess) { submission =>
          val newSubmissionId = UUID.randomUUID()
          val filterWorkFlows = submissionRetry.retryType.filterWorkflows(submission.workflows)
          if (filterWorkFlows.isEmpty) {
            throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.BadRequest, "no workflows to retry"))
          }
          val filteredAndResetWorkflows = filterWorkFlows.map(wf =>
            wf.copy(workflowId = None, status = WorkflowStatuses.Queued, statusLastChangedDate = DateTime.now)
          )
          val newSubmission = submission.copy(
            submissionId = newSubmissionId.toString,
            submissionDate = DateTime.now(),
            submissionRoot = submissionRootPath(workspaceContext, newSubmissionId),
            workflows = filteredAndResetWorkflows,
            status = SubmissionStatuses.Submitted,
            userComment =
              Option(s"retry of submission ${submission.submissionId} with retry type ${submissionRetry.retryType}"),
            submitter = WorkbenchEmail(ctx.userInfo.userEmail.value)
          )

          for {
            retriedSub <- logAndCreateDbSubmission(workspaceContext, newSubmissionId, newSubmission, dataAccess)
          } yield RetriedSubmissionReport(
            submissionId,
            retriedSub.submissionId,
            retriedSub.submissionDate,
            retriedSub.submitter.value,
            retriedSub.status,
            submissionRetry.retryType,
            filteredAndResetWorkflows
          )
        }
      }
    }

  def createSubmission(workspaceName: WorkspaceName, submissionRequest: SubmissionRequest): Future[SubmissionReport] =
    for {
      ps <- prepareSubmission(workspaceName, submissionRequest)
      submission <- saveSubmission(
        ps.workspace,
        ps.id,
        submissionRequest,
        ps.submissionRoot,
        ps.inputs,
        ps.failureMode,
        ps.header
      )
    } yield SubmissionReport(
      submissionRequest,
      submission.submissionId,
      submission.submissionDate,
      ctx.userInfo.userEmail.value,
      submission.status,
      ps.header,
      ps.inputs.filter(_.inputResolutions.forall(_.error.isEmpty))
    )

  private def prepareSubmission(workspaceName: WorkspaceName,
                                submissionRequest: SubmissionRequest
  ): Future[PreparedSubmission] = {

    val submissionId: UUID = UUID.randomUUID()

    for {
      _ <- requireComputePermission(workspaceName)
      workspaceContext <- getV2WorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.write)
      methodConfigOption <- dataSource.inTransaction { dataAccess =>
        dataAccess.methodConfigurationQuery.get(workspaceContext,
                                                submissionRequest.methodConfigurationNamespace,
                                                submissionRequest.methodConfigurationName
        )
      }
      methodConfig = methodConfigOption.getOrElse(
        throw new RawlsExceptionWithErrorReport(
          errorReport = ErrorReport(
            StatusCodes.NotFound,
            s"${submissionRequest.methodConfigurationNamespace}/${submissionRequest.methodConfigurationName} does not exist in ${workspaceContext}"
          )
        )
      )
      _ = SubmissionRequestValidation.staticValidation(submissionRequest, methodConfig)

      entityProvider <- getEntityProviderForMethodConfig(workspaceContext, methodConfig)

      gatherInputsResult <- MethodConfigurationUtils.gatherMethodConfigInputs(ctx,
                                                                              methodRepoDAO,
                                                                              methodConfig,
                                                                              methodConfigResolver
      )

      _ = entityProvider.expressionValidator.validateExpressionsForSubmission(methodConfig, gatherInputsResult)

      methodConfigInputs = gatherInputsResult.processableInputs.map { methodInput =>
        SubmissionValidationInput(methodInput.workflowInput.getName, methodInput.expression)
      }
      header = SubmissionValidationHeader(methodConfig.rootEntityType, methodConfigInputs, entityProvider.entityStoreId)

      workspaceExpressionResults <- evaluateWorkspaceExpressions(workspaceContext, gatherInputsResult)
      submissionParameters <- entityProvider.evaluateExpressions(
        ExpressionEvaluationContext(submissionRequest.entityType,
                                    submissionRequest.entityName,
                                    submissionRequest.expression,
                                    methodConfig.rootEntityType
        ),
        gatherInputsResult,
        workspaceExpressionResults
      )
    } yield PreparedSubmission(
      workspaceContext,
      submissionId,
      submissionParameters,
      submissionRequest.workflowFailureMode.map(WorkflowFailureModes.withName),
      header,
      submissionRootPath(workspaceContext, submissionId)
    )
  }

  def updateSubmissionUserComment(workspaceName: WorkspaceName,
                                  submissionId: String,
                                  newComment: UserCommentUpdateOperation
  ): Future[Int] = {
    validateMaxStringLength(newComment.userComment, "userComment", UserCommentMaxLength)

    getV2WorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.write) flatMap { workspaceContext =>
      dataSource.inTransaction { dataAccess =>
        withSubmissionId(workspaceContext, submissionId, dataAccess) { submissionId =>
          dataAccess.submissionQuery.updateSubmissionUserComment(submissionId, newComment.userComment)
        }
      }
    }
  }

  def abortSubmission(workspaceName: WorkspaceName, submissionId: String): Future[Int] =
    getV2WorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.write) flatMap { workspaceContext =>
      dataSource.inTransaction { dataAccess =>
        abortSubmission(workspaceContext, submissionId, dataAccess)
      }
    }

  private def abortSubmission(workspaceContext: Workspace,
                              submissionId: String,
                              dataAccess: DataAccess
  ): ReadWriteAction[Int] =
    withSubmissionId(workspaceContext, submissionId, dataAccess) { submissionId =>
      // implicitly passed to SubmissionComponent.updateStatus
      implicit val subStatusCounter = submissionStatusCounter(workspaceMetricBuilder(workspaceContext.toWorkspaceName))
      dataAccess.submissionQuery.updateStatus(submissionId, SubmissionStatuses.Aborting)
    }

  private def withSubmission[T](workspaceContext: Workspace, submissionId: String, dataAccess: DataAccess)(
    op: (Submission) => ReadWriteAction[T]
  ): ReadWriteAction[T] =
    Try {
      UUID.fromString(submissionId)
    } match {
      case Failure(_) =>
        DBIO.failed(
          new RawlsExceptionWithErrorReport(
            errorReport =
              ErrorReport(StatusCodes.NotFound, s"Submission id ${submissionId} is not a valid submission id")
          )
        )
      case _ =>
        dataAccess.submissionQuery.get(workspaceContext, submissionId) flatMap {
          case None =>
            DBIO.failed(
              new RawlsExceptionWithErrorReport(
                errorReport = ErrorReport(
                  StatusCodes.NotFound,
                  s"Submission with id ${submissionId} not found in workspace ${workspaceContext.toWorkspaceName}"
                )
              )
            )
          case Some(submission) => op(submission)
        }
    }

  // confirm that the Submission is a member of this workspace, but don't unmarshal it from the DB
  private def withSubmissionId[T](workspaceContext: Workspace, submissionId: String, dataAccess: DataAccess)(
    op: UUID => ReadWriteAction[T]
  ): ReadWriteAction[T] =
    Try {
      UUID.fromString(submissionId)
    } match {
      case Failure(_) =>
        DBIO.failed(
          new RawlsExceptionWithErrorReport(
            errorReport =
              ErrorReport(StatusCodes.NotFound, s"Submission id ${submissionId} is not a valid submission id")
          )
        )
      case Success(uuid) =>
        dataAccess.submissionQuery.confirmInWorkspace(workspaceContext.workspaceIdAsUUID, uuid) flatMap {
          case None =>
            val report = ErrorReport(
              StatusCodes.NotFound,
              s"Submission with id ${submissionId} not found in workspace ${workspaceContext.toWorkspaceName}"
            )
            DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = report))
          case Some(_) => op(uuid)
        }
    }

  // used as part of the workflow metadata permission check - more detail at workflowMetadata()

  // require submission to be present, but don't require the workflow to reference it
  // if the workflow does reference the submission, return its executionServiceKey

  private def withSubmissionAndWorkflowExecutionServiceKey[T](workspaceContext: Workspace,
                                                              submissionId: String,
                                                              workflowId: String,
                                                              dataAccess: DataAccess
  )(op: Option[ExecutionServiceId] => ReadWriteAction[T]): ReadWriteAction[T] =
    withSubmissionId(workspaceContext, submissionId, dataAccess) { _ =>
      dataAccess.workflowQuery.getExecutionServiceIdByExternalId(workflowId, submissionId) flatMap {
        case Some(id) => op(Option(ExecutionServiceId(id)))
        case _        => op(None)
      }
    }

  def getSpendReportTableName(billingProjectName: RawlsBillingProjectName): Future[Option[String]] =
    dataSource.inTransaction { dataAccess =>
      dataAccess.rawlsBillingProjectQuery.load(billingProjectName).map { billingProject =>
        billingProject match {
          case None =>
            throw new RawlsExceptionWithErrorReport(
              ErrorReport(StatusCodes.NotFound, s"Could not find billing project ${billingProjectName.value}")
            )
          case Some(
                RawlsBillingProject(_,
                                    _,
                                    _,
                                    _,
                                    _,
                                    _,
                                    _,
                                    _,
                                    Some(spendReportDataset),
                                    Some(spendReportTable),
                                    Some(spendReportDatasetGoogleProject),
                                    _,
                                    _,
                                    _
                )
              ) =>
            Option(s"${spendReportDatasetGoogleProject}.${spendReportDataset}.${spendReportTable}")
          case _ => None
        }
      }
    }

  private def getEntityProviderForMethodConfig(workspaceContext: Workspace,
                                               methodConfiguration: MethodConfiguration
  ): Future[EntityProvider] =
    entityManager.resolveProviderFuture(
      EntityRequestArguments(workspaceContext, ctx, methodConfiguration.dataReferenceName, None)
    )

  private def saveSubmission(workspaceContext: Workspace,
                             submissionId: UUID,
                             submissionRequest: SubmissionRequest,
                             submissionRoot: String,
                             submissionParameters: Seq[SubmissionValidationEntityInputs],
                             workflowFailureMode: Option[WorkflowFailureMode],
                             header: SubmissionValidationHeader
  ): Future[Submission] =
    dataSource.inTransaction { dataAccess =>
      val (successes, failures) = submissionParameters.partition { entityInputs =>
        entityInputs.inputResolutions.forall(_.error.isEmpty)
      }
      val workflows = successes map { entityInputs =>
        val workflowEntityOpt = header.entityType.map(_ =>
          AttributeEntityReference(entityType = header.entityType.get, entityName = entityInputs.entityName)
        )
        Workflow(
          workflowId = None,
          status = WorkflowStatuses.Queued,
          statusLastChangedDate = DateTime.now,
          workflowEntity = workflowEntityOpt,
          inputResolutions = entityInputs.inputResolutions.toSeq
        )
      }

      val workflowFailures = failures map { entityInputs =>
        val workflowEntityOpt = header.entityType.map(_ =>
          AttributeEntityReference(entityType = header.entityType.get, entityName = entityInputs.entityName)
        )
        Workflow(
          workflowId = None,
          status = WorkflowStatuses.Failed,
          statusLastChangedDate = DateTime.now,
          workflowEntity = workflowEntityOpt,
          inputResolutions = entityInputs.inputResolutions.toSeq,
          messages = (for (entityValue <- entityInputs.inputResolutions if entityValue.error.isDefined)
            yield AttributeString(entityValue.inputName + " - " + entityValue.error.get)).toSeq
        )
      }

      val submissionEntityOpt = if (header.entityType.isEmpty || submissionRequest.entityName.isEmpty) {
        None
      } else {
        Some(
          AttributeEntityReference(entityType = submissionRequest.entityType.get,
                                   entityName = submissionRequest.entityName.get
          )
        )
      }

      val submission = Submission(
        submissionId = submissionId.toString,
        submissionDate = DateTime.now(),
        submitter = WorkbenchEmail(ctx.userInfo.userEmail.value),
        methodConfigurationNamespace = submissionRequest.methodConfigurationNamespace,
        methodConfigurationName = submissionRequest.methodConfigurationName,
        submissionEntity = submissionEntityOpt,
        workflows = workflows ++ workflowFailures,
        status = SubmissionStatuses.Submitted,
        useCallCache = submissionRequest.useCallCache,
        deleteIntermediateOutputFiles = submissionRequest.deleteIntermediateOutputFiles,
        submissionRoot = submissionRoot,
        useReferenceDisks = submissionRequest.useReferenceDisks,
        memoryRetryMultiplier = submissionRequest.memoryRetryMultiplier,
        workflowFailureMode = workflowFailureMode,
        externalEntityInfo = for {
          entityType <- header.entityType
          dataStoreId <- header.entityStoreId
        } yield ExternalEntityInfo(dataStoreId, entityType),
        userComment = submissionRequest.userComment,
        ignoreEmptyOutputs = submissionRequest.ignoreEmptyOutputs,
        monitoringScript = submissionRequest.monitoringScript,
        monitoringImage = submissionRequest.monitoringImage,
        monitoringImageScript = submissionRequest.monitoringImageScript,
        costCapThreshold = submissionRequest.costCapThreshold
      )

      logAndCreateDbSubmission(workspaceContext, submissionId, submission, dataAccess)
    }

  private def logAndCreateDbSubmission(workspaceContext: Workspace,
                                       submissionId: UUID,
                                       submission: Submission,
                                       dataAccess: DataAccess
  ): ReadWriteAction[Submission] = {
    // implicitly passed to SubmissionComponent.create
    implicit val subStatusCounter = submissionStatusCounter(workspaceMetricBuilder(workspaceContext.toWorkspaceName))
    implicit val wfStatusCounter = (status: WorkflowStatus) =>
      if (config.trackDetailedSubmissionMetrics)
        Option(
          workflowStatusCounter(workspaceSubmissionMetricBuilder(workspaceContext.toWorkspaceName, submissionId))(
            status
          )
        )
      else None

    dataAccess.submissionQuery.create(workspaceContext, submission)
  }

  def validateSubmission(workspaceName: WorkspaceName,
                         submissionRequest: SubmissionRequest
  ): Future[SubmissionValidationReport] =
    for {
      ps <- prepareSubmission(workspaceName, submissionRequest)
    } yield {
      val (failed, succeeded) = ps.inputs.partition(_.inputResolutions.exists(_.error.isDefined))
      SubmissionValidationReport(submissionRequest, ps.header, succeeded, failed)
    }

  private def evaluateWorkspaceExpressions(workspace: Workspace,
                                           gatherInputResults: GatherInputsResult
  ): Future[Map[LookupExpression, Try[Iterable[AttributeValue]]]] =
    dataSource.inTransaction { dataAccess =>
      ExpressionEvaluator.withNewExpressionEvaluator(dataAccess, None) { expressionEvaluator =>
        val expressionQueries = gatherInputResults.processableInputs.map { input =>
          expressionEvaluator.evalWorkspaceExpressionsOnly(workspace, input.expression)
        }

        // reduce(_ ++ _) collapses the series of maps into a single map
        // duplicate map keys are dropped but that is ok as the values should be duplicate
        DBIO.sequence(expressionQueries.toSeq).map {
          case Seq()   => Map.empty[LookupExpression, Try[Iterable[AttributeValue]]]
          case results => results.reduce(_ ++ _)
        }
      }
    }

  /**
    * Munges together the output of Cromwell's /outputs and /logs endpoints, grouping them by task name */
  private def mergeWorkflowOutputs(execOuts: ExecutionServiceOutputs,
                                   execLogs: ExecutionServiceLogs,
                                   workflowId: String
  ): WorkflowOutputs = {
    val outs = execOuts.outputs
    val logs = execLogs.calls getOrElse Map()

    // Cromwell workflow outputs look like workflow_name.task_name.output_name.
    // Under perverse conditions it might just be workflow_name.output_name.
    // Group outputs by everything left of the rightmost dot.
    val outsByTask = outs groupBy { case (k, _) => k.split('.').dropRight(1).mkString(".") }

    val taskMap =
      (outsByTask.keySet ++ logs.keySet).map(key => key -> TaskOutput(logs.get(key), outsByTask.get(key))).toMap
    WorkflowOutputs(workflowId, taskMap)
  }

  /**
    * Get the list of outputs for a given workflow in this submission */
  def workflowOutputs(workspaceName: WorkspaceName, submissionId: String, workflowId: String) =
    getV2WorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.read) flatMap { workspaceContext =>
      dataSource.inTransaction { dataAccess =>
        withWorkflowRecord(workspaceName, submissionId, workflowId, dataAccess) { wr =>
          val outputFTs = toFutureTry(executionServiceCluster.outputs(wr, ctx.userInfo))
          val logFTs = toFutureTry(executionServiceCluster.logs(wr, ctx.userInfo))
          DBIO.from(outputFTs zip logFTs map {
            case (Success(outputs), Success(logs)) =>
              mergeWorkflowOutputs(outputs, logs, workflowId)
            case (Failure(outputsFailure), Success(logs)) =>
              throw new RawlsExceptionWithErrorReport(
                errorReport = ErrorReport(StatusCodes.BadGateway,
                                          s"Unable to get outputs for ${submissionId}.",
                                          executionServiceCluster.toErrorReport(outputsFailure)
                )
              )
            case (Success(outputs), Failure(logsFailure)) =>
              throw new RawlsExceptionWithErrorReport(
                errorReport = ErrorReport(StatusCodes.BadGateway,
                                          s"Unable to get logs for ${submissionId}.",
                                          executionServiceCluster.toErrorReport(logsFailure)
                )
              )
            case (Failure(outputsFailure), Failure(logsFailure)) =>
              throw new RawlsExceptionWithErrorReport(
                errorReport = ErrorReport(
                  StatusCodes.BadGateway,
                  s"Unable to get outputs and unable to get logs for ${submissionId}.",
                  Seq(executionServiceCluster.toErrorReport(outputsFailure),
                      executionServiceCluster.toErrorReport(logsFailure)
                  )
                )
              )
          })
        }
      }
    }

  private def withWorkflowRecord[T](workspaceName: WorkspaceName,
                                    submissionId: String,
                                    workflowId: String,
                                    dataAccess: DataAccess
  )(op: (WorkflowRecord) => ReadWriteAction[T]): ReadWriteAction[T] =
    dataAccess.workflowQuery
      .findWorkflowByExternalIdAndSubmissionId(workflowId, UUID.fromString(submissionId))
      .result flatMap {
      case Seq() =>
        DBIO.failed(
          new RawlsExceptionWithErrorReport(
            errorReport = ErrorReport(
              StatusCodes.NotFound,
              s"WorkflowRecord with id ${workflowId} not found in submission ${submissionId} in workspace ${workspaceName}"
            )
          )
        )
      case Seq(one) => op(one)
      case tooMany =>
        DBIO.failed(
          new RawlsExceptionWithErrorReport(
            errorReport = ErrorReport(
              StatusCodes.InternalServerError,
              s"found multiple WorkflowRecords with id ${workflowId} in submission ${submissionId} in workspace ${workspaceName}"
            )
          )
        )
    }

}
