package org.broadinstitute.dsde.rawls.workspace

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.stream.Materializer
import bio.terra.workspace.client.ApiException
import bio.terra.workspace.model.WorkspaceDescription
import cats.implicits._
import cats.{Applicative, ApplicativeThrow, MonadThrow}
import com.google.api.services.cloudbilling.model.ProjectBillingInfo
import com.typesafe.scalalogging.LazyLogging
import io.opencensus.scala.Tracing.startSpanWithParent
import io.opencensus.trace.{AttributeValue => OpenCensusAttributeValue, Span, Status}
import org.broadinstitute.dsde.rawls._
import org.broadinstitute.dsde.rawls.config.WorkspaceServiceConfig
import slick.jdbc.TransactionIsolation
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.dataaccess.slick._
import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.WorkspaceManagerDAO
import org.broadinstitute.dsde.rawls.entities.base.ExpressionEvaluationSupport.LookupExpression
import org.broadinstitute.dsde.rawls.entities.base.{EntityProvider, ExpressionEvaluationContext}
import org.broadinstitute.dsde.rawls.entities.{EntityManager, EntityRequestArguments}
import org.broadinstitute.dsde.rawls.expressions.ExpressionEvaluator
import org.broadinstitute.dsde.rawls.genomics.GenomicsService
import org.broadinstitute.dsde.rawls.jobexec.MethodConfigResolver
import org.broadinstitute.dsde.rawls.jobexec.MethodConfigResolver.GatherInputsResult
import org.broadinstitute.dsde.rawls.metrics.RawlsInstrumented
import org.broadinstitute.dsde.rawls.model.AttributeUpdateOperations._
import org.broadinstitute.dsde.rawls.model.WorkflowFailureModes.WorkflowFailureMode
import org.broadinstitute.dsde.rawls.model.WorkflowStatuses.WorkflowStatus
import org.broadinstitute.dsde.rawls.model.WorkspaceAccessLevels._
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._
import org.broadinstitute.dsde.rawls.model.WorkspaceType.WorkspaceType
import org.broadinstitute.dsde.rawls.model.WorkspaceVersions.WorkspaceVersion
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.monitor.migration.MigrationUtils.Implicits.monadThrowDBIOAction
import org.broadinstitute.dsde.rawls.monitor.migration.WorkspaceMigrationMetadata
import org.broadinstitute.dsde.rawls.resourcebuffer.ResourceBufferService
import org.broadinstitute.dsde.rawls.serviceperimeter.ServicePerimeterService
import org.broadinstitute.dsde.rawls.user.UserService
import org.broadinstitute.dsde.rawls.util.TracingUtils._
import org.broadinstitute.dsde.rawls.util._
import org.broadinstitute.dsde.workbench.dataaccess.NotificationDAO
import org.broadinstitute.dsde.workbench.google.GoogleIamDAO
import org.broadinstitute.dsde.workbench.google.GoogleIamDAO.MemberType
import org.broadinstitute.dsde.workbench.model.Notifications.{WorkspaceName => NotificationWorkspaceName}
import org.broadinstitute.dsde.workbench.model.google.{GcsBucketName, GoogleProject}
import org.broadinstitute.dsde.workbench.model.{Notifications, WorkbenchEmail, WorkbenchGroupName, WorkbenchUserId}
import org.joda.time.DateTime
import spray.json.DefaultJsonProtocol._
import spray.json._

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._
import scala.language.postfixOps
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

/**
 * Created by dvoet on 4/27/15.
 */
//noinspection TypeAnnotation
object WorkspaceService {
  def constructor(dataSource: SlickDataSource,
                  methodRepoDAO: MethodRepoDAO,
                  cromiamDAO: ExecutionServiceDAO,
                  executionServiceCluster: ExecutionServiceCluster,
                  execServiceBatchSize: Int,
                  workspaceManagerDAO: WorkspaceManagerDAO,
                  methodConfigResolver: MethodConfigResolver,
                  gcsDAO: GoogleServicesDAO,
                  samDAO: SamDAO,
                  notificationDAO: NotificationDAO,
                  userServiceConstructor: RawlsRequestContext => UserService,
                  genomicsServiceConstructor: RawlsRequestContext => GenomicsService,
                  maxActiveWorkflowsTotal: Int,
                  maxActiveWorkflowsPerUser: Int,
                  workbenchMetricBaseName: String,
                  submissionCostService: SubmissionCostService,
                  config: WorkspaceServiceConfig,
                  requesterPaysSetupService: RequesterPaysSetupService,
                  entityManager: EntityManager,
                  resourceBufferService: ResourceBufferService,
                  resourceBufferSaEmail: String,
                  servicePerimeterService: ServicePerimeterService,
                  googleIamDao: GoogleIamDAO,
                  terraBillingProjectOwnerRole: String,
                  terraWorkspaceCanComputeRole: String,
                  terraWorkspaceNextflowRole: String,
                  rawlsWorkspaceAclManager: RawlsWorkspaceAclManager,
                  multiCloudWorkspaceAclManager: MultiCloudWorkspaceAclManager
  )(
    ctx: RawlsRequestContext
  )(implicit materializer: Materializer, executionContext: ExecutionContext): WorkspaceService =
    new WorkspaceService(
      ctx,
      dataSource,
      entityManager,
      methodRepoDAO,
      cromiamDAO,
      executionServiceCluster,
      execServiceBatchSize,
      workspaceManagerDAO,
      methodConfigResolver,
      gcsDAO,
      samDAO,
      notificationDAO,
      userServiceConstructor,
      genomicsServiceConstructor,
      maxActiveWorkflowsTotal,
      maxActiveWorkflowsPerUser,
      workbenchMetricBaseName,
      submissionCostService,
      config,
      requesterPaysSetupService,
      resourceBufferService,
      resourceBufferSaEmail,
      servicePerimeterService,
      googleIamDao,
      terraBillingProjectOwnerRole,
      terraWorkspaceCanComputeRole,
      terraWorkspaceNextflowRole,
      rawlsWorkspaceAclManager,
      multiCloudWorkspaceAclManager
    )

  val SECURITY_LABEL_KEY = "security"
  val HIGH_SECURITY_LABEL = "high"
  val LOW_SECURITY_LABEL = "low"

  private[workspace] def extractOperationIdsFromCromwellMetadata(metadataJson: JsObject): Iterable[String] = {
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

  private[workspace] def getTerminalStatusDate(submission: Submission, workflowID: Option[String]): Option[DateTime] = {
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
}

//noinspection TypeAnnotation,MatchToPartialFunction,SimplifyBooleanMatch,RedundantBlock,NameBooleanParameters,MapGetGet,ScalaDocMissingParameterDescription,AccessorLikeMethodIsEmptyParen,ScalaUnnecessaryParentheses,EmptyParenMethodAccessedAsParameterless,ScalaUnusedSymbol,EmptyCheck,ScalaUnusedSymbol,RedundantDefaultArgument
class WorkspaceService(protected val ctx: RawlsRequestContext,
                       val dataSource: SlickDataSource,
                       val entityManager: EntityManager,
                       val methodRepoDAO: MethodRepoDAO,
                       cromiamDAO: ExecutionServiceDAO,
                       executionServiceCluster: ExecutionServiceCluster,
                       execServiceBatchSize: Int,
                       val workspaceManagerDAO: WorkspaceManagerDAO,
                       val methodConfigResolver: MethodConfigResolver,
                       protected val gcsDAO: GoogleServicesDAO,
                       val samDAO: SamDAO,
                       notificationDAO: NotificationDAO,
                       userServiceConstructor: RawlsRequestContext => UserService,
                       genomicsServiceConstructor: RawlsRequestContext => GenomicsService,
                       maxActiveWorkflowsTotal: Int,
                       maxActiveWorkflowsPerUser: Int,
                       override val workbenchMetricBaseName: String,
                       submissionCostService: SubmissionCostService,
                       config: WorkspaceServiceConfig,
                       requesterPaysSetupService: RequesterPaysSetupService,
                       resourceBufferService: ResourceBufferService,
                       resourceBufferSaEmail: String,
                       servicePerimeterService: ServicePerimeterService,
                       googleIamDao: GoogleIamDAO,
                       terraBillingProjectOwnerRole: String,
                       terraWorkspaceCanComputeRole: String,
                       terraWorkspaceNextflowRole: String,
                       rawlsWorkspaceAclManager: RawlsWorkspaceAclManager,
                       multiCloudWorkspaceAclManager: MultiCloudWorkspaceAclManager
)(implicit protected val executionContext: ExecutionContext)
    extends RoleSupport
    with LibraryPermissionsSupport
    with FutureSupport
    with MethodWiths
    with UserWiths
    with LazyLogging
    with RawlsInstrumented
    with JsonFilterUtils
    with WorkspaceSupport
    with EntitySupport
    with AttributeSupport
    with StringValidationUtils {

  import dataSource.dataAccess.driver.api._

  implicit val errorReportSource = ErrorReportSource("rawls")

  // Note: this limit is also hard-coded in the terra-ui code to allow client-side validation.
  // If it is changed, it must also be updated in that repository.
  private val UserCommentMaxLength: Int = 1000

  def createWorkspace(workspaceRequest: WorkspaceRequest, parentContext: RawlsRequestContext = ctx): Future[Workspace] =
    for {
      _ <- traceWithParent("withAttributeNamespaceCheck", parentContext)(_ =>
        withAttributeNamespaceCheck(workspaceRequest)(Future.successful())
      )
      _ <- failIfBucketRegionInvalid(workspaceRequest.bucketLocation)
      billingProject <- traceWithParent("getBillingProjectContext", parentContext)(s =>
        getBillingProjectContext(RawlsBillingProjectName(workspaceRequest.namespace), s)
      )
      _ <- failUnlessBillingAccountHasAccess(billingProject, parentContext)
      workspace <- traceWithParent("createNewWorkspaceContext", parentContext)(s =>
        dataSource.inTransactionWithAttrTempTable(Set(AttributeTempTableType.Workspace))(
          dataAccess =>
            for {
              newWorkspace <- createNewWorkspaceContext(workspaceRequest,
                                                        billingProject,
                                                        sourceBucketName = None,
                                                        dataAccess,
                                                        s
              )
              _ = createdWorkspaceCounter.inc()
            } yield newWorkspace,
          TransactionIsolation.ReadCommitted
        )
      ) // read committed to avoid deadlocks on workspace attr scratch table
    } yield workspace

  /** Returns the Set of legal field names supplied by the user, trimmed of whitespace.
    * Throws an error if the user supplied an unrecognized field name.
    * Legal field names are any member of `WorkspaceResponse`, `WorkspaceDetails`,
    * or any arbitrary key starting with "workspace.attributes."
    *
    * @param params the raw strings supplied by the user
    * @return the set of field names to be included in the response
    */
  def validateParams(params: WorkspaceFieldSpecs, default: Set[String]): Set[String] = {
    // be lenient to whitespace, e.g. some user included spaces in their delimited string ("one, two, three")
    val args = params.fields.getOrElse(default).map(_.trim)
    // did the user specify any fields that we don't know about?
    // include custom leniency here for attributes: we can't validate attribute names because they are arbitrary,
    // so allow any field that starts with "workspace.attributes."
    val unrecognizedFields: Set[String] = args.diff(default).filter(!_.startsWith("workspace.attributes."))
    if (unrecognizedFields.nonEmpty) {
      throw new RawlsException(s"Unrecognized field names: ${unrecognizedFields.toList.sorted.mkString(", ")}")
    }
    args
  }

  def getWorkspace(workspaceName: WorkspaceName,
                   params: WorkspaceFieldSpecs,
                   parentContext: RawlsRequestContext = ctx
  ): Future[JsObject] = {
    val span = startSpanWithParent("optionsProcessing", parentContext.tracingSpan.orNull)

    // validate the inbound parameters
    val options = Try(validateParams(params, WorkspaceFieldNames.workspaceResponseFieldNames)) match {
      case Success(opts) => opts
      case Failure(ex)   => throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.BadRequest, ex))
    }

    // dummy function that returns a Future(None)
    def noFuture = Future.successful(None)

    // if user requested the entire attributes map, or any individual attributes, retrieve attributes.
    val attrSpecs = WorkspaceAttributeSpecs(
      options.contains("workspace.attributes"),
      options
        .filter(_.startsWith("workspace.attributes."))
        .map(str => AttributeName.fromDelimitedName(str.replaceFirst("workspace.attributes.", "")))
        .toList
    )
    span.setStatus(Status.OK)
    span.end()

    traceWithParent("getWorkspaceContextAndPermissions", parentContext)(s1 =>
      getWorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.read, Option(attrSpecs)) flatMap {
        workspaceContext =>
          dataSource.inTransaction { dataAccess =>
            val azureInfo: Option[AzureManagedAppCoordinates] =
              getAzureCloudContextFromWorkspaceManager(workspaceContext, s1)

            // maximum access level is required to calculate canCompute and canShare. Therefore, if any of
            // accessLevel, canCompute, canShare is specified, we have to get it.
            def accessLevelFuture(): Future[WorkspaceAccessLevels.WorkspaceAccessLevel] =
              if (options.contains("accessLevel") || options.contains("canCompute") || options.contains("canShare")) {
                getMaximumAccessLevel(workspaceContext.workspaceIdAsUUID.toString)
              } else {
                Future.successful(WorkspaceAccessLevels.NoAccess)
              }

            // determine whether or not to retrieve attributes
            val useAttributes = options.contains("workspace") || attrSpecs.all || attrSpecs.attrsToSelect.nonEmpty

            traceDBIOWithParent("accessLevelFuture", s1)(s2 => DBIO.from(accessLevelFuture())) flatMap { accessLevel =>
              // we may have calculated accessLevel because canShare/canCompute needs it;
              // but if the user didn't ask for it, don't return it
              val optionalAccessLevelForResponse = if (options.contains("accessLevel")) { Option(accessLevel) }
              else { None }

              // determine which functions to use for the various part of the response
              def bucketOptionsFuture(): Future[Option[WorkspaceBucketOptions]] =
                if (options.contains("bucketOptions")) {
                  azureInfo match {
                    case None =>
                      traceWithParent("getBucketDetails", s1)(_ =>
                        gcsDAO
                          .getBucketDetails(workspaceContext.bucketName, workspaceContext.googleProjectId)
                          .map(Option(_))
                      )
                    case _ => noFuture
                  }
                } else {
                  noFuture
                }
              def canComputeFuture(): Future[Option[Boolean]] = if (options.contains("canCompute")) {
                traceWithParent("getUserComputePermissions", s1)(_ =>
                  getUserComputePermissions(workspaceContext.workspaceIdAsUUID.toString, accessLevel).map(Option(_))
                )
              } else {
                noFuture
              }
              def canShareFuture(): Future[Option[Boolean]] = if (options.contains("canShare")) {
                // convoluted but accessLevel for both params because user could at most share with their own access level
                traceWithParent("getUserSharePermissions", s1)(_ =>
                  getUserSharePermissions(workspaceContext.workspaceIdAsUUID.toString, accessLevel, accessLevel)
                    .map(Option(_))
                )
              } else {
                noFuture
              }
              def catalogFuture(): Future[Option[Boolean]] = if (options.contains("catalog")) {
                traceWithParent("getUserCatalogPermissions", s1)(_ =>
                  getUserCatalogPermissions(workspaceContext.workspaceIdAsUUID.toString).map(Option(_))
                )
              } else {
                noFuture
              }

              def ownersFuture(): Future[Option[Set[String]]] = if (options.contains("owners")) {
                traceWithParent("getWorkspaceOwners", s1)(_ =>
                  getWorkspaceOwners(workspaceContext.workspaceIdAsUUID.toString).map(_.map(_.value)).map(Option(_))
                )
              } else {
                noFuture
              }

              def workspaceAuthorizationDomainFuture(): Future[Option[Set[ManagedGroupRef]]] =
                if (options.contains("workspace.authorizationDomain") || options.contains("workspace")) {
                  traceWithParent("loadResourceAuthDomain", s1)(_ =>
                    loadResourceAuthDomain(SamResourceTypeNames.workspace, workspaceContext.workspaceId, ctx.userInfo)
                      .map(Option(_))
                  )
                } else {
                  noFuture
                }

              def workspaceSubmissionStatsFuture(): slick.ReadAction[Option[WorkspaceSubmissionStats]] =
                if (options.contains("workspaceSubmissionStats")) {
                  getWorkspaceSubmissionStats(workspaceContext, dataAccess).map(Option(_))
                } else {
                  DBIO.from(noFuture)
                }

              // run these futures in parallel. this is equivalent to running the for-comp with the futures already defined and running
              val futuresInParallel = (
                catalogFuture(),
                canShareFuture(),
                canComputeFuture(),
                ownersFuture(),
                workspaceAuthorizationDomainFuture(),
                bucketOptionsFuture()
              ).tupled

              for {
                (canCatalog, canShare, canCompute, owners, authDomain, bucketDetails) <- DBIO.from(futuresInParallel)
                stats <- traceDBIOWithParent("workspaceSubmissionStatsFuture", s1)(_ =>
                  workspaceSubmissionStatsFuture()
                )
              } yield {
                // post-process JSON to remove calculated-but-undesired keys
                val workspaceResponse = WorkspaceResponse(
                  optionalAccessLevelForResponse,
                  canShare,
                  canCompute,
                  canCatalog,
                  WorkspaceDetails.fromWorkspaceAndOptions(workspaceContext, authDomain, useAttributes),
                  stats,
                  bucketDetails,
                  owners,
                  azureInfo
                )
                val filteredJson = deepFilterJsObject(workspaceResponse.toJson.asJsObject, options)
                filteredJson
              }
            }
          }
      }
    )
  }

  private def getAzureCloudContextFromWorkspaceManager(workspaceContext: Workspace,
                                                       parentContext: RawlsRequestContext
  ) =
    workspaceContext.workspaceType match {
      case WorkspaceType.McWorkspace =>
        val span = startSpanWithParent("getWorkspaceFromWorkspaceManager", parentContext.tracingSpan.orNull)

        try {
          val wsmInfo = workspaceManagerDAO.getWorkspace(workspaceContext.workspaceIdAsUUID, ctx)

          Option(wsmInfo.getAzureContext) match {
            case Some(azureContext) =>
              Some(
                AzureManagedAppCoordinates(UUID.fromString(azureContext.getTenantId),
                                           UUID.fromString(azureContext.getSubscriptionId),
                                           azureContext.getResourceGroupId
                )
              )
            case None =>
              throw new RawlsExceptionWithErrorReport(
                errorReport = ErrorReport(
                  StatusCodes.NotImplemented,
                  s"Non-azure MC workspaces not supported [id=${workspaceContext.workspaceId}]"
                )
              )
          }
        } catch {
          case e: ApiException =>
            logger.warn(
              s"Error retrieving MC workspace from workspace manager [id=${workspaceContext.workspaceId}, code=${e.getCode}]"
            )

            if (e.getCode == StatusCodes.NotFound.intValue) {
              span.setStatus(Status.NOT_FOUND)
              throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.NotFound, e))
            } else {
              span.setStatus(Status.INTERNAL)
              throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(e.getCode, e))
            }
        } finally
          span.end()
      case _ => None
    }

  def getWorkspaceById(workspaceId: String,
                       params: WorkspaceFieldSpecs,
                       parentContext: RawlsRequestContext = ctx
  ): Future[JsObject] = {
    val workspaceUuid = Try(UUID.fromString(workspaceId)) match {
      case Success(uid) => uid
      case Failure(_) =>
        throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.BadRequest, "invalid UUID"))
    }
    // retrieve the namespace/name for this workspace and then delegate to getWorkspace(WorkspaceName).
    // note that this id -> namespace/name lookup does not enforce security; that's enforced in getWorkspace(WorkspaceName).
    val workspaceRecords = dataSource.inTransaction { dataAccess =>
      dataAccess.workspaceQuery.findByIdQuery(workspaceUuid).map(r => (r.namespace, r.name)).take(1).result
    }
    workspaceRecords.flatMap { recsFound: Seq[(String, String)] =>
      recsFound.headOption match {
        case Some(ws) =>
          // if the call to getWorkspace(WorkspaceName) fails with an exception
          // map exceptions containing the workspace name to an exception that uses the workspace id instead
          getWorkspace(WorkspaceName(ws._1, ws._2), params, parentContext).recover { e =>
            throw e match {
              case workspaceException: WorkspaceException => workspaceException.usingId(workspaceId)
              case _                                      => e
            }
          }
        case None =>
          throw NoSuchWorkspaceException(workspaceId)
      }
    }
  }

  def getBucketOptions(workspaceName: WorkspaceName): Future[WorkspaceBucketOptions] =
    getWorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.read) flatMap { workspaceContext =>
      dataSource.inTransaction { dataAccess =>
        DBIO.from(gcsDAO.getBucketDetails(workspaceContext.bucketName, workspaceContext.googleProjectId)) map {
          details =>
            details
        }
      }
    }

  private def loadResourceAuthDomain(resourceTypeName: SamResourceTypeName,
                                     resourceId: String,
                                     userInfo: UserInfo
  ): Future[Set[ManagedGroupRef]] =
    samDAO
      .getResourceAuthDomain(resourceTypeName, resourceId, ctx)
      .map(_.map(g => ManagedGroupRef(RawlsGroupName(g))).toSet)

  def getUserComputePermissions(workspaceId: String, userAccessLevel: WorkspaceAccessLevel): Future[Boolean] =
    if (userAccessLevel >= WorkspaceAccessLevels.Owner) Future.successful(true)
    else samDAO.userHasAction(SamResourceTypeNames.workspace, workspaceId, SamWorkspaceActions.compute, ctx)

  def getUserSharePermissions(workspaceId: String,
                              userAccessLevel: WorkspaceAccessLevel,
                              accessLevelToShareWith: WorkspaceAccessLevel
  ): Future[Boolean] =
    if (userAccessLevel < WorkspaceAccessLevels.Read) Future.successful(false)
    else if (userAccessLevel >= WorkspaceAccessLevels.Owner) Future.successful(true)
    else
      samDAO.userHasAction(SamResourceTypeNames.workspace,
                           workspaceId,
                           SamWorkspaceActions.sharePolicy(accessLevelToShareWith.toString.toLowerCase),
                           ctx
      )

  def getUserCatalogPermissions(workspaceId: String): Future[Boolean] =
    samDAO.userHasAction(SamResourceTypeNames.workspace, workspaceId, SamWorkspaceActions.catalog, ctx)

  /**
    * Sums up all the user's roles in a workspace to a single access level.
    *
    * USE FOR DISPLAY/USABILITY PURPOSES ONLY, NOT FOR REAL ACCESS DECISIONS
    * for real access decisions check actions in sam
    *
    * @param workspaceId
    * @return
    */
  def getMaximumAccessLevel(workspaceId: String): Future[WorkspaceAccessLevel] =
    samDAO.listUserRolesForResource(SamResourceTypeNames.workspace, workspaceId, ctx).map { roles =>
      roles.flatMap(role => WorkspaceAccessLevels.withRoleName(role.value)).fold(WorkspaceAccessLevels.NoAccess)(max)
    }

  def getWorkspaceOwners(workspaceId: String): Future[Set[WorkbenchEmail]] =
    samDAO
      .getPolicy(SamResourceTypeNames.workspace, workspaceId, SamWorkspacePolicyNames.owner, ctx)
      .map(_.memberEmails)

  def deleteWorkspace(workspaceName: WorkspaceName): Future[Option[String]] =
    traceWithParent("getWorkspaceContextAndPermissions", ctx)(_ =>
      getWorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.delete) flatMap { workspace =>
        traceWithParent("maybeLoadMCWorkspace", ctx)(_ => maybeLoadMcWorkspace(workspace)) flatMap { maybeMcWorkspace =>
          traceWithParent("deleteWorkspaceInternal", ctx)(s1 =>
            deleteWorkspaceInternal(workspace, maybeMcWorkspace, s1)
          )
        }
      }
    )

  def maybeLoadMcWorkspace(workspaceContext: Workspace): Future[Option[WorkspaceDescription]] =
    workspaceContext.workspaceType match {
      case WorkspaceType.McWorkspace =>
        Future(Option(workspaceManagerDAO.getWorkspace(workspaceContext.workspaceIdAsUUID, ctx)))
      case WorkspaceType.RawlsWorkspace => Future(None)
    }

  private def gatherWorkflowsToAbortAndSetStatusToAborted(workspaceName: WorkspaceName, workspaceContext: Workspace) =
    dataSource.inTransaction { dataAccess =>
      for {
        // Gather any active workflows with external ids
        workflowsToAbort <- dataAccess.workflowQuery.findActiveWorkflowsWithExternalIds(workspaceContext)

        // If a workflow is not done, automatically change its status to Aborted
        _ <- dataAccess.workflowQuery.findWorkflowsByWorkspace(workspaceContext).result.map { workflowRecords =>
          workflowRecords
            .filter(workflowRecord => !WorkflowStatuses.withName(workflowRecord.status).isDone)
            .foreach { workflowRecord =>
              dataAccess.workflowQuery.updateStatus(workflowRecord, WorkflowStatuses.Aborted) { status =>
                if (config.trackDetailedSubmissionMetrics)
                  Option(
                    workflowStatusCounter(workspaceSubmissionMetricBuilder(workspaceName, workflowRecord.submissionId))(
                      status
                    )
                  )
                else None
              }
            }
        }
      } yield workflowsToAbort
    }

  private def deleteWorkspaceTransaction(workspaceContext: Workspace) =
    dataSource.inTransaction { dataAccess =>
      for {
        // Delete components of the workspace
        _ <- dataAccess.submissionQuery.deleteFromDb(workspaceContext.workspaceIdAsUUID)
        _ <- dataAccess.methodConfigurationQuery.deleteFromDb(workspaceContext.workspaceIdAsUUID)
        _ <- dataAccess.entityQuery.deleteFromDb(workspaceContext)

        // Schedule bucket for deletion
        _ <- dataAccess.pendingBucketDeletionQuery.save(PendingBucketDeletionRecord(workspaceContext.bucketName))

        // Delete the workspace
        _ <- dataAccess.workspaceQuery.delete(workspaceContext.toWorkspaceName)
      } yield ()
    }

  private def deleteWorkspaceInternal(workspaceContext: Workspace,
                                      maybeMcWorkspace: Option[WorkspaceDescription],
                                      parentContext: RawlsRequestContext
  ): Future[Option[String]] =
    for {
      _ <- traceWithParent("requesterPaysSetupService.revokeAllUsersFromWorkspace", parentContext)(_ =>
        requesterPaysSetupService.revokeAllUsersFromWorkspace(workspaceContext) recoverWith { case t: Throwable =>
          logger.warn(
            s"Unexpected failure deleting workspace (while revoking 'requester pays' users) for workspace `${workspaceContext.toWorkspaceName}`",
            t
          )
          Future.failed(t)
        }
      )

      workflowsToAbort <- traceWithParent("gatherWorkflowsToAbortAndSetStatusToAborted", parentContext)(_ =>
        gatherWorkflowsToAbortAndSetStatusToAborted(workspaceContext.toWorkspaceName, workspaceContext) recoverWith {
          case t: Throwable =>
            logger.warn(
              s"Unexpected failure deleting workspace (while gathering workflows that need to be aborted) for workspace `${workspaceContext.toWorkspaceName}`",
              t
            )
            Future.failed(t)
        }
      )

      // Attempt to abort any running workflows so they don't write any more to the bucket.
      // Notice that we're kicking off Futures to do the aborts concurrently, but we never collect their results!
      // This is because there's nothing we can do if Cromwell fails, so we might as well move on and let the
      // ExecutionContext run the futures whenever
      aborts = traceWithParent("abortRunningWorkflows", parentContext)(_ =>
        Future.traverse(workflowsToAbort)(wf => executionServiceCluster.abort(wf, ctx.userInfo)) recoverWith {
          case t: Throwable =>
            logger.warn(
              s"Unexpected failure deleting workspace (while aborting workflows) for workspace `${workspaceContext.toWorkspaceName}`",
              t
            )
            Future.failed(t)
        }
      )

      // Delete Google Project
      _ <- traceWithParent("maybeDeleteGoogleProject", parentContext)(_ =>
        if (!isAzureMcWorkspace(maybeMcWorkspace)) {
          maybeDeleteGoogleProject(workspaceContext.googleProjectId,
                                   workspaceContext.workspaceVersion,
                                   ctx.userInfo
          ) recoverWith { case t: Throwable =>
            logger.error(
              s"Unexpected failure deleting workspace (while deleting google project) for workspace `${workspaceContext.toWorkspaceName}`",
              t
            )
            Future.failed(t)
          }
        } else Future.successful()
      )

      // Delete the workspace records in Rawls. Do this after deleting the google project to prevent service perimeter leaks.
      _ <- traceWithParent("deleteWorkspaceTransaction", parentContext)(_ =>
        deleteWorkspaceTransaction(workspaceContext) recoverWith { case t: Throwable =>
          logger.error(
            s"Unexpected failure deleting workspace (while deleting workspace in Rawls DB) for workspace `${workspaceContext.toWorkspaceName}`",
            t
          )
          Future.failed(t)
        }
      )

      // Delete workflowCollection resource in sam outside of DB transaction
      _ <- traceWithParent("deleteWorkflowCollectionSamResource", parentContext)(_ =>
        workspaceContext.workflowCollectionName
          .map(cn => samDAO.deleteResource(SamResourceTypeNames.workflowCollection, cn, ctx))
          .getOrElse(Future.successful(())) recoverWith {
          case t: RawlsExceptionWithErrorReport if t.errorReport.statusCode.contains(StatusCodes.NotFound) =>
            logger.warn(
              s"Received 404 from delete workflowCollection resource in Sam (while deleting workspace) for workspace `${workspaceContext.toWorkspaceName}`: [${t.errorReport.message}]"
            )
            Future.successful()
          case t: RawlsExceptionWithErrorReport =>
            logger.error(
              s"Unexpected failure deleting workspace (while deleting workflowCollection in Sam) for workspace `${workspaceContext.toWorkspaceName}`.",
              t
            )
            Future.failed(t)
        }
      )

      // Delete workspace manager record (which will only exist if there had ever been a TDR snapshot in the WS)
      _ <- traceWithParent("deleteWorkspaceInWSM", parentContext)(_ =>
        Future(workspaceManagerDAO.deleteWorkspace(workspaceContext.workspaceIdAsUUID, ctx)).recoverWith {
          // this will only ever succeed if a TDR snapshot had been created in the WS, so we gracefully handle all exceptions here
          case e: ApiException =>
            if (e.getCode != StatusCodes.NotFound.intValue) {
              logger.warn(
                s"Unexpected failure deleting workspace (while deleting in Workspace Manager) for workspace `${workspaceContext.toWorkspaceName}. Received ${e.getCode}: [${e.getResponseBody}]"
              )
            }
            Future.successful()
        }
      )

      _ <- traceWithParent("deleteWorkspaceSamResource", parentContext)(_ =>
        if (workspaceContext.workspaceType != WorkspaceType.McWorkspace) { // WSM will delete Sam resources for McWorkspaces
          samDAO.deleteResource(SamResourceTypeNames.workspace,
                                workspaceContext.workspaceIdAsUUID.toString,
                                ctx
          ) recoverWith {
            case t: RawlsExceptionWithErrorReport if t.errorReport.statusCode.contains(StatusCodes.NotFound) =>
              logger.warn(
                s"Received 404 from delete workspace resource in Sam (while deleting workspace) for workspace `${workspaceContext.toWorkspaceName}`: [${t.errorReport.message}]"
              )
              Future.successful()
            case t: RawlsExceptionWithErrorReport =>
              logger.error(
                s"Unexpected failure deleting workspace (while deleting workspace in Sam) for workspace `${workspaceContext.toWorkspaceName}`.",
                t
              )
              Future.failed(t)
          }
        } else { Future.successful() }
      )
    } yield {
      aborts.onComplete {
        case Failure(t) =>
          logger.info(s"failure aborting workflows while deleting workspace ${workspaceContext.toWorkspaceName}", t)
        case _ => /* ok */
      }

      if (!isAzureMcWorkspace(maybeMcWorkspace)) {
        Option(workspaceContext.bucketName)
      } else None
    }

  private def isAzureMcWorkspace(maybeMcWorkspace: Option[WorkspaceDescription]): Boolean =
    maybeMcWorkspace.flatMap(mcWorkspace => Option(mcWorkspace.getAzureContext)).isDefined

  // TODO - once workspace migration is complete and there are no more v1 workspaces or v1 billing projects, we can remove this https://broadworkbench.atlassian.net/browse/CA-1118
  private def maybeDeleteGoogleProject(googleProjectId: GoogleProjectId,
                                       workspaceVersion: WorkspaceVersion,
                                       userInfoForSam: UserInfo
  ): Future[Unit] =
    if (workspaceVersion == WorkspaceVersions.V2) {
      deleteGoogleProject(googleProjectId, userInfoForSam)
    } else {
      Future.successful()
    }

  def deleteGoogleProject(googleProjectId: GoogleProjectId, userInfoForSam: UserInfo): Future[Unit] =
    for {
      _ <- deletePetsInProject(googleProjectId, userInfoForSam)
      _ <- gcsDAO.deleteGoogleProject(googleProjectId)
      _ <- samDAO
        .deleteResource(SamResourceTypeNames.googleProject, googleProjectId.value, ctx.copy(userInfo = userInfoForSam))
        .recover {
          case regrets: RawlsExceptionWithErrorReport
              if regrets.errorReport.statusCode == Option(StatusCodes.NotFound) =>
            logger.info(
              s"google-project resource ${googleProjectId.value} not found in Sam. Continuing with workspace deletion"
            )
        }
    } yield ()

  private def deletePetsInProject(projectName: GoogleProjectId, userInfo: UserInfo): Future[Unit] =
    for {
      projectUsers <- samDAO
        .listAllResourceMemberIds(SamResourceTypeNames.googleProject, projectName.value, ctx)
        .recover {
          case regrets: RawlsExceptionWithErrorReport
              if regrets.errorReport.statusCode == Option(StatusCodes.NotFound) =>
            logger.info(
              s"google-project resource ${projectName.value} not found in Sam. Continuing with workspace deletion"
            )
            Set[UserIdInfo]()
        }
      _ <- projectUsers.toList.traverse(destroyPet(_, projectName))
    } yield ()

  private def destroyPet(userIdInfo: UserIdInfo, projectName: GoogleProjectId): Future[Unit] =
    for {
      petSAJson <- samDAO.getPetServiceAccountKeyForUser(projectName, RawlsUserEmail(userIdInfo.userEmail))
      petUserInfo <- gcsDAO.getUserInfoUsingJson(petSAJson)
      _ <- samDAO.deleteUserPetServiceAccount(projectName, ctx.copy(userInfo = petUserInfo))
    } yield ()

  def updateLibraryAttributes(workspaceName: WorkspaceName,
                              operations: Seq[AttributeUpdateOperation]
  ): Future[WorkspaceDetails] =
    withLibraryAttributeNamespaceCheck(operations.map(_.name)) {
      for {
        isCurator <- tryIsCurator(ctx.userInfo.userEmail)
        workspace <- getWorkspaceContext(workspaceName) flatMap { workspace =>
          withLibraryPermissions(workspace, operations, ctx.userInfo, isCurator) {
            dataSource.inTransactionWithAttrTempTable(Set(AttributeTempTableType.Workspace))(
              dataAccess => updateWorkspace(operations, dataAccess)(workspace.toWorkspaceName),
              TransactionIsolation.ReadCommitted
            ) // read committed to avoid deadlocks on workspace attr scratch table
          }
        }
        authDomain <- loadResourceAuthDomain(SamResourceTypeNames.workspace, workspace.workspaceId, ctx.userInfo)
      } yield WorkspaceDetails(workspace, authDomain)
    }

  def updateWorkspace(workspaceName: WorkspaceName,
                      operations: Seq[AttributeUpdateOperation]
  ): Future[WorkspaceDetails] =
    withAttributeNamespaceCheck(operations.map(_.name)) {
      for {
        workspace <- getWorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.write)
        workspace <- dataSource.inTransactionWithAttrTempTable(Set(AttributeTempTableType.Workspace))(
          dataAccess => updateWorkspace(operations, dataAccess)(workspace.toWorkspaceName),
          TransactionIsolation.ReadCommitted
        ) // read committed to avoid deadlocks on workspace attr scratch table
        authDomain <- loadResourceAuthDomain(SamResourceTypeNames.workspace, workspace.workspaceId, ctx.userInfo)
      } yield WorkspaceDetails(workspace, authDomain)
    }

  private def updateWorkspace(operations: Seq[AttributeUpdateOperation], dataAccess: DataAccess)(
    workspaceName: WorkspaceName
  ): ReadWriteAction[Workspace] =
    // get the source workspace again, to avoid race conditions where the workspace was updated outside of this transaction
    withWorkspaceContext(workspaceName, dataAccess) { workspaceContext =>
      val workspace = workspaceContext
      Try {
        val updatedWorkspace = applyOperationsToWorkspace(workspace, operations)
        dataAccess.workspaceQuery.createOrUpdate(updatedWorkspace)
      } match {
        case Success(result) => result
        case Failure(e: AttributeUpdateOperationException) =>
          DBIO.failed(
            new RawlsExceptionWithErrorReport(
              errorReport = ErrorReport(StatusCodes.BadRequest, s"Unable to update ${workspace.name}", ErrorReport(e))
            )
          )
        case Failure(regrets) => DBIO.failed(regrets)
      }
    }

  def getTags(query: Option[String], limit: Option[Int] = None): Future[Seq[WorkspaceTag]] =
    for {
      workspacesForUser <- samDAO.listUserResources(SamResourceTypeNames.workspace, ctx)
      // Filter out non-UUID workspaceIds, which are possible in Sam but not valid in Rawls
      workspaceIdsForUser = workspacesForUser
        .map(resource => Try(UUID.fromString(resource.resourceId)))
        .collect { case Success(workspaceId) =>
          workspaceId
        }
        .toSeq
      result <- dataSource.inTransaction { dataAccess =>
        dataAccess.workspaceQuery.getTags(query, limit, Some(workspaceIdsForUser))
      }
    } yield result

  def listWorkspaces(params: WorkspaceFieldSpecs): Future[JsValue] = {

    val s = ctx.tracingSpan.map(startSpanWithParent("optionHandling", _))

    // validate the inbound parameters
    val options = Try(validateParams(params, WorkspaceFieldNames.workspaceListResponseFieldNames)) match {
      case Success(opts) => opts
      case Failure(ex) =>
        throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.BadRequest, ex))
    }

    // if user requested the entire attributes map, or any individual attributes, retrieve attributes.
    val attributeSpecs = WorkspaceAttributeSpecs(
      options.contains("workspace.attributes"),
      options
        .filter(_.startsWith("workspace.attributes."))
        .map(str => AttributeName.fromDelimitedName(str.replaceFirst("workspace.attributes.", "")))
        .toList
    )

    // Can this be shared with get-workspace somehow?
    val optionsExist = options.nonEmpty
    val submissionStatsEnabled = options.contains("workspaceSubmissionStats")
    val attributesEnabled = attributeSpecs.all || attributeSpecs.attrsToSelect.nonEmpty

    s.foreach { span =>
      span.setStatus(Status.OK)
      span.end()
    }

    for {
      workspaceResources <- samDAO.listUserResources(SamResourceTypeNames.workspace, ctx)

      // filter out the resources that do not have any roles related to access levels
      // also filter out any policy whose resourceId is not a UUID; these will never match a known workspace
      accessLevelWorkspaceResources = workspaceResources.filter(resource =>
        resource.allRoles.exists(role => WorkspaceAccessLevels.withRoleName(role.value).nonEmpty) &&
          Try(UUID.fromString(resource.resourceId)).isSuccess
      )
      accessLevelWorkspaceUUIDs = accessLevelWorkspaceResources.map(resource => UUID.fromString(resource.resourceId))
      result <- dataSource.inTransaction(
        { dataAccess =>
          def workspaceSubmissionStatsFuture(): slick.ReadAction[Map[UUID, WorkspaceSubmissionStats]] =
            if (submissionStatsEnabled) {
              dataAccess.workspaceQuery.listSubmissionSummaryStats(accessLevelWorkspaceUUIDs)
            } else {
              DBIO.from(Future(Map()))
            }

          val query: ReadAction[(Map[UUID, WorkspaceSubmissionStats], Seq[Workspace])] = for {
            submissionSummaryStats <- traceDBIOWithParent("submissionStats", ctx)(_ => workspaceSubmissionStatsFuture())
            workspaces <- traceDBIOWithParent("listByIds", ctx)(_ =>
              dataAccess.workspaceQuery.listByIds(accessLevelWorkspaceUUIDs, Option(attributeSpecs))
            )
          } yield (submissionSummaryStats, workspaces)

          val results = traceDBIOWithParent("finalResults", ctx)(_ =>
            query.map { case (submissionSummaryStats, workspaces) =>
              val highestAccessLevelByWorkspaceId =
                accessLevelWorkspaceResources.map { resource =>
                  resource.resourceId -> resource.allRoles
                    .flatMap(role => WorkspaceAccessLevels.withRoleName(role.value))
                    .max
                }.toMap
              val workspaceSamResourceByWorkspaceId = accessLevelWorkspaceResources.map(r => r.resourceId -> r).toMap
              workspaces.mapFilter { workspace =>
                try {
                  val cloudPlatform = workspace.workspaceType match {
                    case WorkspaceType.McWorkspace =>
                      Option(workspaceManagerDAO.getWorkspace(workspace.workspaceIdAsUUID, ctx)) match {
                        case Some(mcWorkspace) if mcWorkspace.getAzureContext != null =>
                          Option(WorkspaceCloudPlatform.Azure)
                        case Some(mcWorkspace) if mcWorkspace.getGcpContext != null =>
                          Option(WorkspaceCloudPlatform.Gcp)
                        case _ =>
                          throw new RawlsException(
                            s"unexpected state, no cloud context found for workspace ${workspace.workspaceId}"
                          )
                      }
                    case WorkspaceType.RawlsWorkspace => Option(WorkspaceCloudPlatform.Gcp)
                  }
                  val wsId = UUID.fromString(workspace.workspaceId)
                  val workspaceSamResource = workspaceSamResourceByWorkspaceId(workspace.workspaceId)
                  val accessLevel =
                    if (workspaceSamResource.missingAuthDomainGroups.nonEmpty) {
                      WorkspaceAccessLevels.NoAccess
                    } else {
                      highestAccessLevelByWorkspaceId.getOrElse(workspace.workspaceId, WorkspaceAccessLevels.NoAccess)
                    }
                  // remove attributes if they were not requested
                  val workspaceDetails =
                    WorkspaceDetails.fromWorkspaceAndOptions(
                      workspace,
                      Option(
                        workspaceSamResource.authDomainGroups.map(groupName =>
                          ManagedGroupRef(RawlsGroupName(groupName.value))
                        )
                      ),
                      attributesEnabled,
                      cloudPlatform
                    )
                  // remove submission stats if they were not requested
                  val submissionStats: Option[WorkspaceSubmissionStats] = if (submissionStatsEnabled) {
                    Option(submissionSummaryStats(wsId))
                  } else {
                    None
                  }
                  Option(
                    WorkspaceListResponse(
                      accessLevel,
                      workspaceDetails,
                      submissionStats,
                      workspaceSamResource.public.roles.nonEmpty || workspaceSamResource.public.actions.nonEmpty
                    )
                  )
                } catch {
                  // Internal folks may create MCWorkspaces in local WorkspaceManager instances, and those will not
                  // be reachable when running against the dev environment.
                  case ex: ApiException
                      if (WorkspaceType.McWorkspace == workspace.workspaceType) && (ex.getCode == StatusCodes.NotFound.intValue) =>
                    logger.warn(
                      s"MC Workspace ${workspace.name} (${workspace.workspaceIdAsUUID}) does not exist in the current WSM instance. "
                    )
                    None
                }
              }
            }
          )

          results.map { responses =>
            if (!optionsExist) {
              responses.toJson
            } else {
              // perform json-filtering of payload
              deepFilterJsValue(responses.toJson, options)
            }
          }
        },
        TransactionIsolation.ReadCommitted
      )
    } yield result
  }

  private def getWorkspaceSubmissionStats(workspaceContext: Workspace,
                                          dataAccess: DataAccess
  ): ReadAction[WorkspaceSubmissionStats] =
    // listSubmissionSummaryStats works against a sequence of workspaces; we call it just for this one workspace
    dataAccess.workspaceQuery
      .listSubmissionSummaryStats(Seq(workspaceContext.workspaceIdAsUUID))
      .map(p => p.get(workspaceContext.workspaceIdAsUUID).get)

  // NOTE: Orchestration has its own implementation of cloneWorkspace. When changing something here, you may also need to update orchestration's implementation (maybe helpful search term: `Post(workspacePath + "/clone"`).
  def cloneWorkspace(sourceWorkspace: Workspace,
                     billingProject: RawlsBillingProject,
                     destWorkspaceRequest: WorkspaceRequest,
                     parentContext: RawlsRequestContext = ctx
  ): Future[Workspace] =
    for {
      _ <- destWorkspaceRequest.copyFilesWithPrefix.traverse_(validateFileCopyPrefix)

      (libraryAttributeNames, workspaceAttributeNames) =
        destWorkspaceRequest.attributes.keys.partition(_.namespace == AttributeName.libraryNamespace)

      _ <- withAttributeNamespaceCheck(workspaceAttributeNames)(Future.successful())
      _ <- withLibraryAttributeNamespaceCheck(libraryAttributeNames)(Future.successful())
      _ <- failUnlessBillingAccountHasAccess(billingProject, parentContext)
      _ <- failIfBucketRegionInvalid(destWorkspaceRequest.bucketLocation)
      // if bucket location is specified, then we just use that for the destination workspace's bucket location.
      // if bucket location is NOT specified then we want to use the same location as the source workspace.
      // Since the destination workspace's Google project has not been claimed at this point, we cannot charge
      // the Google request that checks the source workspace bucket's location to the destination workspace's
      // Google project. To get around this, we pass in the source workspace bucket's name to
      // withNewWorkspaceContext and get the source workspace bucket's location after we've claimed a Google
      // project and before we create the destination workspace's bucket.
      sourceBucketNameOption: Option[String] = destWorkspaceRequest.bucketLocation match {
        case Some(_) => None
        case None    => Option(sourceWorkspace.bucketName)
      }

      (sourceWorkspaceContext, destWorkspaceContext) <- dataSource.inTransactionWithAttrTempTable(
        Set(AttributeTempTableType.Workspace)
      )(
        dataAccess =>
          for {
            // get the source workspace again, to avoid race conditions where the workspace was updated outside of this transaction
            sourceWorkspaceContext <- withWorkspaceContext(sourceWorkspace.toWorkspaceName, dataAccess)(
              DBIO.successful
            )
            sourceAuthDomains <- DBIO.from(
              samDAO.getResourceAuthDomain(SamResourceTypeNames.workspace, sourceWorkspaceContext.workspaceId, ctx)
            )

            newAuthDomain <- withClonedAuthDomain(
              sourceAuthDomains.map(n => ManagedGroupRef(RawlsGroupName(n))).toSet,
              destWorkspaceRequest.authorizationDomain.getOrElse(Set.empty)
            )(DBIO.successful)

            // add to or replace current attributes, on an individual basis
            newAttrs = sourceWorkspaceContext.attributes ++ destWorkspaceRequest.attributes
            destWorkspaceContext <- traceDBIOWithParent("createNewWorkspaceContext (cloneWorkspace)", ctx) { s =>
              createNewWorkspaceContext(
                destWorkspaceRequest.copy(authorizationDomain = Option(newAuthDomain), attributes = newAttrs),
                billingProject,
                sourceBucketNameOption,
                dataAccess,
                s
              )
            }

            (clonedEntityCount, clonedAttrCount) <- dataAccess.entityQuery.copyEntitiesToNewWorkspace(
              sourceWorkspaceContext.workspaceIdAsUUID,
              destWorkspaceContext.workspaceIdAsUUID
            )

            _ = clonedWorkspaceEntityHistogram += clonedEntityCount
            _ = clonedWorkspaceAttributeHistogram += clonedAttrCount

            methodConfigShorts <- dataAccess.methodConfigurationQuery.listActive(sourceWorkspaceContext)
            _ <- DBIO.sequence(methodConfigShorts.map { methodConfigShort =>
              for {
                methodConfig <- dataAccess.methodConfigurationQuery.get(
                  sourceWorkspaceContext,
                  methodConfigShort.namespace,
                  methodConfigShort.name
                )
                _ <- methodConfig.traverse_(dataAccess.methodConfigurationQuery.create(destWorkspaceContext, _))
              } yield ()
            })

            _ = clonedWorkspaceCounter.inc()

          } yield (sourceWorkspaceContext, destWorkspaceContext),
        // read committed to avoid deadlocks on workspace attr scratch table
        TransactionIsolation.ReadCommitted
      )

      // we will fire and forget this. a more involved, but robust, solution involves using the Google Storage Transfer APIs
      // in most of our use cases, these files should copy quickly enough for there to be no noticeable delay to the user
      // we also don't want to block returning a response on this call because it's already a slow endpoint
      _ <- dataSource.inTransaction { dataAccess =>
        destWorkspaceRequest.copyFilesWithPrefix.traverse_ { prefix =>
          dataAccess.cloneWorkspaceFileTransferQuery.save(destWorkspaceContext.workspaceIdAsUUID,
                                                          sourceWorkspaceContext.workspaceIdAsUUID,
                                                          prefix
          )
        }
      }
    } yield destWorkspaceContext

  private def validateFileCopyPrefix(copyFilesWithPrefix: String): Future[Unit] =
    ApplicativeThrow[Future].raiseWhen(copyFilesWithPrefix.isEmpty) {
      RawlsExceptionWithErrorReport(
        ErrorReport(
          StatusCodes.BadRequest,
          """You may not specify an empty string for `copyFilesWithPrefix`. Did you mean to specify "/" or leave the field out entirely?"""
        )
      )
    }

  def listPendingFileTransfersForWorkspace(
    workspaceName: WorkspaceName
  ): Future[Seq[PendingCloneWorkspaceFileTransfer]] =
    getWorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.read) flatMap { workspaceContext =>
      dataSource.inTransaction { dataAccess =>
        dataAccess.cloneWorkspaceFileTransferQuery.listPendingTransfers(Option(workspaceContext.workspaceIdAsUUID))
      }
    }

  private def withClonedAuthDomain[T](sourceWorkspaceADs: Set[ManagedGroupRef], destWorkspaceADs: Set[ManagedGroupRef])(
    op: (Set[ManagedGroupRef]) => ReadWriteAction[T]
  ): ReadWriteAction[T] =
    // if the source has an auth domain, the dest must also have that auth domain as a subset
    // otherwise, the caller may choose to add to the auth domain
    if (sourceWorkspaceADs.subsetOf(destWorkspaceADs)) op(sourceWorkspaceADs ++ destWorkspaceADs)
    else {
      val missingGroups = sourceWorkspaceADs -- destWorkspaceADs
      val errorMsg =
        s"Source workspace has an Authorization Domain containing the groups ${missingGroups.map(_.membersGroupName.value).mkString(", ")}, which are missing on the destination workspace"
      DBIO.failed(new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.UnprocessableEntity, errorMsg)))
    }

  def getACL(workspaceName: WorkspaceName): Future[WorkspaceACL] =
    for {
      workspace <- getWorkspaceContext(workspaceName)
      workspaceAclManager = workspace.workspaceType match {
        case WorkspaceType.RawlsWorkspace => rawlsWorkspaceAclManager
        case WorkspaceType.McWorkspace    => multiCloudWorkspaceAclManager
      }
      workspaceACL <- workspaceAclManager.getAcl(workspace.workspaceIdAsUUID, ctx)
    } yield workspaceACL

  def getCatalog(workspaceName: WorkspaceName): Future[Set[WorkspaceCatalog]] =
    loadWorkspaceId(workspaceName).flatMap { workspaceId =>
      samDAO
        .getPolicy(SamResourceTypeNames.workspace, workspaceId, SamWorkspacePolicyNames.canCatalog, ctx)
        .map(members => members.memberEmails.map(email => WorkspaceCatalog(email.value, true)))
    }

  private def loadWorkspaceId(workspaceName: WorkspaceName): Future[String] =
    dataSource.inTransaction(dataAccess => dataAccess.workspaceQuery.getWorkspaceId(workspaceName)).map {
      case None =>
        throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.NotFound, "unable to load workspace"))
      case Some(id) => id.toString
    }

  def updateCatalog(workspaceName: WorkspaceName,
                    input: Seq[WorkspaceCatalog]
  ): Future[WorkspaceCatalogUpdateResponseList] =
    for {
      workspaceId <- loadWorkspaceId(workspaceName)
      results <- Future.traverse(input) {
        case WorkspaceCatalog(email, true) =>
          toFutureTry(
            samDAO.addUserToPolicy(SamResourceTypeNames.workspace,
                                   workspaceId,
                                   SamWorkspacePolicyNames.canCatalog,
                                   email,
                                   ctx
            )
          ).map(
            _.map(_ => Either.right[String, WorkspaceCatalogResponse](WorkspaceCatalogResponse(email, true))).recover {
              case t: RawlsExceptionWithErrorReport if t.errorReport.statusCode.contains(StatusCodes.BadRequest) =>
                Left(email)
            }
          )

        case WorkspaceCatalog(email, false) =>
          toFutureTry(
            samDAO.removeUserFromPolicy(SamResourceTypeNames.workspace,
                                        workspaceId,
                                        SamWorkspacePolicyNames.canCatalog,
                                        email,
                                        ctx
            )
          ).map(
            _.map(_ => Either.right[String, WorkspaceCatalogResponse](WorkspaceCatalogResponse(email, false))).recover {
              case t: RawlsExceptionWithErrorReport if t.errorReport.statusCode.contains(StatusCodes.BadRequest) =>
                Left(email)
            }
          )
      }
    } yield {
      val failures = results.collect { case Failure(regrets) =>
        ErrorReport(regrets)
      }
      if (failures.nonEmpty) {
        throw new RawlsExceptionWithErrorReport(ErrorReport("Error setting catalog permissions", failures))
      } else {
        WorkspaceCatalogUpdateResponseList(results.collect { case Success(Right(wc)) => wc },
                                           results.collect { case Success(Left(email)) => email }
        )
      }
    }

  def collectMissingUsers(userEmails: Set[String]): Future[Set[String]] =
    Future
      .traverse(userEmails) { email =>
        samDAO.getUserIdInfo(email, ctx).map {
          case SamDAO.NotFound => Option(email)
          case _               => None
        }
      }
      .map(_.flatten)

  /**
   * updates acls for a workspace
   * @param workspaceName
   * @param aclUpdates changes to make, if an entry already exists it will be changed to the level indicated in this
   *                   Set, use NoAccess to remove an entry, all other preexisting accesses remain unchanged
   * @return
   */
  def updateACL(workspaceName: WorkspaceName,
                aclUpdates: Set[WorkspaceACLUpdate],
                inviteUsersNotFound: Boolean
  ): Future[WorkspaceACLUpdateResponseList] = {
    if (aclUpdates.map(_.email).size < aclUpdates.size) {
      throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.BadRequest, s"Only 1 entry per email allowed."))
    }

    /**
      * convert a set of policy names to the corresponding WorkspaceAclUpdate representation
      *
      * @param userEmail
      * @param samWorkspacePolicyNames
      * @return
      */
    def policiesToAclUpdate(userEmail: String,
                            samWorkspacePolicyNames: Set[SamResourcePolicyName]
    ): WorkspaceACLUpdate = {
      val accessLevel = samWorkspacePolicyNames
        .flatMap(n => WorkspaceAccessLevels.withPolicyName(n.value))
        .fold(WorkspaceAccessLevels.NoAccess)(WorkspaceAccessLevels.max)
      val ownerLevel =
        samWorkspacePolicyNames.contains(SamWorkspacePolicyNames.projectOwner) || samWorkspacePolicyNames.contains(
          SamWorkspacePolicyNames.owner
        )
      val canShare = ownerLevel ||
        (samWorkspacePolicyNames.contains(SamWorkspacePolicyNames.reader) && samWorkspacePolicyNames.contains(
          SamWorkspacePolicyNames.shareReader
        )) ||
        (samWorkspacePolicyNames.contains(SamWorkspacePolicyNames.writer) && samWorkspacePolicyNames.contains(
          SamWorkspacePolicyNames.shareWriter
        ))
      val canCompute = ownerLevel || samWorkspacePolicyNames.contains(SamWorkspacePolicyNames.canCompute)
      WorkspaceACLUpdate(userEmail, accessLevel, Option(canShare), Option(canCompute))
    }

    /**
      * convert a WorkspaceAclUpdate to the set of policy names that implement it
      *
      * @param workspaceACLUpdate
      * @return
      */
    def aclUpdateToPolicies(workspaceACLUpdate: WorkspaceACLUpdate) = {
      val sharePolicy = workspaceACLUpdate match {
        case WorkspaceACLUpdate(_, WorkspaceAccessLevels.Read, Some(true), _) =>
          SamWorkspacePolicyNames.shareReader.some
        case WorkspaceACLUpdate(_, WorkspaceAccessLevels.Write, Some(true), _) =>
          SamWorkspacePolicyNames.shareWriter.some
        case _ => None
      }

      // canCompute is only applicable to write access, readers can't have it and owners have it implicitly
      val computePolicy = workspaceACLUpdate.canCompute match {
        case Some(false) => None
        case _ if workspaceACLUpdate.accessLevel == WorkspaceAccessLevels.Write =>
          SamWorkspacePolicyNames.canCompute.some
        case _ => None
      }

      Set(workspaceACLUpdate.accessLevel.toPolicyName.map(SamResourcePolicyName), sharePolicy, computePolicy).flatten
    }

    def normalize(aclUpdates: Set[WorkspaceACLUpdate]) =
      aclUpdates.map { update =>
        val ownerLevel = update.accessLevel >= WorkspaceAccessLevels.Owner
        val normalizedCanCompute =
          ownerLevel || update.canCompute.getOrElse(update.accessLevel == WorkspaceAccessLevels.Write)
        update.copy(canShare = Option(ownerLevel || update.canShare.getOrElse(false)),
                    canCompute = Option(normalizedCanCompute)
        )
      }

    collectMissingUsers(aclUpdates.map(_.email)).flatMap { userToInvite =>
      if (userToInvite.isEmpty || inviteUsersNotFound) {
        for {
          workspace <- getWorkspaceContext(workspaceName)
          workspaceAclManager = workspace.workspaceType match {
            case WorkspaceType.RawlsWorkspace => rawlsWorkspaceAclManager
            case WorkspaceType.McWorkspace    => multiCloudWorkspaceAclManager
          }
          existingPoliciesWithMembers <- workspaceAclManager.getWorkspacePolicies(workspace.workspaceIdAsUUID, ctx)

          // convert all the existing policy memberships into WorkspaceAclUpdate objects
          existingAcls = existingPoliciesWithMembers
            .groupBy(_._1)
            .map { case (email, policyNames) =>
              policiesToAclUpdate(email.value, policyNames.map(_._2))
            }
            .toSet

          // figure out which of the incoming aclUpdates are actually changes by removing all the existingAcls
          aclChanges = normalize(aclUpdates) -- existingAcls
          _ = validateAclChanges(aclChanges, existingAcls, workspace)

          // find users to remove from policies: existing policy members that are not in policies implied by aclChanges
          // note that access level No Access corresponds to 0 desired policies so all existing policies will be removed
          policyRemovals = aclChanges.flatMap { aclChange =>
            val desiredPolicies = aclUpdateToPolicies(aclChange)
            existingPoliciesWithMembers.collect {
              case (email, policyName)
                  if email.value.equalsIgnoreCase(aclChange.email) && !desiredPolicies.contains(policyName) =>
                (policyName, aclChange.email)
            }
          }

          // find users to add to policies: users that are not existing policy members of policies implied by aclChanges
          policyAdditions = aclChanges.flatMap { aclChange =>
            val desiredPolicies = aclUpdateToPolicies(aclChange)
            desiredPolicies.collect {
              case policyName
                  if !existingPoliciesWithMembers
                    .exists(x => x._1.value.equalsIgnoreCase(aclChange.email) && x._2 == policyName) =>
                (policyName, aclChange.email)
            }
          }

          // now do all the work: invites, additions, removals, notifications
          inviteNotifications <- Future.traverse(userToInvite) { invite =>
            samDAO.inviteUser(invite, ctx).map { _ =>
              Notifications.WorkspaceInvitedNotification(
                WorkbenchEmail(invite),
                WorkbenchUserId(ctx.userInfo.userSubjectId.value),
                NotificationWorkspaceName(workspaceName.namespace, workspaceName.name),
                workspace.bucketName
              )
            }
          }

          // do additions before removals so users are not left unable to access the workspace in case of errors that
          // lead to incomplete application of these changes, remember: this is not transactional
          _ <- Future.traverse(policyAdditions) { case (policyName, email) =>
            workspaceAclManager.addUserToPolicy(workspace, policyName, WorkbenchEmail(email), ctx)
          }

          _ <- Future.traverse(policyRemovals) { case (policyName, email) =>
            workspaceAclManager.removeUserFromPolicy(workspace, policyName, WorkbenchEmail(email), ctx)
          }

          // only revoke requester pays if there's a Google project to revoke it for
          _ <-
            if (workspace.googleProjectId.value.nonEmpty) {
              revokeRequesterPaysForLinkedSAs(workspace, policyRemovals, policyAdditions)
            } else Future.successful()

          _ <- maybeShareProjectComputePolicy(policyAdditions, workspaceName)
        } yield {
          val (invites, updates) = aclChanges.partition(acl => userToInvite.contains(acl.email))
          sendACLUpdateNotifications(workspaceName,
                                     updates
          ) // we can blindly fire off this future because we don't care about the results and it happens async anyway
          notificationDAO.fireAndForgetNotifications(inviteNotifications)
          WorkspaceACLUpdateResponseList(updates, invites, Set.empty)
        }
      } else
        Future.successful(
          WorkspaceACLUpdateResponseList(Set.empty, Set.empty, aclUpdates.filter(au => userToInvite.contains(au.email)))
        )
    }
  }

  /**
    * Revoke any linked SAs for users removed from workspace. This happens during the acl update process. This process
    * can remove a user from one policy and add to another or simply remove a user altogether. Only removals/additions
    * to policies that can spend money count (owner, writer). Removal from applicable policy with a corresponding
    * addition to a different applicable policy should not result in revocation. This is done by first finding all the
    * removals from applicable policies then removing all the additions to applicable policies. Revoke linked SAs for
    * all resulting users.
    *
    * @param workspace
    * @param policyRemovals
    * @param policyAdditions
    * @return
    */
  private def revokeRequesterPaysForLinkedSAs(workspace: Workspace,
                                              policyRemovals: Set[(SamResourcePolicyName, String)],
                                              policyAdditions: Set[(SamResourcePolicyName, String)]
  ): Future[Unit] = {
    val applicablePolicies = Set(SamWorkspacePolicyNames.owner, SamWorkspacePolicyNames.writer)
    val applicableRemovals = policyRemovals.collect {
      case (policy, email) if applicablePolicies.contains(policy) => RawlsUserEmail(email)
    }
    val applicableAdditions = policyAdditions.collect {
      case (policy, email) if applicablePolicies.contains(policy) => RawlsUserEmail(email)
    }
    Future
      .traverse(applicableRemovals -- applicableAdditions) { emailToRevoke =>
        requesterPaysSetupService.revokeUserFromWorkspace(emailToRevoke, workspace)
      }
      .void
  }

  private def validateAclChanges(aclChanges: Set[WorkspaceACLUpdate],
                                 existingAcls: Set[WorkspaceACLUpdate],
                                 workspace: Workspace
  ) = {
    val emailsBeingChanged = aclChanges.map(_.email.toLowerCase)
    if (
      aclChanges.exists(_.accessLevel == WorkspaceAccessLevels.ProjectOwner) || existingAcls.exists(existingAcl =>
        existingAcl.accessLevel == ProjectOwner && emailsBeingChanged.contains(existingAcl.email.toLowerCase)
      )
    ) {
      throw new InvalidWorkspaceAclUpdateException(
        ErrorReport(StatusCodes.BadRequest, "project owner permissions cannot be changed")
      )
    }
    if (aclChanges.exists(_.email.equalsIgnoreCase(ctx.userInfo.userEmail.value))) {
      throw new InvalidWorkspaceAclUpdateException(
        ErrorReport(StatusCodes.BadRequest, "you may not change your own permissions")
      )
    }
    if (
      aclChanges.exists {
        case WorkspaceACLUpdate(_, WorkspaceAccessLevels.Read, _, Some(true)) => true
        case _                                                                => false
      }
    ) {
      throw new InvalidWorkspaceAclUpdateException(
        ErrorReport(StatusCodes.BadRequest, "may not grant readers compute access")
      )
    }
    if (workspace.workspaceType.equals(WorkspaceType.McWorkspace)) {
      val invalidMcWorkspaceACLUpdates = aclChanges.collect {
        case WorkspaceACLUpdate(_, WorkspaceAccessLevels.Write, _, Some(true)) =>
          ErrorReport(StatusCodes.BadRequest, "may not grant writers compute access")
        case WorkspaceACLUpdate(_, WorkspaceAccessLevels.Write, Some(true), _) =>
          ErrorReport(StatusCodes.BadRequest, "may not grant writers share access")
        case WorkspaceACLUpdate(_, WorkspaceAccessLevels.Read, Some(true), _) =>
          ErrorReport(StatusCodes.BadRequest, "may not grant readers share access")
      }.toSeq

      if (invalidMcWorkspaceACLUpdates.nonEmpty) {
        throw new InvalidWorkspaceAclUpdateException(
          ErrorReport(StatusCodes.BadRequest, "invalid acl updates provided", invalidMcWorkspaceACLUpdates)
        )
      }
    }
  }

  // called from test harness
  private[workspace] def maybeShareProjectComputePolicy(policyAdditions: Set[(SamResourcePolicyName, String)],
                                                        workspaceName: WorkspaceName
  ): Future[Unit] = {
    val newWriterEmails = policyAdditions.collect { case (SamWorkspacePolicyNames.canCompute, email) =>
      email
    }
    Future
      .traverse(newWriterEmails) { email =>
        samDAO
          .addUserToPolicy(SamResourceTypeNames.billingProject,
                           workspaceName.namespace,
                           SamBillingProjectPolicyNames.canComputeUser,
                           email,
                           ctx
          )
          .recoverWith { case regrets: Throwable =>
            logger.info(
              s"error adding user to canComputeUser policy of Terra billing project while updating ${workspaceName.toString} likely because it is a v2 billing project which does not have a canComputeUser policy. regrets: ${regrets.getMessage}"
            )
            Future.successful(())
          }
      }
      .map(_ => ())
  }

  private def sendACLUpdateNotifications(workspaceName: WorkspaceName, usersModified: Set[WorkspaceACLUpdate]): Unit =
    Future.traverse(usersModified) { accessUpdate =>
      for {
        userIdInfo <- samDAO.getUserIdInfo(accessUpdate.email, ctx)
      } yield userIdInfo match {
        case SamDAO.User(UserIdInfo(_, _, Some(googleSubjectId))) =>
          if (accessUpdate.accessLevel == WorkspaceAccessLevels.NoAccess)
            notificationDAO.fireAndForgetNotification(
              Notifications.WorkspaceRemovedNotification(
                WorkbenchUserId(googleSubjectId),
                NoAccess.toString,
                NotificationWorkspaceName(workspaceName.namespace, workspaceName.name),
                WorkbenchUserId(ctx.userInfo.userSubjectId.value)
              )
            )
          else
            notificationDAO.fireAndForgetNotification(
              Notifications.WorkspaceAddedNotification(
                WorkbenchUserId(googleSubjectId),
                accessUpdate.accessLevel.toString,
                NotificationWorkspaceName(workspaceName.namespace, workspaceName.name),
                WorkbenchUserId(ctx.userInfo.userSubjectId.value)
              )
            )
        case _ =>
      }
    }

  def sendChangeNotifications(workspaceName: WorkspaceName): Future[String] =
    for {
      workspaceContext <- getWorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.own)

      userIdInfos <- samDAO.listAllResourceMemberIds(SamResourceTypeNames.workspace, workspaceContext.workspaceId, ctx)

      notificationMessages = userIdInfos.collect { case UserIdInfo(_, _, Some(userId)) =>
        Notifications.WorkspaceChangedNotification(
          WorkbenchUserId(userId),
          NotificationWorkspaceName(workspaceName.namespace, workspaceName.name)
        )
      }
    } yield {
      notificationDAO.fireAndForgetNotifications(notificationMessages)
      notificationMessages.size.toString
    }

  def lockWorkspace(workspaceName: WorkspaceName): Future[Boolean] =
    // don't do the sam REST call inside the db transaction.
    getWorkspaceContext(workspaceName) flatMap { workspaceContext =>
      requireAccessIgnoreLockF(workspaceContext, SamWorkspaceActions.own) {
        // if we get here, we passed all the hoops

        dataSource.inTransaction { dataAccess =>
          lockWorkspaceInternal(workspaceContext, dataAccess)
        }
      }
    }

  private def lockWorkspaceInternal(workspaceContext: Workspace, dataAccess: DataAccess) =
    dataAccess.submissionQuery.list(workspaceContext).flatMap { submissions =>
      if (!submissions.forall(_.status.isTerminated)) {
        DBIO.failed(
          new RawlsExceptionWithErrorReport(
            errorReport = ErrorReport(
              StatusCodes.Conflict,
              s"There are running submissions in workspace ${workspaceContext.toWorkspaceName}, so it cannot be locked."
            )
          )
        )
      } else {
        import dataAccess.WorkspaceExtensions
        dataAccess.workspaceQuery.withWorkspaceId(workspaceContext.workspaceIdAsUUID).lock
      }
    }

  def unlockWorkspace(workspaceName: WorkspaceName): Future[Boolean] =
    // don't do the sam REST call inside the db transaction.
    getWorkspaceContext(workspaceName) flatMap { workspaceContext =>
      requireAccessIgnoreLockF(workspaceContext, SamWorkspaceActions.own) {
        // if we get here, we passed all the hoops

        dataSource.inTransaction { dataAccess =>
          import dataAccess.WorkspaceExtensions
          dataAccess.workspaceMigrationQuery.isMigrating(workspaceContext).flatMap {
            case true =>
              DBIO.failed(
                new RawlsExceptionWithErrorReport(
                  ErrorReport(StatusCodes.BadRequest, "cannot unlock migrating workspace")
                )
              )
            case false => dataAccess.workspaceQuery.withWorkspaceId(workspaceContext.workspaceIdAsUUID).unlock
          }
        }
      }
    }

  /**
   * Applies the sequence of operations in order to the workspace.
   *
   * @param workspace to update
   * @param operations sequence of operations
   * @throws AttributeNotFoundException when removing from a list attribute that does not exist
   * @throws AttributeUpdateOperationException when adding or removing from an attribute that is not a list
   * @return the updated entity
   */
  def applyOperationsToWorkspace(workspace: Workspace, operations: Seq[AttributeUpdateOperation]): Workspace =
    workspace.copy(attributes = applyAttributeUpdateOperations(workspace, operations))

  // validates the expressions in the method configuration, taking into account optional inputs
  private def validateMethodConfiguration(methodConfiguration: MethodConfiguration,
                                          workspaceContext: Workspace
  ): Future[ValidatedMethodConfiguration] =
    for {
      entityProvider <- getEntityProviderForMethodConfig(workspaceContext, methodConfiguration)
      gatherInputsResult <- gatherMethodConfigInputs(methodConfiguration)
      vmc <- entityProvider.expressionValidator.validateMCExpressions(methodConfiguration, gatherInputsResult)
    } yield vmc

  private def getEntityProviderForMethodConfig(workspaceContext: Workspace,
                                               methodConfiguration: MethodConfiguration
  ): Future[EntityProvider] =
    entityManager.resolveProviderFuture(
      EntityRequestArguments(workspaceContext, ctx, methodConfiguration.dataReferenceName, None)
    )

  def getAndValidateMethodConfiguration(workspaceName: WorkspaceName,
                                        methodConfigurationNamespace: String,
                                        methodConfigurationName: String
  ): Future[ValidatedMethodConfiguration] =
    getWorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.read) flatMap { workspaceContext =>
      for {
        methodConfig <- dataSource.inTransaction { dataAccess =>
          withMethodConfig(workspaceContext, methodConfigurationNamespace, methodConfigurationName, dataAccess) {
            methodConfig =>
              DBIO.successful(methodConfig)
          }
        }
        vmc <- validateMethodConfiguration(methodConfig, workspaceContext)
      } yield vmc
    }

  def createMethodConfiguration(workspaceName: WorkspaceName,
                                methodConfiguration: MethodConfiguration
  ): Future[ValidatedMethodConfiguration] =
    withAttributeNamespaceCheck(methodConfiguration) {
      getWorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.write) flatMap { workspaceContext =>
        dataSource
          .inTransaction { dataAccess =>
            dataAccess.methodConfigurationQuery.get(workspaceContext,
                                                    methodConfiguration.namespace,
                                                    methodConfiguration.name
            ) flatMap {
              case Some(_) =>
                DBIO.failed(
                  new RawlsExceptionWithErrorReport(
                    errorReport = ErrorReport(StatusCodes.Conflict,
                                              s"${methodConfiguration.name} already exists in ${workspaceName}"
                    )
                  )
                )
              case None => dataAccess.methodConfigurationQuery.create(workspaceContext, methodConfiguration)
            }
          }
          .flatMap { methodConfig =>
            validateMethodConfiguration(methodConfig, workspaceContext)
          }
      }
    }

  def deleteMethodConfiguration(workspaceName: WorkspaceName,
                                methodConfigurationNamespace: String,
                                methodConfigurationName: String
  ): Future[Boolean] =
    getWorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.write) flatMap { workspaceContext =>
      dataSource.inTransaction { dataAccess =>
        withMethodConfig(workspaceContext, methodConfigurationNamespace, methodConfigurationName, dataAccess) {
          methodConfig =>
            dataAccess.methodConfigurationQuery.delete(workspaceContext,
                                                       methodConfigurationNamespace,
                                                       methodConfigurationName
            )
        }
      }
    }

  def renameMethodConfiguration(workspaceName: WorkspaceName,
                                methodConfigurationNamespace: String,
                                methodConfigurationName: String,
                                newName: MethodConfigurationName
  ): Future[MethodConfiguration] =
    getWorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.write) flatMap { workspaceContext =>
      dataSource.inTransaction { dataAccess =>
        // It's terrible that we pass unnecessary junk that we don't read in the payload, but a big refactor of the API is going to have to wait until Some Other Time.
        if (newName.workspaceName != workspaceName) {
          DBIO.failed(
            new RawlsExceptionWithErrorReport(
              ErrorReport(StatusCodes.BadRequest, "Workspace name and namespace in payload must match those in the URI")
            )
          )
        } else {
          withMethodConfig(workspaceContext, methodConfigurationNamespace, methodConfigurationName, dataAccess) {
            methodConfiguration =>
              // If a different MC exists at the target location, return 409. But it's okay to want to overwrite your own MC.
              dataAccess.methodConfigurationQuery.get(workspaceContext, newName.namespace, newName.name) flatMap {
                case Some(_)
                    if methodConfigurationNamespace != newName.namespace || methodConfigurationName != newName.name =>
                  DBIO.failed(
                    new RawlsExceptionWithErrorReport(
                      errorReport = ErrorReport(
                        StatusCodes.Conflict,
                        s"There is already a method configuration at ${methodConfiguration.namespace}/${methodConfiguration.name} in ${workspaceName}."
                      )
                    )
                  )
                case Some(_) => DBIO.successful(methodConfiguration) // renaming self to self: no-op
                case None =>
                  dataAccess.methodConfigurationQuery.update(workspaceContext,
                                                             methodConfigurationNamespace,
                                                             methodConfigurationName,
                                                             methodConfiguration.copy(name = newName.name,
                                                                                      namespace = newName.namespace
                                                             )
                  )
              }
          }
        }
      }
    }

  // Overwrite the method configuration at methodConfiguration[namespace|name] with the new method configuration.
  def overwriteMethodConfiguration(workspaceName: WorkspaceName,
                                   methodConfigurationNamespace: String,
                                   methodConfigurationName: String,
                                   methodConfiguration: MethodConfiguration
  ): Future[ValidatedMethodConfiguration] =
    withAttributeNamespaceCheck(methodConfiguration) {
      // check permissions
      getWorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.write) flatMap { workspaceContext =>
        // create transaction
        dataSource
          .inTransaction { dataAccess =>
            if (
              methodConfiguration.namespace != methodConfigurationNamespace || methodConfiguration.name != methodConfigurationName
            ) {
              DBIO.failed(
                new RawlsExceptionWithErrorReport(
                  errorReport = ErrorReport(
                    StatusCodes.BadRequest,
                    s"The method configuration name and namespace in the URI should match the method configuration name and namespace in the request body. If you want to move this method configuration, use POST."
                  )
                )
              )
            } else {
              dataAccess.methodConfigurationQuery.create(workspaceContext, methodConfiguration)
            }
          }
          .flatMap { methodConfig =>
            validateMethodConfiguration(methodConfig, workspaceContext)
          }
      }
    }

  // Move the method configuration at methodConfiguration[namespace|name] to the location specified in methodConfiguration, _and_ update it.
  // It's like a rename and upsert all rolled into one.
  def updateMethodConfiguration(workspaceName: WorkspaceName,
                                methodConfigurationNamespace: String,
                                methodConfigurationName: String,
                                methodConfiguration: MethodConfiguration
  ): Future[ValidatedMethodConfiguration] =
    withAttributeNamespaceCheck(methodConfiguration) {
      // check permissions
      getWorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.write) flatMap { workspaceContext =>
        // create transaction
        dataSource
          .inTransaction { dataAccess =>
            withMethodConfig(workspaceContext, methodConfigurationNamespace, methodConfigurationName, dataAccess) { _ =>
              dataAccess.methodConfigurationQuery.get(workspaceContext,
                                                      methodConfiguration.namespace,
                                                      methodConfiguration.name
              ) flatMap {
                // If a different MC exists at the target location, return 409. But it's okay to want to overwrite your own MC.
                case Some(_)
                    if methodConfigurationNamespace != methodConfiguration.namespace || methodConfigurationName != methodConfiguration.name =>
                  DBIO.failed(
                    new RawlsExceptionWithErrorReport(
                      errorReport = ErrorReport(
                        StatusCodes.Conflict,
                        s"There is already a method configuration at ${methodConfiguration.namespace}/${methodConfiguration.name} in ${workspaceName}."
                      )
                    )
                  )
                case _ =>
                  dataAccess.methodConfigurationQuery.update(workspaceContext,
                                                             methodConfigurationNamespace,
                                                             methodConfigurationName,
                                                             methodConfiguration
                  )
              }
            }
          }
          .flatMap { updatedMethodConfig =>
            validateMethodConfiguration(updatedMethodConfig, workspaceContext)
          }
      }
    }

  def getMethodConfiguration(workspaceName: WorkspaceName,
                             methodConfigurationNamespace: String,
                             methodConfigurationName: String
  ): Future[MethodConfiguration] =
    getWorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.read) flatMap { workspaceContext =>
      dataSource.inTransaction { dataAccess =>
        withMethodConfig(workspaceContext, methodConfigurationNamespace, methodConfigurationName, dataAccess) {
          methodConfig =>
            DBIO.successful(methodConfig)
        }
      }
    }

  def copyMethodConfiguration(mcnp: MethodConfigurationNamePair): Future[ValidatedMethodConfiguration] = {
    // split into two transactions because we need to call out to Google after retrieving the source MC

    val transaction1Result =
      getWorkspaceContextAndPermissions(mcnp.destination.workspaceName, SamWorkspaceActions.write) flatMap {
        destContext =>
          getWorkspaceContextAndPermissions(mcnp.source.workspaceName, SamWorkspaceActions.read) flatMap {
            sourceContext =>
              dataSource.inTransaction { dataAccess =>
                dataAccess.methodConfigurationQuery.get(sourceContext,
                                                        mcnp.source.namespace,
                                                        mcnp.source.name
                ) flatMap {
                  case None =>
                    val err = ErrorReport(
                      StatusCodes.NotFound,
                      s"There is no method configuration named ${mcnp.source.namespace}/${mcnp.source.name} in ${mcnp.source.workspaceName}."
                    )
                    DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = err))
                  case Some(methodConfig) => DBIO.successful((methodConfig, destContext))
                }
              }
          }
      }

    transaction1Result flatMap { case (methodConfig, destContext) =>
      withAttributeNamespaceCheck(methodConfig) {
        dataSource
          .inTransaction { dataAccess =>
            saveCopiedMethodConfiguration(methodConfig, mcnp.destination, destContext, dataAccess)
          }
          .flatMap { methodConfig =>
            validateMethodConfiguration(methodConfig, destContext)
          }
      }
    }
  }

  def copyMethodConfigurationFromMethodRepo(
    methodRepoQuery: MethodRepoConfigurationImport
  ): Future[ValidatedMethodConfiguration] =
    methodRepoDAO.getMethodConfig(methodRepoQuery.methodRepoNamespace,
                                  methodRepoQuery.methodRepoName,
                                  methodRepoQuery.methodRepoSnapshotId,
                                  ctx.userInfo
    ) flatMap {
      case None =>
        val name =
          s"${methodRepoQuery.methodRepoNamespace}/${methodRepoQuery.methodRepoName}/${methodRepoQuery.methodRepoSnapshotId}"
        val err = ErrorReport(StatusCodes.NotFound, s"There is no method configuration named $name in the repository.")
        Future.failed(new RawlsExceptionWithErrorReport(errorReport = err))
      case Some(agoraEntity) =>
        Future.fromTry(parseAgoraEntity(agoraEntity)) flatMap { targetMethodConfig =>
          withAttributeNamespaceCheck(targetMethodConfig) {
            getWorkspaceContextAndPermissions(methodRepoQuery.destination.workspaceName,
                                              SamWorkspaceActions.write
            ) flatMap { destContext =>
              dataSource
                .inTransaction { dataAccess =>
                  saveCopiedMethodConfiguration(targetMethodConfig,
                                                methodRepoQuery.destination,
                                                destContext,
                                                dataAccess
                  )
                }
                .flatMap { methodConfig =>
                  validateMethodConfiguration(methodConfig, destContext)
                }
            }
          }
        }
    }

  private def parseAgoraEntity(agoraEntity: AgoraEntity): Try[MethodConfiguration] = {
    val parsed = Try {
      agoraEntity.payload.map(JsonParser(_).convertTo[AgoraMethodConfiguration])
    } recoverWith { case e: Exception =>
      Failure(
        new RawlsExceptionWithErrorReport(
          errorReport =
            ErrorReport(StatusCodes.UnprocessableEntity, "Error parsing Method Repo response message.", ErrorReport(e))
        )
      )
    }

    parsed flatMap {
      case Some(agoraMC) => Success(convertToMethodConfiguration(agoraMC))
      case None =>
        Failure(
          new RawlsExceptionWithErrorReport(
            errorReport = ErrorReport(StatusCodes.UnprocessableEntity, "Method Repo missing configuration payload")
          )
        )
    }
  }

  private def convertToMethodConfiguration(agoraMethodConfig: AgoraMethodConfiguration): MethodConfiguration =
    MethodConfiguration(
      agoraMethodConfig.namespace,
      agoraMethodConfig.name,
      Some(agoraMethodConfig.rootEntityType),
      Some(Map.empty[String, AttributeString]),
      agoraMethodConfig.inputs,
      agoraMethodConfig.outputs,
      agoraMethodConfig.methodRepoMethod
    )

  def copyMethodConfigurationToMethodRepo(methodRepoQuery: MethodRepoConfigurationExport): Future[AgoraEntity] =
    getWorkspaceContextAndPermissions(methodRepoQuery.source.workspaceName, SamWorkspaceActions.read) flatMap {
      workspaceContext =>
        dataSource.inTransaction { dataAccess =>
          withMethodConfig(workspaceContext,
                           methodRepoQuery.source.namespace,
                           methodRepoQuery.source.name,
                           dataAccess
          ) { methodConfig =>
            DBIO.from(
              methodRepoDAO.postMethodConfig(
                methodRepoQuery.methodRepoNamespace,
                methodRepoQuery.methodRepoName,
                methodConfig.copy(namespace = methodRepoQuery.methodRepoNamespace,
                                  name = methodRepoQuery.methodRepoName
                ),
                ctx.userInfo
              )
            )
          }
        }
    }

  private def saveCopiedMethodConfiguration(methodConfig: MethodConfiguration,
                                            dest: MethodConfigurationName,
                                            destContext: Workspace,
                                            dataAccess: DataAccess
  ) = {
    val target = methodConfig.copy(name = dest.name, namespace = dest.namespace)

    dataAccess.methodConfigurationQuery.get(destContext, dest.namespace, dest.name).flatMap {
      case Some(existingMethodConfig) =>
        DBIO.failed(
          new RawlsExceptionWithErrorReport(
            errorReport = ErrorReport(
              StatusCodes.Conflict,
              s"A method configuration named ${dest.namespace}/${dest.name} already exists in ${dest.workspaceName}"
            )
          )
        )
      case None => dataAccess.methodConfigurationQuery.create(destContext, target)
    }
  }

  def listAgoraMethodConfigurations(workspaceName: WorkspaceName): Future[List[MethodConfigurationShort]] =
    getWorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.read) flatMap { workspaceContext =>
      dataSource.inTransaction { dataAccess =>
        dataAccess.methodConfigurationQuery.listActive(workspaceContext).map { r =>
          r.toList.filter(_.methodRepoMethod.repo == Agora)
        }
      }
    }

  def listMethodConfigurations(workspaceName: WorkspaceName): Future[List[MethodConfigurationShort]] =
    getWorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.read) flatMap { workspaceContext =>
      dataSource.inTransaction { dataAccess =>
        dataAccess.methodConfigurationQuery.listActive(workspaceContext).map(_.toList)
      }
    }

  def createMethodConfigurationTemplate(methodRepoMethod: MethodRepoMethod): Future[MethodConfiguration] =
    dataSource.inTransaction { _ =>
      withMethod(methodRepoMethod, ctx.userInfo) { wdl: WDL =>
        methodConfigResolver.toMethodConfiguration(ctx.userInfo, wdl, methodRepoMethod) match {
          case Failure(exception) =>
            DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.BadRequest, exception)))
          case Success(methodConfig) => DBIO.successful(methodConfig)
        }
      }
    }

  def getMethodInputsOutputs(userInfo: UserInfo, methodRepoMethod: MethodRepoMethod): Future[MethodInputsOutputs] =
    dataSource.inTransaction { _ =>
      withMethod(methodRepoMethod, userInfo) { wdl: WDL =>
        methodConfigResolver.getMethodInputsOutputs(userInfo, wdl) match {
          case Failure(exception) =>
            DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.BadRequest, exception)))
          case Success(inputsOutputs) => DBIO.successful(inputsOutputs)
        }
      }
    }

  def listSubmissions(workspaceName: WorkspaceName): Future[Seq[SubmissionListResponse]] = {
    val costlessSubmissionsFuture = getWorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.read) flatMap {
      workspaceContext =>
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

  def countSubmissions(workspaceName: WorkspaceName): Future[Map[String, Int]] =
    getWorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.read) flatMap { workspaceContext =>
      dataSource.inTransaction { dataAccess =>
        dataAccess.submissionQuery.countByStatus(workspaceContext)
      }
    }

  def retrySubmission(workspaceName: WorkspaceName,
                      submissionRetry: SubmissionRetry,
                      submissionId: String
  ): Future[RetriedSubmissionReport] =
    getWorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.write) flatMap { workspaceContext =>
      dataSource.inTransaction { dataAccess =>
        withSubmission(workspaceContext, submissionId, dataAccess) { submission =>
          val newSubmissionId = UUID.randomUUID()
          val newSubmissionRoot = s"gs://${workspaceContext.bucketName}/submissions/${newSubmissionId}"
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
            submissionRoot = newSubmissionRoot,
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
      (workspaceContext, submissionId, submissionParameters, workflowFailureMode, header, submissionRoot) <-
        prepareSubmission(workspaceName, submissionRequest)
      submission <- saveSubmission(workspaceContext,
                                   submissionId,
                                   submissionRequest,
                                   submissionRoot,
                                   submissionParameters,
                                   workflowFailureMode,
                                   header
      )
    } yield SubmissionReport(
      submissionRequest,
      submission.submissionId,
      submission.submissionDate,
      ctx.userInfo.userEmail.value,
      submission.status,
      header,
      submissionParameters.filter(_.inputResolutions.forall(_.error.isEmpty))
    )

  private def prepareSubmission(workspaceName: WorkspaceName, submissionRequest: SubmissionRequest): Future[
    (Workspace,
     UUID,
     Stream[SubmissionValidationEntityInputs],
     Option[WorkflowFailureMode],
     SubmissionValidationHeader,
     String
    )
  ] = {

    val submissionId: UUID = UUID.randomUUID()

    for {
      _ <- requireComputePermission(workspaceName)

      // getWorkflowFailureMode early because it does validation and better to error early
      workflowFailureMode <- getWorkflowFailureMode(submissionRequest)

      workspaceContext <- getWorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.write)

      submissionRoot = s"gs://${workspaceContext.bucketName}/submissions/${submissionId}"

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

      entityProvider <- getEntityProviderForMethodConfig(workspaceContext, methodConfig)

      _ = validateSubmissionRootEntity(submissionRequest, methodConfig)

      _ = submissionRequest.userComment.map(validateMaxStringLength(_, "userComment", UserCommentMaxLength))

      gatherInputsResult <- gatherMethodConfigInputs(methodConfig)

      validationResult <- entityProvider.expressionValidator.validateExpressionsForSubmission(methodConfig,
                                                                                              gatherInputsResult
      )

      // calling .get on the Try will throw the validation error
      _ = validationResult.get

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
    } yield (workspaceContext, submissionId, submissionParameters, workflowFailureMode, header, submissionRoot)
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

  private def gatherMethodConfigInputs(
    methodConfig: MethodConfiguration
  ): Future[MethodConfigResolver.GatherInputsResult] =
    toFutureTry(methodRepoDAO.getMethod(methodConfig.methodRepoMethod, ctx.userInfo)).map {
      case Success(None) =>
        throw new RawlsExceptionWithErrorReport(
          errorReport = ErrorReport(StatusCodes.NotFound,
                                    s"Cannot get ${methodConfig.methodRepoMethod.methodUri} from method repo."
          )
        )
      case Success(Some(wdl)) =>
        methodConfigResolver
          .gatherInputs(ctx.userInfo, methodConfig, wdl)
          .recoverWith { case regrets =>
            Failure(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.BadRequest, regrets)))
          }
          .get
      case Failure(throwable) =>
        throw new RawlsExceptionWithErrorReport(
          errorReport = ErrorReport(StatusCodes.BadGateway,
                                    s"Unable to query the method repo.",
                                    methodRepoDAO.toErrorReport(throwable)
          )
        )
    }

  def saveSubmission(workspaceContext: Workspace,
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
        ignoreEmptyOutputs = submissionRequest.ignoreEmptyOutputs
      )

      logAndCreateDbSubmission(workspaceContext, submissionId, submission, dataAccess)
    }

  def logAndCreateDbSubmission(workspaceContext: Workspace,
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
      (_, _, submissionParameters, _, header, _) <- prepareSubmission(workspaceName, submissionRequest)
    } yield {
      val (failed, succeeded) = submissionParameters.partition(_.inputResolutions.exists(_.error.isDefined))
      SubmissionValidationReport(submissionRequest, header, succeeded, failed)
    }

  def getSubmissionMethodConfiguration(workspaceName: WorkspaceName,
                                       submissionId: String
  ): Future[MethodConfiguration] =
    getWorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.read) flatMap { workspaceContext =>
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

  def getSubmissionStatus(workspaceName: WorkspaceName, submissionId: String): Future[Submission] = {
    val submissionWithoutCostsAndWorkspace =
      getWorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.read) flatMap { workspaceContext =>
        dataSource.inTransaction { dataAccess =>
          withSubmission(workspaceContext, submissionId, dataAccess) { submission =>
            DBIO.successful((submission, workspaceContext))
          }
        }
      }

    submissionWithoutCostsAndWorkspace flatMap { case (submission, workspace) =>
      val allWorkflowIds: Seq[String] = submission.workflows.flatMap(_.workflowId)
      val submissionDoneDate: Option[DateTime] = WorkspaceService.getTerminalStatusDate(submission, None)

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

  def updateSubmissionUserComment(workspaceName: WorkspaceName,
                                  submissionId: String,
                                  newComment: UserCommentUpdateOperation
  ): Future[Int] = {
    validateMaxStringLength(newComment.userComment, "userComment", UserCommentMaxLength)

    getWorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.write) flatMap { workspaceContext =>
      dataSource.inTransaction { dataAccess =>
        withSubmissionId(workspaceContext, submissionId, dataAccess) { submissionId =>
          dataAccess.submissionQuery.updateSubmissionUserComment(submissionId, newComment.userComment)
        }
      }
    }
  }

  def abortSubmission(workspaceName: WorkspaceName, submissionId: String): Future[Int] =
    getWorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.write) flatMap { workspaceContext =>
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
    getWorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.read) flatMap { workspaceContext =>
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

  // retrieve the cost of this Workflow from BigQuery, if available
  def workflowCost(workspaceName: WorkspaceName, submissionId: String, workflowId: String): Future[WorkflowCost] = {

    // confirm: the user can Read this Workspace, the Submission is in this Workspace,
    // and the Workflow is in the Submission

    val execIdFutOpt = getWorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.read) flatMap {
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
      submissionDoneDate = WorkspaceService.getTerminalStatusDate(submission, Option(workflowId))
      costs <- submissionCostService.getWorkflowCost(workflowId,
                                                     workspace.googleProjectId,
                                                     submission.submissionDate,
                                                     submissionDoneDate,
                                                     tableName
      )
    } yield WorkflowCost(workflowId, costs.get(workflowId))
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
      getWorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.read) flatMap { workspaceContext =>
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

  /*
   If the user only has read access, check the bucket using the default pet.
   If the user has a higher level of access, check the bucket using the pet for this workspace's project.

   We use the default pet when possible because the default pet is created in a per-user shell project, i.e. not in
     this workspace's project. This prevents proliferation of service accounts within this workspace's project. For
     FireCloud's common read-only public workspaces, this is an important safeguard; else those common projects
     would constantly hit limits on the number of allowed service accounts.

   If the user has write access, we need to use the pet for this workspace's project in order to get accurate results.
   */
  def checkBucketReadAccess(workspaceName: WorkspaceName): Future[Unit] =
    for {
      (workspace, maxAccessLevel) <- getWorkspaceContextAndPermissions(workspaceName,
                                                                       SamWorkspaceActions.read
      ) flatMap { workspaceContext =>
        dataSource.inTransaction { dataAccess =>
          DBIO.from(getMaximumAccessLevel(workspaceContext.workspaceIdAsUUID.toString)).map { accessLevel =>
            (workspaceContext, accessLevel)
          }
        }
      }

      petKey <-
        if (maxAccessLevel >= WorkspaceAccessLevels.Write)
          samDAO.getPetServiceAccountKeyForUser(workspace.googleProjectId, ctx.userInfo.userEmail)
        else
          samDAO.getDefaultPetServiceAccountKeyForUser(ctx)

      accessToken <- gcsDAO.getAccessTokenUsingJson(petKey)

      (petEmail, petSubjectId) = petKey.parseJson match {
        case JsObject(fields) =>
          (RawlsUserEmail(fields("client_email").toString), RawlsUserSubjectId(fields("client_id").toString))
        case _ => throw new RawlsException("pet service account key was not a json object")
      }

      resultsForPet <- gcsDAO.diagnosticBucketRead(UserInfo(petEmail, OAuth2BearerToken(accessToken), 60, petSubjectId),
                                                   workspace.bucketName
      )
    } yield resultsForPet match {
      case None         => ()
      case Some(report) => throw new RawlsExceptionWithErrorReport(report)
    }

  def checkSamActionWithLock(workspaceName: WorkspaceName, samAction: SamResourceAction): Future[Boolean] = {
    val wsCtxFuture = dataSource.inTransaction { dataAccess =>
      withWorkspaceContext(workspaceName, dataAccess, Some(WorkspaceAttributeSpecs(all = false))) { workspaceContext =>
        DBIO.successful(workspaceContext)
      }
    }

    // don't do the sam REST call inside the db transaction.
    val access: Future[Boolean] = wsCtxFuture flatMap { workspaceContext =>
      requireAccessF(workspaceContext, samAction) {
        Future.successful(true) // if we get here, we passed all the hoops
      }
    }

    // if we failed for any reason, the user can't do that thing on the workspace
    access.recover { case _ => false }
  }

  def adminListAllActiveSubmissions(): Future[Seq[ActiveSubmission]] =
    asFCAdmin {
      dataSource.inTransaction { dataAccess =>
        dataAccess.submissionQuery.listAllActiveSubmissions()
      }
    }

  def adminAbortSubmission(workspaceName: WorkspaceName, submissionId: String): Future[Int] =
    asFCAdmin {
      dataSource.inTransaction { dataAccess =>
        withWorkspaceContext(workspaceName, dataAccess) { workspaceContext =>
          abortSubmission(workspaceContext, submissionId, dataAccess)
        }
      }
    }

  def listAllWorkspaces() =
    asFCAdmin {
      dataSource.inTransaction { dataAccess =>
        dataAccess.workspaceQuery.listAll.map(workspaces => workspaces.map(w => WorkspaceDetails(w, Set.empty)))
      }
    }

  def adminListWorkspacesWithAttribute(attributeName: AttributeName,
                                       attributeValue: AttributeValue
  ): Future[Seq[WorkspaceDetails]] =
    asFCAdmin {
      for {
        workspaces <- dataSource.inTransaction { dataAccess =>
          dataAccess.workspaceQuery.listWithAttribute(attributeName, attributeValue)
        }
        results <- Future.traverse(workspaces) { workspace =>
          loadResourceAuthDomain(SamResourceTypeNames.workspace, workspace.workspaceId, ctx.userInfo).map(
            WorkspaceDetails(workspace, _)
          )
        }
      } yield results
    }

  def adminListWorkspaceFeatureFlags(workspaceName: WorkspaceName): Future[Seq[WorkspaceFeatureFlag]] =
    asFCAdmin {
      dataSource.inTransaction { dataAccess =>
        withWorkspaceContext(workspaceName, dataAccess) { workspaceContext =>
          dataAccess.workspaceFeatureFlagQuery.listAllForWorkspace(workspaceContext.workspaceIdAsUUID)
        }
      }
    }

  def adminOverwriteWorkspaceFeatureFlags(workspaceName: WorkspaceName,
                                          flagNames: List[String]
  ): Future[Seq[WorkspaceFeatureFlag]] =
    asFCAdmin {
      val flags = flagNames.map(WorkspaceFeatureFlag)
      dataSource.inTransaction { dataAccess =>
        withWorkspaceContext(workspaceName, dataAccess) { workspaceContext =>
          for {
            _ <- dataAccess.workspaceFeatureFlagQuery.deleteAllForWorkspace(workspaceContext.workspaceIdAsUUID)
            _ <- dataAccess.workspaceFeatureFlagQuery.saveAll(workspaceContext.workspaceIdAsUUID, flags)
          } yield flags
        }
      }
    }

  def getBucketUsage(workspaceName: WorkspaceName): Future[BucketUsageResponse] =
    // don't do the sam REST call inside the db transaction.
    getWorkspaceContext(workspaceName) flatMap { workspaceContext =>
      requireAccessIgnoreLockF(workspaceContext, SamWorkspaceActions.write) {
        // if we get here, we passed all the hoops, otherwise an exception would have been thrown

        gcsDAO.getBucketUsage(workspaceContext.googleProjectId, workspaceContext.bucketName)
      }
    }

  def getAccessInstructions(workspaceName: WorkspaceName): Future[Seq[ManagedGroupAccessInstructions]] =
    for {
      workspaceId <- loadWorkspaceId(workspaceName)
      authDomains <- samDAO.getResourceAuthDomain(SamResourceTypeNames.workspace, workspaceId, ctx)
      instructions <- Future.traverse(authDomains) { adGroup =>
        samDAO.getAccessInstructions(WorkbenchGroupName(adGroup), ctx).map { maybeInstruction =>
          maybeInstruction.map(i => ManagedGroupAccessInstructions(adGroup, i))
        }
      }
    } yield instructions.flatten

  def getGenomicsOperationV2(workflowId: String, operationId: List[String]): Future[Option[JsObject]] =
    // note that cromiam should only give back metadata if the user is authorized to see it
    cromiamDAO.callLevelMetadata(workflowId, MetadataParams(includeKeys = Set("jobId")), ctx.userInfo).flatMap {
      metadataJson =>
        val operationIds: Iterable[String] = WorkspaceService.extractOperationIdsFromCromwellMetadata(metadataJson)

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

  def enableRequesterPaysForLinkedSAs(workspaceName: WorkspaceName): Future[Unit] =
    for {
      maybeWorkspace <- dataSource.inTransaction(dataAccess => dataAccess.workspaceQuery.findByName(workspaceName))
      workspace <- maybeWorkspace match {
        case None            => Future.failed(NoSuchWorkspaceException(workspaceName))
        case Some(workspace) => Future.successful(workspace)
      }
      _ <- accessCheck(workspace, SamWorkspaceActions.compute, ignoreLock = false)
      _ <- requesterPaysSetupService.grantRequesterPaysToLinkedSAs(ctx.userInfo, workspace)
    } yield {}

  def disableRequesterPaysForLinkedSAs(workspaceName: WorkspaceName): Future[Unit] =
    // note that this does not throw an error if the workspace does not exist
    // the user may no longer have access to the workspace so we can't confirm it exists
    // but the user does have the right to remove their linked SAs
    for {
      maybeWorkspace <- dataSource.inTransaction { dataaccess =>
        dataaccess.workspaceQuery.findByName(workspaceName)
      }
      _ <- Future.traverse(maybeWorkspace.toList) { workspace =>
        requesterPaysSetupService.revokeUserFromWorkspace(ctx.userInfo.userEmail, workspace)
      }
    } yield {}

  // helper methods

  private def createWorkflowCollectionForWorkspace(workspaceId: String,
                                                   policyEmailsByName: Map[SamResourcePolicyName, WorkbenchEmail],
                                                   parentContext: RawlsRequestContext
  ) =
    for {
      _ <- traceWithParent("createResourceFull", parentContext)(_ =>
        samDAO.createResourceFull(
          SamResourceTypeNames.workflowCollection,
          workspaceId,
          Map(
            SamWorkflowCollectionPolicyNames.workflowCollectionOwnerPolicyName ->
              SamPolicy(
                Set(policyEmailsByName(SamWorkspacePolicyNames.projectOwner),
                    policyEmailsByName(SamWorkspacePolicyNames.owner)
                ),
                Set.empty,
                Set(SamWorkflowCollectionRoles.owner)
              ),
            SamWorkflowCollectionPolicyNames.workflowCollectionWriterPolicyName ->
              SamPolicy(Set(policyEmailsByName(SamWorkspacePolicyNames.canCompute)),
                        Set.empty,
                        Set(SamWorkflowCollectionRoles.writer)
              ),
            SamWorkflowCollectionPolicyNames.workflowCollectionReaderPolicyName ->
              SamPolicy(
                Set(policyEmailsByName(SamWorkspacePolicyNames.reader),
                    policyEmailsByName(SamWorkspacePolicyNames.writer)
                ),
                Set.empty,
                Set(SamWorkflowCollectionRoles.reader)
              )
          ),
          Set.empty,
          ctx,
          None
        )
      )
    } yield {}

  def createGoogleProject(billingProject: RawlsBillingProject,
                          rbsHandoutRequestId: String,
                          parentContext: RawlsRequestContext = ctx
  ): Future[(GoogleProjectId, GoogleProjectNumber)] =
    for {
      googleProjectId <- traceWithParent("getGoogleProjectFromBuffer", parentContext) { _ =>
        resourceBufferService.getGoogleProjectFromBuffer(
          if (billingProject.servicePerimeter.isDefined)
            ProjectPoolType.ExfiltrationControlled
          else
            ProjectPoolType.Regular,
          rbsHandoutRequestId
        )
      }

      _ <- traceWithParent("maybeMoveGoogleProjectToFolder", parentContext) { _ =>
        billingProject.servicePerimeter.traverse_ {
          logger.info(s"Moving google project ${googleProjectId} to service perimeter folder.")
          userServiceConstructor(ctx).moveGoogleProjectToServicePerimeterFolder(_, googleProjectId)
        }
      }

      googleProject <- gcsDAO.getGoogleProject(googleProjectId)
      _ <- traceWithParent("remove RBS SA from owner policy", parentContext) { _ =>
        gcsDAO.removePolicyBindings(googleProjectId,
                                    Map(
                                      "roles/owner" -> Set("serviceAccount:" + resourceBufferSaEmail)
                                    )
        )
      }
    } yield (googleProjectId, gcsDAO.getGoogleProjectNumber(googleProject))

  def setProjectBillingAccount(googleProjectId: GoogleProjectId,
                               billingProject: RawlsBillingProject,
                               billingAccount: RawlsBillingAccountName,
                               workspaceId: String,
                               parentContext: RawlsRequestContext = ctx
  ): Future[ProjectBillingInfo] =
    traceWithParent("updateGoogleProjectBillingAccount", parentContext) { childContext =>
      logger.info(
        s"Setting billing account for ${googleProjectId} to ${billingAccount} replacing existing billing account."
      )
      childContext.tracingSpan.foreach { s =>
        s.putAttribute("workspaceId", OpenCensusAttributeValue.stringAttributeValue(workspaceId))
        s.putAttribute("googleProjectId", OpenCensusAttributeValue.stringAttributeValue(googleProjectId.value))
        s.putAttribute("billingAccount", OpenCensusAttributeValue.stringAttributeValue(billingAccount.value))
      }
      gcsDAO.setBillingAccountName(googleProjectId, billingAccount, childContext.tracingSpan.orNull)
    }

  def setupGoogleProjectIam(googleProjectId: GoogleProjectId,
                            policyEmailsByName: Map[SamResourcePolicyName, WorkbenchEmail],
                            billingProjectOwnerPolicyEmail: WorkbenchEmail,
                            parentContext: RawlsRequestContext = ctx
  ): Future[Unit] =
    traceWithParent("updateGoogleProjectIam", parentContext) { _ =>
      logger.info(s"Updating google project IAM ${googleProjectId}.")

      // organizations/$ORG_ID/roles/terra-billing-project-owner AND organizations/$ORG_ID/roles/terra-workspace-can-compute
      // billing project owner
      // organizations/$ORG_ID/roles/terra-workspace-can-compute
      // workspace owner
      // workspace can-compute

      // Add lifesciences.workflowsRunner (part of enabling nextflow in notebooks: https://broadworkbench.atlassian.net/browse/IA-3326) outside
      // of the canCompute policy to give the flexibility to fine-tune which workspaces it's added to if needed. This
      // role gives the user the ability to launch compute in any region, which may be counter to some data regionality policies.

      // todo: update this line as part of https://broadworkbench.atlassian.net/browse/CA-1220
      // This is done sequentially intentionally in order to avoid conflict exceptions as a result of concurrent IAM updates.
      List(
        billingProjectOwnerPolicyEmail -> Set(terraBillingProjectOwnerRole,
                                              terraWorkspaceCanComputeRole,
                                              terraWorkspaceNextflowRole
        ),
        policyEmailsByName(SamWorkspacePolicyNames.owner) -> Set(terraWorkspaceCanComputeRole,
                                                                 terraWorkspaceNextflowRole
        ),
        policyEmailsByName(SamWorkspacePolicyNames.canCompute) -> Set(terraWorkspaceCanComputeRole,
                                                                      terraWorkspaceNextflowRole
        )
      )
        .traverse_ { case (email, roles) =>
          googleIamDao.addIamRoles(
            GoogleProject(googleProjectId.value),
            email,
            MemberType.Group,
            roles,
            retryIfGroupDoesNotExist = true
          )
        }
    }

  /**
    * Update google project with the labels and google project name to reduce the number of calls made to google so we can avoid quota issues
    */
  def renameAndLabelProject(googleProjectId: GoogleProjectId,
                            workspaceId: String,
                            workspaceName: WorkspaceName,
                            parentContext: RawlsRequestContext = ctx
  ): Future[Unit] =
    traceWithParent("renameAndLabelProject", parentContext) { _ =>
      for {
        googleProject <- gcsDAO.getGoogleProject(googleProjectId)

        newLabels = gcsDAO.labelSafeMap(Map(
                                          "workspaceNamespace" -> workspaceName.namespace,
                                          "workspaceName" -> workspaceName.name,
                                          "workspaceId" -> workspaceId
                                        ),
                                        ""
        )

        googleProjectName = gcsDAO.googleProjectNameSafeString(s"${workspaceName.namespace}--${workspaceName.name}")
        // RBS projects already come with some labels so combine them to not lose the old ones
        labels = Option(googleProject.getLabels).map(_.asScala).getOrElse(Map.empty) ++ newLabels
        updatedProject = googleProject.setName(googleProjectName).setLabels(labels.toMap.asJava)
        _ <- gcsDAO.updateGoogleProject(googleProjectId, updatedProject)
      } yield ()
    }

  /**
    * If a ServicePerimeter is specified on the BillingProject, then we should update the list of Google Projects in the
    * Service Perimeter.  All newly created Workspaces (and their newly claimed Google Projects) should already be
    * persisted in the Rawls database prior to calling this method.  If no ServicePerimeter is specified on the Billing
    * Project, do nothing
    *
    * @param billingProject
    * @param span
    * @return Future[Unit]
    */
  private def maybeUpdateGoogleProjectsInPerimeter(billingProject: RawlsBillingProject,
                                                   dataAccess: DataAccess,
                                                   span: Span = null
  ): ReadAction[Unit] =
    billingProject.servicePerimeter.traverse_ { servicePerimeterName =>
      servicePerimeterService.overwriteGoogleProjectsInPerimeter(servicePerimeterName, dataAccess)
    }

  def getWorkspaceMigrationAttempts(workspaceName: WorkspaceName): Future[List[WorkspaceMigrationMetadata]] =
    asFCAdmin {
      for {
        workspace <- getWorkspaceContext(workspaceName)
        attempts <- dataSource.inTransaction { dataAccess =>
          dataAccess.workspaceMigrationQuery.getMigrationAttempts(workspace)
        }
      } yield attempts.mapWithIndex(WorkspaceMigrationMetadata.fromWorkspaceMigration)
    }

  def migrateWorkspace(workspaceName: WorkspaceName): Future[WorkspaceMigrationMetadata] =
    asFCAdmin {
      logger.info(s"Scheduling Workspace '$workspaceName' for migration")
      dataSource.inTransaction { dataAccess =>
        dataAccess.workspaceMigrationQuery.scheduleAndGetMetadata(workspaceName)
      }
    }

  def migrateAll(workspaceNames: Iterable[WorkspaceName]): Future[Iterable[WorkspaceMigrationMetadata]] =
    asFCAdmin {
      dataSource.inTransaction { dataAccess =>
        for {
          errorsOrMigrationAttempts <- workspaceNames.toList.traverse { workspaceName =>
            MonadThrow[ReadWriteAction].attempt {
              dataAccess.workspaceMigrationQuery.scheduleAndGetMetadata(workspaceName)
            }
          }

          (errors, migrationAttempts) = errorsOrMigrationAttempts.partitionMap(identity)
          _ <- MonadThrow[ReadWriteAction].raiseUnless(errors.isEmpty) {
            new RawlsExceptionWithErrorReport(
              ErrorReport(StatusCodes.BadRequest,
                          "One or more workspaces could not be scheduled for migration",
                          errors.map(ErrorReport.apply)
              )
            )
          }

        } yield migrationAttempts
      }
    }

  def failUnlessBillingAccountHasAccess(billingProject: RawlsBillingProject,
                                        parentContext: RawlsRequestContext = ctx
  ): Future[Unit] =
    traceWithParent("updateAndGetBillingAccountAccess", parentContext) { s =>
      updateAndGetBillingAccountAccess(billingProject, s).map { hasAccess =>
        if (!hasAccess)
          throw RawlsExceptionWithErrorReport(
            ErrorReport(
              StatusCodes.Forbidden,
              s"Terra does not have required permissions on Billing Account: ${billingProject.billingAccount}. " +
                "Please ensure that 'terra-billing@terra.bio' is a member of your Billing Account with " +
                "the 'Billing Account User' role."
            )
          )
      }
    }

  /**
    * takes a RawlsBillingProject and checks that Rawls has the appropriate permissions on the underlying Billing
    * Account on Google.  Does NOT check if Terra _User_ has necessary permissions on the Billing Account.  Updates
    * BillingProject to persist latest 'invalidBillingAccount' info.  Returns TRUE if user has right IAM access, else
    * FALSE
    */
  def updateAndGetBillingAccountAccess(billingProject: RawlsBillingProject,
                                       parentContext: RawlsRequestContext
  ): Future[Boolean] =
    for {
      billingAccountName <- billingProject.billingAccount
        .map(Future.successful)
        .getOrElse(
          throw RawlsExceptionWithErrorReport(
            ErrorReport(
              StatusCodes.BadRequest,
              s"Billing Project ${billingProject.projectName} has no Billing Account associated with it"
            )
          )
        )

      hasAccess <- traceWithParent("checkBillingAccountIAM", parentContext)(_ =>
        gcsDAO.testDMBillingAccountAccess(billingAccountName)
      )

      invalidBillingAccount = !hasAccess
      _ <- Applicative[Future].whenA(billingProject.invalidBillingAccount != invalidBillingAccount) {
        dataSource.inTransaction { dataAccess =>
          traceDBIOWithParent("updateInvalidBillingAccountField", parentContext)(_ =>
            dataAccess.rawlsBillingProjectQuery.updateBillingAccountValidity(billingAccountName, invalidBillingAccount)
          )
        }
      }
    } yield hasAccess

  private def createWorkspaceResourceInSam(workspaceId: String,
                                           billingProjectOwnerPolicyEmail: WorkbenchEmail,
                                           workspaceRequest: WorkspaceRequest,
                                           parentContext: RawlsRequestContext
  ): ReadWriteAction[SamCreateResourceResponse] = {

    val projectOwnerPolicy =
      SamWorkspacePolicyNames.projectOwner -> SamPolicy(Set(billingProjectOwnerPolicyEmail),
                                                        Set.empty,
                                                        Set(SamWorkspaceRoles.owner, SamWorkspaceRoles.projectOwner)
      )
    val ownerPolicyMembership: Set[WorkbenchEmail] = if (workspaceRequest.noWorkspaceOwner.getOrElse(false)) {
      Set.empty
    } else {
      Set(WorkbenchEmail(ctx.userInfo.userEmail.value))
    }
    val ownerPolicy =
      SamWorkspacePolicyNames.owner -> SamPolicy(ownerPolicyMembership, Set.empty, Set(SamWorkspaceRoles.owner))
    val writerPolicy = SamWorkspacePolicyNames.writer -> SamPolicy(Set.empty, Set.empty, Set(SamWorkspaceRoles.writer))
    val readerPolicy = SamWorkspacePolicyNames.reader -> SamPolicy(Set.empty, Set.empty, Set(SamWorkspaceRoles.reader))
    val shareReaderPolicy =
      SamWorkspacePolicyNames.shareReader -> SamPolicy(Set.empty, Set.empty, Set(SamWorkspaceRoles.shareReader))
    val shareWriterPolicy =
      SamWorkspacePolicyNames.shareWriter -> SamPolicy(Set.empty, Set.empty, Set(SamWorkspaceRoles.shareWriter))
    val canComputePolicy =
      SamWorkspacePolicyNames.canCompute -> SamPolicy(Set.empty, Set.empty, Set(SamWorkspaceRoles.canCompute))
    val canCatalogPolicy =
      SamWorkspacePolicyNames.canCatalog -> SamPolicy(Set.empty, Set.empty, Set(SamWorkspaceRoles.canCatalog))

    val allPolicies = Map(projectOwnerPolicy,
                          ownerPolicy,
                          writerPolicy,
                          readerPolicy,
                          shareReaderPolicy,
                          shareWriterPolicy,
                          canComputePolicy,
                          canCatalogPolicy
    )

    DBIO.from(
      traceWithParent("createResourceFull (workspace)", parentContext)(_ =>
        samDAO.createResourceFull(
          SamResourceTypeNames.workspace,
          workspaceId,
          allPolicies,
          workspaceRequest.authorizationDomain.getOrElse(Set.empty).map(_.membersGroupName.value),
          ctx,
          None
        )
      )
    )
  }

  private def syncPolicies(workspaceId: String,
                           policyEmailsByName: Map[SamResourcePolicyName, WorkbenchEmail],
                           workspaceRequest: WorkspaceRequest,
                           parentContext: RawlsRequestContext
  ) =
    traceWithParent("traversePolicies", parentContext)(s1 =>
      Future.traverse(policyEmailsByName.keys) { policyName =>
        if (
          policyName == SamWorkspacePolicyNames.projectOwner && workspaceRequest.authorizationDomain
            .getOrElse(Set.empty)
            .isEmpty
        ) {
          // when there isn't an auth domain, we will use the billing project admin policy email directly on workspace
          // resources instead of synching an extra group. This helps to keep the number of google groups a user is in below
          // the limit of 2000
          Future.successful(())
        } else if (
          WorkspaceAccessLevels
            .withPolicyName(policyName.value)
            .isDefined || policyName == SamWorkspacePolicyNames.canCompute
        ) {
          // only sync policies that have corresponding WorkspaceAccessLevels to google because only those are
          // granted bucket access (and thus need a google group)
          traceWithParent(s"syncPolicy-${policyName}", s1)(_ =>
            samDAO.syncPolicyToGoogle(SamResourceTypeNames.workspace, workspaceId, policyName)
          )
        } else {
          Future.successful(())
        }
      }
    )

  private def createWorkspaceInDatabase(workspaceId: String,
                                        workspaceRequest: WorkspaceRequest,
                                        bucketName: String,
                                        billingProjectOwnerPolicyEmail: WorkbenchEmail,
                                        googleProjectId: GoogleProjectId,
                                        googleProjectNumber: Option[GoogleProjectNumber],
                                        currentBillingAccountOnWorkspace: Option[RawlsBillingAccountName],
                                        dataAccess: DataAccess,
                                        parentContext: RawlsRequestContext,
                                        workspaceType: WorkspaceType = WorkspaceType.RawlsWorkspace
  ): ReadWriteAction[Workspace] = {
    val currentDate = DateTime.now
    val completedCloneWorkspaceFileTransfer = workspaceRequest.copyFilesWithPrefix match {
      case Some(_) => None
      case None    => Option(currentDate)
    }

    val workspace = Workspace(
      namespace = workspaceRequest.namespace,
      name = workspaceRequest.name,
      workspaceId = workspaceId,
      bucketName = bucketName,
      workflowCollectionName = Some(workspaceId),
      createdDate = currentDate,
      lastModified = currentDate,
      createdBy = ctx.userInfo.userEmail.value,
      attributes = workspaceRequest.attributes,
      isLocked = false,
      workspaceVersion = WorkspaceVersions.V2,
      googleProjectId = googleProjectId,
      googleProjectNumber = googleProjectNumber,
      currentBillingAccountOnWorkspace,
      billingAccountErrorMessage = None,
      completedCloneWorkspaceFileTransfer = completedCloneWorkspaceFileTransfer,
      workspaceType
    )
    traceDBIOWithParent("save", parentContext)(_ => dataAccess.workspaceQuery.createOrUpdate(workspace))
      .map(_ => workspace)
  }

  // TODO: find and assess all usages. This is written to reside inside a DB transaction, but it makes external REST calls.
  private def createNewWorkspaceContext[T](workspaceRequest: WorkspaceRequest,
                                           billingProject: RawlsBillingProject,
                                           sourceBucketName: Option[String],
                                           dataAccess: DataAccess,
                                           parentContext: RawlsRequestContext
  ): ReadWriteAction[Workspace] = {

    def getBucketName(workspaceId: String, secure: Boolean) =
      s"${config.workspaceBucketNamePrefix}-${if (secure) "secure-" else ""}${workspaceId}"

    def getLabels(authDomain: List[ManagedGroupRef]) = authDomain match {
      case Nil => Map(WorkspaceService.SECURITY_LABEL_KEY -> WorkspaceService.LOW_SECURITY_LABEL)
      case ads =>
        Map(WorkspaceService.SECURITY_LABEL_KEY -> WorkspaceService.HIGH_SECURITY_LABEL) ++ ads.map(ad =>
          gcsDAO.labelSafeString(ad.membersGroupName.value, "ad-") -> ""
        )
    }

    for {
      _ <- traceDBIOWithParent("requireCreateWorkspaceAccess", parentContext) { childContext =>
        DBIO.from(requireCreateWorkspaceAction(billingProject.projectName, childContext))
      }
      _ <- traceDBIOWithParent("maybeRequireBillingProjectOwnerAccess", parentContext) { childContext =>
        DBIO.from(requireBillingProjectOwnerAccess(workspaceRequest, childContext))
      }
      _ <- failIfWorkspaceExists(workspaceRequest.toWorkspaceName)

      workspaceId = UUID.randomUUID.toString
      _ = logger.info(s"createWorkspace - workspace:'${workspaceRequest.name}' - UUID:${workspaceId}")
      bucketName = getBucketName(workspaceId, workspaceRequest.authorizationDomain.exists(_.nonEmpty))
      // We should never get here with a missing or invalid Billing Account, but we still need to get the value out of the
      // Option, so we are being thorough
      billingAccount <- billingProject.billingAccount match {
        case Some(ba) if !billingProject.invalidBillingAccount => DBIO.successful(ba)
        case _ =>
          DBIO.failed(
            RawlsExceptionWithErrorReport(
              ErrorReport(StatusCodes.BadRequest,
                          s"Billing Account is missing or invalid for Billing Project: ${billingProject}"
              )
            )
          )
      }
      workspaceName = WorkspaceName(workspaceRequest.namespace, workspaceRequest.name)
      // add the workspace id to the span so we can find and correlate it later with other services
      _ = parentContext.tracingSpan.foreach(
        _.putAttribute("workspaceId", OpenCensusAttributeValue.stringAttributeValue(workspaceId))
      )

      billingProjectOwnerPolicyEmail <- traceDBIOWithParent("getPolicySyncStatus", parentContext)(context =>
        DBIO.from(
          samDAO
            .getPolicySyncStatus(SamResourceTypeNames.billingProject,
                                 workspaceRequest.namespace,
                                 SamBillingProjectPolicyNames.owner,
                                 context
            )
            .map(_.email)
        )
      )
      resource <- createWorkspaceResourceInSam(workspaceId,
                                               billingProjectOwnerPolicyEmail,
                                               workspaceRequest,
                                               parentContext
      )
      policyEmailsByName: Map[SamResourcePolicyName, WorkbenchEmail] = resource.accessPolicies
        .map(x => SamResourcePolicyName(x.id.accessPolicyName) -> WorkbenchEmail(x.email))
        .toMap
      _ <- DBIO.from {
        // declare these next two Futures so they start in parallel
        List(
          createWorkflowCollectionForWorkspace(workspaceId, policyEmailsByName, parentContext),
          syncPolicies(workspaceId, policyEmailsByName, workspaceRequest, parentContext)
        ).sequence_
      }
      (googleProjectId, googleProjectNumber) <- traceDBIOWithParent("setupGoogleProject", parentContext) { span =>
        DBIO.from(
          for {
            (googleProjectId, googleProjectNumber) <- createGoogleProject(billingProject,
                                                                          rbsHandoutRequestId = workspaceId,
                                                                          span
            )
            _ <- setProjectBillingAccount(googleProjectId, billingProject, billingAccount, workspaceId, span)
            _ <- renameAndLabelProject(googleProjectId, workspaceId, workspaceName, span)
            _ <- setupGoogleProjectIam(googleProjectId, policyEmailsByName, billingProjectOwnerPolicyEmail, span)
          } yield (googleProjectId, googleProjectNumber)
        )
      }
      savedWorkspace <- traceDBIOWithParent("createWorkspaceInDatabase", parentContext)(span =>
        createWorkspaceInDatabase(
          workspaceId,
          workspaceRequest,
          bucketName,
          billingProjectOwnerPolicyEmail,
          googleProjectId,
          Some(googleProjectNumber),
          Option(billingAccount),
          dataAccess,
          span
        )
      )

      _ <- traceDBIOWithParent("updateServicePerimeter", parentContext)(context =>
        maybeUpdateGoogleProjectsInPerimeter(billingProject, dataAccess, context.tracingSpan.orNull)
      )

      // After the workspace has been created, create the google-project resource in Sam with the workspace as the resource parent
      _ <- traceDBIOWithParent("createResourceFull (google project)", parentContext)(context =>
        DBIO.from(
          samDAO.createResourceFull(
            SamResourceTypeNames.googleProject,
            googleProjectId.value,
            Map.empty,
            Set.empty,
            context,
            Option(SamFullyQualifiedResourceId(workspaceId, SamResourceTypeNames.workspace.value))
          )
        )
      )

      // there's potential for another perf improvement here for workspaces with auth domains. if a workspace is in an auth domain, we'll already have
      // the projectOwnerEmail, so we don't need to get it from sam. in a pinch, we could also store the project owner email in the rawls DB since it
      // will never change, which would eliminate the call to sam entirely
      policyEmails <- DBIO.successful(
        policyEmailsByName
          .map { case (policyName, policyEmail) =>
            if (
              policyName == SamWorkspacePolicyNames.projectOwner && workspaceRequest.authorizationDomain
                .getOrElse(Set.empty)
                .isEmpty
            ) {
              // when there isn't an auth domain, we will use the billing project admin policy email directly on workspace
              // resources instead of synching an extra group. This helps to keep the number of google groups a user is in below
              // the limit of 2000
              Option(WorkspaceAccessLevels.ProjectOwner -> billingProjectOwnerPolicyEmail)
            } else {
              WorkspaceAccessLevels.withPolicyName(policyName.value).map(_ -> policyEmail)
            }
          }
          .flatten
          .toMap
      )

      workspaceBucketLocation <- traceDBIOWithParent("determineWorkspaceBucketLocation", parentContext)(_ =>
        DBIO.from(
          determineWorkspaceBucketLocation(workspaceRequest.bucketLocation, sourceBucketName, googleProjectId)
        )
      )
      _ <- traceDBIOWithParent("gcsDAO.setupWorkspace", parentContext)(childContext =>
        DBIO.from(
          gcsDAO.setupWorkspace(
            childContext.userInfo,
            savedWorkspace.googleProjectId,
            policyEmails,
            GcsBucketName(bucketName),
            getLabels(workspaceRequest.authorizationDomain.getOrElse(Set.empty).toList),
            childContext.tracingSpan.orNull,
            workspaceBucketLocation
          )
        )
      )
      _ = workspaceRequest.bucketLocation.foreach(location =>
        logger.info(
          s"Internal bucket for workspace `${workspaceRequest.name}` in namespace `${workspaceRequest.namespace}` was created in region `$location`."
        )
      )
    } yield savedWorkspace
  }

  // A new workspace request may specify the region where the bucket should be created. In the case of cloning a
  // workspace, if no bucket location is provided, then the cloned workspace's bucket will be created in the same region
  // as the source workspace's bucket. Rawls does not store bucket regions, so in order to get this information we need
  // to query Google and this query costs money, so we need to make sure that the target Google Project is the one that
  // gets charged. If neither a bucket location nor a source bucket name are provided, a default bucket location from
  // the rawls configuration will be used
  private def determineWorkspaceBucketLocation(maybeBucketLocation: Option[String],
                                               maybeSourceBucketName: Option[String],
                                               googleProjectId: GoogleProjectId
  ): Future[Option[String]] =
    (maybeBucketLocation, maybeSourceBucketName) match {
      case (bucketLocation @ Some(_), _) => Future(bucketLocation)
      case (None, Some(sourceBucketName)) =>
        gcsDAO.getRegionForRegionalBucket(sourceBucketName, Option(googleProjectId))
      case (None, None) => Future(Some(config.defaultLocation))
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

  /** Validates the workflow failure mode in the submission request. */
  private def getWorkflowFailureMode(submissionRequest: SubmissionRequest): Future[Option[WorkflowFailureMode]] =
    Try(submissionRequest.workflowFailureMode.map(WorkflowFailureModes.withName)) match {
      case Success(failureMode) => Future.successful(failureMode)
      case Failure(NonFatal(e)) =>
        Future.failed(
          new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.BadRequest, e.getMessage))
        )
      case Failure(e) =>
        Future.failed(
          new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.InternalServerError, e.getMessage))
        )
    }

  private def validateSubmissionRootEntity(submissionRequest: SubmissionRequest,
                                           methodConfig: MethodConfiguration
  ): Unit = {
    if (submissionRequest.entityName.isDefined != submissionRequest.entityType.isDefined) {
      throw new RawlsExceptionWithErrorReport(
        errorReport = ErrorReport(
          StatusCodes.BadRequest,
          s"You must set both entityType and entityName to run on an entity, or neither (to run with literal or workspace inputs)."
        )
      )
    }
    if (
      methodConfig.dataReferenceName.isEmpty && methodConfig.rootEntityType.isDefined != submissionRequest.entityName.isDefined
    ) {
      if (methodConfig.rootEntityType.isDefined) {
        throw new RawlsExceptionWithErrorReport(
          errorReport = ErrorReport(
            StatusCodes.BadRequest,
            s"Your method config defines a root entity but you haven't passed one to the submission."
          )
        )
      } else {
        // This isn't _strictly_ necessary, since a single submission entity will create one workflow.
        // However, passing in a submission entity + an expression doesn't make sense for two reasons:
        // 1. you'd have to write an expression from your submission entity to an entity of "no entity necessary" type
        // 2. even if you _could_ do this, you'd kick off a bunch of identical workflows.
        // More likely than not, an MC with no root entity + a submission entity = you're doing something wrong. So we'll just say no here.
        throw new RawlsExceptionWithErrorReport(
          errorReport = ErrorReport(StatusCodes.BadRequest,
                                    s"Your method config uses no root entity, but you passed one to the submission."
          )
        )
      }
    }
    if (methodConfig.dataReferenceName.isDefined && submissionRequest.entityName.isDefined) {
      throw new RawlsExceptionWithErrorReport(
        errorReport = ErrorReport(
          StatusCodes.BadRequest,
          "Your method config defines a data reference and an entity name. Running on a submission on a single entity in a data reference is not yet supported."
        )
      )
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

}

class AttributeUpdateOperationException(message: String) extends RawlsException(message)
class AttributeNotFoundException(message: String) extends AttributeUpdateOperationException(message)

class InvalidWorkspaceAclUpdateException(errorReport: ErrorReport) extends RawlsExceptionWithErrorReport(errorReport)
