package org.broadinstitute.dsde.rawls.workspace

import akka.http.scaladsl.model.{StatusCode, StatusCodes}
import akka.stream.Materializer
import bio.terra.workspace.client.ApiException
import cats.implicits._
import cats.{Applicative, ApplicativeThrow}
import com.google.api.client.googleapis.json.GoogleJsonResponseException
import com.google.api.services.cloudbilling.model.ProjectBillingInfo
import com.google.cloud.storage.StorageException
import com.typesafe.scalalogging.LazyLogging
import io.opentelemetry.api.common.AttributeKey
import org.broadinstitute.dsde.rawls._
import org.broadinstitute.dsde.rawls.config.WorkspaceServiceConfig
import slick.jdbc.TransactionIsolation
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.dataaccess.leonardo.LeonardoService
import org.broadinstitute.dsde.rawls.dataaccess.slick._
import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.WorkspaceManagerDAO
import org.broadinstitute.dsde.rawls.entities.base.ExpressionEvaluationSupport.LookupExpression
import org.broadinstitute.dsde.rawls.fastpass.FastPassService
import org.broadinstitute.dsde.rawls.metrics.RawlsInstrumented
import org.broadinstitute.dsde.rawls.model.AttributeUpdateOperations._
import org.broadinstitute.dsde.rawls.model.WorkspaceAccessLevels._
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._
import org.broadinstitute.dsde.rawls.model.WorkspaceState.WorkspaceState
import org.broadinstitute.dsde.rawls.model.WorkspaceType.WorkspaceType
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.monitor.migration.MigrationUtils.Implicits.monadThrowDBIOAction
import org.broadinstitute.dsde.rawls.resourcebuffer.ResourceBufferService
import org.broadinstitute.dsde.rawls.serviceperimeter.ServicePerimeterService
import org.broadinstitute.dsde.rawls.user.UserService
import org.broadinstitute.dsde.rawls.util.TracingUtils._
import org.broadinstitute.dsde.rawls.util.{
  AttributeNotFoundException,
  AttributeSupport,
  AttributeUpdateOperationException,
  BillingProjectSupport,
  JsonFilterUtils,
  LibraryPermissionsSupport,
  UserUtils,
  UserWiths,
  WorkspaceSupport
}
import org.broadinstitute.dsde.rawls.workspace.WorkspaceService.{BUCKET_GET_PERMISSION, QueryOptions}
import org.broadinstitute.dsde.workbench.dataaccess.NotificationDAO
import org.broadinstitute.dsde.workbench.google.GoogleIamDAO
import org.broadinstitute.dsde.workbench.model.Notifications.{WorkspaceName => NotificationWorkspaceName}
import org.broadinstitute.dsde.workbench.model.google.iam.IamMemberTypes
import org.broadinstitute.dsde.workbench.model.google.{GcsBucketName, GoogleProject, IamPermission}
import org.broadinstitute.dsde.workbench.model.{Notifications, WorkbenchEmail, WorkbenchGroupName, WorkbenchUserId}
import org.joda.time.DateTime
import spray.json.DefaultJsonProtocol._
import spray.json._
import org.broadinstitute.dsde.rawls.metrics.MetricsHelper
import org.broadinstitute.dsde.rawls.billing.BillingRepository
import org.broadinstitute.dsde.rawls.submissions.SubmissionsRepository

import java.io.IOException
import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}

/**
 * Created by dvoet on 4/27/15.
 */

object WorkspaceService {
  def constructor(dataSource: SlickDataSource,
                  executionServiceCluster: ExecutionServiceCluster,
                  workspaceManagerDAO: WorkspaceManagerDAO,
                  leonardoService: LeonardoService,
                  gcsDAO: GoogleServicesDAO,
                  samDAO: SamDAO,
                  notificationDAO: NotificationDAO,
                  userServiceConstructor: RawlsRequestContext => UserService,
                  workbenchMetricBaseName: String,
                  config: WorkspaceServiceConfig,
                  requesterPaysSetupService: RequesterPaysSetupService,
                  resourceBufferService: ResourceBufferService,
                  servicePerimeterService: ServicePerimeterService,
                  googleIamDao: GoogleIamDAO,
                  terraBillingProjectOwnerRole: String,
                  terraWorkspaceCanComputeRole: String,
                  terraWorkspaceNextflowRole: String,
                  terraBucketReaderRole: String,
                  terraBucketWriterRole: String,
                  rawlsWorkspaceAclManager: RawlsWorkspaceAclManager,
                  multiCloudWorkspaceAclManager: MultiCloudWorkspaceAclManager,
                  fastPassServiceConstructor: (RawlsRequestContext, SlickDataSource) => FastPassService
  )(
    ctx: RawlsRequestContext
  )(implicit materializer: Materializer, executionContext: ExecutionContext): WorkspaceService =
    new WorkspaceService(
      ctx,
      dataSource,
      executionServiceCluster,
      workspaceManagerDAO,
      leonardoService,
      gcsDAO,
      samDAO,
      notificationDAO,
      userServiceConstructor,
      workbenchMetricBaseName,
      config,
      requesterPaysSetupService,
      resourceBufferService,
      servicePerimeterService,
      googleIamDao,
      terraBillingProjectOwnerRole,
      terraWorkspaceCanComputeRole,
      terraWorkspaceNextflowRole,
      terraBucketReaderRole,
      terraBucketWriterRole,
      rawlsWorkspaceAclManager,
      multiCloudWorkspaceAclManager,
      (context: RawlsRequestContext) => fastPassServiceConstructor(context, dataSource),
      new WorkspaceRepository(dataSource),
      new BillingRepository(dataSource),
      new SubmissionsRepository(dataSource, config.trackDetailedSubmissionMetrics, workbenchMetricBaseName)
    )

  val SECURITY_LABEL_KEY: String = "security"
  val HIGH_SECURITY_LABEL: String = "high"
  val LOW_SECURITY_LABEL: String = "low"
  val BUCKET_GET_PERMISSION: String = "storage.buckets.get"

  case class QueryOptions(options: Set[LookupExpression], attrSpecs: WorkspaceAttributeSpecs) {

    val useAttributes: Boolean = options.contains("workspace") || attrSpecs.all || attrSpecs.attrsToSelect.nonEmpty

    def anyPresent[T](opts: String*)(present: => T): Option[T] =
      if (opts.exists(opt => options.contains(opt))) Some(present) else None

    def anyPresentFuture[T](opts: String*)(present: => Future[T])(implicit ex: ExecutionContext): Future[Option[T]] =
      if (opts.exists(opt => options.contains(opt))) present.map(Option(_)) else Future(None)

  }

}

class WorkspaceService(
  val ctx: RawlsRequestContext,
  val dataSource: SlickDataSource,
  executionServiceCluster: ExecutionServiceCluster,
  val workspaceManagerDAO: WorkspaceManagerDAO,
  val leonardoService: LeonardoService,
  val gcsDAO: GoogleServicesDAO,
  val samDAO: SamDAO,
  notificationDAO: NotificationDAO,
  userServiceConstructor: RawlsRequestContext => UserService,
  override val workbenchMetricBaseName: String,
  config: WorkspaceServiceConfig,
  requesterPaysSetupService: RequesterPaysSetupService,
  resourceBufferService: ResourceBufferService,
  servicePerimeterService: ServicePerimeterService,
  googleIamDao: GoogleIamDAO,
  val terraBillingProjectOwnerRole: String,
  val terraWorkspaceCanComputeRole: String,
  val terraWorkspaceNextflowRole: String,
  val terraBucketReaderRole: String,
  val terraBucketWriterRole: String,
  rawlsWorkspaceAclManager: RawlsWorkspaceAclManager,
  multiCloudWorkspaceAclManager: MultiCloudWorkspaceAclManager,
  val fastPassServiceConstructor: RawlsRequestContext => FastPassService,
  val workspaceRepository: WorkspaceRepository,
  val billingRepository: BillingRepository,
  val submissionsRepository: SubmissionsRepository
)(implicit protected val executionContext: ExecutionContext)
    extends LazyLogging
    with LibraryPermissionsSupport
    with UserWiths
    with UserUtils
    with RawlsInstrumented
    with JsonFilterUtils
    with WorkspaceSupport
    with BillingProjectSupport
    with AttributeSupport
    with StringValidationUtils {

  import dataSource.dataAccess.driver.api._

  implicit val errorReportSource: ErrorReportSource = ErrorReportSource("rawls")

  def createWorkspace(workspaceRequest: WorkspaceRequest,
                      parentContext: RawlsRequestContext = ctx
  ): Future[Workspace] = {
    def failIfPoliciesIncluded(workspaceRequest: WorkspaceRequest): Future[Unit] =
      workspaceRequest.policies match {
        case None                               => Future.successful()
        case Some(policies) if policies.isEmpty => Future.successful()
        case Some(_) =>
          Future.failed(
            RawlsExceptionWithErrorReport(
              ErrorReport(StatusCodes.BadRequest, "Policies are not supported for GCP workspaces")
            )
          )
      }
    for {
      _ <- traceFutureWithParent("withAttributeNamespaceCheck", parentContext)(_ =>
        withAttributeNamespaceCheck(workspaceRequest)(Future.successful())
      )
      _ <- failIfBucketRegionInvalid(workspaceRequest.bucketLocation)
      billingProject <- traceFutureWithParent("getBillingProjectContext", parentContext)(s =>
        getBillingProjectContext(RawlsBillingProjectName(workspaceRequest.namespace), s)
      )
      // policies are not supported on GCP workspaces
      _ <- failIfPoliciesIncluded(workspaceRequest)
      _ <- failUnlessBillingAccountHasAccess(billingProject, parentContext)
      workspace <- traceFutureWithParent("createNewWorkspaceContext", parentContext)(s =>
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
      ) // read committed to avoid deadlocks on workspace attribute scratch table
      _ <- traceFutureWithParent("FastPassService.setupFastPassNewWorkspace", parentContext)(childContext =>
        fastPassServiceConstructor(childContext).syncFastPassesForUserInWorkspace(workspace)
      )
    } yield workspace
  }

  def getWorkspace(workspaceName: WorkspaceName, params: WorkspaceFieldSpecs): Future[JsObject] = {
    val options = processOptions(params)
    traceFutureWithParent("getV2WorkspaceContextAndPermissions", ctx)(_ =>
      for {
        workspace <-
          getV2WorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.read, Option(options.attrSpecs))
        workspaceResponse <- getWorkspaceDetails(workspace, options)
      } yield
      // post-process JSON to remove calculated-but-undesired keys
      deepFilterJsObject(workspaceResponse.toJson.asJsObject, options.options)
    )
  }

  def getWorkspaceById(workspaceId: String, params: WorkspaceFieldSpecs): Future[JsObject] = {
    val options = processOptions(params)
    traceFutureWithParent("getV2WorkspaceContextAndPermissions", ctx)(_ =>
      for {
        workspace <-
          getV2WorkspaceContextAndPermissionsById(workspaceId, SamWorkspaceActions.read, Option(options.attrSpecs))
        workspaceResponse <- getWorkspaceDetails(workspace, options)
      } yield
      // post-process JSON to remove calculated-but-undesired keys
      deepFilterJsObject(workspaceResponse.toJson.asJsObject, options.options)
    )
  }

  def getWorkspaceDetails(workspace: Workspace, options: QueryOptions): Future[WorkspaceResponse] = {
    val workspaceId = workspace.workspaceId
    /*
    If we're looking to improve performance, we could potentially use this, instead trying to run futures in parallel:
    userRoles <- samDAO.listUserActionsForResource(SamResourceTypeNames.workspace, workspaceId, ctx)
     */
    for {
      canCatalog <- options.anyPresentFuture("catalog") {
        samDAO.userHasAction(SamResourceTypeNames.workspace, workspaceId, SamWorkspaceActions.catalog, ctx)
      }

      stats <- options.anyPresentFuture("workspaceSubmissionStats") {
        workspaceRepository.getSubmissionSummaryStats(workspace.workspaceIdAsUUID)
      }
      authDomain <- options.anyPresentFuture("workspace.authorizationDomain", "workspace") {
        loadResourceAuthDomain(SamResourceTypeNames.workspace, workspaceId)
      }

      accessLevels <- options.anyPresentFuture("accessLevel", "canCompute", "canShare") {
        samDAO.listUserRolesForResource(SamResourceTypeNames.workspace, workspaceId, ctx)
      }
      // Sum up all the user's roles in a workspace to a single access level
      // FOR DISPLAY/USABILITY PURPOSES ONLY, NOT REAL ACCESS DECISIONS
      // for real access decisions check actions in sam
      accessLevel = accessLevels
        .map { roles =>
          roles
            .flatMap(role => WorkspaceAccessLevels.withRoleName(role.value))
            .fold(WorkspaceAccessLevels.NoAccess)(max)
        }
        .getOrElse(WorkspaceAccessLevels.NoAccess)
      ownersPolicy <- options.anyPresentFuture("owners") {
        samDAO.getPolicy(SamResourceTypeNames.workspace, workspaceId, SamWorkspacePolicyNames.owner, ctx)
      }
      owners = ownersPolicy.map(policy => policy.memberEmails.map(_.value))
      canShare <- options.anyPresentFuture("canShare") {
        if (accessLevel >= WorkspaceAccessLevels.Owner) Future.successful(true)
        else {
          val sharePolicy = SamWorkspaceActions.sharePolicy(accessLevel.toString.toLowerCase())
          samDAO.userHasAction(SamResourceTypeNames.workspace, workspaceId, sharePolicy, ctx)
        }
      }
      wsmService = new AggregatedWorkspaceService(workspaceManagerDAO)
      wsmContext = Try(wsmService.fetchAggregatedWorkspace(workspace, ctx)).recover {
        // return workspace with no WSM information for gcp workspace
        case _: AggregateWorkspaceNotFoundException if workspace.workspaceType == WorkspaceType.RawlsWorkspace =>
          AggregatedWorkspace(workspace, Some(workspace.googleProjectId), None, List.empty)
      }.get

      canCompute <- options.anyPresentFuture("canCompute") {
        wsmContext.getCloudPlatform match {
          case Some(WorkspaceCloudPlatform.Azure) => Future.successful(accessLevel >= WorkspaceAccessLevels.Write)
          case _ if accessLevel >= WorkspaceAccessLevels.Owner => Future.successful(true)
          case _ => samDAO.userHasAction(SamResourceTypeNames.workspace, workspaceId, SamWorkspaceActions.compute, ctx)
        }
      }
      bucketDetails: Option[WorkspaceBucketOptions] <- wsmContext.googleProjectId match {
        case None     => Future.successful(None)
        case Some(id) => options.anyPresentFuture("bucketOptions")(gcsDAO.getBucketDetails(workspace.bucketName, id))
      }
    } yield WorkspaceResponse(
      options.anyPresent("accessLevel")(accessLevel),
      canShare,
      canCompute,
      canCatalog,
      WorkspaceDetails.fromWorkspaceAndOptions(
        workspace,
        authDomain,
        options.useAttributes,
        wsmContext.getCloudPlatform
      ),
      stats.flatten,
      bucketDetails,
      owners,
      wsmContext.azureCloudContext,
      Some(wsmContext.policies)
    )
  }

  def listWorkspaces(params: WorkspaceFieldSpecs, stringAttributeMaxLength: Int): Future[JsValue] = {
    val options = processOptions(params, stringAttributeMaxLength, WorkspaceFieldNames.workspaceListResponseFieldNames)

    def processDetails(workspace: AggregatedWorkspace,
                       samResource: SamUserResource,
                       accessLevel: WorkspaceAccessLevel,
                       stats: Option[WorkspaceSubmissionStats]
    ): WorkspaceListResponse = {
      val workspaceDetails =
        WorkspaceDetails.fromWorkspaceAndOptions(
          workspace.baseWorkspace,
          Option(
            samResource.authDomainGroups.map(groupName => ManagedGroupRef(RawlsGroupName(groupName.value)))
          ),
          useAttributes = options.attrSpecs.all || options.attrSpecs.attrsToSelect.nonEmpty,
          workspace.getCloudPlatform
        )

      val canShare = options.anyPresent("canShare") {
        accessLevel match {
          case WorkspaceAccessLevels.Read  => samResource.hasRole(SamWorkspaceRoles.shareReader)
          case WorkspaceAccessLevels.Write => samResource.hasRole(SamWorkspaceRoles.shareWriter)
          case _                           => accessLevel > WorkspaceAccessLevels.Write
        }
      }
      val canCompute: Option[Boolean] = options
        .anyPresent("canCompute") {
          workspace.getCloudPlatform.map {
            case WorkspaceCloudPlatform.Azure => accessLevel >= WorkspaceAccessLevels.Write
            case WorkspaceCloudPlatform.Gcp if accessLevel >= WorkspaceAccessLevels.Owner => true
            case WorkspaceCloudPlatform.Gcp => samResource.hasRole(SamWorkspaceRoles.canCompute)
          }
        }
        .flatten
      WorkspaceListResponse(
        accessLevel,
        canShare,
        canCompute,
        workspaceDetails,
        stats,
        samResource.public.roles.nonEmpty || samResource.public.actions.nonEmpty,
        Some(workspace.policies)
      )
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
      submissionSummaryStats <- options.anyPresentFuture("workspaceSubmissionStats") {
        workspaceRepository.listSubmissionSummaryStats(accessLevelWorkspaceUUIDs)
      }
      workspaces <- workspaceRepository.listWorkspacesByIds(accessLevelWorkspaceUUIDs, Option(options.attrSpecs))
      highestAccessLevelByWorkspaceId = accessLevelWorkspaceResources.map { resource =>
        resource.resourceId -> resource.allRoles.flatMap(role => WorkspaceAccessLevels.withRoleName(role.value)).max
      }.toMap
      workspaceSamResourceByWorkspaceId = accessLevelWorkspaceResources.map(r => r.resourceId -> r).toMap
      aggregatedWorkspaces = new AggregatedWorkspaceService(workspaceManagerDAO)
        .fetchAggregatedWorkspaces(workspaces, ctx)
        // Filter out workspaces with no cloud contexts
        .filter(ws => Try(ws.getCloudPlatform).map(context => context.isDefined).getOrElse(false))

      responseWorkspaces = aggregatedWorkspaces.map { wsmContext =>
        val workspace = wsmContext.baseWorkspace
        val workspaceResource = workspaceSamResourceByWorkspaceId(workspace.workspaceId)
        val accessLevel =
          if (workspaceResource.missingAuthDomainGroups.nonEmpty) WorkspaceAccessLevels.NoAccess
          else highestAccessLevelByWorkspaceId.getOrElse(workspace.workspaceId, WorkspaceAccessLevels.NoAccess)
        val stats = submissionSummaryStats.flatMap {
          _.get(wsmContext.baseWorkspace.workspaceIdAsUUID)
        }
        processDetails(wsmContext, workspaceResource, accessLevel, stats)
      }

    } yield deepFilterJsValue(responseWorkspaces.toJson, options.options)
  }

  /** Returns the Set of legal field names supplied by the user, trimmed of whitespace.
    * Throws an error if the user supplied an unrecognized field name.
    * Legal field names are any member of `WorkspaceResponse`, `WorkspaceDetails`,
    * or any arbitrary key starting with "workspace.attributes."
    *
    * @return QueryOptions consisting of the set of field names to be included in the response,
    *         and the attribute spec to be used in queries based on the options
    */
  private def processOptions(params: WorkspaceFieldSpecs,
                             stringAttributeMaxLength: Int = -1,
                             fieldNames: Set[LookupExpression] = WorkspaceFieldNames.workspaceResponseFieldNames
  ): QueryOptions = {
    // validate the inbound parameters
    val options = {
      // be lenient to whitespace, e.g. some user included spaces in their delimited string ("one, two, three")
      val args = params.fields.getOrElse(fieldNames).map(_.trim)
      // did the user specify any fields that we don't know about?
      // include custom leniency here for attributes: we can't validate attribute names because they are arbitrary,
      // so allow any field that starts with "workspace.attributes."
      val unrecognizedFields: Set[String] = args.diff(fieldNames).filter(!_.startsWith("workspace.attributes."))
      if (unrecognizedFields.nonEmpty) {
        throw new RawlsExceptionWithErrorReport(
          ErrorReport(StatusCodes.BadRequest,
                      s"Unrecognized field names: ${unrecognizedFields.toList.sorted.mkString(", ")}"
          )
        )
      }
      args
    }
    // if user requested the entire attributes map, or any individual attributes, retrieve attributes.
    val attrSpecs = WorkspaceAttributeSpecs(
      options.contains("workspace.attributes"),
      options
        .filter(_.startsWith("workspace.attributes."))
        .map(str => AttributeName.fromDelimitedName(str.replaceFirst("workspace.attributes.", "")))
        .toList,
      stringAttributeMaxLength
    )
    QueryOptions(options, attrSpecs)

  }

  private def loadResourceAuthDomain(resourceTypeName: SamResourceTypeName,
                                     resourceId: String
  ): Future[Set[ManagedGroupRef]] =
    samDAO
      .getResourceAuthDomain(resourceTypeName, resourceId, ctx)
      .map(_.map(g => ManagedGroupRef(RawlsGroupName(g))).toSet)

  def deleteWorkspace(workspaceName: WorkspaceName): Future[WorkspaceDeletionResult] = for {
    workspace <- getWorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.delete)
    _ = workspace.workspaceType match {
      case WorkspaceType.McWorkspace =>
        throw RawlsExceptionWithErrorReport(StatusCodes.BadRequest, "Multi Cloud workspaces not supported")
      case WorkspaceType.RawlsWorkspace => ()
    }
    _ <- requesterPaysSetupService.deleteAllRecordsForWorkspace(workspace)
    workflowsToAbort <- traceFutureWithParent("gatherWorkflowsToAbortAndSetStatusToAborted", ctx)(_ =>
      submissionsRepository.getActiveWorkflowsAndSetStatusToAborted(workspace)
    )
    // Attempt to abort any running workflows so they don't write any more to the bucket.
    // Notice that we're kicking off Futures to do the aborts concurrently, but we never collect their results!
    // This is because there's nothing we can do if Cromwell fails, so we might as well move on and let the
    // ExecutionContext run the futures whenever
    aborts = traceFutureWithParent("abortRunningWorkflows", ctx)(_ =>
      Future.traverse(workflowsToAbort)(wf => executionServiceCluster.abort(wf, ctx.userInfo))
    )
    _ <- traceFutureWithParent("deleteFastPassGrantsTransaction", ctx)(childContext =>
      fastPassServiceConstructor(childContext).removeFastPassGrantsForWorkspace(workspace)
    )
    // notify leonardo so it can cleanup any dangling sam resources and other non-cloud state
    _ <- traceFutureWithParent("notifyLeonardo", ctx)(_ =>
      leonardoService.cleanupResources(workspace.googleProjectId, workspace.workspaceIdAsUUID, ctx)
    )
    // Delete Google Project
    _ <- traceFutureWithParent("deleteGoogleProject", ctx)(_ => deleteGoogleProject(workspace.googleProjectId))
    // attempt to delete workspace in WSM, in case thsi is a TDR snapshot - but don't fail on it
    _ = Try(workspaceManagerDAO.deleteWorkspace(workspace.workspaceIdAsUUID, ctx)).recover {
      case e: ApiException if e.getCode != StatusCodes.NotFound.intValue =>
        logger.warn(s"Unexpected failure deleting workspace in WSM for workspace `${workspace.toWorkspaceName}]", e)
    }
    // Delete the workspace records in Rawls. Do this after deleting the google project to prevent service perimeter leaks.
    _ <- traceFutureWithParent("deleteWorkspaceTransaction", ctx)(_ =>
      workspaceRepository.deleteRawlsWorkspace(workspace)
    )
    // Delete workflowCollection resource in sam outside of DB transaction
    _ <- traceFutureWithParent("deleteWorkflowCollectionSamResource", ctx)(_ =>
      workspace.workflowCollectionName
        .map(cn => samDAO.deleteResource(SamResourceTypeNames.workflowCollection, cn, ctx))
        .getOrElse(Future.successful(())) recover {
        case t: RawlsExceptionWithErrorReport if t.errorReport.statusCode.contains(StatusCodes.NotFound) =>
          logger.warn(
            s"Received 404 from delete workflowCollection resource in Sam (while deleting workspace) for workspace `${workspace.toWorkspaceName}`: [${t.errorReport.message}]"
          )
        case t: RawlsExceptionWithErrorReport =>
          logger.error(
            s"Unexpected failure deleting workspace (while deleting workflowCollection in Sam) for workspace `${workspace.toWorkspaceName}`.",
            t
          )
          throw t
      }
    )
    _ <- traceFutureWithParent("deleteWorkspaceSamResource", ctx)(_ =>
      samDAO.deleteResource(SamResourceTypeNames.workspace, workspace.workspaceId, ctx) recover {
        case t: RawlsExceptionWithErrorReport if t.errorReport.statusCode.contains(StatusCodes.NotFound) =>
          logger.warn(
            s"Received 404 from delete workspace resource in Sam (while deleting workspace) for workspace `${workspace.toWorkspaceName}`: [${t.errorReport.message}]"
          )
        case t: RawlsExceptionWithErrorReport
            if t.errorReport.message.contains("Cannot delete a resource with children") =>
          MetricsHelper.incrementCounter(
            "leakingSamResourceError",
            labels = Map("cloud" -> "gcp", "projectType" -> workspace.projectType)
          )
          throw t
      }
    )
  } yield {
    aborts.onComplete {
      case Failure(t) =>
        logger.info(s"failure aborting workflows while deleting workspace ${workspace.toWorkspaceName}", t)
      case _ => /* ok */
    }
    WorkspaceDeletionResult.fromGcpBucketName(workspace.bucketName)
  }

  private def deleteGoogleProject(googleProjectId: GoogleProjectId): Future[Unit] = {
    def destroyPet(userIdInfo: UserIdInfo, projectName: GoogleProjectId): Future[Unit] =
      for {
        petSAJson <- samDAO.getPetServiceAccountKeyForUser(projectName, RawlsUserEmail(userIdInfo.userEmail))
        petUserInfo <- gcsDAO.getUserInfoUsingJson(petSAJson)
        _ <- samDAO.deleteUserPetServiceAccount(projectName, ctx.copy(userInfo = petUserInfo))
      } yield ()

    def deletePetsInProject(projectName: GoogleProjectId): Future[Unit] =
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
    for {
      _ <- deletePetsInProject(googleProjectId)
      _ <- gcsDAO.deleteGoogleProject(googleProjectId)
      _ <- samDAO.deleteResource(SamResourceTypeNames.googleProject, googleProjectId.value, ctx).recover {
        case regrets: RawlsExceptionWithErrorReport if regrets.errorReport.statusCode.contains(StatusCodes.NotFound) =>
          logger.info(
            s"google-project resource ${googleProjectId.value} not found in Sam. Continuing with workspace deletion"
          )
      }
    } yield ()
  }

  def updateLibraryAttributes(workspaceName: WorkspaceName,
                              operations: Seq[AttributeUpdateOperation]
  ): Future[WorkspaceDetails] =
    withLibraryAttributeNamespaceCheck(operations.map(_.name)) {
      for {
        isCurator <- gcsDAO.isLibraryCurator(ctx.userInfo.userEmail.value) recoverWith { case t =>
          throw new RawlsException("Unable to query for library curator status.", t)
        }
        workspace <- getV2WorkspaceContext(workspaceName) flatMap { workspace =>
          withLibraryPermissions(workspace, operations, ctx.userInfo, isCurator) {
            dataSource.inTransactionWithAttrTempTable(Set(AttributeTempTableType.Workspace))(
              dataAccess => updateV2Workspace(operations, dataAccess)(workspace.toWorkspaceName),
              TransactionIsolation.ReadCommitted
            ) // read committed to avoid deadlocks on workspace attr scratch table
          }
        }
        authDomain <- loadResourceAuthDomain(SamResourceTypeNames.workspace, workspace.workspaceId)
      } yield WorkspaceDetails(workspace, authDomain)
    }

  def updateWorkspace(workspaceName: WorkspaceName,
                      operations: Seq[AttributeUpdateOperation]
  ): Future[WorkspaceDetails] =
    withAttributeNamespaceCheck(operations.map(_.name)) {
      for {
        workspace <- getV2WorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.write)
        workspace <- dataSource.inTransactionWithAttrTempTable(Set(AttributeTempTableType.Workspace))(
          dataAccess => updateV2Workspace(operations, dataAccess)(workspace.toWorkspaceName),
          TransactionIsolation.ReadCommitted
        ) // read committed to avoid deadlocks on workspace attr scratch table
        authDomain <- loadResourceAuthDomain(SamResourceTypeNames.workspace, workspace.workspaceId)
      } yield WorkspaceDetails(workspace, authDomain)
    }

  private def updateV2Workspace(operations: Seq[AttributeUpdateOperation], dataAccess: DataAccess)(
    workspaceName: WorkspaceName
  ): ReadWriteAction[Workspace] =
    // get the source workspace again, to avoid race conditions where the workspace was updated outside of this transaction
    withV2WorkspaceContext(workspaceName, dataAccess) { workspaceContext =>
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
      // This is just filtering the workspaces for v2 workspace, since the tags query doesn't do this
      v2WorkspaceIdsForUser <- workspaceRepository
        .listWorkspacesByIds(workspaceIdsForUser)
        .map(workspaces => workspaces.map(ws => UUID.fromString(ws.workspaceId)))
      result <- workspaceRepository.getTags(v2WorkspaceIdsForUser, query, limit)
    } yield result

  // NOTE: Orchestration has its own implementation of cloneWorkspace. When changing something here, you may also need to update orchestration's implementation (maybe helpful search term: `Post(workspacePath + "/clone"`).
  def cloneWorkspace(sourceWorkspace: Workspace,
                     billingProject: RawlsBillingProject,
                     destWorkspaceRequest: WorkspaceRequest,
                     parentContext: RawlsRequestContext = ctx
  ): Future[Workspace] = {
    if (destWorkspaceRequest.copyFilesWithPrefix.exists(_.isEmpty))
      throw RawlsExceptionWithErrorReport(
        StatusCodes.BadRequest,
        """You may not specify an empty string for `copyFilesWithPrefix`. Did you mean to specify "/" or leave the field out entirely?"""
      )
    val (libraryAttributeNames, workspaceAttributeNames) =
      destWorkspaceRequest.attributes.keys.partition(_.namespace == AttributeName.libraryNamespace)

    for {
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
            sourceWorkspaceContext <- withV2WorkspaceContext(sourceWorkspace.toWorkspaceName, dataAccess)(
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
              val forceEnhancedBucketMonitoring =
                destWorkspaceRequest.enhancedBucketLogging.exists(identity) || sourceWorkspace.bucketName.startsWith(
                  s"${config.workspaceBucketNamePrefix}-secure"
                )
              createNewWorkspaceContext(
                destWorkspaceRequest.copy(authorizationDomain = Option(newAuthDomain),
                                          attributes = newAttrs,
                                          enhancedBucketLogging = Option(forceEnhancedBucketMonitoring)
                ),
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
      _ <- traceFutureWithParent("FastPassService.setupFastPassClonedWorkspace", parentContext)(childContext =>
        fastPassServiceConstructor(childContext)
          .setupFastPassForUserInClonedWorkspace(sourceWorkspaceContext, destWorkspaceContext)
      )
      _ <- traceFutureWithParent("FastPassService.setupFastPassClonedWorkspaceChild", parentContext)(childContext =>
        fastPassServiceConstructor(childContext)
          .syncFastPassesForUserInWorkspace(destWorkspaceContext)
      )

      _ <- traceFutureWithParent("cloneWsmWorkspace", parentContext)(context =>
        Future {
          workspaceManagerDAO.cloneWorkspace(
            sourceWorkspaceId = sourceWorkspaceContext.workspaceIdAsUUID,
            workspaceId = destWorkspaceContext.workspaceIdAsUUID,
            displayName = destWorkspaceContext.name,
            spendProfile = None,
            billingProjectNamespace = destWorkspaceContext.namespace,
            context
          )
        }.recoverWith { case e: ApiException =>
          if (e.getCode != StatusCodes.NotFound.intValue) {
            logger.warn(
              s"Unexpected failure cloning workspace (while cloning Rawls-stage workspace in Workspace Manager) [sourceWorkspaceId=${sourceWorkspaceContext.workspaceId}, destWorkspaceId=${destWorkspaceContext.workspaceId}]. Received ${e.getCode}: [${e.getResponseBody}]"
            )
            throw e
          } else {
            // 404 == workspace manager does not know about this workspace, move on
            Future.successful()
          }
        }
      )
      // we will fire and forget this. a more involved, but robust, solution involves using the Google Storage Transfer APIs
      // in most of our use cases, these files should copy quickly enough for there to be no noticeable delay to the user
      // we also don't want to block returning a response on this call because it's already a slow endpoint
      _ <- destWorkspaceRequest.copyFilesWithPrefix
        .map { prefix =>
          workspaceRepository.savePendingCloneWorkspaceFileTransfer(
            destWorkspaceContext.workspaceIdAsUUID,
            sourceWorkspaceContext.workspaceIdAsUUID,
            prefix
          )
        }
        .getOrElse(Future.successful())
    } yield destWorkspaceContext
  }

  def listPendingFileTransfersForWorkspace(
    workspaceName: WorkspaceName
  ): Future[Seq[PendingCloneWorkspaceFileTransfer]] = for {
    workspace <- getV2WorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.read)
    transfers <- workspaceRepository.listPendingCloneWorkspaceFileTransferRecords(Some(workspace.workspaceIdAsUUID))
  } yield transfers

  private def withClonedAuthDomain[T](sourceWorkspaceADs: Set[ManagedGroupRef], destWorkspaceADs: Set[ManagedGroupRef])(
    op: Set[ManagedGroupRef] => ReadWriteAction[T]
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
      workspace <- getV2WorkspaceContext(workspaceName)
      workspaceAclManager = workspace.workspaceType match {
        case WorkspaceType.RawlsWorkspace => rawlsWorkspaceAclManager
        case WorkspaceType.McWorkspace    => multiCloudWorkspaceAclManager
      }
      workspaceACL <- workspaceAclManager.getAcl(workspace.workspaceIdAsUUID, ctx)
    } yield workspaceACL

  def getCatalog(workspaceName: WorkspaceName): Future[Set[WorkspaceCatalog]] =
    loadV2WorkspaceId(workspaceName).flatMap { workspaceId =>
      samDAO
        .getPolicy(SamResourceTypeNames.workspace, workspaceId, SamWorkspacePolicyNames.canCatalog, ctx)
        .map(members => members.memberEmails.map(email => WorkspaceCatalog(email.value, true)))
    }

  private def loadV2WorkspaceId(workspaceName: WorkspaceName): Future[String] =
    workspaceRepository.getWorkspaceId(workspaceName).map {
      case None =>
        throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.NotFound, "unable to load workspace"))
      case Some(id) => id.toString
    }

  def updateCatalog(workspaceName: WorkspaceName,
                    input: Seq[WorkspaceCatalog]
  ): Future[WorkspaceCatalogUpdateResponseList] =
    for {
      workspaceId <- loadV2WorkspaceId(workspaceName)
      results <- Future.traverse(input) {
        case WorkspaceCatalog(email, true) =>
          samDAO
            .addUserToPolicy(
              SamResourceTypeNames.workspace,
              workspaceId,
              SamWorkspacePolicyNames.canCatalog,
              email,
              ctx
            )
            .map { _ =>
              Success(Either.right[String, WorkspaceCatalogResponse](WorkspaceCatalogResponse(email, true)))
            }
            .recover {
              case t: RawlsExceptionWithErrorReport if t.errorReport.statusCode.contains(StatusCodes.BadRequest) =>
                Success(Left(email))
              case t: Throwable => Failure(t)
            }
        case WorkspaceCatalog(email, false) =>
          samDAO
            .removeUserFromPolicy(
              SamResourceTypeNames.workspace,
              workspaceId,
              SamWorkspacePolicyNames.canCatalog,
              email,
              ctx
            )
            .map { _ =>
              Success(Either.right[String, WorkspaceCatalogResponse](WorkspaceCatalogResponse(email, false)))
            }
            .recover {
              case t: RawlsExceptionWithErrorReport if t.errorReport.statusCode.contains(StatusCodes.BadRequest) =>
                Success(Left(email))
              case t: Throwable => Failure(t)
            }
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

  /**
   * updates acls for a workspace
   * @param aclUpdates changes to make, if an entry already exists it will be changed to the level indicated in this
   *                   Set, use NoAccess to remove an entry, all other preexisting accesses remain unchanged
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
      */
    def aclUpdateToPolicies(workspaceACLUpdate: WorkspaceACLUpdate): Set[SamResourcePolicyName] = {
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

    collectMissingUsers(aclUpdates.map(_.email), ctx).flatMap { userToInvite =>
      if (userToInvite.isEmpty || inviteUsersNotFound) {
        for {
          workspace <- getV2WorkspaceContext(workspaceName)
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

          _ <- workspaceAclManager.maybeShareWorkspaceNamespaceCompute(policyAdditions, workspaceName, ctx)

          // Sync FastPass grants once ACLs are updated
          _ <- Future.traverse(policyRemovals.map(_._2) ++ policyAdditions.map(_._2)) { email =>
            fastPassServiceConstructor(ctx).syncFastPassesForUserInWorkspace(workspace, email)
          }
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
  ): Unit = {
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
      workspaceContext <- getV2WorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.own)

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

  def lockWorkspace(workspaceName: WorkspaceName): Future[Boolean] = for {
    workspace <- getV2WorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.own, ignoreLock = true)
    locked <- workspaceRepository.lockWorkspace(workspace)
  } yield locked

  def unlockWorkspace(workspaceName: WorkspaceName): Future[Boolean] = for {
    workspace <- getV2WorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.own, ignoreLock = true)
    unlocked <- workspaceRepository.unlockWorkspace(workspace)
  } yield unlocked

  /**
   * Applies the sequence of operations in order to the workspace.
   *
   * @param workspace to update
   * @param operations sequence of operations
   * @throws AttributeNotFoundException when removing from a list attribute that does not exist
   * @throws AttributeUpdateOperationException when adding or removing from an attribute that is not a list
   * @return the updated entity
   */
  private def applyOperationsToWorkspace(workspace: Workspace, operations: Seq[AttributeUpdateOperation]): Workspace =
    workspace.copy(attributes = applyAttributeUpdateOperations(workspace, operations))

  private def getGoogleBucketPermissionsFromRoles(workspaceRoles: Set[SamResourceRole]): Future[Set[IamPermission]] = {
    val googleRole = if (workspaceRoles.intersect(SamWorkspaceRoles.rolesContainingWritePermissions).nonEmpty) {
      // workspace project owner, owner and writer have terraBucketWriterRole
      Option(terraBucketWriterRole)
    } else if (workspaceRoles.contains(SamWorkspaceRoles.reader)) {
      // workspace reader has terraBucketReaderRole
      Option(terraBucketReaderRole)
    } else None

    getPermissionsFromRoles(googleRole.toSet)
  }

  private def getGoogleProjectPermissionsFromRoles(workspaceRoles: Set[SamResourceRole]): Future[Set[IamPermission]] = {
    val googleRoles = workspaceRoles.flatMap {
      case SamWorkspaceRoles.projectOwner =>
        Set(terraBillingProjectOwnerRole, terraWorkspaceCanComputeRole, terraWorkspaceNextflowRole)
      case SamWorkspaceRoles.owner | SamWorkspaceRoles.canCompute =>
        Set(terraWorkspaceCanComputeRole, terraWorkspaceNextflowRole)
      case _ => Set.empty
    }

    getPermissionsFromRoles(googleRoles)
  }

  private def getStatusCodeHandlingUnknown(intCode: Integer): StatusCode =
    StatusCodes
      .getForKey(intCode)
      .getOrElse(
        StatusCodes.custom(intCode, "Google API failure", "failure with non-standard status code", false, true)
      )

  private def getPermissionsFromRoles(googleRoles: Set[String]): Future[Set[IamPermission]] =
    Future
      .traverse(googleRoles) { googleRole =>
        googleIamDao.getOrganizationCustomRole(googleRole)
      }
      .map(_.flatten.flatMap(_.getIncludedPermissions.asScala.map(IamPermission)))

  /*
       If the user only has read access, check the bucket using the default pet.
       If the user has a higher level of access, check the bucket using the pet for this workspace's project.

       We use the default pet when possible because the default pet is created in a per-user shell project, i.e. not in
         this workspace's project. This prevents proliferation of service accounts within this workspace's project. For
         FireCloud's common read-only public workspaces, this is an important safeguard; else those common projects
         would constantly hit limits on the number of allowed service accounts.

       If the user has write access, we need to use the pet for this workspace's project in order to get accurate results.
   */
  def checkWorkspaceCloudPermissions(workspaceName: WorkspaceName): Future[Unit] =
    for {
      workspace <- getV2WorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.read)

      _ <- workspace.workspaceType match {
        case WorkspaceType.McWorkspace =>
          Future.failed(
            new RawlsExceptionWithErrorReport(
              ErrorReport(StatusCodes.NotImplemented, "not implemented for McWorkspace")
            )
          )
        case WorkspaceType.RawlsWorkspace => Future.successful(())
      }

      workspaceRoles <- samDAO.listUserRolesForResource(SamResourceTypeNames.workspace,
                                                        workspace.workspaceIdAsUUID.toString,
                                                        ctx
      )

      useDefaultPet = workspaceRoles.intersect(SamWorkspaceRoles.rolesContainingWritePermissions).isEmpty

      petKey <-
        if (useDefaultPet)
          samDAO.getDefaultPetServiceAccountKeyForUser(ctx)
        else
          samDAO.getPetServiceAccountKeyForUser(workspace.googleProjectId, ctx.userInfo.userEmail)

      // google api will error if any permission starts with something other than "storage."
      expectedGoogleBucketPermissions <- getGoogleBucketPermissionsFromRoles(workspaceRoles).map(
        _.filter(_.value.startsWith("storage."))
      )
      expectedGoogleProjectPermissions <- getGoogleProjectPermissionsFromRoles(workspaceRoles).map(
        _.filterNot(_.value.startsWith("resourcemanager."))
      )

      bucketIamResults <- gcsDAO
        .testSAGoogleBucketIam(
          GcsBucketName(workspace.bucketName),
          petKey,
          expectedGoogleBucketPermissions
        )
        .recoverWith {
          // Throw with the status code of the exception (for example 403 for invalid billing, 400 for requester pays)
          // instead of a 500 to avoid Sentry notifications.
          case t: StorageException =>
            Future.failed(new RawlsExceptionWithErrorReport(ErrorReport(getStatusCodeHandlingUnknown(t.getCode), t)))
          case t: GoogleJsonResponseException =>
            val code = getStatusCodeHandlingUnknown(t.getStatusCode)
            Future.failed(new RawlsExceptionWithErrorReport(ErrorReport(code, t.getDetails.toString)))
        }
      bucketLocationResult <- gcsDAO.testSAGoogleBucketGetLocationOrRequesterPays(
        GoogleProject(workspace.googleProjectId.value),
        GcsBucketName(workspace.bucketName),
        petKey
      )
      _ <- ApplicativeThrow[Future].raiseWhen(useDefaultPet && expectedGoogleProjectPermissions.nonEmpty) {
        new RawlsException("user has workspace read-only access yet has expected google project permissions")
      }

      projectIamResults <- gcsDAO
        .testSAGoogleProjectIam(GoogleProject(workspace.googleProjectId.value),
                                petKey,
                                expectedGoogleProjectPermissions
        )
        .recoverWith {
          case t: GoogleJsonResponseException =>
            val code = getStatusCodeHandlingUnknown(t.getStatusCode)
            Future.failed(new RawlsExceptionWithErrorReport(ErrorReport(code, t.getDetails.toString)))
          case t: IOException =>
            // Throw a 400 to avoid Sentry notifications.
            Future.failed(new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.BadRequest, t)))
        }

      missingBucketPermissions = expectedGoogleBucketPermissions -- bucketIamResults
      missingProjectPermissions = expectedGoogleProjectPermissions -- projectIamResults

      petEmail = petKey.parseJson.asJsObject().fields.getOrElse("client_email", JsString("UNKNOWN"))
      _ <-
        if (missingBucketPermissions.nonEmpty || missingProjectPermissions.nonEmpty) {
          val message = s"user email ${ctx.userInfo.userEmail}, pet email ${petEmail.toString()} missing permissions [${missingProjectPermissions
              .mkString(",")}] on google project ${workspace.googleProjectId.value}, missing permissions [${missingBucketPermissions
              .mkString(",")}] on google bucket ${workspace.bucketName} for workspace ${workspace.toWorkspaceName.toString}"
          logger.info("checkWorkspaceCloudPermissions: " + message)
          fastPassServiceConstructor(ctx)
            .syncFastPassesForUserInWorkspace(workspace)
            .flatMap(_ =>
              Future.failed(
                new RawlsExceptionWithErrorReport(
                  ErrorReport(
                    StatusCodes.Forbidden,
                    message
                  )
                )
              )
            )
        } else {
          val message = s"user email ${ctx.userInfo.userEmail}, pet email ${petEmail
              .toString()} has all permissions on google project ${workspace.googleProjectId.value} and google bucket ${workspace.bucketName} for workspace ${workspace.toWorkspaceName.toString}"
          logger.info("checkWorkspaceCloudPermissions: " + message)
          Future.successful(())
        }
      _ <-
        if (expectedGoogleBucketPermissions.contains(IamPermission(BUCKET_GET_PERMISSION)) && !bucketLocationResult) {
          val message = s"user email ${ctx.userInfo.userEmail}, pet email ${petEmail
              .toString()} was unable to get bucket location for ${workspace.googleProjectId.value}/${workspace.bucketName} for workspace ${workspace.toWorkspaceName.toString}"
          logger.warn("checkWorkspaceCloudPermissions: " + message)
          fastPassServiceConstructor(ctx)
            .syncFastPassesForUserInWorkspace(workspace)
            .flatMap(_ =>
              Future.failed(
                new RawlsExceptionWithErrorReport(
                  ErrorReport(
                    StatusCodes.Forbidden,
                    message
                  )
                )
              )
            )
        } else {
          val message = s"user email ${ctx.userInfo.userEmail}, pet email ${petEmail
              .toString()} was able to get bucket location for ${workspace.googleProjectId.value}/${workspace.bucketName} for workspace ${workspace.toWorkspaceName.toString}"
          logger.info("checkWorkspaceCloudPermissions: " + message)
          Future.successful()
        }
    } yield ()

  def checkSamActionWithLock(workspaceName: WorkspaceName, samAction: SamResourceAction): Future[Boolean] =
    getV2WorkspaceContextAndPermissions(workspaceName, samAction, None)
      .map(_ => true)
      .recover { case _ => false }

  // Finds workspace by workspaceName
  // moved out of WorkspaceSupport because the only usage was in this file,
  // and it has raw datasource/dataAccess usage, which is being refactored out of WorkspaceSupport
  private def withV2WorkspaceContext[T](workspaceName: WorkspaceName,
                                        dataAccess: DataAccess,
                                        attributeSpecs: Option[WorkspaceAttributeSpecs] = None
  )(op: Workspace => ReadWriteAction[T]) =
    dataAccess.workspaceQuery.findV2WorkspaceByName(workspaceName, attributeSpecs) flatMap {
      case None            => throw NoSuchWorkspaceException(workspaceName)
      case Some(workspace) => op(workspace)
    }

  def getBucketOptions(workspaceName: WorkspaceName): Future[WorkspaceBucketOptions] = for {
    workspaceContext <- getV2WorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.read)
    options <- gcsDAO.getBucketDetails(workspaceContext.bucketName, workspaceContext.googleProjectId)
  } yield options

  def getBucketUsage(workspaceName: WorkspaceName): Future[BucketUsageResponse] = (for {
    workspaceContext <- getV2WorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.read)
    bucketUsage <- gcsDAO.getBucketUsage(workspaceContext.googleProjectId, workspaceContext.bucketName, None)
  } yield bucketUsage).recover {
    // Throw with the status code of the google exception (for example 403 for invalid billing, 404 for inactive project)
    // instead of a 500 to avoid Sentry notifications.
    case t: GoogleJsonResponseException =>
      val code = getStatusCodeHandlingUnknown(t.getStatusCode)
      throw new RawlsExceptionWithErrorReport(ErrorReport(code, t.getDetails.toString))
  }

  def getAccessInstructions(workspaceName: WorkspaceName): Future[Seq[ManagedGroupAccessInstructions]] =
    for {
      workspaceId <- loadV2WorkspaceId(workspaceName)
      authDomains <- samDAO.getResourceAuthDomain(SamResourceTypeNames.workspace, workspaceId, ctx)
      instructions <- Future.traverse(authDomains) { adGroup =>
        samDAO.getAccessInstructions(WorkbenchGroupName(adGroup), ctx).map { maybeInstruction =>
          maybeInstruction.map(i => ManagedGroupAccessInstructions(adGroup, i))
        }
      }
    } yield instructions.flatten

  def enableRequesterPaysForLinkedSAs(workspaceName: WorkspaceName): Future[Unit] =
    for {
      maybeWorkspace <- dataSource.inTransaction(dataAccess =>
        dataAccess.workspaceQuery.findV2WorkspaceByName(workspaceName)
      )
      workspace <- maybeWorkspace match {
        case None            => Future.failed(NoSuchWorkspaceException(workspaceName))
        case Some(workspace) => Future.successful(workspace)
      }
      _ <- accessCheck(workspace, SamWorkspaceActions.compute)
      _ <- checkLock(workspace, SamWorkspaceActions.compute)
      _ <- requesterPaysSetupService.grantRequesterPaysToLinkedSAs(ctx.userInfo, workspace)
    } yield {}

  def disableRequesterPaysForLinkedSAs(workspaceName: WorkspaceName): Future[Unit] =
    // note that this does not throw an error if the workspace does not exist
    // the user may no longer have access to the workspace so we can't confirm it exists
    // but the user does have the right to remove their linked SAs
    for {
      maybeWorkspace <- dataSource.inTransaction { dataaccess =>
        dataaccess.workspaceQuery.findV2WorkspaceByName(workspaceName)
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
      _ <- traceFutureWithParent("createResourceFull", parentContext)(_ =>
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
      googleProjectId <- traceFutureWithParent("getGoogleProjectFromBuffer", parentContext) { _ =>
        resourceBufferService.getGoogleProjectFromBuffer(
          if (billingProject.servicePerimeter.isDefined)
            ProjectPoolType.ExfiltrationControlled
          else
            ProjectPoolType.Regular,
          rbsHandoutRequestId
        )
      }

      _ <- traceFutureWithParent("maybeMoveGoogleProjectToFolder", parentContext) { _ =>
        billingProject.servicePerimeter.traverse_ {
          logger.info(s"Moving google project $googleProjectId to service perimeter folder.")
          userServiceConstructor(ctx).moveGoogleProjectToServicePerimeterFolder(_, googleProjectId)
        }
      }

      googleProject <- gcsDAO.getGoogleProject(googleProjectId)
      _ <- traceFutureWithParent("remove RBS SA from owner policy", parentContext) { _ =>
        gcsDAO.removePolicyBindings(
          googleProjectId,
          Map(
            "roles/owner" -> Set("serviceAccount:" + resourceBufferService.serviceAccountEmail)
          )
        )
      }
    } yield (googleProjectId, gcsDAO.getGoogleProjectNumber(googleProject))

  def setProjectBillingAccount(googleProjectId: GoogleProjectId,
                               billingAccount: RawlsBillingAccountName,
                               workspaceId: String,
                               parentContext: RawlsRequestContext = ctx
  ): Future[ProjectBillingInfo] =
    traceFutureWithParent("updateGoogleProjectBillingAccount", parentContext) { childContext =>
      logger.info(
        s"Setting billing account for $googleProjectId to $billingAccount replacing existing billing account."
      )
      setTraceSpanAttribute(childContext, AttributeKey.stringKey("workspaceId"), workspaceId)
      setTraceSpanAttribute(childContext, AttributeKey.stringKey("googleProjectId"), googleProjectId.value)
      setTraceSpanAttribute(childContext, AttributeKey.stringKey("billingAccount"), billingAccount.value)
      gcsDAO.setBillingAccountName(googleProjectId, billingAccount, childContext.toTracingContext)
    }

  def setupGoogleProjectIam(googleProjectId: GoogleProjectId,
                            policyEmailsByName: Map[SamResourcePolicyName, WorkbenchEmail],
                            billingProjectOwnerPolicyEmail: WorkbenchEmail,
                            parentContext: RawlsRequestContext = ctx
  ): Future[Unit] =
    traceFutureWithParent("updateGoogleProjectIam", parentContext) { _ =>
      logger.info(s"Updating google project IAM $googleProjectId.")

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
          googleIamDao.addRoles(
            GoogleProject(googleProjectId.value),
            email,
            IamMemberTypes.Group,
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
    traceFutureWithParent("renameAndLabelProject", parentContext) { _ =>
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
    */
  private def maybeUpdateGoogleProjectsInPerimeter(billingProject: RawlsBillingProject,
                                                   dataAccess: DataAccess
  ): ReadWriteAction[Unit] =
    billingProject.servicePerimeter.traverse_ { servicePerimeterName =>
      servicePerimeterService.overwriteGoogleProjectsInPerimeter(servicePerimeterName, dataAccess)
    }

  private def failUnlessBillingAccountHasAccess(billingProject: RawlsBillingProject,
                                                parentContext: RawlsRequestContext = ctx
  ): Future[Unit] =
    traceFutureWithParent("updateAndGetBillingAccountAccess", parentContext) { s =>
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
  private def updateAndGetBillingAccountAccess(billingProject: RawlsBillingProject,
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

      hasAccess <- traceFutureWithParent("checkBillingAccountIAM", parentContext)(_ =>
        gcsDAO.testTerraBillingAccountAccess(billingAccountName)
      )

      invalidBillingAccount = !hasAccess
      _ <- Applicative[Future].whenA(billingProject.invalidBillingAccount != invalidBillingAccount) {
        billingRepository.updateBillingAccountValidity(billingAccountName, invalidBillingAccount)
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
      traceFutureWithParent("createResourceFull (workspace)", parentContext)(_ =>
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
    traceFutureWithParent("traversePolicies", parentContext)(s1 =>
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
          traceFutureWithParent(s"syncPolicy-$policyName", s1)(_ =>
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
                                        googleProjectId: GoogleProjectId,
                                        googleProjectNumber: Option[GoogleProjectNumber],
                                        currentBillingAccountOnWorkspace: Option[RawlsBillingAccountName],
                                        state: WorkspaceState,
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
      errorMessage = None,
      completedCloneWorkspaceFileTransfer = completedCloneWorkspaceFileTransfer,
      workspaceType,
      state
    )

    traceDBIOWithParent("save", parentContext)(_ => dataAccess.workspaceQuery.createOrUpdate(workspace))
      .map(_ => workspace)
  }

  // TODO: find and assess all usages. This is written to reside inside a DB transaction, but it makes external REST calls.
  private def createNewWorkspaceContext(workspaceRequest: WorkspaceRequest,
                                        billingProject: RawlsBillingProject,
                                        sourceBucketName: Option[String],
                                        dataAccess: DataAccess,
                                        parentContext: RawlsRequestContext
  ): ReadWriteAction[Workspace] = {

    def getBucketName(workspaceId: String, enhancedBucketLogging: Boolean) =
      s"${config.workspaceBucketNamePrefix}-${if (enhancedBucketLogging) "secure-" else ""}$workspaceId"

    def getLabels(authDomain: List[ManagedGroupRef]): Map[String, String] = authDomain match {
      case Nil => Map(WorkspaceService.SECURITY_LABEL_KEY -> WorkspaceService.LOW_SECURITY_LABEL)
      case ads =>
        Map(WorkspaceService.SECURITY_LABEL_KEY -> WorkspaceService.HIGH_SECURITY_LABEL) ++ ads.map(ad =>
          gcsDAO.labelSafeString(ad.membersGroupName.value, "ad-") -> ""
        )
    }

    for {
      _ <- failIfWorkspaceExists(workspaceRequest.toWorkspaceName)
      _ <- Applicative[ReadWriteAction].whenA(workspaceRequest.noWorkspaceOwner.contains(true)) {
        traceDBIOWithParent("maybeRequireBillingProjectOwnerAccess", parentContext) { s =>
          DBIO.from(requireBillingProjectOwnerAccess(RawlsBillingProjectName(workspaceRequest.namespace), s))
        }.asInstanceOf[ReadWriteAction[Unit]]
      }

      workspaceId = UUID.randomUUID.toString
      _ = logger.info(s"createWorkspace - workspace:'${workspaceRequest.name}' - UUID:$workspaceId")
      bucketName = getBucketName(
        workspaceId,
        workspaceRequest.authorizationDomain.exists(_.nonEmpty) || workspaceRequest.enhancedBucketLogging.exists(
          identity
        )
      )

      billingAccount <- billingProject.billingAccount match {
        case Some(ba) => DBIO.successful(ba)
        case _ =>
          DBIO.failed(
            RawlsExceptionWithErrorReport(
              ErrorReport(StatusCodes.BadRequest, s"Billing Account is missing: $billingProject")
            )
          )
      }
      workspaceName = WorkspaceName(workspaceRequest.namespace, workspaceRequest.name)
      // add the workspace id to the span so we can find and correlate it later with other services
      _ = setTraceSpanAttribute(parentContext, AttributeKey.stringKey("workspaceId"), workspaceId)

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
            _ <- setProjectBillingAccount(googleProjectId, billingAccount, workspaceId, span)
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
          googleProjectId,
          Some(googleProjectNumber),
          Option(billingAccount),
          WorkspaceState.Ready,
          dataAccess,
          span
        )
      )

      _ <- traceDBIOWithParent("updateServicePerimeter", parentContext)(_ =>
        maybeUpdateGoogleProjectsInPerimeter(billingProject, dataAccess)
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
            childContext,
            workspaceBucketLocation
          )
        )
      )
      _ = workspaceRequest.bucketLocation.foreach(location =>
        logger.info(
          s"Internal bucket for workspace `${workspaceRequest.name}` in namespace `${workspaceRequest.namespace}` was created in region `$location`."
        )
      )

      // proactively create pet service account for user to start propagation of IAM
      _ <- traceDBIOWithParent("samDAO.getPetServiceAccountKeyForUser", parentContext)(_ =>
        DBIO.from(
          samDAO.getPetServiceAccountKeyForUser(savedWorkspace.googleProjectId, ctx.userInfo.userEmail)
        )
      )
    } yield savedWorkspace
  }

  def failIfWorkspaceExists(name: WorkspaceName): ReadWriteAction[Unit] =
    dataSource.dataAccess.workspaceQuery.getWorkspaceId(name).map { workspaceId =>
      if (workspaceId.isDefined)
        throw RawlsExceptionWithErrorReport(
          ErrorReport(StatusCodes.Conflict, s"Workspace '$name' already exists")
        )
    }

  def failIfBucketRegionInvalid(bucketRegion: Option[String]): Future[Unit] =
    bucketRegion.traverse_ { region =>
      // if the user specifies a region for the workspace bucket, it must be in the proper format
      // for a single region or the default bucket location (US multi region)
      val singleRegionPattern = "[A-Za-z]+-[A-Za-z]+[0-9]+"
      val validUSPattern = "US"
      ApplicativeThrow[Future].raiseUnless(region.matches(singleRegionPattern) || region.equals(validUSPattern)) {
        RawlsExceptionWithErrorReport(
          ErrorReport(
            StatusCodes.BadRequest,
            s"Workspace bucket location must be a single " +
              s"region of format: $singleRegionPattern or the default bucket location ('US')."
          )
        )
      }
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

  private def withLibraryAttributeNamespaceCheck[T](attributeNames: Iterable[AttributeName])(op: => T): T = {
    val namespaces = attributeNames.map(_.namespace).toSet

    // only allow library namespace
    val invalidNamespaces = namespaces -- Set(AttributeName.libraryNamespace)
    if (invalidNamespaces.isEmpty) op
    else {
      val err =
        ErrorReport(statusCode = StatusCodes.BadRequest, message = s"All attributes must be in the library namespace")
      throw new RawlsExceptionWithErrorReport(errorReport = err)
    }
  }
}

class InvalidWorkspaceAclUpdateException(errorReport: ErrorReport) extends RawlsExceptionWithErrorReport(errorReport)
