package org.broadinstitute.dsde.rawls.workspace

import java.util.UUID

import akka.actor.ActorSystem
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.stream.Materializer
import bio.terra.workspace.client.ApiException
import cats.implicits._
import com.google.api.services.storage.model.StorageObject
import com.typesafe.scalalogging.LazyLogging
import io.opencensus.scala.Tracing._
import io.opencensus.trace.{Span, Status, AttributeValue => OpenCensusAttributeValue}
import org.broadinstitute.dsde.rawls.config.WorkspaceServiceConfig
import org.broadinstitute.dsde.rawls.{RawlsException, RawlsExceptionWithErrorReport}
import slick.jdbc.TransactionIsolation
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.dataaccess.datarepo.DataRepoDAO
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
import org.broadinstitute.dsde.rawls.model.ExecutionJsonSupport.{ActiveSubmissionFormat, SubmissionFormat, SubmissionListResponseFormat, SubmissionReportFormat, SubmissionValidationReportFormat, WorkflowCostFormat, WorkflowOutputsFormat, WorkflowQueueStatusByUserResponseFormat, WorkflowQueueStatusResponseFormat}
import org.broadinstitute.dsde.rawls.model.MethodRepoJsonSupport.AgoraEntityFormat
import org.broadinstitute.dsde.rawls.model.WorkflowFailureModes.WorkflowFailureMode
import org.broadinstitute.dsde.rawls.model.WorkflowStatuses.WorkflowStatus
import org.broadinstitute.dsde.rawls.model.WorkspaceACLJsonSupport.{WorkspaceACLFormat, WorkspaceACLUpdateResponseListFormat, WorkspaceCatalogFormat, WorkspaceCatalogUpdateResponseListFormat}
import org.broadinstitute.dsde.rawls.model.WorkspaceAccessLevels._
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.resourcebuffer.ResourceBufferService
import org.broadinstitute.dsde.rawls.user.UserService
import org.broadinstitute.dsde.rawls.util.OpenCensusDBIOUtils._
import org.broadinstitute.dsde.rawls.util._
import org.broadinstitute.dsde.rawls.webservice.PerRequest
import org.broadinstitute.dsde.rawls.webservice.PerRequest._
import org.broadinstitute.dsde.workbench.model.{WorkbenchEmail, WorkbenchException, WorkbenchGroupName}
import org.joda.time.DateTime
import spray.json.DefaultJsonProtocol._
import spray.json._

import scala.concurrent.duration._
import scala.language.postfixOps
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

/**
 * Created by dvoet on 4/27/15.
 */
//noinspection TypeAnnotation
object WorkspaceService {
  def constructor(dataSource: SlickDataSource, methodRepoDAO: MethodRepoDAO, cromiamDAO: ExecutionServiceDAO,
                  executionServiceCluster: ExecutionServiceCluster, execServiceBatchSize: Int, workspaceManagerDAO: WorkspaceManagerDAO,
                  dataRepoDAO: DataRepoDAO, methodConfigResolver: MethodConfigResolver, gcsDAO: GoogleServicesDAO, samDAO: SamDAO,
                  notificationDAO: NotificationDAO, userServiceConstructor: UserInfo => UserService,
                  genomicsServiceConstructor: UserInfo => GenomicsService, maxActiveWorkflowsTotal: Int,
                  maxActiveWorkflowsPerUser: Int, workbenchMetricBaseName: String, submissionCostService: SubmissionCostService,
                  config: WorkspaceServiceConfig, requesterPaysSetupService: RequesterPaysSetupService,
                  entityManager: EntityManager, resourceBufferService: ResourceBufferService)
                 (userInfo: UserInfo)
                 (implicit system: ActorSystem, materializer: Materializer, executionContext: ExecutionContext): WorkspaceService = {

    new WorkspaceService(userInfo, dataSource, entityManager, methodRepoDAO, cromiamDAO,
      executionServiceCluster, execServiceBatchSize, workspaceManagerDAO,
      methodConfigResolver, gcsDAO, samDAO,
      notificationDAO, userServiceConstructor,
      genomicsServiceConstructor, maxActiveWorkflowsTotal,
      maxActiveWorkflowsPerUser, workbenchMetricBaseName, submissionCostService,
      config, requesterPaysSetupService, resourceBufferService)
  }

  val SECURITY_LABEL_KEY = "security"
  val HIGH_SECURITY_LABEL = "high"
  val LOW_SECURITY_LABEL = "low"

  private[workspace] def extractOperationIdsFromCromwellMetadata(metadataJson: JsObject): Iterable[String] = {
    case class Call(jobId: Option[String])
    case class OpMetadata(calls: Option[Map[String, Seq[Call]]])
    implicit val callFormat = jsonFormat1(Call)
    implicit val opMetadataFormat = jsonFormat1(OpMetadata)

    for {
      calls <- metadataJson.convertTo[OpMetadata].calls.toList // toList on the Option makes the compiler like the for comp
      call <- calls.values.flatten
      jobId <- call.jobId
    } yield jobId
  }

  private[workspace] def getTerminalStatusDate(submission: Submission, workflowID: Option[String]): Option[DateTime] = {
    // find all workflows that have finished
    val terminalWorkflows = submission.workflows.filter(workflow => WorkflowStatuses.terminalStatuses.contains(workflow.status))
    // optionally limit the list to a specific workflowID
    val workflows = workflowID match {
      case Some(_) => terminalWorkflows.filter(_.workflowId == workflowID)
      case None => terminalWorkflows
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
class WorkspaceService(protected val userInfo: UserInfo, val dataSource: SlickDataSource, val entityManager: EntityManager, val methodRepoDAO: MethodRepoDAO, cromiamDAO: ExecutionServiceDAO, executionServiceCluster: ExecutionServiceCluster, execServiceBatchSize: Int, val workspaceManagerDAO: WorkspaceManagerDAO, val methodConfigResolver: MethodConfigResolver, protected val gcsDAO: GoogleServicesDAO, val samDAO: SamDAO, notificationDAO: NotificationDAO, userServiceConstructor: UserInfo => UserService, genomicsServiceConstructor: UserInfo => GenomicsService, maxActiveWorkflowsTotal: Int, maxActiveWorkflowsPerUser: Int, override val workbenchMetricBaseName: String, submissionCostService: SubmissionCostService, config: WorkspaceServiceConfig, requesterPaysSetupService: RequesterPaysSetupService, resourceBufferService: ResourceBufferService)(implicit val system: ActorSystem, val materializer: Materializer, protected val executionContext: ExecutionContext)
  extends RoleSupport with LibraryPermissionsSupport with FutureSupport with MethodWiths with UserWiths with LazyLogging with RawlsInstrumented with JsonFilterUtils with WorkspaceSupport with EntitySupport with AttributeSupport with Retry {

  import dataSource.dataAccess.driver.api._

  def CreateWorkspace(workspace: WorkspaceRequest, parentSpan: Span = null) = createWorkspace(workspace, parentSpan)
  def GetWorkspace(workspaceName: WorkspaceName, params: WorkspaceFieldSpecs, parentSpan: Span) = getWorkspace(workspaceName, params, parentSpan)
  def DeleteWorkspace(workspaceName: WorkspaceName) = deleteWorkspace(workspaceName)
  def UpdateWorkspace(workspaceName: WorkspaceName, operations: Seq[AttributeUpdateOperation]) = updateWorkspace(workspaceName, operations)
  def UpdateLibraryAttributes(workspaceName: WorkspaceName, operations: Seq[AttributeUpdateOperation]) = updateLibraryAttributes(workspaceName, operations)
  def ListWorkspaces(params: WorkspaceFieldSpecs, parentSpan: Span) = listWorkspaces(params, parentSpan)
  def ListAllWorkspaces = listAllWorkspaces()
  def GetTags(query: Option[String]) = getTags(query)
  def AdminListWorkspacesWithAttribute(attributeName: AttributeName, attributeValue: AttributeValue) = asFCAdmin { listWorkspacesWithAttribute(attributeName, attributeValue) }
  def CloneWorkspace(sourceWorkspace: WorkspaceName, destWorkspace: WorkspaceRequest) = cloneWorkspace(sourceWorkspace, destWorkspace)
  def GetACL(workspaceName: WorkspaceName) = getACL(workspaceName)
  def UpdateACL(workspaceName: WorkspaceName, aclUpdates: Set[WorkspaceACLUpdate], inviteUsersNotFound: Boolean) = updateACL(workspaceName, aclUpdates, inviteUsersNotFound)
  def SendChangeNotifications(workspaceName: WorkspaceName) = sendChangeNotifications(workspaceName)
  def GetCatalog(workspaceName: WorkspaceName) = getCatalog(workspaceName)
  def UpdateCatalog(workspaceName: WorkspaceName, catalogUpdates: Seq[WorkspaceCatalog]) = updateCatalog(workspaceName, catalogUpdates)
  def LockWorkspace(workspaceName: WorkspaceName) = lockWorkspace(workspaceName)
  def UnlockWorkspace(workspaceName: WorkspaceName) = unlockWorkspace(workspaceName)
  def CheckBucketReadAccess(workspaceName: WorkspaceName) = checkBucketReadAccess(workspaceName)
  def CheckSamActionWithLock(workspaceName: WorkspaceName, requiredAction: SamResourceAction) = checkSamActionWithLock(workspaceName, requiredAction)
  def GetBucketUsage(workspaceName: WorkspaceName) = getBucketUsage(workspaceName)
  def GetBucketOptions(workspaceName: WorkspaceName) = getBucketOptions(workspaceName)
  //def UpdateBucketOptions(workspaceName: WorkspaceName, bucketOptions: WorkspaceBucketOptions) = updateBucketOptions(workspaceName, bucketOptions)
  def GetAccessInstructions(workspaceName: WorkspaceName) = getAccessInstructions(workspaceName)
  def EnableRequesterPaysForLinkedSAs(workspaceName: WorkspaceName) = enableRequesterPaysForLinkedSAs(workspaceName)
  def DisableRequesterPaysForLinkedSAs(workspaceName: WorkspaceName) = disableRequesterPaysForLinkedSAs(workspaceName)

  def CreateMethodConfiguration(workspaceName: WorkspaceName, methodConfiguration: MethodConfiguration) = createMethodConfiguration(workspaceName, methodConfiguration)
  def RenameMethodConfiguration(workspaceName: WorkspaceName, methodConfigurationNamespace: String, methodConfigurationName: String, newName: MethodConfigurationName) = renameMethodConfiguration(workspaceName, methodConfigurationNamespace, methodConfigurationName, newName)
  def DeleteMethodConfiguration(workspaceName: WorkspaceName, methodConfigurationNamespace: String, methodConfigurationName: String) = deleteMethodConfiguration(workspaceName, methodConfigurationNamespace, methodConfigurationName)
  def GetMethodConfiguration(workspaceName: WorkspaceName, methodConfigurationNamespace: String, methodConfigurationName: String) = getMethodConfiguration(workspaceName, methodConfigurationNamespace, methodConfigurationName)
  def OverwriteMethodConfiguration(workspaceName: WorkspaceName, methodConfigurationNamespace: String, methodConfigurationName: String, newMethodConfiguration: MethodConfiguration) = overwriteMethodConfiguration(workspaceName, methodConfigurationNamespace, methodConfigurationName, newMethodConfiguration)
  def UpdateMethodConfiguration(workspaceName: WorkspaceName, methodConfigurationNamespace: String, methodConfigurationName: String, newMethodConfiguration: MethodConfiguration) = updateMethodConfiguration(workspaceName, methodConfigurationNamespace, methodConfigurationName, newMethodConfiguration)
  def CopyMethodConfiguration(methodConfigNamePair: MethodConfigurationNamePair) = copyMethodConfiguration(methodConfigNamePair)
  def CopyMethodConfigurationFromMethodRepo(query: MethodRepoConfigurationImport) = copyMethodConfigurationFromMethodRepo(query)
  def CopyMethodConfigurationToMethodRepo(query: MethodRepoConfigurationExport) = copyMethodConfigurationToMethodRepo(query)
  def ListAgoraMethodConfigurations(workspaceName: WorkspaceName) = listAgoraMethodConfigurations(workspaceName)
  def ListMethodConfigurations(workspaceName: WorkspaceName) = listMethodConfigurations(workspaceName)
  def CreateMethodConfigurationTemplate( methodRepoMethod: MethodRepoMethod ) = createMethodConfigurationTemplate(methodRepoMethod)
  def GetMethodInputsOutputs(userInfo: UserInfo, methodRepoMethod: MethodRepoMethod ) = getMethodInputsOutputs(userInfo, methodRepoMethod)
  def GetAndValidateMethodConfiguration(workspaceName: WorkspaceName, methodConfigurationNamespace: String, methodConfigurationName: String) = getAndValidateMethodConfiguration(workspaceName, methodConfigurationNamespace, methodConfigurationName)
  def GetGenomicsOperationV2(workflowId: String, operationId: List[String]) = getGenomicsOperationV2(workflowId, operationId)

  def ListSubmissions(workspaceName: WorkspaceName) = listSubmissions(workspaceName)
  def CountSubmissions(workspaceName: WorkspaceName) = countSubmissions(workspaceName)
  def CreateSubmission(workspaceName: WorkspaceName, submission: SubmissionRequest) = createSubmission(workspaceName, submission)
  def ValidateSubmission(workspaceName: WorkspaceName, submission: SubmissionRequest) = validateSubmission(workspaceName, submission)
  def GetSubmissionStatus(workspaceName: WorkspaceName, submissionId: String) = getSubmissionStatus(workspaceName, submissionId)
  def AbortSubmission(workspaceName: WorkspaceName, submissionId: String) = abortSubmission(workspaceName, submissionId)
  def GetWorkflowOutputs(workspaceName: WorkspaceName, submissionId: String, workflowId: String) = workflowOutputs(workspaceName, submissionId, workflowId)
  def GetWorkflowMetadata(workspaceName: WorkspaceName, submissionId: String, workflowId: String, metadataParams: MetadataParams) = workflowMetadata(workspaceName, submissionId, workflowId, metadataParams)
  def GetWorkflowCost(workspaceName: WorkspaceName, submissionId: String, workflowId: String) = workflowCost(workspaceName, submissionId, workflowId)
  def WorkflowQueueStatus = workflowQueueStatus()

  def AdminListAllActiveSubmissions = asFCAdmin { listAllActiveSubmissions() }
  def AdminAbortSubmission(workspaceName: WorkspaceName, submissionId: String) = adminAbortSubmission(workspaceName,submissionId)
  def AdminWorkflowQueueStatusByUser = adminWorkflowQueueStatusByUser()

  def createWorkspace(workspaceRequest: WorkspaceRequest, parentSpan: Span = null): Future[Workspace] =
    traceWithParent("withAttributeNamespaceCheck", parentSpan)( s1 => withAttributeNamespaceCheck(workspaceRequest) {
      traceWithParent("withBillingProjectContext", s1)(s2 => withBillingProjectContext(workspaceRequest.namespace, s2) { billingProject =>
        for {
          workspace <- traceWithParent("withNewWorkspaceContext", s2) (s3 => dataSource.inTransaction({ dataAccess =>
            withNewWorkspaceContext(workspaceRequest, billingProject, dataAccess, s3) { workspaceContext =>
              DBIO.successful(workspaceContext)
            }
          }, TransactionIsolation.ReadCommitted)) // read committed to avoid deadlocks on workspace attr scratch table
          // After creating the Workspace, THIS is where we want to add the project to the Service Perimeter.  We need
          // to wait until the Workspace is persisted before adding to the Service Perimeter because the database IS the
          // source of record for which Google Projects need to be in the Service Perimeter because the method to add
          // projects to the Service Perimeter overwrites the entire list projects in the Perimeter
          _ <- maybeUpdateGoogleProjectsInPerimeter(billingProject)
        } yield workspace
      })
    })

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

  def getWorkspace(workspaceName: WorkspaceName, params: WorkspaceFieldSpecs, parentSpan: Span = null): Future[PerRequestMessage] = {
    val span = startSpanWithParent("optionsProcessing", parentSpan)

    // validate the inbound parameters
    val options = Try(validateParams(params, WorkspaceFieldNames.workspaceResponseFieldNames)) match {
      case Success(opts) => opts
      case Failure(ex) => throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.BadRequest, ex))
    }

    // dummy function that returns a Future(None)
    def noFuture = Future.successful(None)

    // if user requested the entire attributes map, or any individual attributes, retrieve attributes.
    val attrSpecs = WorkspaceAttributeSpecs(
      options.contains("workspace.attributes"),
      options.filter(_.startsWith("workspace.attributes."))
        .map(str => AttributeName.fromDelimitedName(str.replaceFirst("workspace.attributes.",""))).toList
    )
    span.setStatus(Status.OK)
    span.end()

    traceWithParent("getWorkspaceContextAndPermissions", parentSpan)(s1 => getWorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.read, Option(attrSpecs)) flatMap { workspaceContext =>
      dataSource.inTransaction { dataAccess =>

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
          val optionalAccessLevelForResponse = if (options.contains("accessLevel")) { Option(accessLevel) } else { None }

          // determine which functions to use for the various part of the response
          def bucketOptionsFuture(): Future[Option[WorkspaceBucketOptions]] = if (options.contains("bucketOptions")) {
            traceWithParent("getBucketDetails",s1)(_ =>  gcsDAO.getBucketDetails(workspaceContext.bucketName, workspaceContext.googleProjectId).map(Option(_)))
          } else {
            noFuture
          }
          def canComputeFuture(): Future[Option[Boolean]] = if (options.contains("canCompute")) {
            traceWithParent("getUserComputePermissions",s1)(_ =>  getUserComputePermissions(workspaceContext.workspaceIdAsUUID.toString, accessLevel).map(Option(_)))
          } else {
            noFuture
          }
          def canShareFuture(): Future[Option[Boolean]] = if (options.contains("canShare")) {
            //convoluted but accessLevel for both params because user could at most share with their own access level
            traceWithParent("getUserSharePermissions",s1)(_ =>  getUserSharePermissions(workspaceContext.workspaceIdAsUUID.toString, accessLevel, accessLevel).map(Option(_)))
          } else {
            noFuture
          }
          def catalogFuture(): Future[Option[Boolean]] = if (options.contains("catalog")) {
            traceWithParent("getUserCatalogPermissions",s1)(_ =>  getUserCatalogPermissions(workspaceContext.workspaceIdAsUUID.toString).map(Option(_)))
          } else {
            noFuture
          }

          def ownersFuture(): Future[Option[Set[String]]] = if (options.contains("owners")) {
            traceWithParent("getWorkspaceOwners",s1)(_ =>  getWorkspaceOwners(workspaceContext.workspaceIdAsUUID.toString).map(_.map(_.value)).map(Option(_)))
          } else {
            noFuture
          }

          def workspaceAuthorizationDomainFuture(): Future[Option[Set[ManagedGroupRef]]] = if (options.contains("workspace.authorizationDomain") || options.contains("workspace")) {
            traceWithParent("loadResourceAuthDomain",s1)(_ =>  loadResourceAuthDomain(SamResourceTypeNames.workspace, workspaceContext.workspaceId, userInfo).map(Option(_)))
          } else {
            noFuture
          }

          def workspaceSubmissionStatsFuture(): slick.ReadAction[Option[WorkspaceSubmissionStats]] = if (options.contains("workspaceSubmissionStats")) {
            getWorkspaceSubmissionStats(workspaceContext, dataAccess).map(Option(_))
          } else {
            DBIO.from(noFuture)
          }

          //run these futures in parallel. this is equivalent to running the for-comp with the futures already defined and running
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
            stats <- traceDBIOWithParent("workspaceSubmissionStatsFuture", s1)(_ => workspaceSubmissionStatsFuture())
          } yield {
            // post-process JSON to remove calculated-but-undesired keys
            val workspaceResponse = WorkspaceResponse(optionalAccessLevelForResponse, canShare, canCompute, canCatalog, WorkspaceDetails.fromWorkspaceAndOptions(workspaceContext, authDomain, useAttributes), stats, bucketDetails, owners)
            val filteredJson = deepFilterJsObject(workspaceResponse.toJson.asJsObject, options)
            RequestComplete(StatusCodes.OK, filteredJson)
          }
        }
      }
    })
  }

  def getBucketOptions(workspaceName: WorkspaceName): Future[PerRequestMessage] = {
    getWorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.read) flatMap { workspaceContext =>
      dataSource.inTransaction { dataAccess =>
        DBIO.from(gcsDAO.getBucketDetails(workspaceContext.bucketName, workspaceContext.googleProjectId)) map { details =>
          RequestComplete(StatusCodes.OK, details)
        }
      }
    }
  }

  private def loadResourceAuthDomain(resourceTypeName: SamResourceTypeName, resourceId: String, userInfo: UserInfo): Future[Set[ManagedGroupRef]] = {
    samDAO.getResourceAuthDomain(resourceTypeName, resourceId, userInfo).map(_.map(g => ManagedGroupRef(RawlsGroupName(g))).toSet)
  }

  def getUserComputePermissions(workspaceId: String, userAccessLevel: WorkspaceAccessLevel): Future[Boolean] = {
    if(userAccessLevel >= WorkspaceAccessLevels.Owner) Future.successful(true)
    else samDAO.userHasAction(SamResourceTypeNames.workspace, workspaceId, SamWorkspaceActions.compute, userInfo)
  }

  def getUserSharePermissions(workspaceId: String, userAccessLevel: WorkspaceAccessLevel, accessLevelToShareWith: WorkspaceAccessLevel): Future[Boolean] = {
    if (userAccessLevel < WorkspaceAccessLevels.Read) Future.successful(false)
    else if(userAccessLevel >= WorkspaceAccessLevels.Owner) Future.successful(true)
    else samDAO.userHasAction(SamResourceTypeNames.workspace, workspaceId, SamWorkspaceActions.sharePolicy(accessLevelToShareWith.toString.toLowerCase), userInfo)
  }

  def getUserCatalogPermissions(workspaceId: String): Future[Boolean] = {
    samDAO.userHasAction(SamResourceTypeNames.workspace, workspaceId, SamWorkspaceActions.catalog, userInfo)
  }

  /**
    * Sums up all the user's roles in a workspace to a single access level.
    *
    * USE FOR DISPLAY/USABILITY PURPOSES ONLY, NOT FOR REAL ACCESS DECISIONS
    * for real access decisions check actions in sam
    *
    * @param workspaceId
    * @return
    */
  def getMaximumAccessLevel(workspaceId: String): Future[WorkspaceAccessLevel] = {
    samDAO.listUserRolesForResource(SamResourceTypeNames.workspace, workspaceId, userInfo).map { roles =>
      roles.flatMap(role => WorkspaceAccessLevels.withRoleName(role.value)).fold(WorkspaceAccessLevels.NoAccess)(max)
    }
  }

  def getWorkspaceOwners(workspaceId: String): Future[Set[WorkbenchEmail]] = {
    samDAO.getPolicy(SamResourceTypeNames.workspace, workspaceId, SamWorkspacePolicyNames.owner, userInfo).map(_.memberEmails)
  }

  def deleteWorkspace(workspaceName: WorkspaceName): Future[PerRequestMessage] =  {
     getWorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.delete) flatMap { ctx =>
       deleteWorkspace(workspaceName, ctx)
    }
  }

  private def deleteWorkspace(workspaceName: WorkspaceName, workspaceContext: Workspace): Future[PerRequestMessage] = {
    //Attempt to abort any running workflows so they don't write any more to the bucket.
    //Notice that we're kicking off Futures to do the aborts concurrently, but we never collect their results!
    //This is because there's nothing we can do if Cromwell fails, so we might as well move on and let the
    //ExecutionContext run the futures whenever
    val deletionFuture: Future[Seq[WorkflowRecord]] = dataSource.inTransaction { dataAccess =>
      for {
        // Gather any active workflows with external ids
        workflowsToAbort <- dataAccess.workflowQuery.findActiveWorkflowsWithExternalIds(workspaceContext)

        //If a workflow is not done, automatically change its status to Aborted
        _ <- dataAccess.workflowQuery.findWorkflowsByWorkspace(workspaceContext).result.map { recs => recs.collect {
          case wf if !WorkflowStatuses.withName(wf.status).isDone =>
            dataAccess.workflowQuery.updateStatus(wf, WorkflowStatuses.Aborted) { status =>
              if (config.trackDetailedSubmissionMetrics) Option(workflowStatusCounter(workspaceSubmissionMetricBuilder(workspaceName, wf.submissionId))(status))
              else None
            }
        }}

        // Delete components of the workspace
        _ <- dataAccess.submissionQuery.deleteFromDb(workspaceContext.workspaceIdAsUUID)
        _ <- dataAccess.methodConfigurationQuery.deleteFromDb(workspaceContext.workspaceIdAsUUID)
        _ <- dataAccess.entityQuery.deleteFromDb(workspaceContext.workspaceIdAsUUID)

        // Delete the workspace
        _ <- dataAccess.workspaceQuery.delete(workspaceName)

        // Schedule bucket for deletion
        _ <- dataAccess.pendingBucketDeletionQuery.save(PendingBucketDeletionRecord(workspaceContext.bucketName))

      } yield {
        workflowsToAbort
      }
    }
    for {
      workflowsToAbort <- deletionFuture

      // Abort running workflows
      aborts = Future.traverse(workflowsToAbort) { wf => executionServiceCluster.abort(wf, userInfo) }

      // Delete resource in sam outside of DB transaction
      _ <- workspaceContext.workflowCollectionName.map( cn => samDAO.deleteResource(SamResourceTypeNames.workflowCollection, cn, userInfo) ).getOrElse(Future.successful(()))
      _ <- samDAO.deleteResource(SamResourceTypeNames.workspace, workspaceContext.workspaceIdAsUUID.toString, userInfo)
      // Delete workspace manager record (which will only exist if there had ever been a TDR snapshot in the WS)
      _ = Try(workspaceManagerDAO.deleteWorkspace(workspaceContext.workspaceIdAsUUID, userInfo.accessToken)).recoverWith {
        //this will only ever succeed if a TDR snapshot had been created in the WS, so we gracefully handle all exceptions here
        case e: ApiException => {
          if(e.getCode != StatusCodes.NotFound.intValue) {
            logger.warn(s"Unexpected failure deleting workspace in Workspace Manager. Received ${e.getCode}: [${e.getResponseBody}]")
          }
          Success(())
        }
      }
    } yield {
      aborts.onComplete {
        case Failure(t) => logger.info(s"failure aborting workflows while deleting workspace ${workspaceName}", t)
        case _ => /* ok */
      }
      RequestComplete(StatusCodes.Accepted, s"Your Google bucket ${workspaceContext.bucketName} will be deleted within 24h.")
    }
  }

  def updateLibraryAttributes(workspaceName: WorkspaceName, operations: Seq[AttributeUpdateOperation]): Future[PerRequestMessage] = {
    withLibraryAttributeNamespaceCheck(operations.map(_.name)) {
      for {
        isCurator <- tryIsCurator(userInfo.userEmail)
        workspace <- getWorkspaceContext(workspaceName) flatMap { ctx =>
          withLibraryPermissions(ctx, operations, userInfo, isCurator) {
            dataSource.inTransaction ({ dataAccess =>
              updateWorkspace(operations, dataAccess)(ctx.toWorkspaceName)
            }, TransactionIsolation.ReadCommitted) // read committed to avoid deadlocks on workspace attr scratch table
          }
        }
        authDomain <- loadResourceAuthDomain(SamResourceTypeNames.workspace, workspace.workspaceId, userInfo)
      } yield {
        RequestComplete(StatusCodes.OK, WorkspaceDetails(workspace, authDomain))
      }
    }
  }

  def updateWorkspace(workspaceName: WorkspaceName, operations: Seq[AttributeUpdateOperation]): Future[PerRequestMessage] = {
    withAttributeNamespaceCheck(operations.map(_.name)) {
      for {
        ctx <- getWorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.write)
        workspace <- dataSource.inTransaction({ dataAccess =>
            updateWorkspace(operations, dataAccess)(ctx.toWorkspaceName)
        }, TransactionIsolation.ReadCommitted) // read committed to avoid deadlocks on workspace attr scratch table
        authDomain <- loadResourceAuthDomain(SamResourceTypeNames.workspace, workspace.workspaceId, userInfo)
      } yield {
        RequestComplete(StatusCodes.OK, WorkspaceDetails(workspace, authDomain))
      }
    }
  }

  private def updateWorkspace(operations: Seq[AttributeUpdateOperation], dataAccess: DataAccess)(workspaceName: WorkspaceName): ReadWriteAction[Workspace] = {
    // get the source workspace again, to avoid race conditions where the workspace was updated outside of this transaction
    withWorkspaceContext(workspaceName, dataAccess) { workspaceContext =>
      val workspace = workspaceContext
      Try {
        val updatedWorkspace = applyOperationsToWorkspace(workspace, operations)
        dataAccess.workspaceQuery.createOrUpdate(updatedWorkspace)
      } match {
        case Success(result) => result
        case Failure(e: AttributeUpdateOperationException) =>
          DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.BadRequest, s"Unable to update ${workspace.name}", ErrorReport(e))))
        case Failure(regrets) => DBIO.failed(regrets)
      }
    }
  }

  def getTags(query: Option[String]): Future[PerRequestMessage] =
    dataSource.inTransaction { dataAccess =>
      dataAccess.workspaceQuery.getTags(query).map { result =>
        RequestComplete(StatusCodes.OK, result)
      }
    }

  def listWorkspaces(params: WorkspaceFieldSpecs, parentSpan: Span): Future[PerRequestMessage] = {

    val s = startSpanWithParent("optionHandling", parentSpan)

    // validate the inbound parameters
    val options = Try(validateParams(params, WorkspaceFieldNames.workspaceListResponseFieldNames)) match {
      case Success(opts) => opts
      case Failure(ex) =>
        throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.BadRequest, ex))
    }

    // if user requested the entire attributes map, or any individual attributes, retrieve attributes.
    val attributeSpecs = WorkspaceAttributeSpecs(
      options.contains("workspace.attributes"),
      options.filter(_.startsWith("workspace.attributes."))
        .map(str => AttributeName.fromDelimitedName(str.replaceFirst("workspace.attributes.",""))).toList
    )

    // Can this be shared with get-workspace somehow?
    val optionsExist = options.nonEmpty
    val submissionStatsEnabled = options.contains("workspaceSubmissionStats")
    val attributesEnabled = attributeSpecs.all || attributeSpecs.attrsToSelect.nonEmpty

    s.setStatus(Status.OK)
    s.end()

    for {
      workspacePolicies <- traceWithParent("getPolicies", parentSpan)(_ => samDAO.getPoliciesForType(SamResourceTypeNames.workspace, userInfo))
      // filter out the policies that are not related to access levels, if a user has only those ignore the workspace
      // also filter out any policy whose resourceId is not a UUID; these will never match a known workspace
      accessLevelWorkspacePolicies = workspacePolicies.filter(p =>
        WorkspaceAccessLevels.withPolicyName(p.accessPolicyName.value).nonEmpty &&
        Try(UUID.fromString(p.resourceId)).isSuccess
      )
      accessLevelWorkspacePolicyUUIDs = accessLevelWorkspacePolicies.map(p => UUID.fromString(p.resourceId)).toSeq
      result <- dataSource.inTransaction({ dataAccess =>

        def workspaceSubmissionStatsFuture(): slick.ReadAction[Map[UUID, WorkspaceSubmissionStats]] = if (submissionStatsEnabled) {
          dataAccess.workspaceQuery.listSubmissionSummaryStats(accessLevelWorkspacePolicyUUIDs)
        } else {
          DBIO.from(Future(Map()))
        }

        val query = for {
          submissionSummaryStats <- traceDBIOWithParent("submissionStats", parentSpan)(_ => workspaceSubmissionStatsFuture())
          workspaces <- traceDBIOWithParent("listByIds", parentSpan)(_ => dataAccess.workspaceQuery.listByIds(accessLevelWorkspacePolicyUUIDs, Option(attributeSpecs)))
        } yield (submissionSummaryStats, workspaces)

        val results = traceDBIOWithParent("finalResults", parentSpan)(_ => query.map { case (submissionSummaryStats, workspaces) =>
          val policiesByWorkspaceId = accessLevelWorkspacePolicies.groupBy(_.resourceId).map { case (workspaceId, policies) =>
            workspaceId -> policies.reduce { (p1, p2) =>
              val betterAccessPolicyName = (WorkspaceAccessLevels.withPolicyName(p1.accessPolicyName.value), WorkspaceAccessLevels.withPolicyName(p2.accessPolicyName.value)) match {
                case (Some(p1Level), Some(p2Level)) if p1Level > p2Level => p1.accessPolicyName
                case (Some(_), Some(_)) => p2.accessPolicyName
                case _ => throw new RawlsException(s"unexpected state, both $p1 and $p2 should be related to access levels at this point")
              }
              SamResourceIdWithPolicyName(
                p1.resourceId,
                betterAccessPolicyName,
                p1.authDomainGroups ++ p2.authDomainGroups,
                p1.missingAuthDomainGroups ++ p2.missingAuthDomainGroups,
                p1.public || p2.public
              )
            }
          }
          workspaces.map { workspace =>
            val wsId = UUID.fromString(workspace.workspaceId)
            val workspacePolicy = policiesByWorkspaceId(workspace.workspaceId)
            val accessLevel = if (workspacePolicy.missingAuthDomainGroups.nonEmpty) WorkspaceAccessLevels.NoAccess else WorkspaceAccessLevels.withPolicyName(workspacePolicy.accessPolicyName.value).getOrElse(WorkspaceAccessLevels.NoAccess)
            // remove attributes if they were not requested
            val workspaceDetails = WorkspaceDetails.fromWorkspaceAndOptions(workspace, Option(workspacePolicy.authDomainGroups.map(groupName => ManagedGroupRef(RawlsGroupName(groupName.value)))), attributesEnabled)
            // remove submission stats if they were not requested
            val submissionStats: Option[WorkspaceSubmissionStats] = if (submissionStatsEnabled) {
              Option(submissionSummaryStats(wsId))
            } else {
              None
            }

            WorkspaceListResponse(accessLevel, workspaceDetails, submissionStats, workspacePolicy.public)
          }
        })

        results.map { responses =>
          if (!optionsExist) {
            RequestComplete(StatusCodes.OK, responses)
          } else {
            // perform json-filtering of payload
            RequestComplete(StatusCodes.OK, deepFilterJsValue(responses.toJson, options))
          }
        }
      }, TransactionIsolation.ReadCommitted)
    } yield result
  }

  private def getWorkspaceSubmissionStats(workspaceContext: Workspace, dataAccess: DataAccess): ReadAction[WorkspaceSubmissionStats] = {
    // listSubmissionSummaryStats works against a sequence of workspaces; we call it just for this one workspace
    dataAccess.workspaceQuery
      .listSubmissionSummaryStats(Seq(workspaceContext.workspaceIdAsUUID))
      .map {p => p.get(workspaceContext.workspaceIdAsUUID).get}
  }

  // NOTE: Orchestration has its own implementation of cloneWorkspace. When changing something here, you may also need to update orchestration's implementation (maybe helpful search term: `Post(workspacePath + "/clone"`).
  def cloneWorkspace(sourceWorkspaceName: WorkspaceName, destWorkspaceRequest: WorkspaceRequest): Future[Workspace] = {
    destWorkspaceRequest.copyFilesWithPrefix.foreach(prefix => validateFileCopyPrefix(prefix))

    val (libraryAttributeNames, workspaceAttributeNames) = destWorkspaceRequest.attributes.keys.partition(name => name.namespace == AttributeName.libraryNamespace)
    withAttributeNamespaceCheck(workspaceAttributeNames) {
      withLibraryAttributeNamespaceCheck(libraryAttributeNames) {
        withBillingProjectContext(destWorkspaceRequest.namespace) { destBillingProject =>
          getWorkspaceContextAndPermissions(sourceWorkspaceName, SamWorkspaceActions.read).flatMap { permCtx =>
            for {
              workspaceTuple <- dataSource.inTransaction( { dataAccess =>
                // get the source workspace again, to avoid race conditions where the workspace was updated outside of this transaction
                withWorkspaceContext(permCtx.toWorkspaceName, dataAccess) { sourceWorkspaceContext =>
                  DBIO.from(samDAO.getResourceAuthDomain(SamResourceTypeNames.workspace, sourceWorkspaceContext.workspaceId, userInfo)).flatMap { sourceAuthDomains =>
                    withClonedAuthDomain(sourceAuthDomains.map(n => ManagedGroupRef(RawlsGroupName(n))).toSet, destWorkspaceRequest.authorizationDomain.getOrElse(Set.empty)) { newAuthDomain =>

                      // add to or replace current attributes, on an individual basis
                      val newAttrs = sourceWorkspaceContext.attributes ++ destWorkspaceRequest.attributes

                      withNewWorkspaceContext(destWorkspaceRequest.copy(authorizationDomain = Option(newAuthDomain), attributes = newAttrs), destBillingProject, dataAccess) { destWorkspaceContext =>
                        dataAccess.entityQuery.copyAllEntities(sourceWorkspaceContext, destWorkspaceContext) andThen
                          dataAccess.methodConfigurationQuery.listActive(sourceWorkspaceContext).flatMap { methodConfigShorts =>
                            val inserts = methodConfigShorts.map { methodConfigShort =>
                              dataAccess.methodConfigurationQuery.get(sourceWorkspaceContext, methodConfigShort.namespace, methodConfigShort.name).flatMap { methodConfig =>
                                dataAccess.methodConfigurationQuery.create(destWorkspaceContext, methodConfig.get)
                              }
                            }
                            DBIO.seq(inserts: _*)
                          } andThen {
                          DBIO.successful((sourceWorkspaceContext, destWorkspaceContext))
                        }
                      }
                    }
                  }
                }
              }, TransactionIsolation.ReadCommitted)
              _ <- maybeUpdateGoogleProjectsInPerimeter(destBillingProject)
            } yield workspaceTuple
            // read committed to avoid deadlocks on workspace attr scratch table
          }.map { case (sourceWorkspaceContext, destWorkspaceContext) =>
            //we will fire and forget this. a more involved, but robust, solution involves using the Google Storage Transfer APIs
            //in most of our use cases, these files should copy quickly enough for there to be no noticeable delay to the user
            //we also don't want to block returning a response on this call because it's already a slow endpoint
            destWorkspaceRequest.copyFilesWithPrefix.foreach { prefix =>
              copyBucketFiles(sourceWorkspaceContext, destWorkspaceContext, prefix)
            }

            destWorkspaceContext
          }
        }
      }
    }
  }

  private def validateFileCopyPrefix(copyFilesWithPrefix: String): Unit = {
    if(copyFilesWithPrefix.isEmpty) throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.BadRequest, """You may not specify an empty string for `copyFilesWithPrefix`. Did you mean to specify "/" or leave the field out entirely?"""))
  }

  private def copyBucketFiles(sourceWorkspaceContext: Workspace, destWorkspaceContext: Workspace, copyFilesWithPrefix: String): Future[List[Option[StorageObject]]] = {
    gcsDAO.listObjectsWithPrefix(sourceWorkspaceContext.bucketName, copyFilesWithPrefix).flatMap { objectsToCopy =>
      Future.traverse(objectsToCopy) { objectToCopy =>  gcsDAO.copyFile(sourceWorkspaceContext.bucketName, objectToCopy.getName, destWorkspaceContext.bucketName, objectToCopy.getName) }
    }
  }

  private def withClonedAuthDomain[T](sourceWorkspaceADs: Set[ManagedGroupRef], destWorkspaceADs: Set[ManagedGroupRef])(op: (Set[ManagedGroupRef]) => ReadWriteAction[T]): ReadWriteAction[T] = {
    // if the source has an auth domain, the dest must also have that auth domain as a subset
    // otherwise, the caller may choose to add to the auth domain
    if(sourceWorkspaceADs.subsetOf(destWorkspaceADs)) op(sourceWorkspaceADs ++ destWorkspaceADs)
    else {
      val missingGroups = sourceWorkspaceADs -- destWorkspaceADs
      val errorMsg = s"Source workspace has an Authorization Domain containing the groups ${missingGroups.map(_.membersGroupName.value).mkString(", ")}, which are missing on the destination workspace"
      DBIO.failed(new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.UnprocessableEntity, errorMsg)))
    }
  }

  private def isUserPending(userEmail: String): Future[Boolean] = {
    samDAO.getUserIdInfo(userEmail, userInfo).map {
      case SamDAO.User(x) => x.googleSubjectId.isEmpty
      case SamDAO.NotUser => false
      case SamDAO.NotFound => true
    }
  }

  //API_CHANGE: project owners no longer returned (because it would just show a policy and not everyone can read the members of that policy)
  private def getACLInternal(workspaceName: WorkspaceName): Future[WorkspaceACL] = {

    def loadPolicy(policyName: SamResourcePolicyName, policyList: Set[SamPolicyWithNameAndEmail]): SamPolicyWithNameAndEmail = {
      policyList.find(_.policyName.value.equalsIgnoreCase(policyName.value)).getOrElse(throw new WorkbenchException(s"Could not load $policyName policy"))
    }

    val policyMembers = for {
      workspaceId <- loadWorkspaceId(workspaceName)
      currentACL <- samDAO.listPoliciesForResource(SamResourceTypeNames.workspace, workspaceId, userInfo)
    } yield {
      val ownerPolicyMembers = loadPolicy(SamWorkspacePolicyNames.owner, currentACL).policy.memberEmails
      val writerPolicyMembers = loadPolicy(SamWorkspacePolicyNames.writer, currentACL).policy.memberEmails
      val readerPolicyMembers = loadPolicy(SamWorkspacePolicyNames.reader, currentACL).policy.memberEmails
      val shareReaderPolicyMembers = loadPolicy(SamWorkspacePolicyNames.shareReader, currentACL).policy.memberEmails
      val shareWriterPolicyMembers = loadPolicy(SamWorkspacePolicyNames.shareWriter, currentACL).policy.memberEmails
      val computePolicyMembers = loadPolicy(SamWorkspacePolicyNames.canCompute, currentACL).policy.memberEmails
      //note: can-catalog is a policy on the side and is not a part of the core workspace ACL so we won't load it

      (ownerPolicyMembers, writerPolicyMembers, readerPolicyMembers, shareReaderPolicyMembers, shareWriterPolicyMembers, computePolicyMembers)
    }

    policyMembers.flatMap { case (ownerPolicyMembers, writerPolicyMembers, readerPolicyMembers, shareReaderPolicyMembers, shareWriterPolicyMembers, computePolicyMembers) =>
      val sharers = shareReaderPolicyMembers ++ shareWriterPolicyMembers

      for {
        ownersPending <- Future.traverse(ownerPolicyMembers) { email => isUserPending(email.value).map(pending => email -> pending) }
        writersPending <- Future.traverse(writerPolicyMembers) { email => isUserPending(email.value).map(pending => email -> pending) }
        readersPending <- Future.traverse(readerPolicyMembers) { email => isUserPending(email.value).map(pending => email -> pending) }
      } yield {
        val owners = ownerPolicyMembers.map(email => email.value -> AccessEntry(WorkspaceAccessLevels.Owner, ownersPending.toMap.getOrElse(email, true), true, true)) //API_CHANGE: pending owners used to show as false for canShare and canCompute. they now show true. this is more accurate anyway
        val writers = writerPolicyMembers.map(email => email.value -> AccessEntry(WorkspaceAccessLevels.Write, writersPending.toMap.getOrElse(email, true), sharers.contains(email), computePolicyMembers.contains(email)))
        val readers = readerPolicyMembers.map(email => email.value -> AccessEntry(WorkspaceAccessLevels.Read, readersPending.toMap.getOrElse(email, true), sharers.contains(email), computePolicyMembers.contains(email)))

        WorkspaceACL((owners ++ writers ++ readers).toMap)
      }
    }
  }

  def getACL(workspaceName: WorkspaceName): Future[PerRequestMessage] = {
    getACLInternal(workspaceName).map { acl => RequestComplete(StatusCodes.OK, acl)}
  }

  def getCatalog(workspaceName: WorkspaceName): Future[PerRequestMessage] = {
    loadWorkspaceId(workspaceName).flatMap { workspaceId =>
      samDAO.getPolicy(SamResourceTypeNames.workspace, workspaceId, SamWorkspacePolicyNames.canCatalog, userInfo).map { members => RequestComplete(StatusCodes.OK, members.memberEmails.map(email => WorkspaceCatalog(email.value, true)))}
    }
  }

  private def loadWorkspaceId(workspaceName: WorkspaceName): Future[String] = {
    dataSource.inTransaction { dataAccess => dataAccess.workspaceQuery.getWorkspaceId(workspaceName) }.map {
      case None => throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.NotFound, "unable to load workspace"))
      case Some(id) => id.toString
    }
  }

  def updateCatalog(workspaceName: WorkspaceName, input: Seq[WorkspaceCatalog]): Future[PerRequestMessage] = {
    for {
      workspaceId <- loadWorkspaceId(workspaceName)
      results <- Future.traverse(input) {
        case WorkspaceCatalog(email, true) =>
          toFutureTry(samDAO.addUserToPolicy(SamResourceTypeNames.workspace, workspaceId, SamWorkspacePolicyNames.canCatalog, email, userInfo)).
            map(_.map(_ => Either.right[String, WorkspaceCatalogResponse](WorkspaceCatalogResponse(email, true))).recover {
              case t: RawlsExceptionWithErrorReport if t.errorReport.statusCode.contains(StatusCodes.BadRequest) => Left(email)
            })

        case WorkspaceCatalog(email, false) =>
          toFutureTry(samDAO.removeUserFromPolicy(SamResourceTypeNames.workspace, workspaceId, SamWorkspacePolicyNames.canCatalog, email, userInfo)).
            map(_.map(_ => Either.right[String, WorkspaceCatalogResponse](WorkspaceCatalogResponse(email, false))).recover {
              case t: RawlsExceptionWithErrorReport if t.errorReport.statusCode.contains(StatusCodes.BadRequest) => Left(email)
            })
      }
    } yield {
      val failures = results.collect {
        case Failure(regrets) => ErrorReport(regrets)
      }
      if (failures.nonEmpty) {
        throw new RawlsExceptionWithErrorReport(ErrorReport("Error setting catalog permissions", failures))
      } else {
        RequestComplete(StatusCodes.OK, WorkspaceCatalogUpdateResponseList(results.collect { case Success(Right(wc)) => wc }, results.collect { case Success(Left(email)) => email }))
      }
    }
  }

  private def getWorkspacePolicies(workspaceName: WorkspaceName): Future[Set[SamPolicyWithNameAndEmail]] = {
    for {
      workspaceId <- loadWorkspaceId(workspaceName)
      policies <- samDAO.listPoliciesForResource(SamResourceTypeNames.workspace, workspaceId, userInfo)
    } yield policies
  }

  def collectMissingUsers(userEmails: Set[String]): Future[Set[String]] = {
    Future.traverse(userEmails) { email =>
      samDAO.getUserIdInfo(email, userInfo).map {
        case SamDAO.NotFound => Option(email)
        case _ => None
      }
    }.map(_.flatten)
  }

  /**
   * updates acls for a workspace
   * @param workspaceName
   * @param aclUpdates changes to make, if an entry already exists it will be changed to the level indicated in this
   *                   Set, use NoAccess to remove an entry, all other preexisting accesses remain unchanged
   * @return
   */
  def updateACL(workspaceName: WorkspaceName, aclUpdates: Set[WorkspaceACLUpdate], inviteUsersNotFound: Boolean): Future[PerRequestMessage] = {
    if (aclUpdates.map(_.email).size < aclUpdates.size) {
      throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.BadRequest, s"Only 1 entry per email allowed."))
    }

    /**
      * convert a set of policy names to the corresponding WorkspaceAclUpdate representation
      * @param userEmail
      * @param samWorkspacePolicyNames
      * @return
      */
    def policiesToAclUpdate(userEmail: String, samWorkspacePolicyNames: Set[SamResourcePolicyName]) = {
      val accessLevel = samWorkspacePolicyNames.flatMap(n => WorkspaceAccessLevels.withPolicyName(n.value)).fold(WorkspaceAccessLevels.NoAccess)(WorkspaceAccessLevels.max)
      val ownerLevel = samWorkspacePolicyNames.contains(SamWorkspacePolicyNames.projectOwner) || samWorkspacePolicyNames.contains(SamWorkspacePolicyNames.owner)
      val canShare = ownerLevel ||
        (samWorkspacePolicyNames.contains(SamWorkspacePolicyNames.reader) && samWorkspacePolicyNames.contains(SamWorkspacePolicyNames.shareReader)) ||
        (samWorkspacePolicyNames.contains(SamWorkspacePolicyNames.writer) && samWorkspacePolicyNames.contains(SamWorkspacePolicyNames.shareWriter))
      val canCompute = ownerLevel || samWorkspacePolicyNames.contains(SamWorkspacePolicyNames.canCompute)
      WorkspaceACLUpdate(userEmail, accessLevel, Option(canShare), Option(canCompute))
    }

    /**
      * convert a WorkspaceAclUpdate to the set of policy names that implement it
      * @param workspaceACLUpdate
      * @return
      */
    def aclUpdateToPolicies(workspaceACLUpdate: WorkspaceACLUpdate)= {
      val sharePolicy = workspaceACLUpdate match {
        case WorkspaceACLUpdate(_, WorkspaceAccessLevels.Read, Some(true), _) => SamWorkspacePolicyNames.shareReader.some
        case WorkspaceACLUpdate(_, WorkspaceAccessLevels.Write, Some(true), _) => SamWorkspacePolicyNames.shareWriter.some
        case _ => None
      }

      // canCompute is only applicable to write access, readers can't have it and owners have it implicitly
      val computePolicy = workspaceACLUpdate.canCompute match {
        case Some(false) => None
        case _ if workspaceACLUpdate.accessLevel == WorkspaceAccessLevels.Write => SamWorkspacePolicyNames.canCompute.some
        case _ => None
      }

      Set(workspaceACLUpdate.accessLevel.toPolicyName.map(SamResourcePolicyName), sharePolicy, computePolicy).flatten
    }

    def normalize(aclUpdates: Set[WorkspaceACLUpdate]) = {
      aclUpdates.map { update =>
        val ownerLevel = update.accessLevel >= WorkspaceAccessLevels.Owner
        val normalizedCanCompute = ownerLevel || update.canCompute.getOrElse(update.accessLevel == WorkspaceAccessLevels.Write)
        update.copy(canShare = Option(ownerLevel || update.canShare.getOrElse(false)), canCompute = Option(normalizedCanCompute)) }
    }

    collectMissingUsers(aclUpdates.map(_.email)).flatMap { userToInvite =>
      if (userToInvite.isEmpty || inviteUsersNotFound) {
        getWorkspacePolicies(workspaceName).flatMap { existingPolicies =>
          // the acl update code does not deal with the can catalog permission, there are separate functions for that.
          // exclude any existing can catalog policies so we don't inadvertently remove them
          val existingPoliciesExcludingCatalog = existingPolicies.filterNot(_.policyName == SamWorkspacePolicyNames.canCatalog)

          // convert all the existing policy memberships into WorkspaceAclUpdate objects
          val existingPoliciesWithMembers = existingPoliciesExcludingCatalog.flatMap(p => p.policy.memberEmails.map(email => email -> p.policyName))
          val existingAcls = existingPoliciesWithMembers.groupBy(_._1).map { case (email, policyNames) =>
            policiesToAclUpdate(email.value, policyNames.map(_._2))
          }.toSet

          // figure out which of the incoming aclUpdates are actually changes by removing all the existingAcls
          val aclChanges = normalize(aclUpdates) -- existingAcls
          validateAclChanges(aclChanges, existingAcls)

          // find users to remove from policies: existing policy members that are not in policies implied by aclChanges
          // note that access level No Access corresponds to 0 desired policies so all existing policies will be removed
          val policyRemovals = aclChanges.flatMap { aclChange =>
            val desiredPolicies = aclUpdateToPolicies(aclChange)
            existingPoliciesWithMembers.collect {
              case (email, policyName) if email.value.equalsIgnoreCase(aclChange.email) && !desiredPolicies.contains(policyName) => (policyName, aclChange.email)
            }
          }

          // find users to add to policies: users that are not existing policy members of policies implied by aclChanges
          val policyAdditions = aclChanges.flatMap { aclChange =>
            val desiredPolicies = aclUpdateToPolicies(aclChange)
            desiredPolicies.collect {
              case policyName if !existingPoliciesWithMembers.exists(x => x._1.value.equalsIgnoreCase(aclChange.email) && x._2 == policyName) => (policyName, aclChange.email)
            }
          }

          // now do all the work: invites, additions, removals, notifications
          for {
            maybeWorkspace <- dataSource.inTransaction { dataAccess => dataAccess.workspaceQuery.findByName(workspaceName) }
            workspace = maybeWorkspace.getOrElse(throw new RawlsException(s"workspace $workspaceName not found"))

            inviteNotifications <- Future.traverse(userToInvite) { invite =>
              samDAO.inviteUser(invite, userInfo).map { _ =>
                Notifications.WorkspaceInvitedNotification(RawlsUserEmail(invite), userInfo.userSubjectId, workspaceName, workspace.bucketName)
              }
            }

            // do additions before removals so users are not left unable to access the workspace in case of errors that
            // lead to incomplete application of these changes, remember: this is not transactional
            _ <- Future.traverse(policyAdditions) { case (policyName, email) =>
                samDAO.addUserToPolicy(SamResourceTypeNames.workspace, workspace.workspaceId, policyName, email, userInfo)
            }

            _ <- Future.traverse(policyRemovals) { case (policyName, email) =>
              samDAO.removeUserFromPolicy(SamResourceTypeNames.workspace, workspace.workspaceId, policyName, email, userInfo)
            }

            _ <- revokeRequesterPaysForLinkedSAs(workspace, policyRemovals, policyAdditions)

            _ <- maybeShareProjectComputePolicy(policyAdditions, workspaceName)

          } yield {
            val (invites, updates) = aclChanges.partition(acl => userToInvite.contains(acl.email))
            sendACLUpdateNotifications(workspaceName, updates) //we can blindly fire off this future because we don't care about the results and it happens async anyway
            notificationDAO.fireAndForgetNotifications(inviteNotifications)
            RequestComplete(StatusCodes.OK, WorkspaceACLUpdateResponseList(updates, invites, Set.empty)) //API_CHANGE: no longer return invitesUpdated because you technically can't do that anymore...
          }
        }
      }
      else Future.successful(RequestComplete(StatusCodes.OK, WorkspaceACLUpdateResponseList(Set.empty, Set.empty, aclUpdates.filter(au => userToInvite.contains(au.email)))))
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
  private def revokeRequesterPaysForLinkedSAs(workspace: Workspace, policyRemovals: Set[(SamResourcePolicyName, String)], policyAdditions: Set[(SamResourcePolicyName, String)]): Future[Unit] = {
    val applicablePolicies = Set(SamWorkspacePolicyNames.owner, SamWorkspacePolicyNames.writer)
    val applicableRemovals = policyRemovals.collect {
      case (policy, email) if applicablePolicies.contains(policy) => RawlsUserEmail(email)
    }
    val applicableAdditions = policyAdditions.collect {
      case (policy, email) if applicablePolicies.contains(policy) => RawlsUserEmail(email)
    }
    Future.traverse(applicableRemovals -- applicableAdditions) { emailToRevoke => requesterPaysSetupService.revokeUserFromWorkspace(emailToRevoke, workspace) }.void
  }

  private def validateAclChanges(aclChanges: Set[WorkspaceACLUpdate], existingAcls: Set[WorkspaceACLUpdate]) = {
    val emailsBeingChanged = aclChanges.map(_.email.toLowerCase)
    if (aclChanges.exists(_.accessLevel == WorkspaceAccessLevels.ProjectOwner) || existingAcls.exists(existingAcl => existingAcl.accessLevel == ProjectOwner && emailsBeingChanged.contains(existingAcl.email.toLowerCase))) {
      throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.BadRequest, "project owner permissions cannot be changed"))
    }
    if (aclChanges.exists(_.email.equalsIgnoreCase(userInfo.userEmail.value))) {
      throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.BadRequest, "you may not change your own permissions"))
    }
    if (aclChanges.exists {
      case WorkspaceACLUpdate(_, WorkspaceAccessLevels.Read, _, Some(true)) => true
      case _ => false
    }) {
      throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.BadRequest, "may not grant readers compute access"))
    }
  }

  // called from test harness
  private[workspace] def maybeShareProjectComputePolicy(policyAdditions: Set[(SamResourcePolicyName, String)], workspaceName: WorkspaceName): Future[Unit] = {
    val newWriterEmails = policyAdditions.collect {
      case (SamWorkspacePolicyNames.canCompute, email)  => email
    }
    Future.traverse(newWriterEmails) { email =>
      samDAO.addUserToPolicy(SamResourceTypeNames.billingProject, workspaceName.namespace, SamBillingProjectPolicyNames.canComputeUser, email, userInfo)
    }.map(_ => ())
  }

  private def sendACLUpdateNotifications(workspaceName: WorkspaceName, usersModified: Set[WorkspaceACLUpdate]): Unit = {
    Future.traverse(usersModified) { accessUpdate =>
      for {
        userIdInfo <- samDAO.getUserIdInfo(accessUpdate.email, userInfo)
      } yield {
        userIdInfo match {
          case SamDAO.User(UserIdInfo(_, _, Some(googleSubjectId))) =>
            if(accessUpdate.accessLevel == WorkspaceAccessLevels.NoAccess)
              notificationDAO.fireAndForgetNotification(Notifications.WorkspaceRemovedNotification(RawlsUserSubjectId(googleSubjectId), NoAccess.toString, workspaceName, userInfo.userSubjectId))
            else
              notificationDAO.fireAndForgetNotification(Notifications.WorkspaceAddedNotification(RawlsUserSubjectId(googleSubjectId), accessUpdate.accessLevel.toString, workspaceName, userInfo.userSubjectId))
          case _ =>
        }
      }
    }
  }

  def sendChangeNotifications(workspaceName: WorkspaceName): Future[PerRequestMessage] = {
    for {
      workspaceContext <- getWorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.own)

      userIdInfos <- samDAO.listAllResourceMemberIds(SamResourceTypeNames.workspace, workspaceContext.workspaceId, userInfo)

      notificationMessages = userIdInfos.collect {
        case UserIdInfo(_, _, Some(userId)) => Notifications.WorkspaceChangedNotification(RawlsUserSubjectId(userId), workspaceName)
      }
    } yield {
      notificationDAO.fireAndForgetNotifications(notificationMessages)
      RequestComplete(StatusCodes.OK, notificationMessages.size.toString)
    }
  }

  def lockWorkspace(workspaceName: WorkspaceName): Future[PerRequestMessage] = {
    //don't do the sam REST call inside the db transaction.
    getWorkspaceContext(workspaceName) flatMap { workspaceContext =>
      requireAccessIgnoreLockF(workspaceContext, SamWorkspaceActions.own) {
        //if we get here, we passed all the hoops

        dataSource.inTransaction { dataAccess =>
          dataAccess.submissionQuery.list(workspaceContext).flatMap { submissions =>
            if (!submissions.forall(_.status.isTerminated)) {
              DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.Conflict, s"There are running submissions in workspace $workspaceName, so it cannot be locked.")))
            } else {
              dataAccess.workspaceQuery.lock(workspaceContext.toWorkspaceName).map(_ => RequestComplete(StatusCodes.NoContent))
            }
          }
        }
      }
    }
  }

  def unlockWorkspace(workspaceName: WorkspaceName): Future[PerRequestMessage] = {
    //don't do the sam REST call inside the db transaction.
    getWorkspaceContext(workspaceName) flatMap { workspaceContext =>
      requireAccessIgnoreLockF(workspaceContext, SamWorkspaceActions.own) {
        //if we get here, we passed all the hoops

        dataSource.inTransaction { dataAccess =>
          dataAccess.workspaceQuery.unlock(workspaceContext.toWorkspaceName).map(_ => RequestComplete(StatusCodes.NoContent))
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
  def applyOperationsToWorkspace(workspace: Workspace, operations: Seq[AttributeUpdateOperation]): Workspace = {
    workspace.copy(attributes = applyAttributeUpdateOperations(workspace, operations))
  }

  //validates the expressions in the method configuration, taking into account optional inputs
  private def validateMethodConfiguration(methodConfiguration: MethodConfiguration, workspaceContext: Workspace): Future[ValidatedMethodConfiguration] = {
    for {
      entityProvider <- getEntityProviderForMethodConfig(workspaceContext, methodConfiguration)
      gatherInputsResult <- gatherMethodConfigInputs(methodConfiguration)
      vmc <- entityProvider.expressionValidator.validateMCExpressions(methodConfiguration, gatherInputsResult)
    } yield vmc
  }

  private def getEntityProviderForMethodConfig(workspaceContext: Workspace, methodConfiguration: MethodConfiguration): Future[EntityProvider] = {
    entityManager.resolveProviderFuture(EntityRequestArguments(workspaceContext, userInfo, methodConfiguration.dataReferenceName, None))
  }

  def getAndValidateMethodConfiguration(workspaceName: WorkspaceName, methodConfigurationNamespace: String, methodConfigurationName: String): Future[PerRequestMessage] = {
    getWorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.read) flatMap { workspaceContext =>
      for {
        methodConfig <- dataSource.inTransaction { dataAccess =>
          withMethodConfig(workspaceContext, methodConfigurationNamespace, methodConfigurationName, dataAccess) { methodConfig =>
            DBIO.successful(methodConfig)
          }
        }
        vmc <- validateMethodConfiguration(methodConfig, workspaceContext)
      } yield PerRequest.RequestComplete(StatusCodes.OK, vmc)
    }
  }

  def createMethodConfiguration(workspaceName: WorkspaceName, methodConfiguration: MethodConfiguration): Future[ValidatedMethodConfiguration] = {
    withAttributeNamespaceCheck(methodConfiguration) {
      getWorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.write) flatMap { workspaceContext =>
        dataSource.inTransaction { dataAccess =>
          dataAccess.methodConfigurationQuery.get(workspaceContext, methodConfiguration.namespace, methodConfiguration.name) flatMap {
            case Some(_) => DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.Conflict, s"${methodConfiguration.name} already exists in ${workspaceName}")))
            case None => dataAccess.methodConfigurationQuery.create(workspaceContext, methodConfiguration)
          }
        }.flatMap { methodConfig =>
          validateMethodConfiguration(methodConfig, workspaceContext) }
      }
    }
  }

  def deleteMethodConfiguration(workspaceName: WorkspaceName, methodConfigurationNamespace: String, methodConfigurationName: String): Future[PerRequestMessage] =
    getWorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.write) flatMap { workspaceContext =>
      dataSource.inTransaction { dataAccess =>
        withMethodConfig(workspaceContext, methodConfigurationNamespace, methodConfigurationName, dataAccess) { methodConfig =>
          dataAccess.methodConfigurationQuery.delete(workspaceContext, methodConfigurationNamespace, methodConfigurationName).map(_ => RequestComplete(StatusCodes.NoContent))
        }
      }
    }

  def renameMethodConfiguration(workspaceName: WorkspaceName, methodConfigurationNamespace: String, methodConfigurationName: String, newName: MethodConfigurationName): Future[PerRequestMessage] =
    getWorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.write) flatMap { workspaceContext =>
      dataSource.inTransaction { dataAccess =>
        //It's terrible that we pass unnecessary junk that we don't read in the payload, but a big refactor of the API is going to have to wait until Some Other Time.
        if(newName.workspaceName != workspaceName) {
          DBIO.failed(new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.BadRequest, "Workspace name and namespace in payload must match those in the URI")))
        } else {
          withMethodConfig(workspaceContext, methodConfigurationNamespace, methodConfigurationName, dataAccess) { methodConfiguration =>
            //If a different MC exists at the target location, return 409. But it's okay to want to overwrite your own MC.
            dataAccess.methodConfigurationQuery.get(workspaceContext, newName.namespace, newName.name) flatMap {
              case Some(_) if methodConfigurationNamespace != newName.namespace || methodConfigurationName != newName.name =>
                DBIO.failed(new RawlsExceptionWithErrorReport(errorReport =
                  ErrorReport(StatusCodes.Conflict, s"There is already a method configuration at ${methodConfiguration.namespace}/${methodConfiguration.name} in ${workspaceName}.")))
              case Some(_) => DBIO.successful(()) //renaming self to self: no-op
              case None =>
                dataAccess.methodConfigurationQuery.update(workspaceContext, methodConfigurationNamespace, methodConfigurationName, methodConfiguration.copy(name = newName.name, namespace = newName.namespace))
            } map (_ => RequestComplete(StatusCodes.NoContent))
          }
        }
      }
    }

  //Overwrite the method configuration at methodConfiguration[namespace|name] with the new method configuration.
  def overwriteMethodConfiguration(workspaceName: WorkspaceName, methodConfigurationNamespace: String, methodConfigurationName: String, methodConfiguration: MethodConfiguration): Future[ValidatedMethodConfiguration] = {
    withAttributeNamespaceCheck(methodConfiguration) {
      // check permissions
      getWorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.write) flatMap { workspaceContext =>
        // create transaction
        dataSource.inTransaction { dataAccess =>
          if (methodConfiguration.namespace != methodConfigurationNamespace || methodConfiguration.name != methodConfigurationName) {
            DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.BadRequest,
              s"The method configuration name and namespace in the URI should match the method configuration name and namespace in the request body. If you want to move this method configuration, use POST.")))
          } else {
            dataAccess.methodConfigurationQuery.create(workspaceContext, methodConfiguration)
          }
        }.flatMap { methodConfig =>
          validateMethodConfiguration(methodConfig, workspaceContext)
        }
      }
    }
  }

  //Move the method configuration at methodConfiguration[namespace|name] to the location specified in methodConfiguration, _and_ update it.
  //It's like a rename and upsert all rolled into one.
  def updateMethodConfiguration(workspaceName: WorkspaceName, methodConfigurationNamespace: String, methodConfigurationName: String, methodConfiguration: MethodConfiguration): Future[PerRequestMessage] = {
    withAttributeNamespaceCheck(methodConfiguration) {
      // check permissions
      getWorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.write) flatMap { workspaceContext =>
        // create transaction
        dataSource.inTransaction { dataAccess =>
          withMethodConfig(workspaceContext, methodConfigurationNamespace, methodConfigurationName, dataAccess) { _ =>
              dataAccess.methodConfigurationQuery.get(workspaceContext, methodConfiguration.namespace, methodConfiguration.name) flatMap {
                //If a different MC exists at the target location, return 409. But it's okay to want to overwrite your own MC.
                case Some(_) if methodConfigurationNamespace != methodConfiguration.namespace || methodConfigurationName != methodConfiguration.name =>
                  DBIO.failed(new RawlsExceptionWithErrorReport(errorReport =
                    ErrorReport(StatusCodes.Conflict, s"There is already a method configuration at ${methodConfiguration.namespace}/${methodConfiguration.name} in ${workspaceName}.")))
                case _ =>
                  dataAccess.methodConfigurationQuery.update(workspaceContext, methodConfigurationNamespace, methodConfigurationName, methodConfiguration)
              }
          }
        }.flatMap { updatedMethodConfig =>
          validateMethodConfiguration(updatedMethodConfig, workspaceContext)
        }  map (RequestComplete(StatusCodes.OK, _))
      }
    }
  }

  def getMethodConfiguration(workspaceName: WorkspaceName, methodConfigurationNamespace: String, methodConfigurationName: String): Future[PerRequestMessage] =
    getWorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.read) flatMap { workspaceContext =>
      dataSource.inTransaction { dataAccess =>
        withMethodConfig(workspaceContext, methodConfigurationNamespace, methodConfigurationName, dataAccess) { methodConfig =>
          DBIO.successful(PerRequest.RequestComplete(StatusCodes.OK, methodConfig))
        }
      }
    }

  def copyMethodConfiguration(mcnp: MethodConfigurationNamePair): Future[ValidatedMethodConfiguration] = {
    // split into two transactions because we need to call out to Google after retrieving the source MC

    val transaction1Result = getWorkspaceContextAndPermissions(mcnp.destination.workspaceName, SamWorkspaceActions.write) flatMap { destContext =>
      getWorkspaceContextAndPermissions(mcnp.source.workspaceName, SamWorkspaceActions.read) flatMap { sourceContext =>
        dataSource.inTransaction { dataAccess =>
          dataAccess.methodConfigurationQuery.get(sourceContext, mcnp.source.namespace, mcnp.source.name) flatMap {
            case None =>
              val err = ErrorReport(StatusCodes.NotFound, s"There is no method configuration named ${mcnp.source.namespace}/${mcnp.source.name} in ${mcnp.source.workspaceName}.")
              DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = err))
            case Some(methodConfig) => DBIO.successful((methodConfig, destContext))
          }
        }
      }
    }

    transaction1Result flatMap { case (methodConfig, destContext) =>
      withAttributeNamespaceCheck(methodConfig) {
        dataSource.inTransaction { dataAccess =>
          saveCopiedMethodConfiguration(methodConfig, mcnp.destination, destContext, dataAccess)
        }.flatMap { methodConfig =>
          validateMethodConfiguration(methodConfig, destContext)
        }
      }
    }
  }

  def copyMethodConfigurationFromMethodRepo(methodRepoQuery: MethodRepoConfigurationImport): Future[ValidatedMethodConfiguration] =
    methodRepoDAO.getMethodConfig(methodRepoQuery.methodRepoNamespace, methodRepoQuery.methodRepoName, methodRepoQuery.methodRepoSnapshotId, userInfo) flatMap {
      case None =>
        val name = s"${methodRepoQuery.methodRepoNamespace}/${methodRepoQuery.methodRepoName}/${methodRepoQuery.methodRepoSnapshotId}"
        val err = ErrorReport(StatusCodes.NotFound, s"There is no method configuration named $name in the repository.")
        Future.failed(new RawlsExceptionWithErrorReport(errorReport = err))
      case Some(agoraEntity) => Future.fromTry(parseAgoraEntity(agoraEntity)) flatMap { targetMethodConfig =>
        withAttributeNamespaceCheck(targetMethodConfig) {
          getWorkspaceContextAndPermissions(methodRepoQuery.destination.workspaceName, SamWorkspaceActions.write) flatMap { destContext =>
            dataSource.inTransaction { dataAccess =>
              saveCopiedMethodConfiguration(targetMethodConfig, methodRepoQuery.destination, destContext, dataAccess)
            }.flatMap { methodConfig =>
              validateMethodConfiguration(methodConfig, destContext)
            }
          }
        }
      }
    }

  private def parseAgoraEntity(agoraEntity: AgoraEntity): Try[MethodConfiguration] = {
    val parsed = Try {
      agoraEntity.payload.map(JsonParser(_).convertTo[AgoraMethodConfiguration])
    } recoverWith {
      case e: Exception =>
        Failure(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.UnprocessableEntity, "Error parsing Method Repo response message.", ErrorReport(e))))
    }

    parsed flatMap {
      case Some(agoraMC) => Success(convertToMethodConfiguration(agoraMC))
      case None => Failure(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.UnprocessableEntity, "Method Repo missing configuration payload")))
    }
  }

  private def convertToMethodConfiguration(agoraMethodConfig: AgoraMethodConfiguration): MethodConfiguration = {
    MethodConfiguration(agoraMethodConfig.namespace, agoraMethodConfig.name, Some(agoraMethodConfig.rootEntityType), Some(Map.empty[String, AttributeString]), agoraMethodConfig.inputs, agoraMethodConfig.outputs, agoraMethodConfig.methodRepoMethod)
  }

  def copyMethodConfigurationToMethodRepo(methodRepoQuery: MethodRepoConfigurationExport): Future[PerRequestMessage] = {
    getWorkspaceContextAndPermissions(methodRepoQuery.source.workspaceName, SamWorkspaceActions.read) flatMap { workspaceContext =>
      dataSource.inTransaction { dataAccess =>
        withMethodConfig(workspaceContext, methodRepoQuery.source.namespace, methodRepoQuery.source.name, dataAccess) { methodConfig =>

          DBIO.from(methodRepoDAO.postMethodConfig(
            methodRepoQuery.methodRepoNamespace,
            methodRepoQuery.methodRepoName,
            methodConfig.copy(namespace = methodRepoQuery.methodRepoNamespace, name = methodRepoQuery.methodRepoName),
            userInfo)) map { RequestComplete(StatusCodes.OK, _) }
        }
      }
    }
  }

  private def saveCopiedMethodConfiguration(methodConfig: MethodConfiguration, dest: MethodConfigurationName, destContext: Workspace, dataAccess: DataAccess) = {
    val target = methodConfig.copy(name = dest.name, namespace = dest.namespace)

    dataAccess.methodConfigurationQuery.get(destContext, dest.namespace, dest.name).flatMap {
      case Some(existingMethodConfig) => DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.Conflict, s"A method configuration named ${dest.namespace}/${dest.name} already exists in ${dest.workspaceName}")))
      case None => dataAccess.methodConfigurationQuery.create(destContext, target)
    }
  }

  def listAgoraMethodConfigurations(workspaceName: WorkspaceName): Future[PerRequestMessage] =
    getWorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.read) flatMap { workspaceContext =>
      dataSource.inTransaction { dataAccess =>
        dataAccess.methodConfigurationQuery.listActive(workspaceContext).map { r =>
          RequestComplete(StatusCodes.OK, r.toList.filter(_.methodRepoMethod.repo == Agora))
        }
      }
    }

  def listMethodConfigurations(workspaceName: WorkspaceName): Future[PerRequestMessage] =
    getWorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.read) flatMap { workspaceContext =>
      dataSource.inTransaction { dataAccess =>
        dataAccess.methodConfigurationQuery.listActive(workspaceContext).map { r =>
          RequestComplete(StatusCodes.OK, r.toList)
        }
      }
    }

  def createMethodConfigurationTemplate(methodRepoMethod: MethodRepoMethod ): Future[PerRequestMessage] = {
    dataSource.inTransaction { _ =>
      withMethod(methodRepoMethod, userInfo) { wdl: WDL =>
        methodConfigResolver.toMethodConfiguration(userInfo, wdl, methodRepoMethod) match {
          case Failure(exception) => DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.BadRequest, exception)))
          case Success(methodConfig) => DBIO.successful(RequestComplete(StatusCodes.OK, methodConfig))
        }
      }
    }
  }

  def getMethodInputsOutputs(userInfo: UserInfo, methodRepoMethod: MethodRepoMethod ): Future[PerRequestMessage] = {
    dataSource.inTransaction { _ =>
      withMethod(methodRepoMethod, userInfo) { wdl: WDL =>
        methodConfigResolver.getMethodInputsOutputs(userInfo, wdl) match {
          case Failure(exception) => DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.BadRequest, exception)))
          case Success(inputsOutputs) => DBIO.successful(RequestComplete(StatusCodes.OK, inputsOutputs))
        }
      }
    }
  }

  def listSubmissions(workspaceName: WorkspaceName): Future[PerRequestMessage] = {
    val costlessSubmissionsFuture = getWorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.read) flatMap { workspaceContext =>
      dataSource.inTransaction { dataAccess =>
        dataAccess.submissionQuery.listWithSubmitter(workspaceContext)
      }
    }

    // TODO David An 2018-05-30: temporarily disabling cost calculations for submission list due to potential performance hit
    // val costMapFuture = costlessSubmissionsFuture flatMap { submissions =>
    //   submissionCostService.getWorkflowCosts(submissions.flatMap(_.workflowIds).flatten, workspaceName.namespace)
    // }
    val costMapFuture = Future.successful(Map.empty[String,Float])

    toFutureTry(costMapFuture) flatMap { costMapTry =>
      val costMap: Map[String,Float] = costMapTry match {
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
        RequestComplete(StatusCodes.OK, costedSubmissions)
      }
    }
  }

  def countSubmissions(workspaceName: WorkspaceName): Future[PerRequestMessage] =
    getWorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.read) flatMap { workspaceContext =>
      dataSource.inTransaction { dataAccess =>
        dataAccess.submissionQuery.countByStatus(workspaceContext).map(RequestComplete(StatusCodes.OK, _))
      }
    }

  def createSubmission(workspaceName: WorkspaceName, submissionRequest: SubmissionRequest): Future[PerRequestMessage] = {
    for {
      (workspaceContext, submissionParameters, workflowFailureMode, header) <- prepareSubmission(workspaceName, submissionRequest)
      submission <- saveSubmission(workspaceContext, submissionRequest, submissionParameters, workflowFailureMode, header)
    } yield {
      RequestComplete(StatusCodes.Created, SubmissionReport(submissionRequest, submission.submissionId, submission.submissionDate, userInfo.userEmail.value, submission.status, header, submissionParameters.filter(_.inputResolutions.forall(_.error.isEmpty))))
    }
  }

  private def prepareSubmission(workspaceName: WorkspaceName, submissionRequest: SubmissionRequest):
  Future[(Workspace, Stream[SubmissionValidationEntityInputs], Option[WorkflowFailureMode], SubmissionValidationHeader)] = {
    for {
      _ <- requireComputePermission(workspaceName)

      // getWorkflowFailureMode early because it does validation and better to error early
      workflowFailureMode <- getWorkflowFailureMode(submissionRequest)

      workspaceContext <- getWorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.write)
      methodConfigOption <- dataSource.inTransaction { dataAccess =>
        dataAccess.methodConfigurationQuery.get(workspaceContext, submissionRequest.methodConfigurationNamespace, submissionRequest.methodConfigurationName)
      }
      methodConfig = methodConfigOption.getOrElse(
        throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.NotFound, s"${submissionRequest.methodConfigurationNamespace}/${submissionRequest.methodConfigurationName} does not exist in ${workspaceContext}"))
      )

      entityProvider <- getEntityProviderForMethodConfig(workspaceContext, methodConfig)

      _ = validateSubmissionRootEntity(submissionRequest, methodConfig)

      gatherInputsResult <- gatherMethodConfigInputs(methodConfig)

      validationResult <- entityProvider.expressionValidator.validateExpressionsForSubmission(methodConfig, gatherInputsResult)

      // calling .get on the Try will throw the validation error
      _ = validationResult.get

      methodConfigInputs = gatherInputsResult.processableInputs.map { methodInput => SubmissionValidationInput(methodInput.workflowInput.getName, methodInput.expression) }
      header = SubmissionValidationHeader(methodConfig.rootEntityType, methodConfigInputs, entityProvider.entityStoreId)

      workspaceExpressionResults <- evaluateWorkspaceExpressions(workspaceContext, gatherInputsResult)
      submissionParameters <- entityProvider.evaluateExpressions(ExpressionEvaluationContext(submissionRequest.entityType, submissionRequest.entityName, submissionRequest.expression, methodConfig.rootEntityType), gatherInputsResult, workspaceExpressionResults)
    } yield {
      (workspaceContext, submissionParameters, workflowFailureMode, header)
    }
  }

  private def evaluateWorkspaceExpressions(workspace: Workspace, gatherInputResults: GatherInputsResult): Future[Map[LookupExpression, Try[Iterable[AttributeValue]]]] = {
    dataSource.inTransaction { dataAccess =>
      ExpressionEvaluator.withNewExpressionEvaluator(dataAccess, None) { expressionEvaluator =>
        val expressionQueries = gatherInputResults.processableInputs.map { input =>
          expressionEvaluator.evalWorkspaceExpressionsOnly(workspace, input.expression)
        }

        // reduce(_ ++ _) collapses the series of maps into a single map
        // duplicate map keys are dropped but that is ok as the values should be duplicate
        DBIO.sequence(expressionQueries.toSeq).map {
          case Seq() => Map.empty[LookupExpression, Try[Iterable[AttributeValue]]]
          case results => results.reduce(_ ++ _)
        }
      }
    }
  }

  private def gatherMethodConfigInputs(methodConfig: MethodConfiguration): Future[MethodConfigResolver.GatherInputsResult] = {
    toFutureTry(methodRepoDAO.getMethod(methodConfig.methodRepoMethod, userInfo)).map {
      case Success(None) => throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.NotFound, s"Cannot get ${methodConfig.methodRepoMethod.methodUri} from method repo."))
      case Success(Some(wdl)) => methodConfigResolver.gatherInputs(userInfo, methodConfig, wdl).recoverWith { case regrets =>
        Failure(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.BadRequest, regrets)))
      }.get
      case Failure(throwable) => throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.BadGateway, s"Unable to query the method repo.", methodRepoDAO.toErrorReport(throwable)))
    }
  }

  def saveSubmission(workspaceContext: Workspace, submissionRequest: SubmissionRequest, submissionParameters: Seq[SubmissionValidationEntityInputs], workflowFailureMode: Option[WorkflowFailureMode], header: SubmissionValidationHeader): Future[Submission] = {
    dataSource.inTransaction { dataAccess =>
      val submissionId: UUID = UUID.randomUUID()
      val (successes, failures) = submissionParameters.partition({ entityInputs => entityInputs.inputResolutions.forall(_.error.isEmpty) })
      val workflows = successes map { entityInputs =>
        val workflowEntityOpt = header.entityType.map(_ => AttributeEntityReference(entityType = header.entityType.get, entityName = entityInputs.entityName))
        Workflow(workflowId = None,
          status = WorkflowStatuses.Queued,
          statusLastChangedDate = DateTime.now,
          workflowEntity = workflowEntityOpt,
          inputResolutions = entityInputs.inputResolutions.toSeq
        )
      }

      val workflowFailures = failures map { entityInputs =>
        val workflowEntityOpt = header.entityType.map(_ => AttributeEntityReference(entityType = header.entityType.get, entityName = entityInputs.entityName))
        Workflow(workflowId = None,
          status = WorkflowStatuses.Failed,
          statusLastChangedDate = DateTime.now,
          workflowEntity = workflowEntityOpt,
          inputResolutions = entityInputs.inputResolutions.toSeq,
          messages = (for (entityValue <- entityInputs.inputResolutions if entityValue.error.isDefined) yield AttributeString(entityValue.inputName + " - " + entityValue.error.get)).toSeq
        )
      }

      val submissionEntityOpt = if (header.entityType.isEmpty || submissionRequest.entityName.isEmpty) {
        None
      } else {
        Some(AttributeEntityReference(entityType = submissionRequest.entityType.get, entityName = submissionRequest.entityName.get))
      }

      val submission = Submission(submissionId = submissionId.toString,
        submissionDate = DateTime.now(),
        submitter = WorkbenchEmail(userInfo.userEmail.value),
        methodConfigurationNamespace = submissionRequest.methodConfigurationNamespace,
        methodConfigurationName = submissionRequest.methodConfigurationName,
        submissionEntity = submissionEntityOpt,
        workflows = workflows ++ workflowFailures,
        status = SubmissionStatuses.Submitted,
        useCallCache = submissionRequest.useCallCache,
        deleteIntermediateOutputFiles = submissionRequest.deleteIntermediateOutputFiles,
        workflowFailureMode = workflowFailureMode,
        externalEntityInfo = for {
          entityType <- header.entityType
          dataStoreId <- header.entityStoreId
        } yield ExternalEntityInfo(dataStoreId, entityType)
      )

      // implicitly passed to SubmissionComponent.create
      implicit val subStatusCounter = submissionStatusCounter(workspaceMetricBuilder(workspaceContext.toWorkspaceName))
      implicit val wfStatusCounter = (status: WorkflowStatus) =>
        if (config.trackDetailedSubmissionMetrics) Option(workflowStatusCounter(workspaceSubmissionMetricBuilder(workspaceContext.toWorkspaceName, submissionId))(status))
        else None

      dataAccess.submissionQuery.create(workspaceContext, submission)
    }
  }

  def validateSubmission(workspaceName: WorkspaceName, submissionRequest: SubmissionRequest): Future[PerRequestMessage] = {
    for {
      (_, submissionParameters, _, header) <- prepareSubmission(workspaceName, submissionRequest)
    } yield {
      val (failed, succeeded) = submissionParameters.partition(_.inputResolutions.exists(_.error.isDefined))
      RequestComplete(StatusCodes.OK, SubmissionValidationReport(submissionRequest, header, succeeded, failed))
    }
  }

  def getSubmissionStatus(workspaceName: WorkspaceName, submissionId: String): Future[PerRequestMessage] = {
    val submissionWithoutCosts = getWorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.read) flatMap { workspaceContext =>
      dataSource.inTransaction { dataAccess =>
        withSubmission(workspaceContext, submissionId, dataAccess) { submission =>
            DBIO.successful(submission)
        }
      }
    }

    submissionWithoutCosts flatMap {
      case (submission) => {
        val allWorkflowIds: Seq[String] = submission.workflows.flatMap(_.workflowId)
        val submissionDoneDate: Option[DateTime] = WorkspaceService.getTerminalStatusDate(submission, None)
        toFutureTry(submissionCostService.getSubmissionCosts(submissionId, allWorkflowIds, workspaceName.namespace, submission.submissionDate, submissionDoneDate)) map {
          case Failure(ex) =>
            logger.error(s"Unable to get workflow costs for submission $submissionId", ex)
            RequestComplete((StatusCodes.OK, submission))
          case Success(costMap) =>
            val costedWorkflows = submission.workflows.map { workflow =>
              workflow.workflowId match {
                case Some(wfId) => workflow.copy(cost = costMap.get(wfId))
                case None => workflow
              }
            }
            val costedSubmission = submission.copy(cost = Some(costMap.values.sum), workflows = costedWorkflows)
            RequestComplete((StatusCodes.OK, costedSubmission))
        }
      }
    }
  }

  def abortSubmission(workspaceName: WorkspaceName, submissionId: String): Future[PerRequestMessage] = {
    getWorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.write) flatMap { workspaceContext =>
      dataSource.inTransaction { dataAccess =>
        abortSubmission(workspaceContext, submissionId, dataAccess)
      }
    }
  }

  private def abortSubmission(workspaceContext: Workspace, submissionId: String, dataAccess: DataAccess): ReadWriteAction[PerRequestMessage] = {
    withSubmissionId(workspaceContext, submissionId, dataAccess) { submissionId =>
      // implicitly passed to SubmissionComponent.updateStatus
      implicit val subStatusCounter = submissionStatusCounter(workspaceMetricBuilder(workspaceContext.toWorkspaceName))
      dataAccess.submissionQuery.updateStatus(submissionId, SubmissionStatuses.Aborting) map { rows =>
        if(rows == 1)
          RequestComplete(StatusCodes.NoContent)
        else
          RequestComplete(ErrorReport(StatusCodes.NotFound, s"Unable to abort submission. Submission ${submissionId} could not be found."))
      }
    }
  }

  /**
   * Munges together the output of Cromwell's /outputs and /logs endpoints, grouping them by task name */
  private def mergeWorkflowOutputs(execOuts: ExecutionServiceOutputs, execLogs: ExecutionServiceLogs, workflowId: String): PerRequestMessage = {
    val outs = execOuts.outputs
    val logs = execLogs.calls getOrElse Map()

    //Cromwell workflow outputs look like workflow_name.task_name.output_name.
    //Under perverse conditions it might just be workflow_name.output_name.
    //Group outputs by everything left of the rightmost dot.
    val outsByTask = outs groupBy { case (k,_) => k.split('.').dropRight(1).mkString(".") }

    val taskMap = (outsByTask.keySet ++ logs.keySet).map( key => key -> TaskOutput( logs.get(key), outsByTask.get(key)) ).toMap
    RequestComplete(StatusCodes.OK, WorkflowOutputs(workflowId, taskMap))
  }

  /**
   * Get the list of outputs for a given workflow in this submission */
  def workflowOutputs(workspaceName: WorkspaceName, submissionId: String, workflowId: String) = {
    getWorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.read) flatMap { workspaceContext =>
      dataSource.inTransaction { dataAccess =>
        withWorkflowRecord(workspaceName, submissionId, workflowId, dataAccess) { wr =>
          val outputFTs = toFutureTry(executionServiceCluster.outputs(wr, userInfo))
          val logFTs = toFutureTry(executionServiceCluster.logs(wr, userInfo))
          DBIO.from(outputFTs zip logFTs map {
            case (Success(outputs), Success(logs)) =>
              mergeWorkflowOutputs(outputs, logs, workflowId)
            case (Failure(outputsFailure), Success(logs)) =>
              throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.BadGateway, s"Unable to get outputs for ${submissionId}.", executionServiceCluster.toErrorReport(outputsFailure)))
            case (Success(outputs), Failure(logsFailure)) =>
              throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.BadGateway, s"Unable to get logs for ${submissionId}.", executionServiceCluster.toErrorReport(logsFailure)))
            case (Failure(outputsFailure), Failure(logsFailure)) =>
              throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.BadGateway, s"Unable to get outputs and unable to get logs for ${submissionId}.",
                Seq(executionServiceCluster.toErrorReport(outputsFailure),executionServiceCluster.toErrorReport(logsFailure))))
          })
        }
      }
    }
  }

  // retrieve the cost of this Workflow from BigQuery, if available
  def workflowCost(workspaceName: WorkspaceName, submissionId: String, workflowId: String) = {

    // confirm: the user can Read this Workspace, the Submission is in this Workspace,
    // and the Workflow is in the Submission

    val execIdFutOpt = getWorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.read) flatMap { workspaceContext =>
      dataSource.inTransaction { dataAccess =>
        withSubmissionAndWorkflowExecutionServiceKey(workspaceContext, submissionId, workflowId, dataAccess) { optExecKey =>
          withSubmission(workspaceContext, submissionId, dataAccess) { submission =>
            DBIO.successful((optExecKey, submission))
          }
        }
      }
    }

    for {
      (optExecId, submission) <- execIdFutOpt
      // we don't need the Execution Service ID, but we do need to confirm the Workflow is in one for this Submission
      // if we weren't able to do so above
      _ <- executionServiceCluster.findExecService(submissionId, workflowId, userInfo, optExecId)
      submissionDoneDate = WorkspaceService.getTerminalStatusDate(submission, Option(workflowId))
      costs <- submissionCostService.getWorkflowCost(workflowId, workspaceName.namespace, submission.submissionDate, submissionDoneDate)
    } yield RequestComplete(StatusCodes.OK, WorkflowCost(workflowId, costs.get(workflowId)))
  }

  def workflowMetadata(workspaceName: WorkspaceName, submissionId: String, workflowId: String, metadataParams: MetadataParams): Future[PerRequestMessage] = {

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
    val execIdFutOpt: Future[Option[ExecutionServiceId]] = getWorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.read) flatMap { workspaceContext =>
      dataSource.inTransaction { dataAccess =>
        withSubmissionAndWorkflowExecutionServiceKey(workspaceContext, submissionId, workflowId, dataAccess) { optExecKey =>
          DBIO.successful(optExecKey)
        }
      }
    }

    // query the execution service(s) for the metadata
    execIdFutOpt flatMap {
      executionServiceCluster.callLevelMetadata(submissionId, workflowId, metadataParams, _, userInfo)
    } map {
      metadata =>
        RequestComplete(StatusCodes.OK, metadata)
    }
  }

  def workflowQueueStatus() = {
    dataSource.inTransaction { dataAccess =>
      dataAccess.workflowQuery.countWorkflowsByQueueStatus.flatMap { statusMap =>
        // determine the current size of the workflow queue
        statusMap.get(WorkflowStatuses.Queued.toString) match {
          case Some(x) if x > 0 =>
            for {
              timeEstimate <- dataAccess.workflowAuditStatusQuery.queueTimeMostRecentSubmittedWorkflow
              workflowsAhead <- dataAccess.workflowQuery.countWorkflowsAheadOfUserInQueue(userInfo)
            } yield {
              RequestComplete(StatusCodes.OK, WorkflowQueueStatusResponse(timeEstimate, workflowsAhead, statusMap))
            }
          case _ => DBIO.successful(RequestComplete(StatusCodes.OK, WorkflowQueueStatusResponse(0, 0, statusMap)))
        }
      }
    }
  }

  def adminWorkflowQueueStatusByUser() = {
    asFCAdmin {
      dataSource.inTransaction ({ dataAccess =>
        for {
          global <- dataAccess.workflowQuery.countWorkflowsByQueueStatus
          perUser <- dataAccess.workflowQuery.countWorkflowsByQueueStatusByUser
        } yield RequestComplete(StatusCodes.OK, WorkflowQueueStatusByUserResponse(global, perUser, maxActiveWorkflowsTotal, maxActiveWorkflowsPerUser))
      }, TransactionIsolation.ReadUncommitted)
    }
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
  def checkBucketReadAccess(workspaceName: WorkspaceName) = {
    for {
      (workspace, maxAccessLevel) <- getWorkspaceContextAndPermissions(workspaceName, SamWorkspaceActions.read) flatMap { workspaceContext =>
        dataSource.inTransaction { dataAccess =>
          DBIO.from(getMaximumAccessLevel(workspaceContext.workspaceIdAsUUID.toString)).map { accessLevel =>
            (workspaceContext, accessLevel)
          }
        }
      }

      petKey <- if (maxAccessLevel >= WorkspaceAccessLevels.Write)
        samDAO.getPetServiceAccountKeyForUser(workspace.googleProjectId, userInfo.userEmail)
      else
        samDAO.getDefaultPetServiceAccountKeyForUser(userInfo)

      accessToken <- gcsDAO.getAccessTokenUsingJson(petKey)

      (petEmail, petSubjectId) = petKey.parseJson match {
        case JsObject(fields) => (RawlsUserEmail(fields("client_email").toString), RawlsUserSubjectId(fields("client_id").toString))
        case _ => throw new RawlsException("pet service account key was not a json object")
      }

      resultsForPet <- gcsDAO.diagnosticBucketRead(UserInfo(petEmail, OAuth2BearerToken(accessToken), 60, petSubjectId), workspace.bucketName)
    } yield {
      resultsForPet match {
        case None => RequestComplete(StatusCodes.OK)
        case Some(report) => RequestComplete(report)
      }
    }
  }

  def checkSamActionWithLock(workspaceName: WorkspaceName, samAction: SamResourceAction): Future[PerRequestMessage] = {
    val wsCtxFuture = dataSource.inTransaction { dataAccess =>
      withWorkspaceContext(workspaceName, dataAccess, Some(WorkspaceAttributeSpecs(all = false))) { workspaceContext =>
        DBIO.successful(workspaceContext)
      }
    }

    //don't do the sam REST call inside the db transaction.
    val access: Future[PerRequestMessage] = wsCtxFuture flatMap { workspaceContext =>
      requireAccessF(workspaceContext, samAction) {
        Future.successful(RequestComplete(StatusCodes.NoContent)) //if we get here, we passed all the hoops
      }
    }

    //if we failed for any reason, the user can't do that thing on the workspace
    access.recover { case _ =>
      RequestComplete(StatusCodes.Forbidden) }
  }

  def listAllActiveSubmissions() = {
    dataSource.inTransaction { dataAccess =>
      dataAccess.submissionQuery.listAllActiveSubmissions().map(RequestComplete(StatusCodes.OK, _))
    }
  }

  def adminAbortSubmission(workspaceName: WorkspaceName, submissionId: String) = {
    asFCAdmin {
      dataSource.inTransaction { dataAccess =>
        withWorkspaceContext(workspaceName, dataAccess) { workspaceContext =>
          abortSubmission(workspaceContext, submissionId, dataAccess)
        }
      }
    }
  }

  def listAllWorkspaces() = {
    asFCAdmin {
      dataSource.inTransaction { dataAccess =>
        dataAccess.workspaceQuery.listAll.map(workspaces => RequestComplete(StatusCodes.OK, workspaces.map(w => WorkspaceDetails(w, Set.empty))))
      }
    }
  }

  def listWorkspacesWithAttribute(attributeName: AttributeName, attributeValue: AttributeValue): Future[PerRequestMessage] = {
    for {
      workspaces <- dataSource.inTransaction { dataAccess =>
        dataAccess.workspaceQuery.listWithAttribute(attributeName, attributeValue)
      }
      results <- Future.traverse(workspaces) { workspace =>
        loadResourceAuthDomain(SamResourceTypeNames.workspace, workspace.workspaceId, userInfo).map(WorkspaceDetails(workspace, _))
      }
    } yield {
      RequestComplete(StatusCodes.OK, results)
    }

  }

  def getBucketUsage(workspaceName: WorkspaceName): Future[PerRequestMessage] = {
    //don't do the sam REST call inside the db transaction.
    getWorkspaceContext(workspaceName) flatMap { workspaceContext =>
      requireAccessIgnoreLockF(workspaceContext, SamWorkspaceActions.write) {
        //if we get here, we passed all the hoops, otherwise an exception would have been thrown

        gcsDAO.getBucketUsage(workspaceContext.googleProjectId, workspaceContext.bucketName).map { usage =>
          RequestComplete(BucketUsageResponse(usage))
        }
      }
    }
  }

  def getAccessInstructions(workspaceName: WorkspaceName): Future[PerRequestMessage] = {
    for {
      workspaceId <- loadWorkspaceId(workspaceName)
      authDomains <- samDAO.getResourceAuthDomain(SamResourceTypeNames.workspace, workspaceId, userInfo)
      instructions <- Future.traverse(authDomains) { adGroup =>
        samDAO.getAccessInstructions(WorkbenchGroupName(adGroup), userInfo).map { maybeInstruction =>
          maybeInstruction.map(i => ManagedGroupAccessInstructions(adGroup, i))
        }
      }
    } yield RequestComplete(StatusCodes.OK, instructions.flatten)
  }

  def getGenomicsOperationV2(workflowId: String, operationId: List[String]): Future[PerRequestMessage] = {
    // note that cromiam should only give back metadata if the user is authorized to see it
    cromiamDAO.callLevelMetadata(workflowId, MetadataParams(includeKeys = Set("jobId")), userInfo).flatMap { metadataJson =>
      val operationIds: Iterable[String] = WorkspaceService.extractOperationIdsFromCromwellMetadata(metadataJson)

      val operationIdString = operationId.mkString("/")
      // check that the requested operation id actually exists in the workflow
      if (operationIds.toList.contains(operationIdString)) {
        val genomicsServiceRef = genomicsServiceConstructor(userInfo)
        genomicsServiceRef.GetOperation(operationIdString)
      } else {
        Future.failed(new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.NotFound, s"operation id ${operationIdString} not found in workflow $workflowId")))
      }
    }
  }

  def enableRequesterPaysForLinkedSAs(workspaceName: WorkspaceName): Future[PerRequestMessage] = {
    for {
      maybeWorkspace <- dataSource.inTransaction { dataAccess => dataAccess.workspaceQuery.findByName(workspaceName) }
      workspace <- maybeWorkspace match {
        case None => Future.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.NotFound, noSuchWorkspaceMessage(workspaceName))))
        case Some(workspace) => Future.successful(workspace)
      }
      _ <- accessCheck(workspace, SamWorkspaceActions.compute, ignoreLock = false)
      _ <- requesterPaysSetupService.grantRequesterPaysToLinkedSAs(userInfo, workspace)
    } yield {
      RequestComplete(StatusCodes.NoContent)
    }
  }

  def disableRequesterPaysForLinkedSAs(workspaceName: WorkspaceName): Future[PerRequestMessage] = {
    // note that this does not throw an error if the workspace does not exist
    // the user may no longer have access to the workspace so we can't confirm it exists
    // but the user does have the right to remove their linked SAs
    for {
      maybeWorkspace <- dataSource.inTransaction { dataaccess =>
        dataaccess.workspaceQuery.findByName(workspaceName)
      }
      _ <- Future.traverse(maybeWorkspace.toList) { workspace =>
        requesterPaysSetupService.revokeUserFromWorkspace(userInfo.userEmail, workspace)
      }
    } yield {
      RequestComplete(StatusCodes.NoContent)
    }
  }

  // helper methods

  private def createWorkflowCollectionForWorkspace(workspaceId: String, policyMap: Map[SamResourcePolicyName, WorkbenchEmail], parentSpan: Span = null) = {
    for {
      _ <- traceWithParent("createResourceFull",parentSpan)( _ => samDAO.createResourceFull(
              SamResourceTypeNames.workflowCollection,
              workspaceId,
              Map(
                SamWorkflowCollectionPolicyNames.workflowCollectionOwnerPolicyName ->
                  SamPolicy(Set(policyMap(SamWorkspacePolicyNames.projectOwner), policyMap(SamWorkspacePolicyNames.owner)), Set.empty, Set(SamWorkflowCollectionRoles.owner)),
                SamWorkflowCollectionPolicyNames.workflowCollectionWriterPolicyName ->
                  SamPolicy(Set(policyMap(SamWorkspacePolicyNames.canCompute)), Set.empty, Set(SamWorkflowCollectionRoles.writer)),
                SamWorkflowCollectionPolicyNames.workflowCollectionReaderPolicyName ->
                  SamPolicy(Set(policyMap(SamWorkspacePolicyNames.reader), policyMap(SamWorkspacePolicyNames.writer)), Set.empty, Set(SamWorkflowCollectionRoles.reader))
              ),
              Set.empty,
              userInfo,
              None
            ))
    } yield {
    }
  }



  /**
    * Gets a Google Project from the Resource Buffering Service (RBS) and sets it up to be usable by Rawls as the backing
    * Google Project for a Workspace.  The specific entities in the Google Project (like Buckets or compute nodes or
    * whatever) that are used by the Workspace will all get set up later after the Workspace is created in Rawls.  The
    * project should NOT be added to any Service Perimeters yet, that needs to happen AFTER we persist the Workspace
    * record.
    * 1. Claim Project from RBS
    * 2. Update Billing Account information on Google Project
    *
    * @param billingProject
    * @param span
    * @return Future[(GoogleProjectId, GoogleProjectNumber)] of the project that we claimed from RBS
    */
  private def setupGoogleProject(billingProject: RawlsBillingProject, workspaceId: String, span: Span = null) = {
    // We should never get here with a missing or invalid Billing Account, but we still need to get the value out of the
    // Option, so we are being thorough
    val billingAccount = billingProject.billingAccount match {
      case Some(ba) if !billingProject.invalidBillingAccount => ba
      case _ => throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.BadRequest, s"Billing Account is missing or invalid for Billing Project: ${billingProject}"))
    }

    val projectPoolType = billingProject.servicePerimeter match {
      case Some(_) => ProjectPoolType.ServicePerimeter
      case _ => ProjectPoolType.Regular
    }

    for {
      googleProjectId <- traceWithParent("getGoogleProjectFromRBS", span)(_ => resourceBufferService.getGoogleProjectFromRBS(projectPoolType, workspaceId))
      _ <- traceWithParent("updateBillingAccountForProject", span)(_ => gcsDAO.setBillingAccountForProject(googleProjectId, billingAccount))
      googleProjectNumber <- traceWithParent("getProjectNumberFromGoogle", span)(_ => getGoogleProjectNumber(googleProjectId))
    } yield (googleProjectId, googleProjectNumber)
  }

  private def getGoogleProjectNumber(googleProjectId: GoogleProjectId): Future[GoogleProjectNumber] = {
    gcsDAO.getGoogleProject(googleProjectId).map { p => Option(p.getProjectNumber) match {
        case None => throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.BadGateway, s"Failed to retrieve Google Project Number for Google Project ${googleProjectId}"))
        case Some(longProjectNumber) => GoogleProjectNumber(longProjectNumber.toString)
      }
    }
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
  private def maybeUpdateGoogleProjectsInPerimeter(billingProject: RawlsBillingProject, span: Span = null): Future[Unit] = {
    billingProject.servicePerimeter match {
      case Some(servicePerimeterName) => overwriteGoogleProjectsInPerimeter(servicePerimeterName)
      case None => Future.successful()
    }
  }

  /**
    * Some Service Perimeters may required that they have some additional non-Terra Google Projects that need to be in
    * the perimeter for some other reason.  These are provided to us by the Service Perimeter stakeholders and we add
    * them to the Rawls Config so that whenever we update the list of projects for a perimeter, these projects are
    * always included.
    *
    * @param servicePerimeterName
    * @return
    */
  private def loadStaticProjectsForPerimeter(servicePerimeterName: ServicePerimeterName): Seq[GoogleProjectNumber] = {
    config.staticProjectsInPerimeters.getOrElse(servicePerimeterName, Seq.empty)
  }

  /**
    * Takes the the name of a Service Perimeter as the only parameter.  Since multiple Billing Projects can specify the
    * same Service Perimeter, we will:
    * 1. Load all the Billing Projects that also use this servicePerimeterName
    * 2. Load all the Workspaces in all of those Billing Projects
    * 3. Collect all of the GoogleProjectNumbers from those Workspaces
    * 4. Post that list to Google to overwrite the Service Perimeter's list of included Google Projects
    * 5. Poll until Google Operation to update the Service Perimeter gets to some terminal state
    * Throw exceptions if any of this goes awry
    *
    * @param servicePerimeterName
    * @return Future[Unit] indicating whether we succeeded to update the Service Perimeter
    */
  private def overwriteGoogleProjectsInPerimeter(servicePerimeterName: ServicePerimeterName): Future[Unit] = {
    collectWorkspacesInPerimeter(servicePerimeterName).map { workspacesInPerimeter =>
      val projectNumbers = workspacesInPerimeter.flatMap(_.googleProjectNumber) ++ loadStaticProjectsForPerimeter(servicePerimeterName)
      val projectNumberStrings = projectNumbers.map(_.value)

      // Make the call to Google to overwrite the project.  Poll and wait for the Google Operation to complete
      gcsDAO.accessContextManagerDAO.overwriteProjectsInServicePerimeter(servicePerimeterName, projectNumberStrings).map { operation =>
        // Keep retrying the pollOperation until the OperationStatus that gets returned is some terminal status
        retryUntilSuccessOrTimeout(failureLogMessage = s"Google Operation to update Service Perimeter: ${servicePerimeterName} was not successful")(5 seconds, 50 seconds) { () =>
          gcsDAO.pollOperation(OperationId(GoogleApiTypes.AccessContextManagerApi, operation.getName)).map {
            case OperationStatus(false, _) => Future.failed(new RawlsException(s"Google Operation to update Service Perimeter ${servicePerimeterName} is still in progress..."))
            // TODO: If the operation to update the Service Perimeter failed, we need to consider the possibility that
            // the list of Projects in the Perimeter may have been wiped or somehow modified in an undesirable way.  If
            // this happened, it would be possible for Projects intended to be in the Perimeter are NOT in that
            // Perimeter anymore, which is a problem.
            case OperationStatus(true, errorMessage) if !errorMessage.isEmpty => Future.successful(throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.InternalServerError, s"Google Operation to update Service Perimeter ${servicePerimeterName} failed with message: ${errorMessage}")))
            case _ => Future.successful()
          }
        }
      }
    }
  }

  /**
    * In its own transaction, look up all of the Workspaces contained in Billing Projects that use the specified
    * ServicePerimeterName
    *
    * @param servicePerimeterName
    * @return
    */
  private def collectWorkspacesInPerimeter(servicePerimeterName: ServicePerimeterName): Future[Seq[Workspace]] = {
    dataSource.inTransaction { dataAccess =>
      dataAccess.workspaceQuery.getWorkspacesInPerimeter(servicePerimeterName)
    }
  }

  /**
    * takes a RawlsBillingProject and checks that Rawls has the appropriate permissions on the underlying Billing
    * Account on Google.  Does NOT check if Terra _User_ has necessary permissions on the Billing Account.  Updates
    * BillingProject to persist latest 'invalidBillingAccount' info.  Returns TRUE if user has right IAM access, else
    * FALSE
    */
  def updateAndGetBillingAccountAccess(billingProject: RawlsBillingProject, parentSpan: Span = null): Future[Boolean] = {
    val billingAccountName: RawlsBillingAccountName = billingProject.billingAccount.getOrElse(throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.InternalServerError, s"Billing Project ${billingProject.projectName.value} has no Billing Account associated with it")))
    for {
      hasAccess <- traceWithParent("checkBillingAccountIAM", parentSpan)(_ => gcsDAO.testDMBillingAccountAccess(billingAccountName))
      _ <- maybeUpdateInvalidBillingAccountField(billingProject, !hasAccess, parentSpan)
    } yield hasAccess
  }

  private def maybeUpdateInvalidBillingAccountField(billingProject: RawlsBillingProject, invalidBillingAccount: Boolean, span: Span = null): Future[Seq[Int]] = {
    // Only update the Billing Project record if the invalidBillingAccount field has changed
    if (billingProject.invalidBillingAccount != invalidBillingAccount) {
      val updatedBillingProject = billingProject.copy(invalidBillingAccount = invalidBillingAccount)
      dataSource.inTransaction { dataAccess =>
        traceDBIOWithParent("updateInvalidBillingAccountField", span)(_ => dataAccess.rawlsBillingProjectQuery.updateBillingProjects(Seq(updatedBillingProject)))
      }
    } else {
      Future.successful(Seq[Int]())
    }
  }

  /**
    * Checks that Rawls has the right permissions on the BillingProject's Billing Account, and then passes along the
    * BillingProject to op to be used by code in this context
    *
    * @param billingProjectName
    * @param parentSpan
    * @param op
    * @tparam T
    * @return
    */
  private def withBillingProjectContext[T](billingProjectName: String, parentSpan: Span = null)(op: (RawlsBillingProject) => Future[T]): Future[T] = {
    for {
      maybeBillingProject <- dataSource.inTransaction { dataAccess =>
        traceDBIOWithParent("loadBillingProject", parentSpan)(_ => dataAccess.rawlsBillingProjectQuery.load(RawlsBillingProjectName(billingProjectName)))
      }
      billingProject = maybeBillingProject.getOrElse(throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.BadRequest, s"Billing Project ${billingProjectName} does not exist")))
      _ <- updateAndGetBillingAccountAccess(billingProject, parentSpan).map { hasAccess =>
        if (!hasAccess) {
          throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.Forbidden, s"Terra does not have required permissions on Billing Account: ${billingProject.billingAccount}.  Please ensure that 'terra-billing@terra.bio' is a member of your Billing Account with the 'Billing Account User' role"))
        }
      }
      result <- op(billingProject)
    } yield result
  }

  // TODO: find and assess all usages. This is written to reside inside a DB transaction, but it makes external REST calls.
  private def withNewWorkspaceContext[T](workspaceRequest: WorkspaceRequest, billingProject: RawlsBillingProject, dataAccess: DataAccess, parentSpan: Span = null)
                                     (op: (Workspace) => ReadWriteAction[T]): ReadWriteAction[T] = {

    def getBucketName(workspaceId: String, secure: Boolean) = s"${config.workspaceBucketNamePrefix}-${if(secure) "secure-" else ""}${workspaceId}"
    def getLabels(authDomain: List[ManagedGroupRef]) = authDomain match {
      case Nil => Map(WorkspaceService.SECURITY_LABEL_KEY -> WorkspaceService.LOW_SECURITY_LABEL)
      case ads => Map(WorkspaceService.SECURITY_LABEL_KEY -> WorkspaceService.HIGH_SECURITY_LABEL) ++ ads.map(ad => gcsDAO.labelSafeString(ad.membersGroupName.value, "ad-") -> "")
    }

    def saveNewWorkspace(workspaceId: String, workspaceRequest: WorkspaceRequest, bucketName: String, projectOwnerPolicyEmail: WorkbenchEmail, googleProjectId: GoogleProjectId, googleProjectNumber: Option[GoogleProjectNumber], dataAccess: DataAccess, parentSpan: Span = null): ReadWriteAction[(Workspace, Map[SamResourcePolicyName, WorkbenchEmail])] = {
      val currentDate = DateTime.now

      val workspace = Workspace(
        namespace = workspaceRequest.namespace,
        name = workspaceRequest.name,
        workspaceId = workspaceId,
        bucketName = bucketName,
        workflowCollectionName = Some(workspaceId),
        createdDate = currentDate,
        lastModified = currentDate,
        createdBy = userInfo.userEmail.value,
        attributes = workspaceRequest.attributes,
        isLocked = false,
        workspaceVersion = WorkspaceVersions.V2,
        googleProjectId = googleProjectId,
        googleProjectNumber = googleProjectNumber
      )

      traceDBIOWithParent("save", parentSpan)(_ => dataAccess.workspaceQuery.createOrUpdate(workspace)).flatMap { _ =>
        DBIO.from(for {
          resource <- {
            val projectOwnerPolicy = SamWorkspacePolicyNames.projectOwner -> SamPolicy(Set(projectOwnerPolicyEmail), Set.empty, Set(SamWorkspaceRoles.owner, SamWorkspaceRoles.projectOwner))
            val ownerPolicyMembership: Set[WorkbenchEmail] = if (workspaceRequest.noWorkspaceOwner.getOrElse(false)) {
              Set.empty
            } else {
              Set(WorkbenchEmail(userInfo.userEmail.value))
            }
            val ownerPolicy = SamWorkspacePolicyNames.owner -> SamPolicy(ownerPolicyMembership, Set.empty, Set(SamWorkspaceRoles.owner))
            val writerPolicy = SamWorkspacePolicyNames.writer -> SamPolicy(Set.empty, Set.empty, Set(SamWorkspaceRoles.writer))
            val readerPolicy = SamWorkspacePolicyNames.reader -> SamPolicy(Set.empty, Set.empty, Set(SamWorkspaceRoles.reader))
            val shareReaderPolicy = SamWorkspacePolicyNames.shareReader -> SamPolicy(Set.empty, Set.empty, Set(SamWorkspaceRoles.shareReader))
            val shareWriterPolicy = SamWorkspacePolicyNames.shareWriter -> SamPolicy(Set.empty, Set.empty, Set(SamWorkspaceRoles.shareWriter))
            val canComputePolicy = SamWorkspacePolicyNames.canCompute -> SamPolicy(Set.empty, Set.empty, Set(SamWorkspaceRoles.canCompute))
            val canCatalogPolicy = SamWorkspacePolicyNames.canCatalog -> SamPolicy(Set.empty, Set.empty, Set(SamWorkspaceRoles.canCatalog))

            val defaultPolicies = Map(projectOwnerPolicy, ownerPolicy, writerPolicy, readerPolicy, shareReaderPolicy, shareWriterPolicy, canComputePolicy, canCatalogPolicy)

            traceWithParent("createResourceFull", parentSpan)(_ => samDAO.createResourceFull(SamResourceTypeNames.workspace, workspaceId, defaultPolicies, workspaceRequest.authorizationDomain.getOrElse(Set.empty).map(_.membersGroupName.value), userInfo, None))
          }

          // policyMap has policyName -> policyEmail
          policyMap: Map[SamResourcePolicyName, WorkbenchEmail] = resource.accessPolicies.map( x => SamResourcePolicyName(x.id.accessPolicyName) -> WorkbenchEmail(x.email)).toMap

          // declare these next two Futures so they start in parallel
          createWorkflowCollectionFuture = traceWithParent("createWorkflowCollectionForWorkspace", parentSpan)( s1 => createWorkflowCollectionForWorkspace(workspaceId, policyMap, s1))

          traversePolicies = traceWithParent("traversePolicies", parentSpan)( s1 =>
            Future.traverse(policyMap) { x =>
              val policyName = x._1
              if (policyName == SamWorkspacePolicyNames.projectOwner && workspaceRequest.authorizationDomain.getOrElse(Set.empty).isEmpty) {
                // when there isn't an auth domain, we will use the billing project admin policy email directly on workspace
                // resources instead of synching an extra group. This helps to keep the number of google groups a user is in below
                // the limit of 2000
                Future.successful(())
              } else if (WorkspaceAccessLevels.withPolicyName(policyName.value).isDefined) {
                // only sync policies that have corresponding WorkspaceAccessLevels to google because only those are
                // granted bucket access (and thus need a google group)
                traceWithParent(s"syncPolicy-${policyName}", s1)( _ => samDAO.syncPolicyToGoogle(SamResourceTypeNames.workspace, workspaceId, policyName))
              } else {
                Future.successful(())
              }
            }
          )

          _ <- createWorkflowCollectionFuture

          _ <- traversePolicies

        } yield (workspace, policyMap))
      }
    }

    traceDBIOWithParent("requireCreateWorkspaceAccess", parentSpan)(s1 => requireCreateWorkspaceAccess(workspaceRequest, dataAccess, s1) {
      traceDBIOWithParent("maybeRequireBillingProjectOwnerAccess", s1) (_ => maybeRequireBillingProjectOwnerAccess(workspaceRequest) {
        traceDBIOWithParent("findByName", s1)(_ => dataAccess.workspaceQuery.findByName(workspaceRequest.toWorkspaceName, Option(WorkspaceAttributeSpecs(all = false, List.empty[AttributeName])))) flatMap {
          case Some(_) => DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.Conflict, s"Workspace ${workspaceRequest.namespace}/${workspaceRequest.name} already exists")))
          case None =>
            val workspaceId = UUID.randomUUID.toString
            val bucketName = getBucketName(workspaceId, workspaceRequest.authorizationDomain.exists(_.nonEmpty))

            // add the workspace id to the span so we can find and correlate it later with other services
            s1.putAttribute("workspaceId", OpenCensusAttributeValue.stringAttributeValue(workspaceId))

            traceDBIOWithParent("getPolicySyncStatus", s1)(_ => DBIO.from(samDAO.getPolicySyncStatus(SamResourceTypeNames.billingProject, workspaceRequest.namespace, SamBillingProjectPolicyNames.owner, userInfo).map(_.email))).flatMap { projectOwnerPolicyEmail =>
              traceDBIOWithParent("setupGoogleProject", s1)(_ => DBIO.from(setupGoogleProject(billingProject, workspaceId, s1))).flatMap { case (googleProjectId, googleProjectNumber) =>
                traceDBIOWithParent("saveNewWorkspace", s1)(s2 => saveNewWorkspace(workspaceId, workspaceRequest, bucketName, projectOwnerPolicyEmail, googleProjectId, Option(googleProjectNumber), dataAccess, s2).flatMap { case (savedWorkspace, policyMap) =>
                  for {
                    //there's potential for another perf improvement here for workspaces with auth domains. if a workspace is in an auth domain, we'll already have
                    //the projectOwnerEmail, so we don't need to get it from sam. in a pinch, we could also store the project owner email in the rawls DB since it
                    //will never change, which would eliminate the call to sam entirely
                    policyEmails <- DBIO.successful(policyMap.map { case (policyName, policyEmail) =>
                      if (policyName == SamWorkspacePolicyNames.projectOwner && workspaceRequest.authorizationDomain.getOrElse(Set.empty).isEmpty) {
                        // when there isn't an auth domain, we will use the billing project admin policy email directly on workspace
                        // resources instead of synching an extra group. This helps to keep the number of google groups a user is in below
                        // the limit of 2000
                        Option(WorkspaceAccessLevels.ProjectOwner -> projectOwnerPolicyEmail)
                      } else {
                        WorkspaceAccessLevels.withPolicyName(policyName.value).map(_ -> policyEmail)
                      }
                    }.flatten.toMap)

                    _ <- traceDBIOWithParent("gcsDAO.setupWorkspace", s2)(s3 => DBIO.from(gcsDAO.setupWorkspace(userInfo, savedWorkspace.googleProjectId, policyEmails, bucketName, getLabels(workspaceRequest.authorizationDomain.getOrElse(Set.empty).toList), s3)))
                    response <- traceDBIOWithParent("doOp", s2)(_ => op(savedWorkspace))
                  } yield response
                })
              }
            }
        }
      })
    })
  }

  private def withSubmission[T](workspaceContext: Workspace, submissionId: String, dataAccess: DataAccess)(op: (Submission) => ReadWriteAction[T]): ReadWriteAction[T] = {
    Try {
      UUID.fromString(submissionId)
    } match {
      case Failure(_) =>
        DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.NotFound, s"Submission id ${submissionId} is not a valid submission id")))
      case _ =>
        dataAccess.submissionQuery.get(workspaceContext, submissionId) flatMap {
          case None => DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.NotFound, s"Submission with id ${submissionId} not found in workspace ${workspaceContext.toWorkspaceName}")))
          case Some(submission) => op(submission)
        }
    }
  }

  // confirm that the Submission is a member of this workspace, but don't unmarshal it from the DB
  private def withSubmissionId[T](workspaceContext: Workspace, submissionId: String, dataAccess: DataAccess)(op: UUID => ReadWriteAction[T]): ReadWriteAction[T] = {
    Try {
      UUID.fromString(submissionId)
    } match {
      case Failure(_) =>
        DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.NotFound, s"Submission id ${submissionId} is not a valid submission id")))
      case Success(uuid) =>
        dataAccess.submissionQuery.confirmInWorkspace(workspaceContext.workspaceIdAsUUID, uuid) flatMap {
          case None =>
            val report = ErrorReport(StatusCodes.NotFound, s"Submission with id ${submissionId} not found in workspace ${workspaceContext.toWorkspaceName}")
            DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = report))
          case Some(_) => op(uuid)
        }
    }
  }

  private def withWorkflowRecord(workspaceName: WorkspaceName, submissionId: String, workflowId: String, dataAccess: DataAccess)(op: (WorkflowRecord) => ReadWriteAction[PerRequestMessage]): ReadWriteAction[PerRequestMessage] = {
    dataAccess.workflowQuery.findWorkflowByExternalIdAndSubmissionId(workflowId, UUID.fromString(submissionId)).result flatMap {
      case Seq() => DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.NotFound, s"WorkflowRecord with id ${workflowId} not found in submission ${submissionId} in workspace ${workspaceName}")))
      case Seq(one) => op(one)
      case tooMany => DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.InternalServerError, s"found multiple WorkflowRecords with id ${workflowId} in submission ${submissionId} in workspace ${workspaceName}")))
    }
  }

  // used as part of the workflow metadata permission check - more detail at workflowMetadata()

  // require submission to be present, but don't require the workflow to reference it
  // if the workflow does reference the submission, return its executionServiceKey

  private def withSubmissionAndWorkflowExecutionServiceKey[T](workspaceContext: Workspace, submissionId: String, workflowId: String, dataAccess: DataAccess)(op: Option[ExecutionServiceId] => ReadWriteAction[T]): ReadWriteAction[T] = {
    withSubmissionId(workspaceContext, submissionId, dataAccess) { _ =>
      dataAccess.workflowQuery.getExecutionServiceIdByExternalId(workflowId, submissionId) flatMap {
        case Some(id) => op(Option(ExecutionServiceId(id)))
        case _ => op(None)
      }
    }
  }


  /** Validates the workflow failure mode in the submission request. */
  private def getWorkflowFailureMode(submissionRequest: SubmissionRequest): Future[Option[WorkflowFailureMode]] = {
    Try(submissionRequest.workflowFailureMode.map(WorkflowFailureModes.withName)) match {
      case Success(failureMode) => Future.successful(failureMode)
      case Failure(NonFatal(e)) => Future.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.BadRequest, e.getMessage)))
    }
  }

  private def validateSubmissionRootEntity(submissionRequest: SubmissionRequest, methodConfig: MethodConfiguration): Unit = {
    if (submissionRequest.entityName.isDefined != submissionRequest.entityType.isDefined) {
      throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.BadRequest, s"You must set both entityType and entityName to run on an entity, or neither (to run with literal or workspace inputs)."))
    }
    if (methodConfig.dataReferenceName.isEmpty && methodConfig.rootEntityType.isDefined != submissionRequest.entityName.isDefined) {
      if (methodConfig.rootEntityType.isDefined) {
        throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.BadRequest, s"Your method config defines a root entity but you haven't passed one to the submission."))
      } else {
        //This isn't _strictly_ necessary, since a single submission entity will create one workflow.
        //However, passing in a submission entity + an expression doesn't make sense for two reasons:
        // 1. you'd have to write an expression from your submission entity to an entity of "no entity necessary" type
        // 2. even if you _could_ do this, you'd kick off a bunch of identical workflows.
        //More likely than not, an MC with no root entity + a submission entity = you're doing something wrong. So we'll just say no here.
        throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.BadRequest, s"Your method config uses no root entity, but you passed one to the submission."))
      }
    }
    if (methodConfig.dataReferenceName.isDefined && submissionRequest.entityName.isDefined) {
      throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.BadRequest, "Your method config defines a data reference and an entity name. Running on a submission on a single entity in a data reference is not yet supported."))
    }
  }
}

class AttributeUpdateOperationException(message: String) extends RawlsException(message)
class AttributeNotFoundException(message: String) extends AttributeUpdateOperationException(message)
