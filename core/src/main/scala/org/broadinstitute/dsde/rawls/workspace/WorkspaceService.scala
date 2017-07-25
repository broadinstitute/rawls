package org.broadinstitute.dsde.rawls.workspace

import java.util.UUID

import akka.actor._
import akka.pattern._
import akka.util.Timeout
import com.typesafe.scalalogging.LazyLogging
import nl.grons.metrics.scala.Counter
import org.broadinstitute.dsde.rawls.{RawlsException, RawlsExceptionWithErrorReport}
import slick.jdbc.TransactionIsolation
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.dataaccess.slick._
import org.broadinstitute.dsde.rawls.expressions._
import org.broadinstitute.dsde.rawls.genomics.GenomicsService
import org.broadinstitute.dsde.rawls.jobexec.MethodConfigResolver
import org.broadinstitute.dsde.rawls.jobexec.SubmissionSupervisor.SubmissionStarted
import org.broadinstitute.dsde.rawls.metrics.RawlsInstrumented
import org.broadinstitute.dsde.rawls.model.Attributable.AttributeMap
import org.broadinstitute.dsde.rawls.model.AttributeUpdateOperations._
import org.broadinstitute.dsde.rawls.model.ExecutionJsonSupport.{ActiveSubmissionFormat, ExecutionServiceValidationFormat, ExecutionServiceVersionFormat, SubmissionFormat, SubmissionListResponseFormat, SubmissionReportFormat, SubmissionStatusResponseFormat, SubmissionValidationReportFormat, WorkflowOutputsFormat, WorkflowQueueStatusByUserResponseFormat, WorkflowQueueStatusResponseFormat}
import org.broadinstitute.dsde.rawls.model.MethodRepoJsonSupport.AgoraEntityFormat
import org.broadinstitute.dsde.rawls.model.WorkflowFailureModes.WorkflowFailureMode
import org.broadinstitute.dsde.rawls.model.WorkspaceACLJsonSupport.{WorkspaceACLFormat, WorkspaceCatalogFormat, WorkspaceCatalogUpdateResponseListFormat}
import org.broadinstitute.dsde.rawls.model.WorkspaceAccessLevels._
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.monitor.BucketDeletionMonitor
import org.broadinstitute.dsde.rawls.user.UserService
import org.broadinstitute.dsde.rawls.user.UserService.OverwriteGroupMembers
import org.broadinstitute.dsde.rawls.util._
import org.broadinstitute.dsde.rawls.webservice.PerRequest
import org.broadinstitute.dsde.rawls.webservice.PerRequest._
import org.broadinstitute.dsde.rawls.workspace.WorkspaceService._
import org.joda.time.DateTime
import spray.http.{StatusCodes, Uri}
import spray.httpx.SprayJsonSupport._
import spray.json.DefaultJsonProtocol._
import spray.json._

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal

/**
 * Created by dvoet on 4/27/15.
 */

object WorkspaceService {
  sealed trait WorkspaceServiceMessage
  case class CreateWorkspace(workspace: WorkspaceRequest) extends WorkspaceServiceMessage
  case class GetWorkspace(workspaceName: WorkspaceName) extends WorkspaceServiceMessage
  case class DeleteWorkspace(workspaceName: WorkspaceName) extends WorkspaceServiceMessage
  case class UpdateWorkspace(workspaceName: WorkspaceName, operations: Seq[AttributeUpdateOperation]) extends WorkspaceServiceMessage
  case class UpdateLibraryAttributes(workspaceName: WorkspaceName, operations: Seq[AttributeUpdateOperation]) extends WorkspaceServiceMessage
  case object ListWorkspaces extends WorkspaceServiceMessage
  case object ListAllWorkspaces extends WorkspaceServiceMessage
  case class GetTags(query: Option[String]) extends WorkspaceServiceMessage
  case class AdminListWorkspacesWithAttribute(attributeName: AttributeName, attributeValue: AttributeValue) extends WorkspaceServiceMessage
  case class CloneWorkspace(sourceWorkspace: WorkspaceName, destWorkspace: WorkspaceRequest) extends WorkspaceServiceMessage
  case class GetACL(workspaceName: WorkspaceName) extends WorkspaceServiceMessage
  case class UpdateACL(workspaceName: WorkspaceName, aclUpdates: Seq[WorkspaceACLUpdate], inviteUsersNotFound: Boolean) extends WorkspaceServiceMessage
  case class SendChangeNotifications(workspaceName: WorkspaceName) extends WorkspaceServiceMessage
  case class GetCatalog(workspaceName: WorkspaceName) extends WorkspaceServiceMessage
  case class UpdateCatalog(workspaceName: WorkspaceName, catalogUpdates: Seq[WorkspaceCatalog]) extends WorkspaceServiceMessage
  case class LockWorkspace(workspaceName: WorkspaceName) extends WorkspaceServiceMessage
  case class UnlockWorkspace(workspaceName: WorkspaceName) extends WorkspaceServiceMessage
  case class CheckBucketReadAccess(workspaceName: WorkspaceName) extends WorkspaceServiceMessage
  case class GetWorkspaceStatus(workspaceName: WorkspaceName, userSubjectId: Option[String]) extends WorkspaceServiceMessage
  case class GetBucketUsage(workspaceName: WorkspaceName) extends WorkspaceServiceMessage
  case class GetAccessInstructions(workspaceName: WorkspaceName) extends WorkspaceServiceMessage

  case class CreateEntity(workspaceName: WorkspaceName, entity: Entity) extends WorkspaceServiceMessage
  case class GetEntity(workspaceName: WorkspaceName, entityType: String, entityName: String) extends WorkspaceServiceMessage
  case class UpdateEntity(workspaceName: WorkspaceName, entityType: String, entityName: String, operations: Seq[AttributeUpdateOperation]) extends WorkspaceServiceMessage
  case class DeleteEntities(workspaceName: WorkspaceName, entities: Seq[AttributeEntityReference]) extends WorkspaceServiceMessage
  case class RenameEntity(workspaceName: WorkspaceName, entityType: String, entityName: String, newName: String) extends WorkspaceServiceMessage
  case class EvaluateExpression(workspaceName: WorkspaceName, entityType: String, entityName: String, expression: String) extends WorkspaceServiceMessage
  case class GetEntityTypeMetadata(workspaceName: WorkspaceName) extends WorkspaceServiceMessage
  case class QueryEntities(workspaceName: WorkspaceName, entityType: String, query: EntityQuery) extends WorkspaceServiceMessage
  case class ListEntities(workspaceName: WorkspaceName, entityType: String) extends WorkspaceServiceMessage
  case class CopyEntities(entityCopyDefinition: EntityCopyDefinition, uri:Uri, linkExistingEntities: Boolean) extends WorkspaceServiceMessage
  case class BatchUpsertEntities(workspaceName: WorkspaceName, entityUpdates: Seq[EntityUpdateDefinition]) extends WorkspaceServiceMessage
  case class BatchUpdateEntities(workspaceName: WorkspaceName, entityUpdates: Seq[EntityUpdateDefinition]) extends WorkspaceServiceMessage

  case class CreateMethodConfiguration(workspaceName: WorkspaceName, methodConfiguration: MethodConfiguration) extends WorkspaceServiceMessage
  case class GetMethodConfiguration(workspaceName: WorkspaceName, methodConfigurationNamespace: String, methodConfigurationName: String) extends WorkspaceServiceMessage
  case class UpdateMethodConfiguration(workspaceName: WorkspaceName, methodConfigurationNamespace: String, methodConfigurationName: String, newMethodConfiguration: MethodConfiguration) extends WorkspaceServiceMessage
  case class DeleteMethodConfiguration(workspaceName: WorkspaceName, methodConfigurationNamespace: String, methodConfigurationName: String) extends WorkspaceServiceMessage
  case class RenameMethodConfiguration(workspaceName: WorkspaceName, methodConfigurationNamespace: String, methodConfigurationName: String, newName: String) extends WorkspaceServiceMessage
  case class CopyMethodConfiguration(methodConfigNamePair: MethodConfigurationNamePair) extends WorkspaceServiceMessage
  case class CopyMethodConfigurationFromMethodRepo(query: MethodRepoConfigurationImport) extends WorkspaceServiceMessage
  case class CopyMethodConfigurationToMethodRepo(query: MethodRepoConfigurationExport) extends WorkspaceServiceMessage
  case class ListMethodConfigurations(workspaceName: WorkspaceName) extends WorkspaceServiceMessage
  case class CreateMethodConfigurationTemplate( methodRepoMethod: MethodRepoMethod ) extends WorkspaceServiceMessage
  case class GetMethodInputsOutputs( methodRepoMethod: MethodRepoMethod ) extends WorkspaceServiceMessage
  case class GetAndValidateMethodConfiguration(workspaceName: WorkspaceName, methodConfigurationNamespace: String, methodConfigurationName: String) extends WorkspaceServiceMessage
  case class GetGenomicsOperation(workspaceName: WorkspaceName, jobId: String) extends WorkspaceServiceMessage

  case class ListSubmissions(workspaceName: WorkspaceName) extends WorkspaceServiceMessage
  case class CountSubmissions(workspaceName: WorkspaceName) extends WorkspaceServiceMessage
  case class CreateSubmission(workspaceName: WorkspaceName, submission: SubmissionRequest) extends WorkspaceServiceMessage
  case class ValidateSubmission(workspaceName: WorkspaceName, submission: SubmissionRequest) extends WorkspaceServiceMessage
  case class GetSubmissionStatus(workspaceName: WorkspaceName, submissionId: String) extends WorkspaceServiceMessage
  case class AbortSubmission(workspaceName: WorkspaceName, submissionId: String) extends WorkspaceServiceMessage
  case class GetWorkflowOutputs(workspaceName: WorkspaceName, submissionId: String, workflowId: String) extends WorkspaceServiceMessage
  case class GetWorkflowMetadata(workspaceName: WorkspaceName, submissionId: String, workflowId: String) extends WorkspaceServiceMessage
  case object WorkflowQueueStatus extends WorkspaceServiceMessage
  case object ExecutionEngineVersion extends WorkspaceServiceMessage

  case object AdminListAllActiveSubmissions extends WorkspaceServiceMessage
  case class AdminAbortSubmission(workspaceName: WorkspaceName, submissionId: String) extends WorkspaceServiceMessage
  case class AdminDeleteWorkspace(workspaceName: WorkspaceName) extends WorkspaceServiceMessage
  case object AdminWorkflowQueueStatusByUser extends WorkspaceServiceMessage

  case class HasAllUserReadAccess(workspaceName: WorkspaceName) extends WorkspaceServiceMessage
  case class GrantAllUserReadAccess(workspaceName: WorkspaceName) extends WorkspaceServiceMessage
  case class RevokeAllUserReadAccess(workspaceName: WorkspaceName) extends WorkspaceServiceMessage

  def props(workspaceServiceConstructor: UserInfo => WorkspaceService, userInfo: UserInfo): Props = {
    Props(workspaceServiceConstructor(userInfo))
  }

  def constructor(dataSource: SlickDataSource, methodRepoDAO: MethodRepoDAO, executionServiceCluster: ExecutionServiceCluster, execServiceBatchSize: Int, gcsDAO: GoogleServicesDAO, notificationDAO: NotificationDAO, submissionSupervisor: ActorRef, bucketDeletionMonitor: ActorRef, userServiceConstructor: UserInfo => UserService, genomicsServiceConstructor: UserInfo => GenomicsService, maxActiveWorkflowsTotal: Int, maxActiveWorkflowsPerUser: Int, workbenchMetricBaseName: String)(userInfo: UserInfo)(implicit executionContext: ExecutionContext) =
    new WorkspaceService(userInfo, dataSource, methodRepoDAO, executionServiceCluster, execServiceBatchSize, gcsDAO, notificationDAO, submissionSupervisor, bucketDeletionMonitor, userServiceConstructor, genomicsServiceConstructor, maxActiveWorkflowsTotal, maxActiveWorkflowsPerUser, workbenchMetricBaseName)
}

class WorkspaceService(protected val userInfo: UserInfo, val dataSource: SlickDataSource, val methodRepoDAO: MethodRepoDAO, executionServiceCluster: ExecutionServiceCluster, execServiceBatchSize: Int, protected val gcsDAO: GoogleServicesDAO, notificationDAO: NotificationDAO, submissionSupervisor: ActorRef, bucketDeletionMonitor: ActorRef, userServiceConstructor: UserInfo => UserService, genomicsServiceConstructor: UserInfo => GenomicsService, maxActiveWorkflowsTotal: Int, maxActiveWorkflowsPerUser: Int, override val workbenchMetricBaseName: String)(implicit protected val executionContext: ExecutionContext) extends Actor with RoleSupport with LibraryPermissionsSupport with FutureSupport with MethodWiths with UserWiths with LazyLogging with RawlsInstrumented {
  import dataSource.dataAccess.driver.api._

  implicit val timeout = Timeout(5 minutes)

  override def receive = {
    case CreateWorkspace(workspace) => pipe(createWorkspace(workspace)) to sender
    case GetWorkspace(workspaceName) => pipe(getWorkspace(workspaceName)) to sender
    case DeleteWorkspace(workspaceName) => pipe(deleteWorkspace(workspaceName)) to sender
    case UpdateWorkspace(workspaceName, operations) => pipe(updateWorkspace(workspaceName, operations)) to sender
    case UpdateLibraryAttributes(workspaceName, operations) => pipe(updateLibraryAttributes(workspaceName, operations)) to sender
    case ListWorkspaces => pipe(listWorkspaces()) to sender
    case ListAllWorkspaces => pipe(listAllWorkspaces()) to sender
    case GetTags(query) => pipe(getTags(query)) to sender
    case AdminListWorkspacesWithAttribute(attributeName, attributeValue) => asFCAdmin { listWorkspacesWithAttribute(attributeName, attributeValue) } pipeTo sender
    case CloneWorkspace(sourceWorkspace, destWorkspaceRequest) => pipe(cloneWorkspace(sourceWorkspace, destWorkspaceRequest)) to sender
    case GetACL(workspaceName) => pipe(getACL(workspaceName)) to sender
    case UpdateACL(workspaceName, aclUpdates, inviteUsersNotFound) => pipe(updateACL(workspaceName, aclUpdates, inviteUsersNotFound)) to sender
    case SendChangeNotifications(workspaceName) => pipe(sendChangeNotifications(workspaceName)) to sender
    case GetCatalog(workspaceName) => pipe(getCatalog(workspaceName)) to sender
    case UpdateCatalog(workspaceName, catalogUpdates) => pipe(updateCatalog(workspaceName, catalogUpdates)) to sender
    case LockWorkspace(workspaceName: WorkspaceName) => pipe(lockWorkspace(workspaceName)) to sender
    case UnlockWorkspace(workspaceName: WorkspaceName) => pipe(unlockWorkspace(workspaceName)) to sender
    case CheckBucketReadAccess(workspaceName: WorkspaceName) => pipe(checkBucketReadAccess(workspaceName)) to sender
    case GetWorkspaceStatus(workspaceName, userSubjectId) => pipe(getWorkspaceStatus(workspaceName, userSubjectId)) to sender
    case GetBucketUsage(workspaceName) => pipe(getBucketUsage(workspaceName)) to sender
    case GetAccessInstructions(workspaceName) => pipe(getAccessInstructions(workspaceName)) to sender

    case CreateEntity(workspaceName, entity) => pipe(createEntity(workspaceName, entity)) to sender
    case GetEntity(workspaceName, entityType, entityName) => pipe(getEntity(workspaceName, entityType, entityName)) to sender
    case UpdateEntity(workspaceName, entityType, entityName, operations) => pipe(updateEntity(workspaceName, entityType, entityName, operations)) to sender
    case DeleteEntities(workspaceName, entities) => pipe(deleteEntities(workspaceName, entities)) to sender
    case RenameEntity(workspaceName, entityType, entityName, newName) => pipe(renameEntity(workspaceName, entityType, entityName, newName)) to sender
    case EvaluateExpression(workspaceName, entityType, entityName, expression) => pipe(evaluateExpression(workspaceName, entityType, entityName, expression)) to sender
    case GetEntityTypeMetadata(workspaceName) => pipe(entityTypeMetadata(workspaceName)) to sender
    case ListEntities(workspaceName, entityType) => pipe(listEntities(workspaceName, entityType)) to sender
    case QueryEntities(workspaceName, entityType, query) => pipe(queryEntities(workspaceName, entityType, query)) to sender
    case CopyEntities(entityCopyDefinition, uri: Uri, linkExistingEntities: Boolean) => pipe(copyEntities(entityCopyDefinition, uri, linkExistingEntities)) to sender
    case BatchUpsertEntities(workspaceName, entityUpdates) => pipe(batchUpdateEntities(workspaceName, entityUpdates, true)) to sender
    case BatchUpdateEntities(workspaceName, entityUpdates) => pipe(batchUpdateEntities(workspaceName, entityUpdates, false)) to sender

    case CreateMethodConfiguration(workspaceName, methodConfiguration) => pipe(createMethodConfiguration(workspaceName, methodConfiguration)) to sender
    case RenameMethodConfiguration(workspaceName, methodConfigurationNamespace, methodConfigurationName, newName) => pipe(renameMethodConfiguration(workspaceName, methodConfigurationNamespace, methodConfigurationName, newName)) to sender
    case DeleteMethodConfiguration(workspaceName, methodConfigurationNamespace, methodConfigurationName) => pipe(deleteMethodConfiguration(workspaceName, methodConfigurationNamespace, methodConfigurationName)) to sender
    case GetMethodConfiguration(workspaceName, methodConfigurationNamespace, methodConfigurationName) => pipe(getMethodConfiguration(workspaceName, methodConfigurationNamespace, methodConfigurationName)) to sender
    case UpdateMethodConfiguration(workspaceName, methodConfigurationNamespace: String, methodConfigurationName: String, newMethodConfiguration) => pipe(updateMethodConfiguration(workspaceName, methodConfigurationNamespace, methodConfigurationName, newMethodConfiguration)) to sender
    case CopyMethodConfiguration(methodConfigNamePair) => pipe(copyMethodConfiguration(methodConfigNamePair)) to sender
    case CopyMethodConfigurationFromMethodRepo(query) => pipe(copyMethodConfigurationFromMethodRepo(query)) to sender
    case CopyMethodConfigurationToMethodRepo(query) => pipe(copyMethodConfigurationToMethodRepo(query)) to sender
    case ListMethodConfigurations(workspaceName) => pipe(listMethodConfigurations(workspaceName)) to sender
    case CreateMethodConfigurationTemplate( methodRepoMethod: MethodRepoMethod ) => pipe(createMethodConfigurationTemplate(methodRepoMethod)) to sender
    case GetMethodInputsOutputs( methodRepoMethod: MethodRepoMethod ) => pipe(getMethodInputsOutputs(methodRepoMethod)) to sender
    case GetAndValidateMethodConfiguration(workspaceName, methodConfigurationNamespace, methodConfigurationName) => pipe(getAndValidateMethodConfiguration(workspaceName, methodConfigurationNamespace, methodConfigurationName)) to sender
    case GetGenomicsOperation(workspaceName, jobId) => pipe(getGenomicsOperation(workspaceName, jobId)) to sender

    case ListSubmissions(workspaceName) => pipe(listSubmissions(workspaceName)) to sender
    case CountSubmissions(workspaceName) => pipe(countSubmissions(workspaceName)) to sender
    case CreateSubmission(workspaceName, submission) => pipe(createSubmission(workspaceName, submission)) to sender
    case ValidateSubmission(workspaceName, submission) => pipe(validateSubmission(workspaceName, submission)) to sender
    case GetSubmissionStatus(workspaceName, submissionId) => pipe(getSubmissionStatus(workspaceName, submissionId)) to sender
    case AbortSubmission(workspaceName, submissionId) => pipe(abortSubmission(workspaceName, submissionId)) to sender
    case GetWorkflowOutputs(workspaceName, submissionId, workflowId) => pipe(workflowOutputs(workspaceName, submissionId, workflowId)) to sender
    case GetWorkflowMetadata(workspaceName, submissionId, workflowId) => pipe(workflowMetadata(workspaceName, submissionId, workflowId)) to sender
    case WorkflowQueueStatus => pipe(workflowQueueStatus()) to sender
    case ExecutionEngineVersion => pipe(executionEngineVersion()) to sender

    case AdminListAllActiveSubmissions => asFCAdmin { listAllActiveSubmissions() } pipeTo sender
    case AdminAbortSubmission(workspaceName,submissionId) => pipe(adminAbortSubmission(workspaceName,submissionId)) to sender
    case AdminDeleteWorkspace(workspaceName) => pipe(adminDeleteWorkspace(workspaceName)) to sender
    case AdminWorkflowQueueStatusByUser => pipe(adminWorkflowQueueStatusByUser()) to sender

    case HasAllUserReadAccess(workspaceName) => pipe(hasAllUserReadAccess(workspaceName)) to sender
    case GrantAllUserReadAccess(workspaceName) => pipe(grantAllUserReadAccess(workspaceName)) to sender
    case RevokeAllUserReadAccess(workspaceName) => pipe(revokeAllUserReadAccess(workspaceName)) to sender
  }

  def createWorkspace(workspaceRequest: WorkspaceRequest): Future[PerRequestMessage] =
    withAttributeNamespaceCheck(workspaceRequest) {
      dataSource.inTransaction { dataAccess =>
        withNewWorkspaceContext(workspaceRequest, dataAccess) { workspaceContext =>
          DBIO.successful(RequestCompleteWithLocation((StatusCodes.Created, workspaceContext.workspace), workspaceRequest.toWorkspaceName.path))
        }
      }
    }

  def getWorkspace(workspaceName: WorkspaceName): Future[PerRequestMessage] =
    dataSource.inTransaction { dataAccess =>
      withWorkspaceContext(workspaceName, dataAccess) { workspaceContext =>
        getMaximumAccessLevel(RawlsUser(userInfo), workspaceContext, dataAccess) flatMap { accessLevel =>
          if (accessLevel < WorkspaceAccessLevels.Read)
            DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.NotFound, noSuchWorkspaceMessage(workspaceName))))
          else {
            for {
              catalog <- getUserCatalogPermissions(workspaceContext, dataAccess)
              canShare <- getUserSharePermissions(workspaceContext, accessLevel, dataAccess)
              stats <- getWorkspaceSubmissionStats(workspaceContext, dataAccess)
              owners <- getWorkspaceOwners(workspaceContext.workspace, dataAccess)
            } yield {
              RequestComplete(StatusCodes.OK, WorkspaceResponse(accessLevel, canShare, catalog, workspaceContext.workspace, stats, owners))
            }
          }
        }
      }
    }

  def getMaximumAccessLevel(user: RawlsUser, workspaceContext: SlickWorkspaceContext, dataAccess: DataAccess): ReadAction[WorkspaceAccessLevel] = {
    val accessLevels = workspaceContext.workspace.authDomainACLs.map { case (accessLevel, groupRef) =>
      dataAccess.rawlsGroupQuery.loadGroupIfMember(groupRef, user).map {
        case Some(_) => accessLevel
        case None => WorkspaceAccessLevels.NoAccess
      }
    }

    DBIO.sequence(accessLevels).map { _.fold(WorkspaceAccessLevels.NoAccess)(WorkspaceAccessLevels.max) }
  }
  
  def getWorkspaceOwners(workspace: Workspace, dataAccess: DataAccess): ReadAction[Seq[String]] = {
    dataAccess.rawlsGroupQuery.load(workspace.accessLevels(WorkspaceAccessLevels.Owner)).flatMap {
      case None => DBIO.failed(new RawlsException(s"Unable to load owners for workspace ${workspace.toWorkspaceName}"))
      case Some(ownerGroup) =>
        val usersAction = DBIO.sequence(ownerGroup.users.map(dataAccess.rawlsUserQuery.load(_).map(_.get.userEmail.value)).toSeq)
        val subGroupsAction = DBIO.sequence(ownerGroup.subGroups.map(dataAccess.rawlsGroupQuery.load(_).map(_.get.groupEmail.value)).toSeq)
        
        for {
          users <- usersAction
          subGroups <- subGroupsAction
        } yield users ++ subGroups
    }
  }

  def getWorkspaceContext(workspaceName: WorkspaceName): Future[SlickWorkspaceContext] = {
    dataSource.inTransaction { dataAccess =>
      withWorkspaceContext(workspaceName, dataAccess) { workspaceContext =>
        DBIO.successful(workspaceContext)
      }
    }
  }

  def getWorkspaceContextAndPermissions(workspaceName: WorkspaceName, accessLevel: WorkspaceAccessLevel): Future[SlickWorkspaceContext] = {
    dataSource.inTransaction { dataAccess =>
      withWorkspaceContextAndPermissions(workspaceName, accessLevel, dataAccess) { workspaceContext =>
        DBIO.successful(workspaceContext)
      }
    }
  }

  def adminDeleteWorkspace(workspaceName: WorkspaceName): Future[PerRequestMessage] = asFCAdmin {
    getWorkspaceContext(workspaceName) flatMap { ctx =>
      deleteWorkspace(workspaceName, ctx)
    }
  }

  def deleteWorkspace(workspaceName: WorkspaceName): Future[PerRequestMessage] =  {
     getWorkspaceContextAndPermissions(workspaceName, WorkspaceAccessLevels.Owner) flatMap { ctx =>
       deleteWorkspace(workspaceName, ctx)
    }
  }

  private def deleteWorkspace(workspaceName: WorkspaceName, workspaceContext: SlickWorkspaceContext): Future[PerRequestMessage] = {
    //Attempt to abort any running workflows so they don't write any more to the bucket.
    //Notice that we're kicking off Futures to do the aborts concurrently, but we never collect their results!
    //This is because there's nothing we can do if Cromwell fails, so we might as well move on and let the
    //ExecutionContext run the futures whenever
    val deletionFuture: Future[(Seq[WorkflowRecord], String, Seq[Option[RawlsGroup]])] = dataSource.inTransaction { dataAccess =>
      for {
        // Gather any active workflows with external ids
        workflowsToAbort <- dataAccess.workflowQuery.findActiveWorkflowsWithExternalIds(workspaceContext)

        //If a workflow is not done, automatically change its status to Aborted
        _ <- dataAccess.workflowQuery.findWorkflowsByWorkspace(workspaceContext).result.map { recs => recs.collect {
          case wf if !WorkflowStatuses.withName(wf.status).isDone =>
            dataAccess.workflowQuery.updateStatus(wf, WorkflowStatuses.Aborted)(workflowStatusCounter(workspaceSubmissionMetricBuilder(workspaceName, wf.submissionId)))
        }}

        //Gather the Google groups to remove, but don't remove project owners group which is used by other workspaces
        groupRefsToRemove: Set[RawlsGroupRef] = workspaceContext.workspace.accessLevels.filterKeys(_ != ProjectOwner).values.toSet ++ workspaceContext.workspace.authorizationDomain.map(_ => workspaceContext.workspace.authDomainACLs.values).getOrElse(Seq.empty)
        groupsToRemove <- DBIO.sequence(groupRefsToRemove.toSeq.map(groupRef => dataAccess.rawlsGroupQuery.load(groupRef)))

        // Delete components of the workspace
        _ <- dataAccess.workspaceQuery.deleteWorkspaceAccessReferences(workspaceContext.workspaceId)
        _ <- dataAccess.workspaceQuery.deleteWorkspaceInvites(workspaceContext.workspaceId)
        _ <- dataAccess.workspaceQuery.deleteWorkspaceSharePermissions(workspaceContext.workspaceId)
        _ <- dataAccess.workspaceQuery.deleteWorkspaceCatalogPermissions(workspaceContext.workspaceId)
        _ <- dataAccess.submissionQuery.deleteFromDb(workspaceContext.workspaceId)
        _ <- dataAccess.methodConfigurationQuery.deleteFromDb(workspaceContext.workspaceId)
        _ <- dataAccess.entityQuery.deleteFromDb(workspaceContext.workspaceId)

        // Delete groups
        _ <- DBIO.seq(groupRefsToRemove.map { group => dataAccess.rawlsGroupQuery.delete(group) }.toSeq: _*)

        // Delete the workspace
        _ <- dataAccess.workspaceQuery.delete(workspaceName)

      } yield {
        (workflowsToAbort, workspaceContext.workspace.bucketName, groupsToRemove)
      }
    }
    for {
      (workflowsToAbort, bucketName, groupsToRemove) <- deletionFuture

      // Abort running workflows
      aborts = Future.traverse(workflowsToAbort) { wf => executionServiceCluster.abort(wf, userInfo) }

      // Send message to delete bucket to BucketDeletionMonitor
      _ <- Future.successful(bucketDeletionMonitor ! BucketDeletionMonitor.DeleteBucket(workspaceContext.workspace.bucketName))

      // Remove Google Groups
      _ <- Future.traverse(groupsToRemove) {
        case Some(group) => gcsDAO.deleteGoogleGroup(group)
        case None => Future.successful(())
      }
    } yield {
      aborts.onFailure {
        case t: Throwable => logger.info(s"failure aborting workflows while deleting workspace ${workspaceName}", t)
      }
      RequestComplete(StatusCodes.Accepted, s"Your Google bucket ${bucketName} will be deleted within 24h.")
    }
  }

  def updateLibraryAttributes(workspaceName: WorkspaceName, operations: Seq[AttributeUpdateOperation]): Future[PerRequestMessage] = {
    withLibraryAttributeNamespaceCheck(operations.map(_.name)) {
      tryIsCurator(userInfo.userEmail) flatMap { isCurator =>
        getWorkspaceContext(workspaceName) flatMap { ctx =>
          dataSource.inTransaction { dataAccess =>
            getMaximumAccessLevel(RawlsUser(userInfo), ctx, dataAccess) flatMap {maxAccessLevel =>
              withLibraryPermissions(ctx, operations, dataAccess, userInfo, isCurator, maxAccessLevel) {
                updateWorkspace(operations, dataAccess)(ctx)
              }
            }
          }
        } map { ws =>
          RequestComplete(StatusCodes.OK, ws)
        }
      }
    }
  }

  def updateWorkspace(workspaceName: WorkspaceName, operations: Seq[AttributeUpdateOperation]): Future[PerRequestMessage] = {
    withAttributeNamespaceCheck(operations.map(_.name)) {
      dataSource.inTransaction { dataAccess =>
        withWorkspaceContextAndPermissions(workspaceName, WorkspaceAccessLevels.Write, dataAccess) {
          updateWorkspace(operations, dataAccess)
        }
      } map { ws =>
        RequestComplete(StatusCodes.OK, ws)
      }
    }
  }

  private def updateWorkspace(operations: Seq[AttributeUpdateOperation], dataAccess: DataAccess)(workspaceContext: SlickWorkspaceContext): ReadWriteAction[Workspace] = {
    val workspace = workspaceContext.workspace
    Try {
      val updatedWorkspace = applyOperationsToWorkspace(workspace, operations)
      dataAccess.workspaceQuery.save(updatedWorkspace)
    } match {
      case Success(result) => result
      case Failure(e: AttributeUpdateOperationException) =>
        DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.BadRequest, s"Unable to update ${workspace.name}", ErrorReport(e))))
      case Failure(regrets) => DBIO.failed(regrets)
    }
  }

  def getTags(query: Option[String]): Future[PerRequestMessage] =
    dataSource.inTransaction { dataAccess =>
      dataAccess.workspaceQuery.getTags(query).map { result =>
        RequestComplete(StatusCodes.OK, result)
      }
    }

  def listWorkspaces(): Future[PerRequestMessage] =
    dataSource.inTransaction ({ dataAccess =>

      val query = for {
        permissionsPairs <- listWorkspaces(RawlsUser(userInfo), dataAccess)
        realmsForUser <- dataAccess.workspaceQuery.getAuthorizedRealms(permissionsPairs.map(_.workspaceId), RawlsUser(userInfo))
        ownerEmails <- dataAccess.workspaceQuery.listAccessGroupMemberEmails(permissionsPairs.map(p => UUID.fromString(p.workspaceId)), WorkspaceAccessLevels.Owner)
        submissionSummaryStats <- dataAccess.workspaceQuery.listSubmissionSummaryStats(permissionsPairs.map(p => UUID.fromString(p.workspaceId)))
        workspaces <- dataAccess.workspaceQuery.listByIds(permissionsPairs.map(p => UUID.fromString(p.workspaceId)))
      } yield (permissionsPairs, realmsForUser, ownerEmails, submissionSummaryStats, workspaces)

      val results = query.map { case (permissionsPairs, realmsForUser, ownerEmails, submissionSummaryStats, workspaces) =>
        val workspacesById = workspaces.groupBy(_.workspaceId).mapValues(_.head)
        permissionsPairs.map { permissionsPair =>
          workspacesById.get(permissionsPair.workspaceId).map { workspace =>
            def trueAccessLevel = workspace.authorizationDomain match {
              case None => permissionsPair.accessLevel
              case Some(realm) =>
                if (realmsForUser.flatten.contains(realm)) permissionsPair.accessLevel
                else WorkspaceAccessLevels.NoAccess
            }
            val wsId = UUID.fromString(workspace.workspaceId)
            WorkspaceListResponse(trueAccessLevel, workspace, submissionSummaryStats(wsId), ownerEmails.getOrElse(wsId, Seq.empty))
          }
        }
      }

      results.map { responses => RequestComplete(StatusCodes.OK, responses) }
    }, TransactionIsolation.ReadCommitted)

  def listWorkspaces(user: RawlsUser, dataAccess: DataAccess): ReadAction[Seq[WorkspacePermissionsPair]] = {
    val rawPairs = for {
      groups <- dataAccess.rawlsGroupQuery.listGroupsForUser(user)
      pairs <- dataAccess.workspaceQuery.listPermissionPairsForGroups(groups)
    } yield pairs

    rawPairs.map { pairs =>
      pairs.groupBy(_.workspaceId).map { case (workspaceId, pairsInWorkspace) =>
        pairsInWorkspace.reduce((a, b) => WorkspacePermissionsPair(workspaceId, WorkspaceAccessLevels.max(a.accessLevel, b.accessLevel)))
      }.toSeq
    }
  }

  private def getWorkspaceSubmissionStats(workspaceContext: SlickWorkspaceContext, dataAccess: DataAccess): ReadAction[WorkspaceSubmissionStats] = {
    // listSubmissionSummaryStats works against a sequence of workspaces; we call it just for this one workspace
    dataAccess.workspaceQuery
      .listSubmissionSummaryStats(Seq(workspaceContext.workspaceId))
      .map {p => p.get(workspaceContext.workspaceId).get}
  }

  def cloneWorkspace(sourceWorkspaceName: WorkspaceName, destWorkspaceRequest: WorkspaceRequest): Future[PerRequestMessage] = {
    val (libraryAttributeNames, workspaceAttributeNames) = destWorkspaceRequest.attributes.keys.partition(name => name.namespace == AttributeName.libraryNamespace)
    withAttributeNamespaceCheck(workspaceAttributeNames) {
      withLibraryAttributeNamespaceCheck(libraryAttributeNames) {
        dataSource.inTransaction { dataAccess =>
          withWorkspaceContextAndPermissions(sourceWorkspaceName, WorkspaceAccessLevels.Read, dataAccess) { sourceWorkspaceContext =>
            withClonedRealm(sourceWorkspaceContext, destWorkspaceRequest) { newRealm =>

              // add to or replace current attributes, on an individual basis
              val newAttrs = sourceWorkspaceContext.workspace.attributes ++ destWorkspaceRequest.attributes

              withNewWorkspaceContext(destWorkspaceRequest.copy(authorizationDomain = newRealm, attributes = newAttrs), dataAccess) { destWorkspaceContext =>
                dataAccess.entityQuery.copyAllEntities(sourceWorkspaceContext, destWorkspaceContext) andThen
                  dataAccess.methodConfigurationQuery.listActive(sourceWorkspaceContext).flatMap { methodConfigShorts =>
                    val inserts = methodConfigShorts.map { methodConfigShort =>
                      dataAccess.methodConfigurationQuery.get(sourceWorkspaceContext, methodConfigShort.namespace, methodConfigShort.name).flatMap { methodConfig =>
                        dataAccess.methodConfigurationQuery.create(destWorkspaceContext, methodConfig.get)
                      }
                    }
                    DBIO.seq(inserts: _*)
                  } andThen {
                  DBIO.successful(RequestCompleteWithLocation((StatusCodes.Created, destWorkspaceContext.workspace), destWorkspaceRequest.toWorkspaceName.path))
                }
              }
            }
          }
        }
      }
    }
  }

  private def withClonedRealm(sourceWorkspaceContext: SlickWorkspaceContext, destWorkspaceRequest: WorkspaceRequest)(op: (Option[ManagedGroupRef]) => ReadWriteAction[PerRequestMessage]): ReadWriteAction[PerRequestMessage] = {
    // if the source has a realm, the dest must also have that realm or no realm, and the source realm is applied to the destination
    // otherwise, the caller may choose to apply a realm
    (sourceWorkspaceContext.workspace.authorizationDomain, destWorkspaceRequest.authorizationDomain) match {
      case (Some(sourceRealm), Some(destRealm)) if sourceRealm != destRealm =>
        val errorMsg = s"Source workspace ${sourceWorkspaceContext.workspace.briefName} has realm $sourceRealm; cannot change it to $destRealm when cloning"
        DBIO.failed(new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.UnprocessableEntity, errorMsg)))
      case (Some(sourceRealm), _) => op(Option(sourceRealm))
      case (None, destOpt) => op(destOpt)
    }
  }

  def getACL(workspaceName: WorkspaceName): Future[PerRequestMessage] =
    dataSource.inTransaction { dataAccess =>
      withWorkspaceContext(workspaceName, dataAccess) { workspaceContext =>
        requireSharePermission(workspaceContext.workspace, dataAccess) { _ =>
          dataAccess.workspaceQuery.listEmailsAndAccessLevel(workspaceContext).flatMap { emailsAndAccess =>
            dataAccess.workspaceQuery.getInvites(workspaceContext.workspaceId).map { invites =>
              // toMap below will drop duplicate keys, keeping the last entry only
              // sort by access level to make sure higher access levels remain in the resulting map

              // Note: we only store share permissions in the database if a user explicitly sets them. Since owners and project owners
              // have implicit sharing permissions, we rectify that with ((accessLevel >= WorkspaceAccessLevels.Owner) || hasSharePermission) so
              // the response from getACL returns canShare = true for owners and project owners
              val granted = emailsAndAccess.sortBy { case (_, accessLevel, _) => accessLevel }.map { case (email, accessLevel, hasSharePermission) => email -> AccessEntry(accessLevel, false, ((accessLevel >= WorkspaceAccessLevels.Owner) || hasSharePermission)) }
              val pending = invites.sortBy { case (_, accessLevel) => accessLevel }.map { case (email, accessLevel) => email -> AccessEntry(accessLevel, true, false) }

              RequestComplete(StatusCodes.OK, WorkspaceACL((granted ++ pending).toMap))
            }
          }
        }
      }
    }

  def getCatalog(workspaceName: WorkspaceName): Future[PerRequestMessage] =
    dataSource.inTransaction { dataAccess =>
      withWorkspaceContext(workspaceName, dataAccess) { workspaceContext =>
        dataAccess.workspaceQuery.listEmailsWithCatalogAccess(workspaceContext).map {RequestComplete(StatusCodes.OK, _)}
      }
    }

  def updateCatalog(workspaceName: WorkspaceName, input: Seq[WorkspaceCatalog]): Future[PerRequestMessage] = {
    dataSource.inTransaction { dataAccess =>
      withWorkspaceContext(workspaceName, dataAccess) { workspaceContext =>
        updateCatalogPermissions(input, dataAccess, workspaceContext).map {RequestComplete(StatusCodes.OK, _)}
      }
    }
  }

  /**
   * updates acls for a workspace
   * @param workspaceName
   * @param aclUpdates changes to make, if an entry already exists it will be changed to the level indicated in this
   *                   Seq, use NoAccess to remove an entry, all other preexisting accesses remain unchanged
   * @return
   */
  def updateACL(workspaceName: WorkspaceName, aclUpdates: Seq[WorkspaceACLUpdate], inviteUsersNotFound: Boolean): Future[PerRequestMessage] = {

    import org.broadinstitute.dsde.rawls.model.WorkspaceACLJsonSupport._

    val overwriteGroupMessagesFuture = dataSource.inTransaction { dataAccess =>
      withWorkspaceContext(workspaceName, dataAccess) { workspaceContext =>
        requireSharePermission(workspaceContext.workspace, dataAccess) { accessLevel =>
          determineCompleteNewAcls(aclUpdates, accessLevel, dataAccess, workspaceContext)
        }
      }
    }

    def getExistingWorkspaceInvites(workspaceName: WorkspaceName) = {
      dataSource.inTransaction { dataAccess =>
        withWorkspaceContext(workspaceName, dataAccess) { workspaceContext =>
          dataAccess.workspaceQuery.getInvites(workspaceContext.workspaceId).map { pairs =>
            pairs.map(pair => WorkspaceACLUpdate(pair._1, pair._2))
          }
        }
      }
    }

    def deleteWorkspaceInvites(invites: Seq[WorkspaceACLUpdate], existingInvites: Seq[WorkspaceACLUpdate], workspaceName: WorkspaceName) = {
      dataSource.inTransaction { dataAccess =>
        withWorkspaceContext(workspaceName, dataAccess) { workspaceContext =>
          val removals = invites.filter(_.accessLevel == WorkspaceAccessLevels.NoAccess)
          val dedupedRemovals = removals.filter(update => existingInvites.map(_.email).contains(update.email))
          DBIO.sequence(dedupedRemovals.map(removal => dataAccess.workspaceQuery.removeInvite(workspaceContext.workspaceId, removal.email))) map { _ =>
            dedupedRemovals
          }
        }
      }
    }

    def saveWorkspaceInvites(invites: Seq[WorkspaceACLUpdate], workspaceName: WorkspaceName): Future[Seq[WorkspaceACLUpdate]] = {
      dataSource.inTransaction { dataAccess =>
        withWorkspaceContext(workspaceName, dataAccess) { workspaceContext =>
          DBIO.sequence(invites.map(invite => dataAccess.workspaceQuery.saveInvite(workspaceContext.workspaceId, userInfo.userSubjectId.value, invite)))
        }
      }
    }

    def getUsersUpdatedResponse(actualChangesToMake: Map[Either[RawlsUserRef, RawlsGroupRef], WorkspaceAccessLevel], invitesUpdated: Seq[WorkspaceACLUpdate], emailsNotFound: Seq[WorkspaceACLUpdate], existingInvites: Seq[WorkspaceACLUpdate]): WorkspaceACLUpdateResponseList = {
      val usersUpdated = actualChangesToMake.map {
        case (Left(userRef), accessLevel) => WorkspaceACLUpdateResponse(userRef.userSubjectId.value, accessLevel)
        case (Right(groupRef), accessLevel) => WorkspaceACLUpdateResponse(groupRef.groupName.value, accessLevel)
      }.toSeq

      val usersNotFound = emailsNotFound.filterNot(aclUpdate => invitesUpdated.map(_.email).contains(aclUpdate.email)).filterNot(aclUpdate => existingInvites.map(_.email).contains(aclUpdate.email))
      val invitesSent = invitesUpdated.filterNot(aclUpdate => existingInvites.map(_.email).contains(aclUpdate.email))

      WorkspaceACLUpdateResponseList(usersUpdated, invitesSent, invitesUpdated.diff(invitesSent), usersNotFound)
    }

    def updateWorkspaceSharePermissions(actualChangesToMake: Map[Either[RawlsUserRef, RawlsGroupRef], Option[Boolean]]) = {
      val (usersToAdd, usersToRemove) = actualChangesToMake.collect{ case (Left(userRef), Some(canShare)) => userRef -> canShare }.toSeq
        .partition { case (_, canShare) => canShare }
      val (groupsToAdd, groupsToRemove) = actualChangesToMake.collect{ case (Right(groupRef), Some(canShare)) => groupRef -> canShare }.toSeq
        .partition { case (_, canShare) => canShare }

      dataSource.inTransaction { dataAccess =>
        withWorkspaceContext(workspaceName, dataAccess) { workspaceContext =>
          dataAccess.workspaceQuery.insertUserSharePermissions(workspaceContext.workspaceId, usersToAdd.map { case (userRef, _) => userRef } ) andThen
            dataAccess.workspaceQuery.deleteUserSharePermissions(workspaceContext.workspaceId, usersToRemove.map { case (userRef, _) => userRef } ) andThen
              dataAccess.workspaceQuery.insertGroupSharePermissions(workspaceContext.workspaceId, groupsToAdd.map { case (groupRef, _) => groupRef } ) andThen
                dataAccess.workspaceQuery.deleteGroupSharePermissions(workspaceContext.workspaceId, groupsToRemove.map { case (groupRef, _) => groupRef } )
        }
      }
    }

    val userServiceRef = context.actorOf(UserService.props(userServiceConstructor, userInfo))
    for {
      (overwriteGroupMessages, emailsNotFound, actualChangesToMake, actualShareChangesToMake) <- overwriteGroupMessagesFuture
      overwriteGroupResults <- Future.traverse(overwriteGroupMessages) { message => (userServiceRef ? message).asInstanceOf[Future[PerRequestMessage]] }
      existingInvites <- getExistingWorkspaceInvites(workspaceName)
      savedPermissions <- updateWorkspaceSharePermissions(actualShareChangesToMake)
      deletedInvites <- deleteWorkspaceInvites(emailsNotFound, existingInvites, workspaceName)
      workspaceContext <- getWorkspaceContext(workspaceName)
      savedInvites <- if(inviteUsersNotFound) {
        val invites = emailsNotFound diff deletedInvites

        // only send invites for those that do not already exist
        val newInviteEmails = invites.map(_.email) diff existingInvites.map((_.email))
        val inviteNotifications = newInviteEmails.map(em => Notifications.WorkspaceInvitedNotification(RawlsUserEmail(em), userInfo.userSubjectId, workspaceName, workspaceContext.workspace.bucketName))
        notificationDAO.fireAndForgetNotifications(inviteNotifications)

        saveWorkspaceInvites(invites, workspaceName)
      } else {
        // save changes to only existing invites
        val invitesToUpdate = emailsNotFound.filter(rec => existingInvites.map(_.email).contains(rec.email)) diff deletedInvites
        saveWorkspaceInvites(invitesToUpdate, workspaceName)
      }
    } yield {
      // fire and forget notifications
      val notificationMessages = actualChangesToMake collect {
        // note that we don't send messages to groups
        case (Left(userRef), NoAccess) => Notifications.WorkspaceRemovedNotification(userRef.userSubjectId, NoAccess.toString, workspaceName, userInfo.userSubjectId)
        case (Left(userRef), access) => Notifications.WorkspaceAddedNotification(userRef.userSubjectId, access.toString, workspaceName, userInfo.userSubjectId)
      }
      notificationDAO.fireAndForgetNotifications(notificationMessages)

      overwriteGroupResults.map {
        case RequestComplete(StatusCodes.NoContent) =>
          RequestComplete(StatusCodes.OK, getUsersUpdatedResponse(actualChangesToMake, (deletedInvites ++ savedInvites), (emailsNotFound diff savedInvites), existingInvites))
        case otherwise => otherwise
      }.reduce { (prior, next) =>
        // this reduce will propagate the first non-NoContent (i.e. error) response
        prior match {
          case RequestComplete(StatusCodes.NoContent) => next
          case otherwise => prior
        }
      }
    }
  }

  def sendChangeNotifications(workspaceName: WorkspaceName): Future[PerRequestMessage] = {

    val getUsers = {
      dataSource.inTransaction{ dataAccess =>
        withWorkspaceContext(workspaceName, dataAccess) {workspaceContext =>
          requireAccessIgnoreLock(workspaceContext.workspace, WorkspaceAccessLevels.Write, dataAccess) {
            DBIO.sequence(workspaceContext.workspace.accessLevels.values.map {group =>
              dataAccess.rawlsGroupQuery.flattenGroupMembership(group)
            })
          }
        }
      }
    }
    getUsers.map { groups =>
      val userIds = groups.flatten.map(user => user.userSubjectId).toSet
      val notificationMessages = userIds.map { userId => Notifications.WorkspaceChangedNotification(userId, workspaceName) }
      val numMessages = notificationMessages.size.toString
      notificationDAO.fireAndForgetNotifications(notificationMessages)
      RequestComplete(StatusCodes.OK, numMessages)
    }

  }

  private def updateCatalogPermissions(catalogUpdates: Seq[WorkspaceCatalog], dataAccess: DataAccess, workspaceContext: SlickWorkspaceContext): ReadWriteAction[WorkspaceCatalogUpdateResponseList] = {

    // map from email to ref
    dataAccess.rawlsGroupQuery.loadRefsFromEmails(catalogUpdates.map(_.email)) flatMap { refsToUpdateByEmail =>
      val (emailsFound, emailsNotFound) = catalogUpdates.partition(catalogUpdate => refsToUpdateByEmail.keySet.contains(catalogUpdate.email))
      val emailsFoundWithRefs:Seq[(WorkspaceCatalog,Either[RawlsUserRef, RawlsGroupRef])] = emailsFound map {wc =>
        (wc, refsToUpdateByEmail(wc.email))
      }
      val (emailsToAddCatalog, emailsToRemoveCatalog) = emailsFoundWithRefs.partition(update => update._1.catalog)

      dataAccess.workspaceQuery.findWorkspaceUsersAndGroupsWithCatalog(workspaceContext.workspaceId) flatMap { case ((usersWithCatalog, groupsWithCatalog)) =>
        val usersToAdd = emailsToAddCatalog.collect {
          case (cat, Left(userref)) if !usersWithCatalog.contains(userref) => (userref, true)
        }
        val usersToRemove = emailsToRemoveCatalog.collect {
          case (cat, Left(userref)) if usersWithCatalog.contains(userref) => (userref, false)
        }
        val groupsToAdd = emailsToAddCatalog.collect {
          case (cat, Right(groupref)) if !groupsWithCatalog.contains(groupref) => (groupref, true)
        }
        val groupsToRemove = emailsToRemoveCatalog.collect {
          case (cat, Right(groupref)) if groupsWithCatalog.contains(groupref) => (groupref, false)
        }

        val emails = emailsNotFound.map { wsCatalog => wsCatalog.email }
        val users = (usersToAdd ++ usersToRemove).map { case (userRef, catalog) => WorkspaceCatalogResponse(userRef.userSubjectId.value, catalog) }
        val groups = (groupsToAdd ++ groupsToRemove).map { case (groupRef, catalog) => WorkspaceCatalogResponse(groupRef.groupName.value, catalog) }

        dataAccess.workspaceQuery.insertUserCatalogPermissions(workspaceContext.workspaceId, usersToAdd.map { case (userRef, _) => userRef }) andThen
          dataAccess.workspaceQuery.deleteUserCatalogPermissions(workspaceContext.workspaceId, usersToRemove.map { case (userRef, _) => userRef }) andThen
          dataAccess.workspaceQuery.insertGroupCatalogPermissions(workspaceContext.workspaceId, groupsToAdd.map { case (groupRef, _) => groupRef }) andThen
          dataAccess.workspaceQuery.deleteGroupCatalogPermissions(workspaceContext.workspaceId, groupsToRemove.map { case (groupRef, _) => groupRef }) map { _ =>
          WorkspaceCatalogUpdateResponseList(users ++ groups, emails)
        }
      }
    }
  }

  /**
   * Determine what the access groups for a workspace should look like after the requested updates are applied
   *
   * @param aclUpdates requested updates
   * @param dataAccess
   * @param workspaceContext
   * @return tuple: messages to send to UserService to overwrite acl groups, email that were not found in the process
   */
  private def determineCompleteNewAcls(aclUpdates: Seq[WorkspaceACLUpdate], userAccessLevel: WorkspaceAccessLevel, dataAccess: DataAccess, workspaceContext: SlickWorkspaceContext): ReadAction[(Iterable[OverwriteGroupMembers], Seq[WorkspaceACLUpdate], Map[Either[RawlsUserRef,RawlsGroupRef], WorkspaceAccessLevels.WorkspaceAccessLevel], Map[Either[RawlsUserRef,RawlsGroupRef], Option[Boolean]])] = {
    for {
      refsToUpdateByEmail <- dataAccess.rawlsGroupQuery.loadRefsFromEmails(aclUpdates.map(_.email))
      existingRefsAndLevels <- dataAccess.workspaceQuery.findWorkspaceUsersAndAccessLevel(workspaceContext.workspaceId)
    } yield {
      val emailsNotFound = aclUpdates.filterNot(aclChange => refsToUpdateByEmail.keySet.contains(aclChange.email))

      // match up elements of aclUpdates and refsToUpdateByEmail ignoring unfound emails
      val refsToUpdate = aclUpdates.map { aclUpdate => (refsToUpdateByEmail.get(aclUpdate.email), aclUpdate.accessLevel, aclUpdate.canShare) }.collect {
        case (Some(ref), WorkspaceAccessLevels.NoAccess, _) => ref -> (WorkspaceAccessLevels.NoAccess, Option(false))
        case (Some(ref), accessLevel, canShare) => ref -> (accessLevel, canShare)
      }.toSet

      val refsToUpdateAndSharePermission = refsToUpdate.map { case (ref, (_, canShare)) => ref -> canShare }
      val refsToUpdateAndAccessLevel = refsToUpdate.map { case (ref, (accessLevel, _)) => ref -> accessLevel }

      val existingRefsAndSharePermission = existingRefsAndLevels.map { case (ref, (_, canShare))  => ref -> canShare }
      val existingRefsAndAccessLevel = existingRefsAndLevels.map { case (ref, (accessLevel, _)) => ref -> accessLevel }

      // remove everything that is not changing
      val actualAccessChangesToMake = refsToUpdateAndAccessLevel.diff(existingRefsAndAccessLevel)
      val actualShareChangesToMake = refsToUpdateAndSharePermission.filter{ case (_, canShare) => canShare.isDefined }
        .diff(existingRefsAndSharePermission.map { case (ref, canShare) => ref -> Option(canShare)})

      val membersWithTooManyEntries = actualAccessChangesToMake.groupBy {
        case (member, _) => member
      }.collect {
        case (member, entries) if entries.size > 1 => refsToUpdateByEmail.collect {
          case (email, member2) if member == member2 => email
        }
      }.flatten

      if (membersWithTooManyEntries.nonEmpty) {
        throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.BadRequest, s"Only 1 entry per email allowed. Emails with more than one entry: $membersWithTooManyEntries"))
      }

      val membersWithHigherExistingAccessLevelThanGranter = existingRefsAndLevels.filterNot { case (_, (accessLevel, _)) => accessLevel == WorkspaceAccessLevels.ProjectOwner }
        .filter { case (member, _) => actualAccessChangesToMake.map { case(ref, _) => ref }.contains(member) }.filter { case (_, (accessLevel, _)) => accessLevel > userAccessLevel }

      if (membersWithHigherExistingAccessLevelThanGranter.nonEmpty) {
        throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.BadRequest, s"You may not alter the access level of users with higher access than yourself. Please correct these entries: $membersWithHigherExistingAccessLevelThanGranter"))
      }

      val membersWithHigherAccessLevelThanGranter = actualAccessChangesToMake.filter { case (_, accessLevel) => accessLevel > userAccessLevel }

      if (membersWithHigherAccessLevelThanGranter.nonEmpty) {
        throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.BadRequest, s"You may not grant higher access than your own access level. Please correct these entries: $membersWithHigherAccessLevelThanGranter"))
      }

      val membersWithSharePermission = actualShareChangesToMake.filter { case (_, canShare) => canShare.isDefined }

      if(membersWithSharePermission.nonEmpty && userAccessLevel < WorkspaceAccessLevels.Owner) {
        throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.BadRequest, s"You may not alter the share permissions of users unless you are a workspace owner. Please correct these entries: $membersWithHigherAccessLevelThanGranter"))
      }

      val actualChangesToMakeByMember = actualAccessChangesToMake.toMap
      val actualShareChangesToMakeByMember = actualShareChangesToMake.toMap

      // some security checks
      if (actualChangesToMakeByMember.contains(Right(UserService.allUsersGroupRef))) {
        // UserService.allUsersGroupRef cannot be updated in this code path, there is an admin end point for that
        throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.BadRequest, s"Please contact an administrator to alter access to ${UserService.allUsersGroupRef.groupName}"))
      }
      if (actualChangesToMakeByMember.contains(Left(RawlsUser(userInfo)))) {
        throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.BadRequest, "You may not change your own permissions"))
      }
      if (actualChangesToMakeByMember.exists { case (_, level) => level == ProjectOwner }) {
        throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.BadRequest, "Project owners can only be added in the billing area of the application."))
      }
      val existingProjectOwners = existingRefsAndLevels.collect { case (member, (ProjectOwner, _)) => member }
      if ((actualChangesToMakeByMember.keySet intersect existingProjectOwners).nonEmpty) {
        throw new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.BadRequest, "Project owners can only be removed in the billing area of the application."))
      }

      // we are not updating ProjectOwner acls here, if any are added we threw an exception above
      // any user/group that is already a project owner and is added to another level should end up in both levels
      // so lets ignore all the existing project owners, we should not update by mistake
      val existingRefsAndLevelsExcludingPO = existingRefsAndAccessLevel.filterNot { case (_, level) => level == ProjectOwner }

      // update levels for all existing refs, add refs that don't exist, remove all no access levels
      val updatedRefsAndLevels =
        (existingRefsAndLevelsExcludingPO.map { case (ref, level) => ref -> actualChangesToMakeByMember.getOrElse(ref, level) } ++
          actualChangesToMakeByMember).filterNot { case (_, level) => level == WorkspaceAccessLevels.NoAccess }

      // formulate the UserService.OverwriteGroupMembers messages to send - 1 per level with users and groups separated
      val updatedRefsByLevel: Map[WorkspaceAccessLevel, Set[(Either[RawlsUserRef, RawlsGroupRef], WorkspaceAccessLevel)]] =
        updatedRefsAndLevels.toSet.groupBy { case (_, level) => level }

      // the above transformations drop empty groups so add those back in
      val emptyLevels = Seq(WorkspaceAccessLevels.Owner, WorkspaceAccessLevels.Write, WorkspaceAccessLevels.Read).map(_ -> Set.empty[(Either[RawlsUserRef, RawlsGroupRef], WorkspaceAccessLevel)]).toMap
      val updatedRefsByLevelWithEmpties = emptyLevels ++ updatedRefsByLevel

      val overwriteGroupMessages = updatedRefsByLevelWithEmpties.map { case (level, refs) =>
        val userSubjectIds = refs.collect { case (Left(userRef), _) => userRef.userSubjectId.value }.toSeq
        val subGroupNames = refs.collect { case (Right(groupRef), _) => groupRef.groupName.value }.toSeq
        UserService.OverwriteGroupMembers(workspaceContext.workspace.accessLevels(level), RawlsGroupMemberList(userSubjectIds = Option(userSubjectIds), subGroupNames = Option(subGroupNames)))
      }

      // voila
      (overwriteGroupMessages, emailsNotFound, actualChangesToMakeByMember, actualShareChangesToMakeByMember)
    }
  }

  def lockWorkspace(workspaceName: WorkspaceName): Future[PerRequestMessage] =
    dataSource.inTransaction { dataAccess =>
      withWorkspaceContext(workspaceName, dataAccess) { workspaceContext =>
        requireAccessIgnoreLock(workspaceContext.workspace, WorkspaceAccessLevels.Owner, dataAccess) {
          dataAccess.submissionQuery.list(workspaceContext).flatMap { submissions =>
            if (!submissions.forall(_.status.isTerminated)) {
              DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.Conflict, s"There are running submissions in workspace $workspaceName, so it cannot be locked.")))
            } else {
              dataAccess.workspaceQuery.lock(workspaceContext.workspace.toWorkspaceName).map(_ => RequestComplete(StatusCodes.NoContent))
            }
          }
        }
      }
    }

  def unlockWorkspace(workspaceName: WorkspaceName): Future[PerRequestMessage] =
    dataSource.inTransaction { dataAccess =>
      withWorkspaceContext(workspaceName, dataAccess) { workspaceContext =>
        requireAccessIgnoreLock(workspaceContext.workspace, WorkspaceAccessLevels.Owner, dataAccess) {
          dataAccess.workspaceQuery.unlock(workspaceContext.workspace.toWorkspaceName).map(_ => RequestComplete(StatusCodes.NoContent))
        }
      }
    }

  def copyEntities(entityCopyDef: EntityCopyDefinition, uri: Uri, linkExistingEntities: Boolean): Future[PerRequestMessage] =
    dataSource.inTransaction { dataAccess =>
      withWorkspaceContextAndPermissions(entityCopyDef.destinationWorkspace, WorkspaceAccessLevels.Write, dataAccess) { destWorkspaceContext =>
        withWorkspaceContextAndPermissions(entityCopyDef.sourceWorkspace, WorkspaceAccessLevels.Read, dataAccess) { sourceWorkspaceContext =>
          realmCheck(sourceWorkspaceContext, destWorkspaceContext) flatMap { _ =>
            val entityNames = entityCopyDef.entityNames
            val entityType = entityCopyDef.entityType
            val copyResults = dataAccess.entityQuery.checkAndCopyEntities(sourceWorkspaceContext, destWorkspaceContext, entityType, entityNames, linkExistingEntities)
            copyResults.map { response =>
              if(response.hardConflicts.isEmpty && (response.softConflicts.isEmpty || linkExistingEntities)) RequestComplete(StatusCodes.Created, response)
              else RequestComplete(StatusCodes.Conflict, response)
            }
          }
        }
      }
    }

  // can't use withClonedRealm because the Realm -> no Realm logic is different
  private def realmCheck(sourceWorkspaceContext: SlickWorkspaceContext, destWorkspaceContext: SlickWorkspaceContext): ReadWriteAction[Boolean] = {
    // if the source has a realm, the dest must also have that realm
    (sourceWorkspaceContext.workspace.authorizationDomain, destWorkspaceContext.workspace.authorizationDomain) match {
      case (Some(sourceRealm), Some(destRealm)) if sourceRealm != destRealm =>
        val errorMsg = s"Source workspace ${sourceWorkspaceContext.workspace.briefName} has realm $sourceRealm; cannot copy entities to realm $destRealm"
        DBIO.failed(new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.UnprocessableEntity, errorMsg)))
      case (Some(sourceRealm), None) =>
        val errorMsg = s"Source workspace ${sourceWorkspaceContext.workspace.briefName} has realm $sourceRealm; cannot copy entities outside of a realm"
        DBIO.failed(new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.UnprocessableEntity, errorMsg)))
      case _ => DBIO.successful(true)
    }
  }

  def createEntity(workspaceName: WorkspaceName, entity: Entity): Future[PerRequestMessage] =
    withAttributeNamespaceCheck(entity) {
      dataSource.inTransaction { dataAccess =>
        withWorkspaceContextAndPermissions(workspaceName, WorkspaceAccessLevels.Write, dataAccess) { workspaceContext =>
          dataAccess.entityQuery.get(workspaceContext, entity.entityType, entity.name) flatMap {
            case Some(_) => DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.Conflict, s"${entity.entityType} ${entity.name} already exists in ${workspaceName}")))
            case None => dataAccess.entityQuery.save(workspaceContext, entity).map(e => RequestCompleteWithLocation((StatusCodes.Created, e), entity.path(workspaceName)))
          }
        }
      }
    }

  def batchUpdateEntities(workspaceName: WorkspaceName, entityUpdates: Seq[EntityUpdateDefinition], upsert: Boolean = false): Future[PerRequestMessage] = {
    val namesToCheck = for {
      update <- entityUpdates
      operation <- update.operations
    } yield operation.name

    withAttributeNamespaceCheck(namesToCheck) {
      dataSource.inTransaction { dataAccess =>
        withWorkspaceContextAndPermissions(workspaceName, WorkspaceAccessLevels.Write, dataAccess) { workspaceContext =>
          val updateTrialsAction = dataAccess.entityQuery.getActiveEntities(workspaceContext, entityUpdates.map(eu => AttributeEntityReference(eu.entityType, eu.name))) map { entities =>
            val entitiesByName = entities.map(e => (e.entityType, e.name) -> e).toMap
            entityUpdates.map { entityUpdate =>
              entityUpdate -> (entitiesByName.get((entityUpdate.entityType, entityUpdate.name)) match {
                case Some(e) =>
                  Try(applyOperationsToEntity(e, entityUpdate.operations))
                case None =>
                  if (upsert) {
                    Try(applyOperationsToEntity(Entity(entityUpdate.name, entityUpdate.entityType, Map.empty), entityUpdate.operations))
                  } else {
                    Failure(new RuntimeException("Entity does not exist"))
                  }
              })
            }
          }

          val saveAction = updateTrialsAction flatMap { updateTrials =>
            val errorReports = updateTrials.collect { case (entityUpdate, Failure(regrets)) =>
              ErrorReport(s"Could not update ${entityUpdate.entityType} ${entityUpdate.name}", ErrorReport(regrets))
            }
            if (!errorReports.isEmpty) {
              DBIO.failed(new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.BadRequest, "Some entities could not be updated.", errorReports)))
            } else {
              val t = updateTrials.collect { case (entityUpdate, Success(entity)) => entity }

              dataAccess.entityQuery.save(workspaceContext, t)
            }
          }

          saveAction.map(_ => RequestComplete(StatusCodes.NoContent))
        }
      }
    }
  }

  def entityTypeMetadata(workspaceName: WorkspaceName): Future[PerRequestMessage] =
    dataSource.inTransaction { dataAccess =>
      withWorkspaceContextAndPermissions(workspaceName, WorkspaceAccessLevels.Read, dataAccess) { workspaceContext =>
        dataAccess.entityQuery.getEntityTypeMetadata(workspaceContext).map(r => RequestComplete(StatusCodes.OK, r))
      }
    }

  def listEntities(workspaceName: WorkspaceName, entityType: String): Future[PerRequestMessage] =
    dataSource.inTransaction { dataAccess =>
      withWorkspaceContextAndPermissions(workspaceName, WorkspaceAccessLevels.Read, dataAccess) { workspaceContext =>
        dataAccess.entityQuery.listActiveEntitiesOfType(workspaceContext, entityType).map(r => RequestComplete(StatusCodes.OK, r.toSeq))
      }
    }

  def queryEntities(workspaceName: WorkspaceName, entityType: String, query: EntityQuery): Future[PerRequestMessage] = {
    dataSource.inTransaction { dataAccess =>
      withWorkspaceContextAndPermissions(workspaceName, WorkspaceAccessLevels.Read, dataAccess) { workspaceContext =>
        dataAccess.entityQuery.loadEntityPage(workspaceContext, entityType, query) map { case (unfilteredCount, filteredCount, entities) =>
          createEntityQueryResponse(query, unfilteredCount, filteredCount, entities.toSeq).get
        }
      }
    }
  }

  def createEntityQueryResponse(query: EntityQuery, unfilteredCount: Int, filteredCount: Int, page: Seq[Entity]): Try[RequestComplete[(StatusCodes.Success, EntityQueryResponse)]] = {
    val pageCount = Math.ceil(filteredCount.toFloat / query.pageSize).toInt
    if (filteredCount > 0 && query.page > pageCount) {
      Failure(new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.BadRequest, s"requested page ${query.page} is greater than the number of pages $pageCount")))

    } else {
      val response = EntityQueryResponse(query, EntityQueryResultMetadata(unfilteredCount, filteredCount, pageCount), page)

      Success(RequestComplete(StatusCodes.OK, response))
    }
  }

  def getEntity(workspaceName: WorkspaceName, entityType: String, entityName: String): Future[PerRequestMessage] =
    dataSource.inTransaction { dataAccess =>
      withWorkspaceContextAndPermissions(workspaceName, WorkspaceAccessLevels.Read, dataAccess) { workspaceContext =>
        withEntity(workspaceContext, entityType, entityName, dataAccess) { entity =>
          DBIO.successful(PerRequest.RequestComplete(StatusCodes.OK, entity))
        }
      }
    }

  def updateEntity(workspaceName: WorkspaceName, entityType: String, entityName: String, operations: Seq[AttributeUpdateOperation]): Future[PerRequestMessage] =
    withAttributeNamespaceCheck(operations.map(_.name)) {
      dataSource.inTransaction { dataAccess =>
        withWorkspaceContextAndPermissions(workspaceName, WorkspaceAccessLevels.Write, dataAccess) { workspaceContext =>
          withEntity(workspaceContext, entityType, entityName, dataAccess) { entity =>
            val updateAction = Try {
              val updatedEntity = applyOperationsToEntity(entity, operations)
              dataAccess.entityQuery.save(workspaceContext, updatedEntity)
            } match {
              case Success(result) => result
              case Failure(e: AttributeUpdateOperationException) =>
                DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.BadRequest, s"Unable to update entity ${entityType}/${entityName} in ${workspaceName}", ErrorReport(e))))
              case Failure(regrets) => DBIO.failed(regrets)
            }
            updateAction.map(RequestComplete(StatusCodes.OK, _))
          }
        }
      }
    }

  def deleteEntities(workspaceName: WorkspaceName, entRefs: Seq[AttributeEntityReference]): Future[PerRequestMessage] =
    dataSource.inTransaction { dataAccess =>
      withWorkspaceContextAndPermissions(workspaceName, WorkspaceAccessLevels.Write, dataAccess) { workspaceContext =>
        withAllEntities(workspaceContext, dataAccess, entRefs) { entities =>
          dataAccess.entityQuery.getAllReferringEntities(workspaceContext, entRefs.toSet) flatMap { referringEntities =>
            if (referringEntities != entRefs.toSet)
              DBIO.successful(RequestComplete(StatusCodes.Conflict, referringEntities))
            else {
              dataAccess.entityQuery.hide(workspaceContext, entRefs).map(_ => RequestComplete(StatusCodes.NoContent))
            }
          }
        }
      }
    }

  def renameEntity(workspaceName: WorkspaceName, entityType: String, entityName: String, newName: String): Future[PerRequestMessage] =
    dataSource.inTransaction { dataAccess =>
      withWorkspaceContextAndPermissions(workspaceName, WorkspaceAccessLevels.Write, dataAccess) { workspaceContext =>
        withEntity(workspaceContext, entityType, entityName, dataAccess) { entity =>
          dataAccess.entityQuery.get(workspaceContext, entity.entityType, newName) flatMap {
            case None => dataAccess.entityQuery.rename(workspaceContext, entity.entityType, entity.name, newName)
            case Some(_) => throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.Conflict, s"Destination ${entity.entityType} ${newName} already exists"))
          } map(_ => RequestComplete(StatusCodes.NoContent))
        }
      }
    }

  def evaluateExpression(workspaceName: WorkspaceName, entityType: String, entityName: String, expression: String): Future[PerRequestMessage] =
    dataSource.inTransaction { dataAccess =>
      withWorkspaceContextAndPermissions(workspaceName, WorkspaceAccessLevels.Read, dataAccess) { workspaceContext =>
        withSingleEntityRec(entityType, entityName, workspaceContext, dataAccess) { entities =>
          ExpressionEvaluator.withNewExpressionEvaluator(dataAccess, entities) { evaluator =>
            evaluator.evalFinalAttribute(workspaceContext, expression).asTry map { tryValuesByEntity => tryValuesByEntity match {
              //parsing failure
              case Failure(regret) => throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.BadRequest, regret))
              case Success(valuesByEntity) =>
                if (valuesByEntity.size != 1) {
                  //wrong number of entities?!
                  throw new RawlsException(s"Expression parsing should have returned a single entity for ${entityType}/$entityName $expression, but returned ${valuesByEntity.size} entities instead")
                } else {
                  assert(valuesByEntity.head._1 == entityName)
                  valuesByEntity.head match {
                    case (_, Success(result)) => RequestComplete(StatusCodes.OK, result.toSeq)
                    case (_, Failure(regret)) =>
                      throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.BadRequest, "Unable to evaluate expression '${expression}' on ${entityType}/${entityName} in ${workspaceName}", ErrorReport(regret)))
                  }
                }
              }
            }
          }
        }
      }
    }

  /**
   * Applies the sequence of operations in order to the entity.
   *
   * @param entity to update
   * @param operations sequence of operations
   * @throws AttributeNotFoundException when removing from a list attribute that does not exist
   * @throws AttributeUpdateOperationException when adding or removing from an attribute that is not a list
   * @return the updated entity
   */
  def applyOperationsToEntity(entity: Entity, operations: Seq[AttributeUpdateOperation]): Entity = {
    entity.copy(attributes = applyAttributeUpdateOperations(entity, operations))
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

  private def applyAttributeUpdateOperations(attributable: Attributable, operations: Seq[AttributeUpdateOperation]): AttributeMap = {
    operations.foldLeft(attributable.attributes) { (startingAttributes, operation) =>

      operation match {
        case AddUpdateAttribute(attributeName, attribute) => startingAttributes + (attributeName -> attribute)

        case RemoveAttribute(attributeName) => startingAttributes - attributeName

        case CreateAttributeEntityReferenceList(attributeName) =>
          if( startingAttributes.contains(attributeName) ) { //non-destructive
            startingAttributes
          } else {
            startingAttributes + (attributeName -> AttributeEntityReferenceEmptyList)
          }

        case CreateAttributeValueList(attributeName) =>
          if( startingAttributes.contains(attributeName) ) { //non-destructive
            startingAttributes
          } else {
            startingAttributes + (attributeName -> AttributeValueEmptyList)
          }

        case AddListMember(attributeListName, newMember) =>
          startingAttributes.get(attributeListName) match {
            case Some(AttributeValueEmptyList) =>
              newMember match {
                case AttributeNull =>
                  startingAttributes
                case newMember: AttributeValue =>
                  startingAttributes + (attributeListName -> AttributeValueList(Seq(newMember)))
                case newMember: AttributeEntityReference =>
                  throw new AttributeUpdateOperationException("Cannot add non-value to list of values.")
                case _ => throw new AttributeUpdateOperationException("Cannot create list with that type.")
              }

            case Some(AttributeEntityReferenceEmptyList) =>
              newMember match {
                case AttributeNull =>
                  startingAttributes
                case newMember: AttributeEntityReference =>
                  startingAttributes + (attributeListName -> AttributeEntityReferenceList(Seq(newMember)))
                case newMember: AttributeValue =>
                  throw new AttributeUpdateOperationException("Cannot add non-reference to list of references.")
                case _ => throw new AttributeUpdateOperationException("Cannot create list with that type.")
              }

            case Some(l: AttributeValueList) =>
              newMember match {
                case AttributeNull =>
                  startingAttributes
                case newMember: AttributeValue =>
                  startingAttributes + (attributeListName -> AttributeValueList(l.list :+ newMember))
                case _ => throw new AttributeUpdateOperationException("Cannot add non-value to list of values.")
              }

            case Some(l: AttributeEntityReferenceList) =>
              newMember match {
                case AttributeNull =>
                  startingAttributes
                case newMember: AttributeEntityReference =>
                  startingAttributes + (attributeListName -> AttributeEntityReferenceList(l.list :+ newMember))
                case _ => throw new AttributeUpdateOperationException("Cannot add non-reference to list of references.")
              }

            case None =>
              newMember match {
                case AttributeNull =>
                  throw new AttributeUpdateOperationException("Cannot use AttributeNull to create empty list. Use CreateEmpty[Ref|Val]List instead.")
                case newMember: AttributeValue =>
                  startingAttributes + (attributeListName -> AttributeValueList(Seq(newMember)))
                case newMember: AttributeEntityReference =>
                  startingAttributes + (attributeListName -> AttributeEntityReferenceList(Seq(newMember)))
                case _ => throw new AttributeUpdateOperationException("Cannot create list with that type.")
              }

            case Some(_) => throw new AttributeUpdateOperationException(s"$attributeListName of ${attributable.briefName} is not a list")
          }

        case RemoveListMember(attributeListName, removeMember) =>
          startingAttributes.get(attributeListName) match {
            case Some(l: AttributeValueList) =>
              startingAttributes + (attributeListName -> AttributeValueList(l.list.filterNot(_ == removeMember)))
            case Some(l: AttributeEntityReferenceList) =>
              startingAttributes + (attributeListName -> AttributeEntityReferenceList(l.list.filterNot(_ == removeMember)))
            case None => throw new AttributeNotFoundException(s"$attributeListName of ${attributable.briefName} does not exist")
            case Some(_) => throw new AttributeUpdateOperationException(s"$attributeListName of ${attributable.briefName} is not a list")
          }
      }
    }
  }

  def createAndValidateMCExpressions(workspaceContext: SlickWorkspaceContext, methodConfiguration: MethodConfiguration, dataAccess: DataAccess): ReadWriteAction[ValidatedMethodConfiguration] = {
    dataAccess.methodConfigurationQuery.create(workspaceContext, methodConfiguration) map { _ =>
      validateMCExpressions(methodConfiguration, dataAccess)
    }
  }

  def updateAndValidateMCExpressions(workspaceContext: SlickWorkspaceContext, methodConfigurationNamespace: String, methodConfigurationName: String, methodConfiguration: MethodConfiguration, dataAccess: DataAccess): ReadWriteAction[ValidatedMethodConfiguration] = {
    dataAccess.methodConfigurationQuery.update(workspaceContext, methodConfigurationNamespace, methodConfigurationName, methodConfiguration) map { _ =>
      validateMCExpressions(methodConfiguration, dataAccess)
    }
  }

  def validateMCExpressions(methodConfiguration: MethodConfiguration, dataAccess: DataAccess): ValidatedMethodConfiguration = {
    val parser = dataAccess  // this makes it compile for some reason (as opposed to inlining parser)
    def parseAndPartition(m: Map[String, AttributeString], parseFunc:String => Try[Boolean] ) = {
      val parsed = m map { case (key, attr) => (key, parseFunc(attr.value)) }
      ( parsed collect { case (key, Success(_)) => key } toSeq,
        parsed collect { case (key, Failure(regret)) => (key, regret.getMessage) } )
    }
    val (successInputs, failedInputs)   = parseAndPartition(methodConfiguration.inputs, ExpressionEvaluator.validateAttributeExpr(parser) )
    val (successOutputs, failedOutputs) = parseAndPartition(methodConfiguration.outputs, ExpressionEvaluator.validateOutputExpr(parser) )

    ValidatedMethodConfiguration(methodConfiguration, successInputs, failedInputs, successOutputs, failedOutputs)
  }

  def getAndValidateMethodConfiguration(workspaceName: WorkspaceName, methodConfigurationNamespace: String, methodConfigurationName: String): Future[PerRequestMessage] = {
    dataSource.inTransaction { dataAccess =>
      withWorkspaceContextAndPermissions(workspaceName, WorkspaceAccessLevels.Read, dataAccess) { workspaceContext =>
        withMethodConfig(workspaceContext, methodConfigurationNamespace, methodConfigurationName, dataAccess) { methodConfig =>
          DBIO.successful(PerRequest.RequestComplete(StatusCodes.OK, validateMCExpressions(methodConfig, dataAccess)))
        }
      }
    }
  }

  def createMethodConfiguration(workspaceName: WorkspaceName, methodConfiguration: MethodConfiguration): Future[PerRequestMessage] = {
    withAttributeNamespaceCheck(methodConfiguration) {
      dataSource.inTransaction { dataAccess =>
        withWorkspaceContextAndPermissions(workspaceName, WorkspaceAccessLevels.Write, dataAccess) { workspaceContext =>
          dataAccess.methodConfigurationQuery.get(workspaceContext, methodConfiguration.namespace, methodConfiguration.name) flatMap {
            case Some(_) => DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.Conflict, s"${methodConfiguration.name} already exists in ${workspaceName}")))
            case None => createAndValidateMCExpressions(workspaceContext, methodConfiguration, dataAccess)
          } map { validatedMethodConfiguration =>
            RequestCompleteWithLocation((StatusCodes.Created, validatedMethodConfiguration), methodConfiguration.path(workspaceName))
          }
        }
      }
    }
  }

  def deleteMethodConfiguration(workspaceName: WorkspaceName, methodConfigurationNamespace: String, methodConfigurationName: String): Future[PerRequestMessage] =
    dataSource.inTransaction { dataAccess =>
      withWorkspaceContextAndPermissions(workspaceName, WorkspaceAccessLevels.Write, dataAccess) { workspaceContext =>
        withMethodConfig(workspaceContext, methodConfigurationNamespace, methodConfigurationName, dataAccess) { methodConfig =>
          dataAccess.methodConfigurationQuery.delete(workspaceContext, methodConfigurationNamespace, methodConfigurationName).map(_ => RequestComplete(StatusCodes.NoContent))
        }
      }
    }

  def renameMethodConfiguration(workspaceName: WorkspaceName, methodConfigurationNamespace: String, methodConfigurationName: String, newName: String): Future[PerRequestMessage] =
    dataSource.inTransaction { dataAccess =>
      withWorkspaceContextAndPermissions(workspaceName, WorkspaceAccessLevels.Write, dataAccess) { workspaceContext =>
        withMethodConfig(workspaceContext, methodConfigurationNamespace, methodConfigurationName, dataAccess) { methodConfiguration =>
          //Check if there are any other method configs that already possess the new name
          dataAccess.methodConfigurationQuery.get(workspaceContext, methodConfigurationNamespace, newName) flatMap {
            case None =>
              dataAccess.methodConfigurationQuery.update(workspaceContext, methodConfigurationNamespace, methodConfigurationName, methodConfiguration.copy(name = newName))
            case Some(_) => DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.Conflict, s"Destination ${newName} already exists")))
          } map(_ => RequestComplete(StatusCodes.NoContent))
        }
      }
    }

  def updateMethodConfiguration(workspaceName: WorkspaceName, methodConfigurationNamespace: String, methodConfigurationName: String, methodConfiguration: MethodConfiguration): Future[PerRequestMessage] = {
    withAttributeNamespaceCheck(methodConfiguration) {
     // create transaction
      dataSource.inTransaction { dataAccess =>
       // check permissions
        withWorkspaceContextAndPermissions(workspaceName, WorkspaceAccessLevels.Write, dataAccess) { workspaceContext =>
          // get old method configuration
          dataAccess.methodConfigurationQuery.get(workspaceContext, methodConfigurationNamespace, methodConfigurationName) flatMap {
            // if old method config exists, save the new one
            case Some(_) => updateAndValidateMCExpressions(workspaceContext, methodConfigurationNamespace, methodConfigurationName, methodConfiguration, dataAccess)
            // if old method config does not exist, fail
            case None => DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.NotFound, s"There is no method configuration named ${methodConfigurationNamespace}/${methodConfigurationName} in ${workspaceName}.")))
          } map (RequestComplete(StatusCodes.OK, _))
        }
      }
    }
  }

  def getMethodConfiguration(workspaceName: WorkspaceName, methodConfigurationNamespace: String, methodConfigurationName: String): Future[PerRequestMessage] =
    dataSource.inTransaction { dataAccess =>
      withWorkspaceContextAndPermissions(workspaceName, WorkspaceAccessLevels.Read, dataAccess) { workspaceContext =>
        withMethodConfig(workspaceContext, methodConfigurationNamespace, methodConfigurationName, dataAccess) { methodConfig =>
          DBIO.successful(PerRequest.RequestComplete(StatusCodes.OK, methodConfig))
        }
      }
    }

  def copyMethodConfiguration(mcnp: MethodConfigurationNamePair): Future[PerRequestMessage] = {
    // split into two transactions because we need to call out to Google after retrieving the source MC

    val transaction1Result = dataSource.inTransaction { dataAccess =>
      withWorkspaceContextAndPermissions(mcnp.destination.workspaceName, WorkspaceAccessLevels.Write, dataAccess) { destContext =>
        withWorkspaceContextAndPermissions(mcnp.source.workspaceName, WorkspaceAccessLevels.Read, dataAccess) { sourceContext =>
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
        }
      }
    }
  }

  def copyMethodConfigurationFromMethodRepo(methodRepoQuery: MethodRepoConfigurationImport): Future[PerRequestMessage] =
    methodRepoDAO.getMethodConfig(methodRepoQuery.methodRepoNamespace, methodRepoQuery.methodRepoName, methodRepoQuery.methodRepoSnapshotId, userInfo) flatMap {
      case None =>
        val name = s"${methodRepoQuery.methodRepoNamespace}/${methodRepoQuery.methodRepoName}/${methodRepoQuery.methodRepoSnapshotId}"
        val err = ErrorReport(StatusCodes.NotFound, s"There is no method configuration named $name in the repository.")
        Future.failed(new RawlsExceptionWithErrorReport(errorReport = err))
      case Some(agoraEntity) => Future.fromTry(parseAgoraEntity(agoraEntity)) flatMap { targetMethodConfig =>
        withAttributeNamespaceCheck(targetMethodConfig) {
          dataSource.inTransaction { dataAccess =>
            withWorkspaceContextAndPermissions(methodRepoQuery.destination.workspaceName, WorkspaceAccessLevels.Write, dataAccess) { destContext =>
              saveCopiedMethodConfiguration(targetMethodConfig, methodRepoQuery.destination, destContext, dataAccess)
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
    MethodConfiguration(agoraMethodConfig.namespace, agoraMethodConfig.name, agoraMethodConfig.rootEntityType, agoraMethodConfig.prerequisites, agoraMethodConfig.inputs, agoraMethodConfig.outputs, agoraMethodConfig.methodRepoMethod)
  }

  def copyMethodConfigurationToMethodRepo(methodRepoQuery: MethodRepoConfigurationExport): Future[PerRequestMessage] = {
    dataSource.inTransaction { dataAccess =>
      withWorkspaceContextAndPermissions(methodRepoQuery.source.workspaceName, WorkspaceAccessLevels.Read, dataAccess) { workspaceContext =>
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

  private def saveCopiedMethodConfiguration(methodConfig: MethodConfiguration, dest: MethodConfigurationName, destContext: SlickWorkspaceContext, dataAccess: DataAccess) = {
    val target = methodConfig.copy(name = dest.name, namespace = dest.namespace)

    dataAccess.methodConfigurationQuery.get(destContext, dest.namespace, dest.name).flatMap {
      case Some(existingMethodConfig) => DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.Conflict, s"A method configuration named ${dest.namespace}/${dest.name} already exists in ${dest.workspaceName}")))
      case None => createAndValidateMCExpressions(destContext, target, dataAccess)
    }.map(validatedTarget => RequestCompleteWithLocation((StatusCodes.Created, validatedTarget), target.path(dest.workspaceName)))
  }

  def listMethodConfigurations(workspaceName: WorkspaceName): Future[PerRequestMessage] =
    dataSource.inTransaction { dataAccess =>
      withWorkspaceContextAndPermissions(workspaceName, WorkspaceAccessLevels.Read, dataAccess) { workspaceContext =>
        dataAccess.methodConfigurationQuery.listActive(workspaceContext).map(r => RequestComplete(StatusCodes.OK, r.toList))
      }
    }

  def createMethodConfigurationTemplate( methodRepoMethod: MethodRepoMethod ): Future[PerRequestMessage] = {
    dataSource.inTransaction { dataAccess =>
      withMethod(methodRepoMethod.methodNamespace,methodRepoMethod.methodName,methodRepoMethod.methodVersion,userInfo) { method =>
        withWdl(method) { wdl => MethodConfigResolver.toMethodConfiguration(wdl, methodRepoMethod) match {
          case Failure(exception) => DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.BadRequest, exception)))
          case Success(methodConfig) => DBIO.successful(RequestComplete(StatusCodes.OK, methodConfig))
        }}
      }
    }
  }

  def getMethodInputsOutputs( methodRepoMethod: MethodRepoMethod ): Future[PerRequestMessage] = {
    dataSource.inTransaction { dataAccess =>
      withMethod(methodRepoMethod.methodNamespace,methodRepoMethod.methodName,methodRepoMethod.methodVersion,userInfo) { method =>
        withWdl(method) { wdl => MethodConfigResolver.getMethodInputsOutputs(wdl) match {
          case Failure(exception) => DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.BadRequest, exception)))
          case Success(inputsOutputs) => DBIO.successful(RequestComplete(StatusCodes.OK, inputsOutputs))
        }}
      }
    }
  }

  def listSubmissions(workspaceName: WorkspaceName): Future[PerRequestMessage] =
    dataSource.inTransaction { dataAccess =>
      withWorkspaceContextAndPermissions(workspaceName, WorkspaceAccessLevels.Read, dataAccess) { workspaceContext =>
        dataAccess.submissionQuery.listWithSubmitter(workspaceContext)
          .map(RequestComplete(StatusCodes.OK, _))
      }
    }

  def countSubmissions(workspaceName: WorkspaceName): Future[PerRequestMessage] =
    dataSource.inTransaction { dataAccess =>
      withWorkspaceContextAndPermissions(workspaceName, WorkspaceAccessLevels.Read, dataAccess) { workspaceContext =>
        dataAccess.submissionQuery.countByStatus(workspaceContext).map(RequestComplete(StatusCodes.OK, _))
      }
    }

  def createSubmission(workspaceName: WorkspaceName, submissionRequest: SubmissionRequest): Future[PerRequestMessage] = {
    val submissionFuture: Future[PerRequestMessage] = withSubmissionParameters(workspaceName, submissionRequest) {
      (dataAccess: DataAccess, workspaceContext: SlickWorkspaceContext, wdl: String, header: SubmissionValidationHeader, successes: Seq[SubmissionValidationEntityInputs], failures: Seq[SubmissionValidationEntityInputs], workflowFailureMode: Option[WorkflowFailureMode]) =>

        val submissionId: UUID = UUID.randomUUID()

        val workflows = successes map { entityInputs =>
          Workflow(workflowId = None,
            status = WorkflowStatuses.Queued,
            statusLastChangedDate = DateTime.now,
            workflowEntity = AttributeEntityReference(entityType = header.entityType, entityName = entityInputs.entityName),
            inputResolutions = entityInputs.inputResolutions
          )
        }

        val workflowFailures = failures map { entityInputs =>
          Workflow(workflowId = None,
            status = WorkflowStatuses.Failed,
            statusLastChangedDate = DateTime.now,
            workflowEntity = AttributeEntityReference(entityType = header.entityType, entityName = entityInputs.entityName),
            inputResolutions = entityInputs.inputResolutions,
            messages = for (entityValue <- entityInputs.inputResolutions if entityValue.error.isDefined) yield (AttributeString(entityValue.inputName + " - " + entityValue.error.get))
           )
        }

        val submission = Submission(submissionId = submissionId.toString,
          submissionDate = DateTime.now(),
          submitter = RawlsUser(userInfo),
          methodConfigurationNamespace = submissionRequest.methodConfigurationNamespace,
          methodConfigurationName = submissionRequest.methodConfigurationName,
          submissionEntity = AttributeEntityReference(entityType = submissionRequest.entityType, entityName = submissionRequest.entityName),
          workflows = workflows ++ workflowFailures,
          status = SubmissionStatuses.Submitted,
          useCallCache = submissionRequest.useCallCache,
          workflowFailureMode = workflowFailureMode
        )

        // implicitly passed to SubmissionComponent.create
        implicit val subStatusCounter = submissionStatusCounter(workspaceMetricBuilder(workspaceName))
        implicit val wfStatusCounter = workflowStatusCounter(workspaceSubmissionMetricBuilder(workspaceName, submissionId))

        dataAccess.submissionQuery.create(workspaceContext, submission) map { _ =>
          RequestComplete(StatusCodes.Created, SubmissionReport(submissionRequest, submission.submissionId, submission.submissionDate, userInfo.userEmail.value, submission.status, header, successes))
        }
    }

    submissionFuture map {
      case RequestComplete((StatusCodes.Created, submissionReport: SubmissionReport)) =>
        if (submissionReport.status == SubmissionStatuses.Submitted) {
          submissionSupervisor ! SubmissionStarted(workspaceName, UUID.fromString(submissionReport.submissionId), gcsDAO.getBucketServiceAccountCredential)
        }
        RequestComplete(StatusCodes.Created, submissionReport)

      case somethingWrong => somethingWrong // this is the case where something was not found in withSubmissionParameters
    }
  }

  def validateSubmission(workspaceName: WorkspaceName, submissionRequest: SubmissionRequest): Future[PerRequestMessage] =
    withSubmissionParameters(workspaceName,submissionRequest) {
      (dataAccess: DataAccess, workspaceContext: SlickWorkspaceContext, wdl: String, header: SubmissionValidationHeader, succeeded: Seq[SubmissionValidationEntityInputs], failed: Seq[SubmissionValidationEntityInputs], _) =>
        DBIO.successful(RequestComplete(StatusCodes.OK, SubmissionValidationReport(submissionRequest, header, succeeded, failed)))
    }

  def getSubmissionStatus(workspaceName: WorkspaceName, submissionId: String) = {
    dataSource.inTransaction { dataAccess =>
      withWorkspaceContextAndPermissions(workspaceName, WorkspaceAccessLevels.Read, dataAccess) { workspaceContext =>
        withSubmission(workspaceContext, submissionId, dataAccess) { submission =>
          withUser(submission.submitter, dataAccess) { user =>
            DBIO.successful(RequestComplete(StatusCodes.OK, new SubmissionStatusResponse(submission, user)))
          }
        }
      }
    }
  }

  def abortSubmission(workspaceName: WorkspaceName, submissionId: String): Future[PerRequestMessage] = {
    dataSource.inTransaction { dataAccess =>
      withWorkspaceContextAndPermissions(workspaceName, WorkspaceAccessLevels.Write, dataAccess) { workspaceContext =>
        abortSubmission(workspaceContext, submissionId, dataAccess)
      }
    }
  }

  private def abortSubmission(workspaceContext: SlickWorkspaceContext, submissionId: String, dataAccess: DataAccess): ReadWriteAction[PerRequestMessage] = {
    withSubmission(workspaceContext, submissionId, dataAccess) { submission =>
      // implicitly passed to SubmissionComponent.updateStatus
      implicit val subStatusCounter = submissionStatusCounter(workspaceMetricBuilder(workspaceContext.workspace.toWorkspaceName))
      dataAccess.submissionQuery.updateStatus(UUID.fromString(submission.submissionId), SubmissionStatuses.Aborting) map { rows =>
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
    val logs = execLogs.logs

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
    dataSource.inTransaction { dataAccess =>
      withWorkspaceContextAndPermissions(workspaceName, WorkspaceAccessLevels.Read, dataAccess) { workspaceContext =>
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

  def workflowMetadata(workspaceName: WorkspaceName, submissionId: String, workflowId: String) = {
    dataSource.inTransaction { dataAccess =>
      withWorkspaceContextAndPermissions(workspaceName, WorkspaceAccessLevels.Read, dataAccess) { workspaceContext =>
        withWorkflowRecord(workspaceName, submissionId, workflowId, dataAccess) { wr =>
          DBIO.from(executionServiceCluster.callLevelMetadata(wr, userInfo).map(em => RequestComplete(StatusCodes.OK, em)))
        }
      }
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

  def executionEngineVersion() = {
    executionServiceCluster.version(userInfo) map { RequestComplete(StatusCodes.OK, _) }
  }

  def checkBucketReadAccess(workspaceName: WorkspaceName) = {
    dataSource.inTransaction { dataAccess =>
      withWorkspaceContextAndPermissions(workspaceName, WorkspaceAccessLevels.Read, dataAccess) { workspaceContext =>
        DBIO.from(gcsDAO.diagnosticBucketRead(userInfo, workspaceContext.workspace.bucketName)).map {
          case Some(report) => RequestComplete(report)
          case None => RequestComplete(StatusCodes.OK)
        }
      }
    }
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

  def hasAllUserReadAccess(workspaceName: WorkspaceName): Future[PerRequestMessage] = {
    asFCAdmin {
      dataSource.inTransaction { dataAccess =>
        withWorkspaceContext(workspaceName, dataAccess) { workspaceContext =>
          dataAccess.rawlsGroupQuery.load(workspaceContext.workspace.accessLevels(WorkspaceAccessLevels.Read)) map { readerGroup =>
            readerGroup match {
              case Some(group) =>
                if (group.subGroups.contains(UserService.allUsersGroupRef)) {
                  RequestComplete(StatusCodes.NoContent)
                } else {
                  RequestComplete(StatusCodes.NotFound)
                }
              case None =>
                throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.InternalServerError, "allUsersGroup not found"))
            }
          }
        }
      }
    }
  }

  def grantAllUserReadAccess(workspaceName: WorkspaceName): Future[PerRequestMessage] = {
    asFCAdmin {
      dataSource.inTransaction { dataAccess =>
        withWorkspaceContext(workspaceName, dataAccess) { workspaceContext =>
          val userServiceRef = context.actorOf(UserService.props(userServiceConstructor, userInfo))
          DBIO.from((userServiceRef ? UserService.AddGroupMembers(
            workspaceContext.workspace.accessLevels(WorkspaceAccessLevels.Read),
            RawlsGroupMemberList(subGroupNames = Option(Seq(UserService.allUsersGroupRef.groupName.value))))).asInstanceOf[Future[PerRequestMessage]])
        } map {
          case RequestComplete(StatusCodes.OK) => RequestComplete(StatusCodes.Created)
          case otherwise => otherwise
        }
      }
    }
  }

  def revokeAllUserReadAccess(workspaceName: WorkspaceName): Future[PerRequestMessage] = {
    asFCAdmin {
      dataSource.inTransaction { dataAccess =>
        withWorkspaceContext(workspaceName, dataAccess) { workspaceContext =>
          val userServiceRef = context.actorOf(UserService.props(userServiceConstructor, userInfo))
          DBIO.from((userServiceRef ? UserService.RemoveGroupMembers(
            workspaceContext.workspace.accessLevels(WorkspaceAccessLevels.Read),
            RawlsGroupMemberList(subGroupNames = Option(Seq(UserService.allUsersGroupRef.groupName.value))))).asInstanceOf[Future[PerRequestMessage]])
        }
      }
    }
  }

  def listAllWorkspaces() = {
    asFCAdmin {
      dataSource.inTransaction { dataAccess =>
        dataAccess.workspaceQuery.listAll.map(RequestComplete(StatusCodes.OK, _))
      }
    }
  }

  def listWorkspacesWithAttribute(attributeName: AttributeName, attributeValue: AttributeValue): Future[PerRequestMessage] = {
    dataSource.inTransaction { dataAccess =>
      dataAccess.workspaceQuery.listWithAttribute(attributeName, attributeValue).map(RequestComplete(StatusCodes.OK, _))
    }
  }

  // this function is a bit naughty because it waits for database results
  // this is acceptable because it is a seldom used admin functions, not part of the mainstream
  // refactoring it to do the propper database action chaining is too much work at this time
  def getWorkspaceStatus(workspaceName: WorkspaceName, userSubjectId: Option[String]): Future[PerRequestMessage] = {
    def run[T](action: DataAccess => ReadWriteAction[T]): T = {
      Await.result(dataSource.inTransaction { dataAccess => action(dataAccess) }, 10 seconds)
    }

    asFCAdmin {
      val workspace = run { _.workspaceQuery.findByName(workspaceName) }.get
      val STATUS_FOUND = "FOUND"
      val STATUS_NOT_FOUND = "NOT_FOUND"
      val STATUS_CAN_WRITE = "USER_CAN_WRITE"
      val STATUS_CANNOT_WRITE = "USER_CANNOT_WRITE"
      val STATUS_NA = "NOT_AVAILABLE"

      val bucketName = workspace.bucketName
      val rawlsAccessGroupRefs = workspace.accessLevels
      val googleAccessGroupRefs = rawlsAccessGroupRefs map { case (accessLevel, groupRef) =>
        accessLevel -> run {
          _.rawlsGroupQuery.load(groupRef)
        }
      }
      val rawlsIntersectionGroupRefs = workspace.authDomainACLs
      val googleIntersectionGroupRefs = rawlsIntersectionGroupRefs map { case (accessLevel, groupRef) =>
        accessLevel -> run {
          _.rawlsGroupQuery.load(groupRef)
        }
      }

      val userRef = userSubjectId.flatMap(id => run {
        _.rawlsUserQuery.load(RawlsUserRef(RawlsUserSubjectId(id)))
      })

      val userStatus = userRef match {
        case Some(user) => "FIRECLOUD_USER: " + user.userSubjectId.value -> STATUS_FOUND
        case None => userSubjectId match {
          case Some(id) => "FIRECLOUD_USER: " + id -> STATUS_NOT_FOUND
          case None => "FIRECLOUD_USER: None Supplied" -> STATUS_NA
        }
      }

      val rawlsAccessGroupStatuses = rawlsAccessGroupRefs map { case (_, groupRef) =>
        run {
          _.rawlsGroupQuery.load(groupRef)
        } match {
          case Some(group) => "WORKSPACE_ACCESS_GROUP: " + group.groupName.value -> STATUS_FOUND
          case None => "WORKSPACE_ACCESS_GROUP: " + groupRef.groupName.value -> STATUS_NOT_FOUND
        }
      }

      val googleAccessGroupStatuses = googleAccessGroupRefs map { case (_, groupRef) =>
        val groupEmail = groupRef.get.groupEmail.value
        toFutureTry(gcsDAO.getGoogleGroup(groupEmail).map(_ match {
          case Some(_) => "GOOGLE_ACCESS_GROUP: " + groupEmail -> STATUS_FOUND
          case None => "GOOGLE_ACCESS_GROUP: " + groupEmail -> STATUS_NOT_FOUND
        }))
      }

      val rawlsIntersectionGroupStatuses = rawlsIntersectionGroupRefs map { case (_, groupRef) =>
        run {
          _.rawlsGroupQuery.load(groupRef)
        } match {
          case Some(group) => "WORKSPACE_INTERSECTION_GROUP: " + group.groupName.value -> STATUS_FOUND
          case None => "WORKSPACE_INTERSECTION_GROUP: " + groupRef.groupName.value -> STATUS_NOT_FOUND
        }
      }

      val googleIntersectionGroupStatuses = googleIntersectionGroupRefs map { case (_, groupRef) =>
        val groupEmail = groupRef.get.groupEmail.value
        toFutureTry(gcsDAO.getGoogleGroup(groupEmail).map(_ match {
          case Some(_) => "GOOGLE_INTERSECTION_GROUP: " + groupEmail -> STATUS_FOUND
          case None => "GOOGLE_INTERSECTION_GROUP: " + groupEmail -> STATUS_NOT_FOUND
        }))
      }

      val bucketStatus = toFutureTry(gcsDAO.getBucket(bucketName).map(_ match {
        case Some(_) => "GOOGLE_BUCKET: " + bucketName -> STATUS_FOUND
        case None => "GOOGLE_BUCKET: " + bucketName -> STATUS_NOT_FOUND
      }))

      val bucketWriteStatus = userStatus match {
        case (_, STATUS_FOUND) => {
          toFutureTry(gcsDAO.diagnosticBucketWrite(userRef.get, bucketName).map(_ match {
            case None => "GOOGLE_BUCKET_WRITE: " + bucketName -> STATUS_CAN_WRITE
            case Some(error) => "GOOGLE_BUCKET_WRITE: " + bucketName -> error.message
          }))
        }
        case (_, _) => Future(Try("GOOGLE_BUCKET_WRITE: " + bucketName -> STATUS_NA))
      }

      val userProxyStatus = userStatus match {
        case (_, STATUS_FOUND) => {
          toFutureTry(gcsDAO.isUserInProxyGroup(userRef.get).map { status =>
            if (status) "FIRECLOUD_USER_PROXY: " + bucketName -> STATUS_FOUND
            else "FIRECLOUD_USER_PROXY: " + bucketName -> STATUS_NOT_FOUND
          })
        }
        case (_, _) => Future(Try("FIRECLOUD_USER_PROXY: " + bucketName -> STATUS_NA))
      }

      val userAccessLevel = userStatus match {
        case (_, STATUS_FOUND) =>
          "WORKSPACE_USER_ACCESS_LEVEL" -> run {
            getMaximumAccessLevel(userRef.get, SlickWorkspaceContext(workspace), _)
          }.toString()
        case (_, _) => "WORKSPACE_USER_ACCESS_LEVEL" -> STATUS_NA
      }

      val googleAccessLevel = userStatus match {
        case (_, STATUS_FOUND) => {
          val accessLevel = run {
            getMaximumAccessLevel(userRef.get, SlickWorkspaceContext(workspace), _)
          }
          if (accessLevel >= WorkspaceAccessLevels.Read) {
            val groupEmail = run {
              _.rawlsGroupQuery.load(workspace.accessLevels.get(accessLevel).get)
            }.get.groupEmail.value
            toFutureTry(gcsDAO.isEmailInGoogleGroup(gcsDAO.toProxyFromUser(userRef.get.userSubjectId), groupEmail).map { status =>
              if (status) "GOOGLE_USER_ACCESS_LEVEL: " + groupEmail -> STATUS_FOUND
              else "GOOGLE_USER_ACCESS_LEVEL: " + groupEmail -> STATUS_NOT_FOUND
            })
          }
          else Future(Try("GOOGLE_USER_ACCESS_LEVEL" -> WorkspaceAccessLevels.NoAccess.toString))
        }
        case (_, _) => Future(Try("GOOGLE_USER_ACCESS_LEVEL" -> STATUS_NA))
      }

      Future.sequence(googleAccessGroupStatuses ++ googleIntersectionGroupStatuses ++ Seq(bucketStatus, bucketWriteStatus, userProxyStatus, googleAccessLevel)).map { tries =>
        val statuses = tries.collect { case Success(s) => s }.toSeq
        RequestComplete(WorkspaceStatus(workspaceName, (rawlsAccessGroupStatuses ++ rawlsIntersectionGroupStatuses ++ statuses ++ Seq(userStatus, userAccessLevel)).toMap))
      }
    }
  }

  def getBucketUsage(workspaceName: WorkspaceName): Future[PerRequestMessage] = {
    for {
      bucketName <- dataSource.inTransaction { dataAccess =>
        withWorkspaceContext(workspaceName, dataAccess) { workspaceContext =>
          requireAccessIgnoreLock(workspaceContext.workspace, WorkspaceAccessLevels.Write, dataAccess) {
            DBIO.successful(workspaceContext.workspace.bucketName)
          }
        }
      }
      usage <- gcsDAO.getBucketUsage(RawlsBillingProjectName(workspaceName.namespace), bucketName)
    } yield {
      RequestComplete(BucketUsageResponse(usage))
    }
  }

  def getAccessInstructions(workspaceName: WorkspaceName): Future[PerRequestMessage] = {
    dataSource.inTransaction { dataAccess =>
      withWorkspaceContext(workspaceName, dataAccess) { workspaceContext =>
        val accessGroups = DBIO.sequence(workspaceContext.workspace.accessLevels.values.map { ref =>
          dataAccess.rawlsGroupQuery.loadGroupIfMember(ref, RawlsUser(userInfo))
        })

        accessGroups.flatMap { memberOf =>
          if (memberOf.flatten.isEmpty) DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.NotFound, noSuchWorkspaceMessage(workspaceName))))
          else {
            workspaceContext.workspace.authorizationDomain match {
              case Some(authDomain) => {
                dataAccess.managedGroupQuery.getManagedGroupAccessInstructions(Seq(workspaceContext.workspace.authorizationDomain.get)) map { instructions =>
                  RequestComplete(StatusCodes.OK, instructions)
                }
              }
              case None => DBIO.successful(RequestComplete(StatusCodes.OK, Seq.empty[ManagedGroupAccessInstructions]))
            }
          }
        }
      }
    }
  }
  
  def getGenomicsOperation(workspaceName: WorkspaceName, jobId: String): Future[PerRequestMessage] = {
    // First check the workspace context and permissions in a DB transaction.
    // We don't need any DB information beyond that, so just return Unit on success.
    dataSource.inTransaction { dataAccess =>
      withWorkspaceContextAndPermissions(workspaceName, WorkspaceAccessLevels.Read, dataAccess) { _ =>
        DBIO.successful(())
      }
    }
    // Next call GenomicsService, which actually makes the Google Genomics API call.
    .flatMap { _ =>
      val genomicsServiceRef = context.actorOf(GenomicsService.props(genomicsServiceConstructor, userInfo))
      (genomicsServiceRef ? GenomicsService.GetOperation(jobId)).mapTo[PerRequestMessage]
    }
  }

  // helper methods

  // note: success is indicated by  Map.empty
  private def attributeNamespaceCheck(attributeNames: Iterable[AttributeName]): Map[String, String] = {
    val namespaces = attributeNames.map(_.namespace).toSet

    // no one can modify attributes with invalid namespaces
    val invalidNamespaces = namespaces -- AttributeName.validNamespaces
    invalidNamespaces.map { ns => ns -> s"Invalid attribute namespace $ns" }.toMap
  }

  private def withAttributeNamespaceCheck[T](attributeNames: Iterable[AttributeName])(op: => T): T = {
    val errors = attributeNamespaceCheck(attributeNames)
    if (errors.isEmpty) op
    else {
      val reasons = errors.values.mkString(", ")
      val err = ErrorReport(statusCode = StatusCodes.Forbidden, message = s"Attribute namespace validation failed: [$reasons]")
      throw new RawlsExceptionWithErrorReport(errorReport = err)
    }
  }

  private def withAttributeNamespaceCheck[T](hasAttributes: Attributable)(op: => Future[T]): Future[T] =
    withAttributeNamespaceCheck(hasAttributes.attributes.keys)(op)

  private def withAttributeNamespaceCheck[T](methodConfiguration: MethodConfiguration)(op: => Future[T]): Future[T] = {
    // TODO: this duplicates expression parsing, the canonical way to do this.  Use that instead?
    // valid method configuration outputs are either in the format this.attrname or workspace.attrname
    // invalid (unparseable) will be caught by expression parsing instead
    val attrNames = methodConfiguration.outputs map { case (_, attr) => AttributeName.fromDelimitedName(attr.value.split('.').last) }
    withAttributeNamespaceCheck(attrNames)(op)
  }

  private def withNewWorkspaceContext(workspaceRequest: WorkspaceRequest, dataAccess: DataAccess)
                                     (op: (SlickWorkspaceContext) => ReadWriteAction[PerRequestMessage]): ReadWriteAction[PerRequestMessage] = {

    def saveNewWorkspace(workspaceId: String, googleWorkspaceInfo: GoogleWorkspaceInfo, workspaceRequest: WorkspaceRequest, dataAccess: DataAccess): ReadWriteAction[Workspace] = {
      val currentDate = DateTime.now

      val accessGroups = googleWorkspaceInfo.accessGroupsByLevel.map { case (a, g) => (a -> RawlsGroup.toRef(g)) }
      val intersectionGroups = googleWorkspaceInfo.intersectionGroupsByLevel map {
        _.map { case (a, g) => (a -> RawlsGroup.toRef(g)) }
      }

      val workspace = Workspace(
        namespace = workspaceRequest.namespace,
        name = workspaceRequest.name,
        authorizationDomain = workspaceRequest.authorizationDomain,
        workspaceId = workspaceId,
        bucketName = googleWorkspaceInfo.bucketName,
        createdDate = currentDate,
        lastModified = currentDate,
        createdBy = userInfo.userEmail.value,
        attributes = workspaceRequest.attributes,
        accessLevels = accessGroups,
        authDomainACLs = intersectionGroups getOrElse accessGroups
      )

      val groupInserts =
        // project owner group should already exsist, don't save it again
        googleWorkspaceInfo.accessGroupsByLevel.filterKeys(_ != ProjectOwner).values.map(dataAccess.rawlsGroupQuery.save) ++
          googleWorkspaceInfo.intersectionGroupsByLevel.map(_.values.map(dataAccess.rawlsGroupQuery.save)).getOrElse(Seq.empty)

      DBIO.seq(groupInserts.toSeq: _*) andThen
        dataAccess.workspaceQuery.save(workspace)
    }


    requireCreateWorkspaceAccess(workspaceRequest, dataAccess) {
      dataAccess.workspaceQuery.findByName(workspaceRequest.toWorkspaceName) flatMap {
        case Some(_) => DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.Conflict, s"Workspace ${workspaceRequest.namespace}/${workspaceRequest.name} already exists")))
        case None =>
          val workspaceId = UUID.randomUUID.toString
          for {
            project <- dataAccess.rawlsBillingProjectQuery.load(RawlsBillingProjectName(workspaceRequest.namespace))

            // we have already verified that the user is in the realm but the project owners might not be
            // so if there is a realm we have to do the intersection. There should not be any readers or writers
            // at this point (brand new workspace) so we don't need to do intersections for those
            realmProjectOwnerIntersection <- DBIOUtils.maybeDbAction(workspaceRequest.authorizationDomain) {
              realm => dataAccess.rawlsGroupQuery.intersectGroupMembership(project.get.groups(ProjectRoles.Owner), realm.toMembersGroupRef)
            }

            googleWorkspaceInfo <- DBIO.from(gcsDAO.setupWorkspace(userInfo, project.get, workspaceId, workspaceRequest.toWorkspaceName, workspaceRequest.authorizationDomain, realmProjectOwnerIntersection))

            savedWorkspace <- saveNewWorkspace(workspaceId, googleWorkspaceInfo, workspaceRequest, dataAccess)
            response <- op(SlickWorkspaceContext(savedWorkspace))
          } yield response
      }
    }
  }

  private def noSuchWorkspaceMessage(workspaceName: WorkspaceName) = s"${workspaceName} does not exist"
  private def accessDeniedMessage(workspaceName: WorkspaceName) = s"insufficient permissions to perform operation on ${workspaceName}"

  private def requireCreateWorkspaceAccess(workspaceRequest: WorkspaceRequest, dataAccess: DataAccess)(op: => ReadWriteAction[PerRequestMessage]): ReadWriteAction[PerRequestMessage] = {
    val projectName = RawlsBillingProjectName(workspaceRequest.namespace)
    dataAccess.rawlsBillingProjectQuery.hasOneOfProjectRole(projectName, RawlsUser(userInfo), ProjectRoles.all) flatMap {
      case true =>
        dataAccess.rawlsBillingProjectQuery.load(projectName).flatMap {
          case Some(RawlsBillingProject(_, _, _, CreationStatuses.Ready, _, _)) =>
            workspaceRequest.authorizationDomain match {
              case Some(realm) => dataAccess.managedGroupQuery.listManagedGroupsForUser(RawlsUser(userInfo)) flatMap { realmAccesses =>
                if(realmAccesses.contains(ManagedGroupAccess(realm, ManagedRoles.Member))) op
                else DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.Forbidden, s"You cannot create a workspace in realm [${realm.membersGroupName.value}] as you do not have access to it.")))
              }
              case None => op
            }

          case Some(RawlsBillingProject(RawlsBillingProjectName(name), _, _, CreationStatuses.Creating, _, _)) =>
            DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.BadRequest, s"${name} is still being created")))

          case Some(RawlsBillingProject(RawlsBillingProjectName(name), _, _, CreationStatuses.Error, _, messageOp)) =>
            DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.BadRequest, s"Error creating ${name}: ${messageOp.getOrElse("no message")}")))

          case None =>
            // this can't happen with the current code but a 404 would be the correct response
            DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.NotFound, s"${workspaceRequest.toWorkspaceName.namespace} does not exist")))
        }
      case false =>
        DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.Forbidden, s"You are not authorized to create a workspace in billing project ${workspaceRequest.toWorkspaceName.namespace}")))
    }
  }

  private def withWorkspaceContextAndPermissions[T](workspaceName: WorkspaceName, accessLevel: WorkspaceAccessLevel, dataAccess: DataAccess)(op: (SlickWorkspaceContext) => ReadWriteAction[T]): ReadWriteAction[T] = {
    withWorkspaceContext(workspaceName, dataAccess) { workspaceContext =>
      requireAccess(workspaceContext.workspace, accessLevel, dataAccess) { op(workspaceContext) }
    }
  }

  private def withWorkspaceContext[T](workspaceName: WorkspaceName, dataAccess: DataAccess)(op: (SlickWorkspaceContext) => ReadWriteAction[T]) = {
    dataAccess.workspaceQuery.findByName(workspaceName) flatMap {
      case None => DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.NotFound, noSuchWorkspaceMessage(workspaceName))))
      case Some(workspace) => op(SlickWorkspaceContext(workspace))
    }
  }

  private def requireAccess[T](workspace: Workspace, requiredLevel: WorkspaceAccessLevel, dataAccess: DataAccess)(codeBlock: => ReadWriteAction[T]): ReadWriteAction[T] = {
    getMaximumAccessLevel(RawlsUser(userInfo), SlickWorkspaceContext(workspace), dataAccess) flatMap { userLevel =>
      if (userLevel >= requiredLevel) {
        if ( (requiredLevel > WorkspaceAccessLevels.Read) && workspace.isLocked )
          DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.Forbidden, s"The workspace ${workspace.toWorkspaceName} is locked.")))
        else codeBlock
      }
      else if (userLevel >= WorkspaceAccessLevels.Read) DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.Forbidden, accessDeniedMessage(workspace.toWorkspaceName))))
      else DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.NotFound, noSuchWorkspaceMessage(workspace.toWorkspaceName))))
    }
  }

  private def requireAccessIgnoreLock[T](workspace: Workspace, requiredLevel: WorkspaceAccessLevel, dataAccess: DataAccess)(op: => ReadWriteAction[T]): ReadWriteAction[T] = {
    requireAccess(workspace.copy(isLocked = false), requiredLevel, dataAccess)(op)
  }

  private def requireSharePermission[T](workspace: Workspace, dataAccess: DataAccess)(codeBlock: (WorkspaceAccessLevel) => ReadWriteAction[T]): ReadWriteAction[T] = {
    getMaximumAccessLevel(RawlsUser(userInfo), SlickWorkspaceContext(workspace), dataAccess) flatMap { userLevel =>
      if (userLevel >= WorkspaceAccessLevels.Owner) codeBlock(userLevel)
      else dataAccess.workspaceQuery.getUserSharePermissions(userInfo.userSubjectId, SlickWorkspaceContext(workspace)) flatMap { canShare =>
        if (canShare) codeBlock(userLevel)
        else if (userLevel >= WorkspaceAccessLevels.Read) DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.Forbidden, accessDeniedMessage(workspace.toWorkspaceName))))
        else DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.NotFound, noSuchWorkspaceMessage(workspace.toWorkspaceName))))
      }
    }
  }

  def getUserSharePermissions(workspaceContext: SlickWorkspaceContext, userAccessLevel: WorkspaceAccessLevel, dataAccess: DataAccess): ReadAction[Boolean] = {
    if (userAccessLevel >= WorkspaceAccessLevels.Owner) DBIO.successful(true)
    else dataAccess.workspaceQuery.getUserSharePermissions(userInfo.userSubjectId, workspaceContext)
  }

  def getUserCatalogPermissions(workspaceContext: SlickWorkspaceContext, dataAccess: DataAccess): ReadAction[Boolean] = {
    dataAccess.workspaceQuery.getUserCatalogPermissions(userInfo.userSubjectId, workspaceContext)
  }

  private def withEntity[T](workspaceContext: SlickWorkspaceContext, entityType: String, entityName: String, dataAccess: DataAccess)(op: (Entity) => ReadWriteAction[T]): ReadWriteAction[T] = {
    dataAccess.entityQuery.get(workspaceContext, entityType, entityName) flatMap {
      case None => DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.NotFound, s"${entityType} ${entityName} does not exist in ${workspaceContext.workspace.toWorkspaceName}")))
      case Some(entity) => op(entity)
    }
  }

  private def withAllEntities[T](workspaceContext: SlickWorkspaceContext, dataAccess: DataAccess, entities: Seq[AttributeEntityReference])(op: (Seq[Entity]) => ReadWriteAction[T]): ReadWriteAction[T] = {
    val entityActions: Seq[ReadAction[Try[Entity]]] = entities map { e =>
      dataAccess.entityQuery.get(workspaceContext, e.entityType, e.entityName) map {
        case None => Failure(new RawlsException(s"${e.entityType} ${e.entityName} does not exist in ${workspaceContext.workspace.toWorkspaceName}"))
        case Some(entity) => Success(entity)
      }
    }

    DBIO.sequence(entityActions) flatMap { entityTries =>
      val failures = entityTries.collect { case Failure(y) => y.getMessage }
      if (failures.isEmpty) op(entityTries collect { case Success(e) => e })
      else {
        val err = ErrorReport(statusCode = StatusCodes.BadRequest, message = (Seq("Entities were not found:") ++ failures) mkString System.lineSeparator())
        DBIO.failed(new RawlsExceptionWithErrorReport(err))
      }
    }
  }

  private def withSubmission(workspaceContext: SlickWorkspaceContext, submissionId: String, dataAccess: DataAccess)(op: (Submission) => ReadWriteAction[PerRequestMessage]): ReadWriteAction[PerRequestMessage] = {
    Try {
      UUID.fromString(submissionId)
    } match {
      case Failure(t: IllegalArgumentException) =>
        DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.NotFound, s"Submission id ${submissionId} is not a valid submission id")))
      case _ =>
        dataAccess.submissionQuery.get(workspaceContext, submissionId) flatMap {
          case None => DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.NotFound, s"Submission with id ${submissionId} not found in workspace ${workspaceContext.workspace.toWorkspaceName}")))
          case Some(submission) => op(submission)
        }
    }
  }

  private def withWorkflow(workspaceName: WorkspaceName, submissionId: String, workflowId: String, dataAccess: DataAccess)(op: (Workflow) => ReadWriteAction[PerRequestMessage]): ReadWriteAction[PerRequestMessage] = {
    dataAccess.workflowQuery.getByExternalId(workflowId, submissionId) flatMap {
      case None => DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.NotFound, s"Workflow with id ${workflowId} not found in submission ${submissionId} in workspace ${workspaceName}")))
      case Some(workflow) => op(workflow)
    }
  }

  private def withWorkflowRecord(workspaceName: WorkspaceName, submissionId: String, workflowId: String, dataAccess: DataAccess)(op: (WorkflowRecord) => ReadWriteAction[PerRequestMessage]): ReadWriteAction[PerRequestMessage] = {
    dataAccess.workflowQuery.findWorkflowByExternalIdAndSubmissionId(workflowId, UUID.fromString(submissionId)).result flatMap {
      case Seq() => DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.NotFound, s"WorkflowRecord with id ${workflowId} not found in submission ${submissionId} in workspace ${workspaceName}")))
      case Seq(one) => op(one)
      case tooMany => DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.InternalServerError, s"found multiple WorkflowRecords with id ${workflowId} in submission ${submissionId} in workspace ${workspaceName}")))
    }
  }

  //Finds a single entity record in the db.
  private def withSingleEntityRec(entityType: String, entityName: String, workspaceContext: SlickWorkspaceContext, dataAccess: DataAccess)(op: (Seq[EntityRecord]) => ReadWriteAction[PerRequestMessage]): ReadWriteAction[PerRequestMessage] = {
    val entityRec = dataAccess.entityQuery.findEntityByName(workspaceContext.workspaceId, entityType, entityName).result
    entityRec flatMap { entities =>
      if (entities.isEmpty) {
        DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.NotFound, s"No entity of type ${entityType} named ${entityName} exists in this workspace.")))
      } else if (entities.size == 1) {
        op(entities)
      } else {
        DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.NotFound, s"More than one entity of type ${entityType} named ${entityName} exists in this workspace?!")))
      }
    }
  }

  def withSubmissionEntityRecs(submissionRequest: SubmissionRequest, workspaceContext: SlickWorkspaceContext, rootEntityType: String, dataAccess: DataAccess)(op: (Seq[EntityRecord]) => ReadWriteAction[PerRequestMessage]): ReadWriteAction[PerRequestMessage] = {
    //If there's an expression, evaluate it to get the list of entities to run this job on.
    //Otherwise, use the entity given in the submission.
    submissionRequest.expression match {
      case None =>
        if ( submissionRequest.entityType != rootEntityType )
          DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.BadRequest, s"Method configuration expects an entity of type ${rootEntityType}, but you gave us an entity of type ${submissionRequest.entityType}.")))
        else {
          withSingleEntityRec(submissionRequest.entityType, submissionRequest.entityName, workspaceContext, dataAccess)(op)
        }
      case Some(expression) =>
        ExpressionEvaluator.withNewExpressionEvaluator(dataAccess, workspaceContext, submissionRequest.entityType, submissionRequest.entityName) { evaluator =>
          evaluator.evalFinalEntity(workspaceContext, expression).asTry
        } flatMap { //gotta close out the expression evaluator to wipe the EXPREVAL_TEMP table
          case Failure(regret) => DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.BadRequest, regret)))
          case Success(entityRecords) =>
            if (entityRecords.isEmpty) {
              DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.BadRequest, "No entities eligible for submission were found.")))
            } else {
              val eligibleEntities = entityRecords.filter(_.entityType == rootEntityType).toSeq
              if (eligibleEntities.isEmpty)
                DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.BadRequest, s"The expression in your SubmissionRequest matched only entities of the wrong type. (Expected type ${rootEntityType}.)")))
              else
                op(eligibleEntities)
            }
        }
    }
  }

  /** Validates the workflow failure mode in the submission request. */
  private def withWorkflowFailureMode(submissionRequest: SubmissionRequest)(op: Option[WorkflowFailureMode] => ReadWriteAction[PerRequestMessage]): ReadWriteAction[PerRequestMessage] = {
    Try(submissionRequest.workflowFailureMode.map(WorkflowFailureModes.withName)) match {
      case Success(failureMode) => op(failureMode)
      case Failure(NonFatal(e)) => DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.BadRequest, e.getMessage)))
    }
  }

  private def withSubmissionParameters(workspaceName: WorkspaceName, submissionRequest: SubmissionRequest)
   ( op: (DataAccess, SlickWorkspaceContext, String, SubmissionValidationHeader, Seq[SubmissionValidationEntityInputs], Seq[SubmissionValidationEntityInputs], Option[WorkflowFailureMode]) => ReadWriteAction[PerRequestMessage]): Future[PerRequestMessage] = {
    dataSource.inTransaction { dataAccess =>
      withWorkspaceContextAndPermissions(workspaceName, WorkspaceAccessLevels.Write, dataAccess) { workspaceContext =>
        withMethodConfig(workspaceContext, submissionRequest.methodConfigurationNamespace, submissionRequest.methodConfigurationName, dataAccess) { methodConfig =>
          withMethodInputs(methodConfig, userInfo) { (wdl, methodInputs) =>
            withSubmissionEntityRecs(submissionRequest, workspaceContext, methodConfig.rootEntityType, dataAccess) { jobEntityRecs =>
              withWorkflowFailureMode(submissionRequest) { workflowFailureMode =>
                //Remove inputs that are both empty and optional
                val inputsToEvaluate = methodInputs.filter(input => !(input.workflowInput.optional && input.expression.isEmpty))
                //Parse out the entity -> results map to a tuple of (successful, failed) SubmissionValidationEntityInputs
                MethodConfigResolver.resolveInputsForEntities(workspaceContext, inputsToEvaluate, jobEntityRecs, dataAccess) flatMap { valuesByEntity =>
                  valuesByEntity
                    .map({ case (entityName, values) => SubmissionValidationEntityInputs(entityName, values) })
                    .partition({ entityInputs => entityInputs.inputResolutions.forall(_.error.isEmpty) }) match {
                    case (succeeded, failed) =>
                      val methodConfigInputs = inputsToEvaluate.map { methodInput => SubmissionValidationInput(methodInput.workflowInput.fqn, methodInput.expression) }
                      val header = SubmissionValidationHeader(methodConfig.rootEntityType, methodConfigInputs)
                      op(dataAccess, workspaceContext, wdl, header, succeeded.toSeq, failed.toSeq, workflowFailureMode)
                  }
                }
              }
            }
          }
        }
      }
    }
  }
}


class AttributeUpdateOperationException(message: String) extends RawlsException(message)
class AttributeNotFoundException(message: String) extends AttributeUpdateOperationException(message)
