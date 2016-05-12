package org.broadinstitute.dsde.rawls.workspace

import java.util.UUID
import _root_.slick.dbio.Effect.Read
import _root_.slick.profile.FixedSqlStreamingAction
import akka.actor._
import akka.pattern._
import akka.util.Timeout
import org.broadinstitute.dsde.rawls.dataaccess.slick.{EntityRecord, ReadAction, ReadWriteAction, DataAccess}
import com.typesafe.scalalogging.LazyLogging
import org.broadinstitute.dsde.rawls.monitor.BucketDeletionMonitor.DeleteBucket
import org.broadinstitute.dsde.rawls.{RawlsExceptionWithErrorReport, RawlsException}
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.jobexec.MethodConfigResolver
import org.broadinstitute.dsde.rawls.jobexec.MethodConfigResolver.MethodInput
import org.broadinstitute.dsde.rawls.jobexec.SubmissionSupervisor.SubmissionStarted
import org.broadinstitute.dsde.rawls.model.WorkspaceAccessLevels.WorkspaceAccessLevel
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.expressions._
import org.broadinstitute.dsde.rawls.user.UserService
import org.broadinstitute.dsde.rawls.util.{MethodWiths, FutureSupport, AdminSupport}
import org.broadinstitute.dsde.rawls.webservice.PerRequest
import org.broadinstitute.dsde.rawls.webservice.PerRequest._
import AttributeUpdateOperations._
import org.broadinstitute.dsde.rawls.workspace.WorkspaceService._
import org.joda.time.DateTime
import spray.http.Uri
import spray.http.StatusCodes
import spray.httpx.UnsuccessfulResponseException
import scala.collection.immutable.Iterable
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}
import spray.json._
import spray.httpx.SprayJsonSupport._
import org.broadinstitute.dsde.rawls.model.WorkspaceACLJsonSupport.WorkspaceACLFormat
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._
import org.broadinstitute.dsde.rawls.model.ExecutionJsonSupport.{ActiveSubmissionFormat, ExecutionMetadataFormat, SubmissionStatusResponseFormat, SubmissionListResponseFormat, SubmissionFormat, SubmissionReportFormat, SubmissionValidationReportFormat, WorkflowOutputsFormat, ExecutionServiceValidationFormat}
import scala.concurrent.duration._
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import scala.concurrent.Await
/**
 * Created by dvoet on 4/27/15.
 */

object WorkspaceService {
  sealed trait WorkspaceServiceMessage
  case class CreateWorkspace(workspace: WorkspaceRequest) extends WorkspaceServiceMessage
  case class GetWorkspace(workspaceName: WorkspaceName) extends WorkspaceServiceMessage
  case class DeleteWorkspace(workspaceName: WorkspaceName) extends WorkspaceServiceMessage
  case class UpdateWorkspace(workspaceName: WorkspaceName, operations: Seq[AttributeUpdateOperation]) extends WorkspaceServiceMessage
  case object ListWorkspaces extends WorkspaceServiceMessage
  case object ListAllWorkspaces extends WorkspaceServiceMessage
  case class CloneWorkspace(sourceWorkspace: WorkspaceName, destWorkspace: WorkspaceRequest) extends WorkspaceServiceMessage
  case class GetACL(workspaceName: WorkspaceName) extends WorkspaceServiceMessage
  case class UpdateACL(workspaceName: WorkspaceName, aclUpdates: Seq[WorkspaceACLUpdate]) extends WorkspaceServiceMessage
  case class LockWorkspace(workspaceName: WorkspaceName) extends WorkspaceServiceMessage
  case class UnlockWorkspace(workspaceName: WorkspaceName) extends WorkspaceServiceMessage
  case class GetWorkspaceStatus(workspaceName: WorkspaceName, userSubjectId: Option[String]) extends WorkspaceServiceMessage

  case class CreateEntity(workspaceName: WorkspaceName, entity: Entity) extends WorkspaceServiceMessage
  case class GetEntity(workspaceName: WorkspaceName, entityType: String, entityName: String) extends WorkspaceServiceMessage
  case class UpdateEntity(workspaceName: WorkspaceName, entityType: String, entityName: String, operations: Seq[AttributeUpdateOperation]) extends WorkspaceServiceMessage
  case class DeleteEntity(workspaceName: WorkspaceName, entityType: String, entityName: String) extends WorkspaceServiceMessage
  case class RenameEntity(workspaceName: WorkspaceName, entityType: String, entityName: String, newName: String) extends WorkspaceServiceMessage
  case class EvaluateExpression(workspaceName: WorkspaceName, entityType: String, entityName: String, expression: String) extends WorkspaceServiceMessage
  case class ListEntityTypes(workspaceName: WorkspaceName) extends WorkspaceServiceMessage
  case class ListEntities(workspaceName: WorkspaceName, entityType: String) extends WorkspaceServiceMessage
  case class CopyEntities(entityCopyDefinition: EntityCopyDefinition, uri:Uri) extends WorkspaceServiceMessage
  case class BatchUpsertEntities(workspaceName: WorkspaceName, entityUpdates: Seq[EntityUpdateDefinition]) extends WorkspaceServiceMessage
  case class BatchUpdateEntities(workspaceName: WorkspaceName, entityUpdates: Seq[EntityUpdateDefinition]) extends WorkspaceServiceMessage

  case class CreateMethodConfiguration(workspaceName: WorkspaceName, methodConfiguration: MethodConfiguration) extends WorkspaceServiceMessage
  case class GetMethodConfiguration(workspaceName: WorkspaceName, methodConfigurationNamespace: String, methodConfigurationName: String) extends WorkspaceServiceMessage
  case class UpdateMethodConfiguration(workspaceName: WorkspaceName, methodConfiguration: MethodConfiguration) extends WorkspaceServiceMessage
  case class DeleteMethodConfiguration(workspaceName: WorkspaceName, methodConfigurationNamespace: String, methodConfigurationName: String) extends WorkspaceServiceMessage
  case class RenameMethodConfiguration(workspaceName: WorkspaceName, methodConfigurationNamespace: String, methodConfigurationName: String, newName: String) extends WorkspaceServiceMessage
  case class CopyMethodConfiguration(methodConfigNamePair: MethodConfigurationNamePair) extends WorkspaceServiceMessage
  case class CopyMethodConfigurationFromMethodRepo(query: MethodRepoConfigurationImport) extends WorkspaceServiceMessage
  case class CopyMethodConfigurationToMethodRepo(query: MethodRepoConfigurationExport) extends WorkspaceServiceMessage
  case class ListMethodConfigurations(workspaceName: WorkspaceName) extends WorkspaceServiceMessage
  case class CreateMethodConfigurationTemplate( methodRepoMethod: MethodRepoMethod ) extends WorkspaceServiceMessage
  case class GetMethodInputsOutputs( methodRepoMethod: MethodRepoMethod ) extends WorkspaceServiceMessage
  case class GetAndValidateMethodConfiguration(workspaceName: WorkspaceName, methodConfigurationNamespace: String, methodConfigurationName: String) extends WorkspaceServiceMessage

  case class ListSubmissions(workspaceName: WorkspaceName) extends WorkspaceServiceMessage
  case class CountSubmissions(workspaceName: WorkspaceName) extends WorkspaceServiceMessage
  case class CreateSubmission(workspaceName: WorkspaceName, submission: SubmissionRequest) extends WorkspaceServiceMessage
  case class ValidateSubmission(workspaceName: WorkspaceName, submission: SubmissionRequest) extends WorkspaceServiceMessage
  case class GetSubmissionStatus(workspaceName: WorkspaceName, submissionId: String) extends WorkspaceServiceMessage
  case class AbortSubmission(workspaceName: WorkspaceName, submissionId: String) extends WorkspaceServiceMessage
  case class GetWorkflowOutputs(workspaceName: WorkspaceName, submissionId: String, workflowId: String) extends WorkspaceServiceMessage
  case class GetWorkflowMetadata(workspaceName: WorkspaceName, submissionId: String, workflowId: String) extends WorkspaceServiceMessage
  case object WorkflowQueueStatus extends WorkspaceServiceMessage

  case object AdminListAllActiveSubmissions extends WorkspaceServiceMessage
  case class AdminAbortSubmission(workspaceNamespace: String, workspaceName: String, submissionId: String) extends WorkspaceServiceMessage

  case class HasAllUserReadAccess(workspaceName: WorkspaceName) extends WorkspaceServiceMessage
  case class GrantAllUserReadAccess(workspaceName: WorkspaceName) extends WorkspaceServiceMessage
  case class RevokeAllUserReadAccess(workspaceName: WorkspaceName) extends WorkspaceServiceMessage

  def props(workspaceServiceConstructor: UserInfo => WorkspaceService, userInfo: UserInfo): Props = {
    Props(workspaceServiceConstructor(userInfo))
  }

  def constructor(dataSource: SlickDataSource, methodRepoDAO: MethodRepoDAO, executionServiceDAO: ExecutionServiceDAO, execServiceBatchSize: Int, gcsDAO: GoogleServicesDAO, submissionSupervisor: ActorRef, bucketDeletionMonitor: ActorRef, userServiceConstructor: UserInfo => UserService)(userInfo: UserInfo)(implicit executionContext: ExecutionContext) =
    new WorkspaceService(userInfo, dataSource, methodRepoDAO, executionServiceDAO, execServiceBatchSize, gcsDAO, submissionSupervisor, bucketDeletionMonitor, userServiceConstructor)
}

class WorkspaceService(protected val userInfo: UserInfo, val dataSource: SlickDataSource, val methodRepoDAO: MethodRepoDAO, executionServiceDAO: ExecutionServiceDAO, execServiceBatchSize: Int, protected val gcsDAO: GoogleServicesDAO, submissionSupervisor: ActorRef, bucketDeletionMonitor: ActorRef, userServiceConstructor: UserInfo => UserService)(implicit protected val executionContext: ExecutionContext) extends Actor with AdminSupport with FutureSupport with MethodWiths with LazyLogging {
  import dataSource.dataAccess.driver.api._

  implicit val timeout = Timeout(5 minutes)

  override def receive = {
    case CreateWorkspace(workspace) => pipe(createWorkspace(workspace)) to sender
    case GetWorkspace(workspaceName) => pipe(getWorkspace(workspaceName)) to sender
    case DeleteWorkspace(workspaceName) => pipe(deleteWorkspace(workspaceName)) to sender
    case UpdateWorkspace(workspaceName, operations) => pipe(updateWorkspace(workspaceName, operations)) to sender
    case ListWorkspaces => pipe(listWorkspaces()) to sender
    case ListAllWorkspaces => pipe(listAllWorkspaces()) to sender
    case CloneWorkspace(sourceWorkspace, destWorkspaceRequest) => pipe(cloneWorkspace(sourceWorkspace, destWorkspaceRequest)) to sender
    case GetACL(workspaceName) => pipe(getACL(workspaceName)) to sender
    case UpdateACL(workspaceName, aclUpdates) => pipe(updateACL(workspaceName, aclUpdates)) to sender
    case LockWorkspace(workspaceName: WorkspaceName) => pipe(lockWorkspace(workspaceName)) to sender
    case UnlockWorkspace(workspaceName: WorkspaceName) => pipe(unlockWorkspace(workspaceName)) to sender
    case GetWorkspaceStatus(workspaceName, userSubjectId) => pipe(getWorkspaceStatus(workspaceName, userSubjectId)) to sender

    case CreateEntity(workspaceName, entity) => pipe(createEntity(workspaceName, entity)) to sender
    case GetEntity(workspaceName, entityType, entityName) => pipe(getEntity(workspaceName, entityType, entityName)) to sender
    case UpdateEntity(workspaceName, entityType, entityName, operations) => pipe(updateEntity(workspaceName, entityType, entityName, operations)) to sender
    case DeleteEntity(workspaceName, entityType, entityName) => pipe(deleteEntity(workspaceName, entityType, entityName)) to sender
    case RenameEntity(workspaceName, entityType, entityName, newName) => pipe(renameEntity(workspaceName, entityType, entityName, newName)) to sender
    case EvaluateExpression(workspaceName, entityType, entityName, expression) => pipe(evaluateExpression(workspaceName, entityType, entityName, expression)) to sender
    case ListEntityTypes(workspaceName) => pipe(listEntityTypes(workspaceName)) to sender
    case ListEntities(workspaceName, entityType) => pipe(listEntities(workspaceName, entityType)) to sender
    case CopyEntities(entityCopyDefinition, uri: Uri) => pipe(copyEntities(entityCopyDefinition, uri)) to sender
    case BatchUpsertEntities(workspaceName, entityUpdates) => pipe(batchUpdateEntities(workspaceName, entityUpdates, true)) to sender
    case BatchUpdateEntities(workspaceName, entityUpdates) => pipe(batchUpdateEntities(workspaceName, entityUpdates, false)) to sender

    case CreateMethodConfiguration(workspaceName, methodConfiguration) => pipe(createMethodConfiguration(workspaceName, methodConfiguration)) to sender
    case RenameMethodConfiguration(workspaceName, methodConfigurationNamespace, methodConfigurationName, newName) => pipe(renameMethodConfiguration(workspaceName, methodConfigurationNamespace, methodConfigurationName, newName)) to sender
    case DeleteMethodConfiguration(workspaceName, methodConfigurationNamespace, methodConfigurationName) => pipe(deleteMethodConfiguration(workspaceName, methodConfigurationNamespace, methodConfigurationName)) to sender
    case GetMethodConfiguration(workspaceName, methodConfigurationNamespace, methodConfigurationName) => pipe(getMethodConfiguration(workspaceName, methodConfigurationNamespace, methodConfigurationName)) to sender
    case UpdateMethodConfiguration(workspaceName, methodConfiguration) => pipe(updateMethodConfiguration(workspaceName, methodConfiguration)) to sender
    case CopyMethodConfiguration(methodConfigNamePair) => pipe(copyMethodConfiguration(methodConfigNamePair)) to sender
    case CopyMethodConfigurationFromMethodRepo(query) => pipe(copyMethodConfigurationFromMethodRepo(query)) to sender
    case CopyMethodConfigurationToMethodRepo(query) => pipe(copyMethodConfigurationToMethodRepo(query)) to sender
    case ListMethodConfigurations(workspaceName) => pipe(listMethodConfigurations(workspaceName)) to sender
    case CreateMethodConfigurationTemplate( methodRepoMethod: MethodRepoMethod ) => pipe(createMethodConfigurationTemplate(methodRepoMethod)) to sender
    case GetMethodInputsOutputs( methodRepoMethod: MethodRepoMethod ) => pipe(getMethodInputsOutputs(methodRepoMethod)) to sender
    case GetAndValidateMethodConfiguration(workspaceName, methodConfigurationNamespace, methodConfigurationName) => pipe(getAndValidateMethodConfiguration(workspaceName, methodConfigurationNamespace, methodConfigurationName)) to sender

    case ListSubmissions(workspaceName) => pipe(listSubmissions(workspaceName)) to sender
    case CountSubmissions(workspaceName) => pipe(countSubmissions(workspaceName)) to sender
    case CreateSubmission(workspaceName, submission) => pipe(createSubmission(workspaceName, submission)) to sender
    case ValidateSubmission(workspaceName, submission) => pipe(validateSubmission(workspaceName, submission)) to sender
    case GetSubmissionStatus(workspaceName, submissionId) => pipe(getSubmissionStatus(workspaceName, submissionId)) to sender
    case AbortSubmission(workspaceName, submissionId) => pipe(abortSubmission(workspaceName, submissionId)) to sender
    case GetWorkflowOutputs(workspaceName, submissionId, workflowId) => pipe(workflowOutputs(workspaceName, submissionId, workflowId)) to sender
    case GetWorkflowMetadata(workspaceName, submissionId, workflowId) => pipe(workflowMetadata(workspaceName, submissionId, workflowId)) to sender
    case WorkflowQueueStatus => pipe(workflowQueueStatus()) to sender

    case AdminListAllActiveSubmissions => asAdmin { listAllActiveSubmissions() } pipeTo sender
    case AdminAbortSubmission(workspaceNamespace,workspaceName,submissionId) => pipe(adminAbortSubmission(WorkspaceName(workspaceNamespace,workspaceName),submissionId)) to sender

    case HasAllUserReadAccess(workspaceName) => pipe(hasAllUserReadAccess(workspaceName)) to sender
    case GrantAllUserReadAccess(workspaceName) => pipe(grantAllUserReadAccess(workspaceName)) to sender
    case RevokeAllUserReadAccess(workspaceName) => pipe(revokeAllUserReadAccess(workspaceName)) to sender
  }

  def createWorkspace(workspaceRequest: WorkspaceRequest): Future[PerRequestMessage] =
    dataSource.inTransaction { dataAccess =>
      withNewWorkspaceContext(workspaceRequest, dataAccess) { workspaceContext =>
        DBIO.successful(RequestCompleteWithLocation((StatusCodes.Created, workspaceContext.workspace), workspaceRequest.toWorkspaceName.path))
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
              stats <- getWorkspaceSubmissionStats(workspaceContext, dataAccess)
              owners <- getWorkspaceOwners(workspaceContext.workspace, dataAccess)
            } yield {
              RequestComplete(StatusCodes.OK, WorkspaceListResponse(accessLevel, workspaceContext.workspace, stats, owners))
            }
          }
        }
      }
    }

  def getMaximumAccessLevel(user: RawlsUser, workspaceContext: SlickWorkspaceContext, dataAccess: DataAccess): ReadAction[WorkspaceAccessLevel] = {
    val accessLevels = DBIO.sequence(workspaceContext.workspace.realmACLs.map { case (accessLevel, groupRef) =>
      dataAccess.rawlsGroupQuery.loadGroupIfMember(groupRef, user).map {
        case Some(_) => accessLevel
        case None => WorkspaceAccessLevels.NoAccess
      }
    })
    
    accessLevels.map { als =>
      if (als.isEmpty) WorkspaceAccessLevels.NoAccess
      else als.reduce(WorkspaceAccessLevels.max)
    }
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

  def deleteWorkspace(workspaceName: WorkspaceName): Future[PerRequestMessage] =
    dataSource.inTransaction { dataAccess =>
      withWorkspaceContextAndPermissions(workspaceName, WorkspaceAccessLevels.Owner, dataAccess) { workspaceContext =>
        //Attempt to abort any running workflows so they don't write any more to the bucket.
        //Notice that we're kicking off Futures to do the aborts concurrently, but we never collect their results!
        //This is because there's nothing we can do if Cromwell fails, so we might as well move on and let the
        //ExecutionContext run the futures whenever
        dataAccess.submissionQuery.list(workspaceContext).map(_.flatMap(_.workflows).toList collect {
          case wf if !wf.status.isDone && wf.workflowId.isDefined => executionServiceDAO.abort(wf.workflowId.get, userInfo).map {
            case Failure(regrets) =>
              logger.info(s"failure aborting workflow ${wf.workflowId} while deleting workspace ${workspaceName}", regrets)
              Failure(regrets)
            case success => success
          }
        }) andThen {
          DBIO.from(gcsDAO.deleteBucket(workspaceContext.workspace.bucketName, bucketDeletionMonitor))
        } andThen {
          val groupRefs: Set[RawlsGroupRef] = workspaceContext.workspace.accessLevels.values.toSet ++ workspaceContext.workspace.realmACLs.values
          DBIO.seq(groupRefs.map { groupRef => dataAccess.rawlsGroupQuery.load(groupRef) flatMap {
            case Some(group) => DBIO.from(gcsDAO.deleteGoogleGroup(group))
            case None => DBIO.successful(Unit)
          } }.toSeq:_* )
        } andThen {
          DBIO.seq(dataAccess.workspaceQuery.deleteWorkspaceAccessReferences(workspaceContext.workspaceId))
        } andThen {
          DBIO.seq(dataAccess.workspaceQuery.deleteWorkspaceSubmissions(workspaceContext.workspaceId))
        } andThen {
          DBIO.seq(dataAccess.workspaceQuery.deleteWorkspaceMethodConfigs(workspaceContext.workspaceId))
        } andThen {
          DBIO.seq(dataAccess.workspaceQuery.deleteWorkspaceEntityAttributes(workspaceContext.workspaceId))
        } andThen {
          DBIO.seq(dataAccess.workspaceQuery.deleteWorkspaceEntities(workspaceContext.workspaceId))
        } andThen {
          DBIO.seq(workspaceContext.workspace.accessLevels.map { case (_, group) =>
            dataAccess.rawlsGroupQuery.delete(group)
          }.toSeq:_*)
        } andThen {
          dataAccess.workspaceQuery.delete(workspaceName)
        } andThen {
          DBIO.successful(RequestComplete(StatusCodes.Accepted, s"Your Google bucket ${workspaceContext.workspace.bucketName} will be deleted within 24h."))
        }
      }
    }

  def updateWorkspace(workspaceName: WorkspaceName, operations: Seq[AttributeUpdateOperation]): Future[PerRequestMessage] =
    dataSource.inTransaction { dataAccess =>
      withWorkspaceContextAndPermissions(workspaceName, WorkspaceAccessLevels.Write, dataAccess) { workspaceContext =>
        val updateAction = Try {
          val updatedWorkspace = applyOperationsToWorkspace(workspaceContext.workspace, operations)
          dataAccess.workspaceQuery.save(updatedWorkspace)
        } match {
          case Success(result) => result
          case Failure(e: AttributeUpdateOperationException) =>
            DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.BadRequest, s"Unable to update ${workspaceName}", ErrorReport(e))))
          case Failure(regrets) => DBIO.failed(regrets)
        }
        updateAction.map { savedWorkspace =>
          RequestComplete(StatusCodes.OK, savedWorkspace)
        }
      }
    }

  def listWorkspaces(): Future[PerRequestMessage] =
    dataSource.inTransaction { dataAccess =>

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
            def trueAccessLevel = workspace.realm match {
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
    }

  def listWorkspaces(user: RawlsUser, dataAccess: DataAccess): ReadAction[Seq[WorkspacePermissionsPair]] = {
    val rawPairs = for {
      groups <- dataAccess.rawlsGroupQuery.listGroupsForUser(user)
      pairs <- dataAccess.workspaceQuery.listPermissionPairsForGroups(groups)
    } yield pairs

    rawPairs.map { pairs =>
      pairs.groupBy(_.workspaceId).map { case (workspaceId, pairs) =>
        pairs.reduce((a, b) => WorkspacePermissionsPair(workspaceId, WorkspaceAccessLevels.max(a.accessLevel, b.accessLevel)))
      }.toSeq
    }
  }
  
  private def getWorkspaceSubmissionStats(workspaceContext: SlickWorkspaceContext, dataAccess: DataAccess): ReadAction[WorkspaceSubmissionStats] = {
    // listSubmissionSummaryStats works against a sequence of workspaces; we call it just for this one workspace
    dataAccess.workspaceQuery
      .listSubmissionSummaryStats(Seq(workspaceContext.workspaceId))
      .map {p => p.get(workspaceContext.workspaceId).get}
  }

  def cloneWorkspace(sourceWorkspaceName: WorkspaceName, destWorkspaceRequest: WorkspaceRequest): Future[PerRequestMessage] =
    dataSource.inTransaction { dataAccess =>
      withWorkspaceContextAndPermissions(sourceWorkspaceName,WorkspaceAccessLevels.Read, dataAccess) { sourceWorkspaceContext =>
        withClonedRealm(sourceWorkspaceContext, destWorkspaceRequest) { newRealm =>

          // add to or replace current attributes, on an individual basis
          val newAttrs = sourceWorkspaceContext.workspace.attributes ++ destWorkspaceRequest.attributes

          withNewWorkspaceContext(destWorkspaceRequest.copy(realm = newRealm, attributes = newAttrs), dataAccess) { destWorkspaceContext =>
            dataAccess.entityQuery.cloneAllEntities(sourceWorkspaceContext, destWorkspaceContext) andThen
              dataAccess.methodConfigurationQuery.list(sourceWorkspaceContext).flatMap { methodConfigShorts =>
                val inserts = methodConfigShorts.map { methodConfigShort => dataAccess.methodConfigurationQuery.get(sourceWorkspaceContext, methodConfigShort.namespace, methodConfigShort.name).flatMap { methodConfig =>
                  dataAccess.methodConfigurationQuery.save(destWorkspaceContext, methodConfig.get)
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

  private def withClonedRealm(sourceWorkspaceContext: SlickWorkspaceContext, destWorkspaceRequest: WorkspaceRequest)(op: (Option[RawlsGroupRef]) => ReadWriteAction[PerRequestMessage]): ReadWriteAction[PerRequestMessage] = {
    // if the source has a realm, the dest must also have that realm or no realm, and the source realm is applied to the destination
    // otherwise, the caller may choose to apply a realm
    (sourceWorkspaceContext.workspace.realm, destWorkspaceRequest.realm) match {
      case (Some(sourceRealm), Some(destRealm)) if sourceRealm != destRealm =>
        val errorMsg = s"Source workspace ${sourceWorkspaceContext.workspace.briefName} has realm $sourceRealm; cannot change it to $destRealm when cloning"
        DBIO.failed(new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.UnprocessableEntity, errorMsg)))
      case (Some(sourceRealm), _) => op(Option(sourceRealm))
      case (None, destOpt) => op(destOpt)
    }
  }

  def withRawlsUser[T](userRef: RawlsUserRef, dataAccess: DataAccess)(op: (RawlsUser) => ReadWriteAction[T]): ReadWriteAction[T] = {
    dataAccess.rawlsUserQuery.load(userRef) flatMap {
      case Some(user) => op(user)
      case None => DBIO.failed(new RawlsException(s"Couldn't find user for userRef $userRef"))
    }
  }

  def withRawlsGroup[T](groupRef: RawlsGroupRef, dataAccess: DataAccess)(op: (RawlsGroup) => ReadWriteAction[T]): ReadWriteAction[T] = {
    dataAccess.rawlsGroupQuery.load(groupRef) flatMap {
      case Some(group) => op(group)
      case None => DBIO.failed(new RawlsException(s"Couldn't find group for groupRef $groupRef"))
    }
  }

  def getACL(workspaceName: WorkspaceName): Future[PerRequestMessage] =
    dataSource.inTransaction { dataAccess =>
      withWorkspaceContext(workspaceName, dataAccess) { workspaceContext =>
        requireOwnerIgnoreLock(workspaceContext.workspace, dataAccess) {
          dataAccess.workspaceQuery.listEmailsAndAccessLevel(workspaceContext).map { emailsAndAccess =>
            RequestComplete(StatusCodes.OK, emailsAndAccess.sortBy(_._2).reverse.toMap)
          }
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
  def updateACL(workspaceName: WorkspaceName, aclUpdates: Seq[WorkspaceACLUpdate]): Future[PerRequestMessage] =
    dataSource.inTransaction { dataAccess =>
      withWorkspaceContext(workspaceName, dataAccess) { workspaceContext =>
        requireOwnerIgnoreLock(workspaceContext.workspace, dataAccess) {

          //collapse the acl updates list so there are no dupe emails, and convert to RawlsGroup/RawlsUser instances
          DBIO.sequence(aclUpdates.map { aclUpdate =>
            dataAccess.rawlsGroupQuery.loadFromEmail(aclUpdate.email).map((_, aclUpdate.accessLevel))
          }).map(_.collect { case (Some(x), a) => (x, a) }.toMap).flatMap { updateMap =>
            //make a list of all the refs we're going to update
            val allTheRefs: Set[UserAuthRef] = updateMap.map {
              case (Left(rawlsUser:RawlsUser), level) => RawlsUser.toRef(rawlsUser)
              case (Right(rawlsGroup:RawlsGroup), level) => RawlsGroup.toRef(rawlsGroup)
            }.toSet
  
            getMaximumAccessLevel(RawlsUser(userInfo), workspaceContext, dataAccess) flatMap { currentUserAccess =>
              if (allTheRefs.contains(UserService.allUsersGroupRef)) {
                // UserService.allUsersGroupRef cannot be updated in this code path, there is an admin end point for that
                DBIO.failed(new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.BadRequest, s"Please contact an administrator to alter access to ${UserService.allUsersGroupRef.groupName}")))
    
              } else if (!updateMap.get(Left(RawlsUser(userInfo))).forall(_ == currentUserAccess)) {
                // don't allow the user to change their own permissions but let it pass if they are in the list and their current access level
                // is the same as the new value
                DBIO.failed(new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.BadRequest, "You may not change your own permissions")))
    
              } else {
                val userServiceRef = context.actorOf(UserService.props(userServiceConstructor, userInfo))
                val groupsByLevel: Map[WorkspaceAccessLevel, Map[Either[RawlsUser, RawlsGroup], WorkspaceAccessLevel]] = updateMap.groupBy({ case (key, value) => value })
    
                // go through the access level groups on the workspace and update them
                val groupUpdateResults = DBIO.sequence(workspaceContext.workspace.accessLevels.map { case (level, groupRef) =>
                  withRawlsGroup(groupRef, dataAccess) { group =>
                    //remove existing records for users and groups in the acl update list
                    val usersNotChanging = group.users.filter(userRef => !allTheRefs.contains(userRef))
                    val groupsNotChanging = group.subGroups.filter(groupRef => !allTheRefs.contains(groupRef))
  
                    //generate the list of new references
                    val newUsers = groupsByLevel.getOrElse(level, Map.empty).keys.collect({ case Left(ru) => RawlsUser.toRef(ru) })
                    val newgroups = groupsByLevel.getOrElse(level, Map.empty).keys.collect({ case Right(rg) => RawlsGroup.toRef(rg) })
  
                    val groupUpdateFuture = (userServiceRef ? UserService.OverwriteGroupMembers(group, RawlsGroupMemberList(
                      userSubjectIds = Option((usersNotChanging ++ newUsers).map(_.userSubjectId.value).toSeq),
                      subGroupNames = Option((groupsNotChanging ++ newgroups).map(_.groupName.value).toSeq)
                    ))).asInstanceOf[Future[PerRequestMessage]]
                    
                    DBIO.from(groupUpdateFuture)
                  }
                }).map(_.reduce { (prior, next) =>
                  // this reduce will propagate the first non-NoContent (i.e. error) response
                  prior match {
                    case RequestComplete(StatusCodes.NoContent) => next
                    case otherwise => prior
                  }
                })
                
                groupUpdateResults.map {
                  case RequestComplete(StatusCodes.NoContent) =>
                    val emailNotFoundReports = (aclUpdates.map( wau => wau.email ) diff updateMap.keys.map({
                      case Left(rawlsUser:RawlsUser) => rawlsUser.userEmail.value
                      case Right(rawlsGroup:RawlsGroup) => rawlsGroup.groupEmail.value
                    }).toSeq)
                      .map( email => ErrorReport( StatusCodes.NotFound, email ) )
    
                    if (emailNotFoundReports.isEmpty) {
                      RequestComplete(StatusCodes.OK)
                    } else {
                      //this is a case where we don't want to rollback the transaction in the event of getting an error.
                      //we will process the emails that are valid, and report any others as invalid
                      RequestComplete( ErrorReport(StatusCodes.NotFound, s"Couldn't find some users/groups by email", emailNotFoundReports) )
                    }
    
                  case otherwise => otherwise
                }
              }
            }
          }  
        }
      }
    }

  def lockWorkspace(workspaceName: WorkspaceName): Future[PerRequestMessage] =
    dataSource.inTransaction { dataAccess =>
      withWorkspaceContext(workspaceName, dataAccess) { workspaceContext =>
        requireOwnerIgnoreLock(workspaceContext.workspace, dataAccess) {
          dataAccess.submissionQuery.list(workspaceContext).flatMap { submissions =>
            if (!submissions.forall(_.status.isDone)) {
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
        requireOwnerIgnoreLock(workspaceContext.workspace, dataAccess) {
          dataAccess.workspaceQuery.unlock(workspaceContext.workspace.toWorkspaceName).map(_ => RequestComplete(StatusCodes.NoContent))
        }
      }
    }

  def copyEntities(entityCopyDef: EntityCopyDefinition, uri: Uri): Future[PerRequestMessage] =
    dataSource.inTransaction { dataAccess =>
      withWorkspaceContextAndPermissions(entityCopyDef.destinationWorkspace, WorkspaceAccessLevels.Write, dataAccess) { destWorkspaceContext =>
        withWorkspaceContextAndPermissions(entityCopyDef.sourceWorkspace, WorkspaceAccessLevels.Read, dataAccess) { sourceWorkspaceContext =>
          realmCheck(sourceWorkspaceContext, destWorkspaceContext) flatMap { _ =>
            val entityNames = entityCopyDef.entityNames
            val entityType = entityCopyDef.entityType
            val copyResults = dataAccess.entityQuery.copyEntities(sourceWorkspaceContext, destWorkspaceContext, entityType, entityNames)
            copyResults.flatMap(conflicts => conflicts.size match {
              case 0 => {
                // get the entities that were copied into the destination workspace
                dataAccess.entityQuery.list(destWorkspaceContext, entityType).map { allEntities =>
                  val entityCopies = allEntities.filter((e: Entity) => entityNames.contains(e.name)).toList
                  RequestComplete(StatusCodes.Created, entityCopies)
                }
              }
              case _ => {
                val basePath = s"/${destWorkspaceContext.workspace.namespace}/${destWorkspaceContext.workspace.name}/entities/"
                val conflictingUris = conflicts.map(conflict => ErrorReport(uri.copy(path = Uri.Path(basePath + s"${conflict.entityType}/${conflict.name}")).toString(), Seq.empty))
                DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.Conflict, "Unable to copy entities. Some entities already exist.", conflictingUris.toSeq)))
              }
            })
          }
        }
      }
    }

  // can't use withClonedRealm because the Realm -> no Realm logic is different
  private def realmCheck(sourceWorkspaceContext: SlickWorkspaceContext, destWorkspaceContext: SlickWorkspaceContext): ReadWriteAction[Boolean] = {
    // if the source has a realm, the dest must also have that realm
    (sourceWorkspaceContext.workspace.realm, destWorkspaceContext.workspace.realm) match {
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
    dataSource.inTransaction { dataAccess =>
      withWorkspaceContextAndPermissions(workspaceName, WorkspaceAccessLevels.Write, dataAccess) { workspaceContext =>
        dataAccess.entityQuery.get(workspaceContext, entity.entityType, entity.name) flatMap {
          case Some(_) => DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.Conflict, s"${entity.entityType} ${entity.name} already exists in ${workspaceName}")))
          case None => dataAccess.entityQuery.save(workspaceContext, entity).map(e => RequestCompleteWithLocation((StatusCodes.Created, e), entity.path(workspaceName)))
        }
      }
    }

  def batchUpdateEntities(workspaceName: WorkspaceName, entityUpdates: Seq[EntityUpdateDefinition], upsert: Boolean = false): Future[PerRequestMessage] =
    dataSource.inTransaction { dataAccess =>
      withWorkspaceContextAndPermissions(workspaceName, WorkspaceAccessLevels.Write, dataAccess) { workspaceContext =>
        val updateTrialsAction = dataAccess.entityQuery.list(workspaceContext, entityUpdates.map(eu => AttributeEntityReference(eu.entityType, eu.name))) map { entities =>
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
            dataAccess.entityQuery.save(workspaceContext, t )
          }
        }

        saveAction.map(_ => RequestComplete(StatusCodes.NoContent))
      }
    }

  def listEntityTypes(workspaceName: WorkspaceName): Future[PerRequestMessage] =
    dataSource.inTransaction { dataAccess =>
      withWorkspaceContextAndPermissions(workspaceName, WorkspaceAccessLevels.Read, dataAccess) { workspaceContext =>
        dataAccess.entityQuery.getEntityTypesWithCounts(workspaceContext).map(r => RequestComplete(StatusCodes.OK, r))
      }
    }

  def listEntities(workspaceName: WorkspaceName, entityType: String): Future[PerRequestMessage] =
    dataSource.inTransaction { dataAccess =>
      withWorkspaceContextAndPermissions(workspaceName, WorkspaceAccessLevels.Read, dataAccess) { workspaceContext =>
        dataAccess.entityQuery.list(workspaceContext, entityType).map(r => RequestComplete(StatusCodes.OK, r.toSeq))
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

  def deleteEntity(workspaceName: WorkspaceName, entityType: String, entityName: String): Future[PerRequestMessage] =
    dataSource.inTransaction { dataAccess =>
      withWorkspaceContextAndPermissions(workspaceName, WorkspaceAccessLevels.Write, dataAccess) { workspaceContext =>
        withEntity(workspaceContext, entityType, entityName, dataAccess) { entity =>
          dataAccess.entityQuery.delete(workspaceContext, entity.entityType, entity.name).map(_ => RequestComplete(StatusCodes.NoContent))
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
          SlickExpressionEvaluator.withNewExpressionEvaluator(dataAccess, entities) { evaluator =>
            evaluator.evalFinalAttribute(workspaceContext, expression).asTry map { tryValuesByEntity => tryValuesByEntity match {
              //parsing failure
              case Failure(regret) => throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(regret, StatusCodes.BadRequest))
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

  private def applyAttributeUpdateOperations(attributable: Attributable, operations: Seq[AttributeUpdateOperation]): Map[String, Attribute] = {
    operations.foldLeft(attributable.attributes) { (startingAttributes, operation) =>
      operation match {
        case AddUpdateAttribute(attributeName, attribute) => startingAttributes + (attributeName -> attribute)

        case RemoveAttribute(attributeName) => startingAttributes - attributeName

        case AddListMember(attributeListName, newMember) =>
          startingAttributes.get(attributeListName) match {
            case Some(AttributeEmptyList) =>
              newMember match {
                case AttributeNull =>
                  startingAttributes
                case newMember: AttributeValue =>
                  startingAttributes + (attributeListName -> AttributeValueList(Seq(newMember)))
                case newMember: AttributeEntityReference =>
                  startingAttributes + (attributeListName -> AttributeEntityReferenceList(Seq(newMember)))
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
                  startingAttributes + (attributeListName -> AttributeEmptyList)
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

  def saveAndValidateMCExpressions(workspaceContext: SlickWorkspaceContext, methodConfiguration: MethodConfiguration, dataAccess: DataAccess): ReadWriteAction[ValidatedMethodConfiguration] = {
    dataAccess.methodConfigurationQuery.save(workspaceContext, methodConfiguration) map { _ =>
      validateMCExpressions(methodConfiguration, dataAccess)
    }
  }

  def validateMCExpressions(methodConfiguration: MethodConfiguration, dataAccess: DataAccess): ValidatedMethodConfiguration = {
    val parser = dataAccess  // this makes it compile for some reason (as opposed to inlining parser)
    def parseAndPartition(m: Map[String, AttributeString], parseFunc:String => Try[parser.PipelineQuery] ) = {
      val parsed = m map { case (key, attr) => (key, parseFunc(attr.value)) }
      ( parsed collect { case (key, Success(_)) => key } toSeq,
        parsed collect { case (key, Failure(regret)) => (key, regret.getMessage) } )
    }
    val (successInputs, failedInputs) = parseAndPartition(methodConfiguration.inputs, parser.parseAttributeExpr)
    val (successOutputs, failedOutputs) = parseAndPartition(methodConfiguration.outputs, parser.parseOutputExpr)

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

  def createMethodConfiguration(workspaceName: WorkspaceName, methodConfiguration: MethodConfiguration): Future[PerRequestMessage] =
    dataSource.inTransaction { dataAccess =>
      withWorkspaceContextAndPermissions(workspaceName, WorkspaceAccessLevels.Write, dataAccess) { workspaceContext =>
        dataAccess.methodConfigurationQuery.get(workspaceContext, methodConfiguration.namespace, methodConfiguration.name) flatMap {
          case Some(_) => DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.Conflict, s"${methodConfiguration.name} already exists in ${workspaceName}")))
          case None => saveAndValidateMCExpressions(workspaceContext, methodConfiguration, dataAccess)
        } map { validatedMethodConfiguration =>
          RequestCompleteWithLocation((StatusCodes.Created, validatedMethodConfiguration), methodConfiguration.path(workspaceName))
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
          dataAccess.methodConfigurationQuery.get(workspaceContext, methodConfigurationNamespace, newName) flatMap {
            case None =>
              dataAccess.methodConfigurationQuery.rename(workspaceContext, methodConfigurationNamespace, methodConfigurationName, newName)
            case Some(_) => DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.Conflict, s"Destination ${newName} already exists")))
          } map(_ => RequestComplete(StatusCodes.NoContent))
        }
      }
    }

  def updateMethodConfiguration(workspaceName: WorkspaceName, methodConfiguration: MethodConfiguration): Future[PerRequestMessage] =
    dataSource.inTransaction { dataAccess =>
      withWorkspaceContextAndPermissions(workspaceName, WorkspaceAccessLevels.Write, dataAccess) { workspaceContext =>
        dataAccess.methodConfigurationQuery.get(workspaceContext, methodConfiguration.namespace, methodConfiguration.name) flatMap {
          case Some(_) => saveAndValidateMCExpressions(workspaceContext, methodConfiguration, dataAccess)
          case None => DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.NotFound,s"There is no method configuration named ${methodConfiguration.namespace}/${methodConfiguration.name} in ${workspaceName}.")))
        } map(RequestComplete(StatusCodes.OK, _))
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
    dataSource.inTransaction { dataAccess =>
      withWorkspaceContextAndPermissions(mcnp.destination.workspaceName, WorkspaceAccessLevels.Write, dataAccess) { destContext =>
        withWorkspaceContextAndPermissions(mcnp.source.workspaceName, WorkspaceAccessLevels.Read, dataAccess) { sourceContext =>
          dataAccess.methodConfigurationQuery.get(sourceContext, mcnp.source.namespace, mcnp.source.name) flatMap {
            case None => DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.NotFound,s"There is no method configuration named ${mcnp.source.namespace}/${mcnp.source.name} in ${mcnp.source.workspaceName}.")))
            case Some(methodConfig) => saveCopiedMethodConfiguration(methodConfig, mcnp.destination, destContext, dataAccess)
          }
        }
      }
    }
  }

  def copyMethodConfigurationFromMethodRepo(methodRepoQuery: MethodRepoConfigurationImport): Future[PerRequestMessage] =
    dataSource.inTransaction { dataAccess =>
      withWorkspaceContextAndPermissions( methodRepoQuery.destination.workspaceName, WorkspaceAccessLevels.Write, dataAccess) { destContext =>
        DBIO.from(methodRepoDAO.getMethodConfig(methodRepoQuery.methodRepoNamespace, methodRepoQuery.methodRepoName, methodRepoQuery.methodRepoSnapshotId, userInfo)) flatMap { agoraEntityOption =>
          agoraEntityOption match {
            case None => DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.NotFound,s"There is no method configuration named ${methodRepoQuery.methodRepoNamespace}/${methodRepoQuery.methodRepoName}/${methodRepoQuery.methodRepoSnapshotId} in the repository.")))
            case Some(entity) =>
              try {
                // if JSON parsing fails, catch below
                val methodConfig = entity.payload.map(JsonParser(_).convertTo[MethodConfiguration])
                methodConfig match {
                  case Some(targetMethodConfig) => saveCopiedMethodConfiguration(targetMethodConfig, methodRepoQuery.destination, destContext, dataAccess)
                  case None => DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.UnprocessableEntity, "Method Repo missing configuration payload")))
                }
              }
              catch {
                case e: Exception =>
                  DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.UnprocessableEntity, "Error parsing Method Repo response message.", ErrorReport(e))))
              }
          }
        }
      }
    }

  def copyMethodConfigurationToMethodRepo(methodRepoQuery: MethodRepoConfigurationExport): Future[PerRequestMessage] = {
    dataSource.inTransaction { dataAccess =>
      withWorkspaceContextAndPermissions(methodRepoQuery.source.workspaceName, WorkspaceAccessLevels.Read, dataAccess) { workspaceContext =>
        withMethodConfig(workspaceContext, methodRepoQuery.source.namespace, methodRepoQuery.source.name, dataAccess) { methodConfig =>
          import org.broadinstitute.dsde.rawls.model.MethodRepoJsonSupport._
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
      case None => saveAndValidateMCExpressions(destContext, target, dataAccess)
    }.map(validatedTarget => RequestCompleteWithLocation((StatusCodes.Created, validatedTarget), target.path(dest.workspaceName)))
  }

  def listMethodConfigurations(workspaceName: WorkspaceName): Future[PerRequestMessage] =
    dataSource.inTransaction { dataAccess =>
      withWorkspaceContextAndPermissions(workspaceName, WorkspaceAccessLevels.Read, dataAccess) { workspaceContext =>
        dataAccess.methodConfigurationQuery.list(workspaceContext).map(r => RequestComplete(StatusCodes.OK, r.toList))
      }
    }

  def createMethodConfigurationTemplate( methodRepoMethod: MethodRepoMethod ): Future[PerRequestMessage] = {
    dataSource.inTransaction { dataAccess => 
      withMethod(methodRepoMethod.methodNamespace,methodRepoMethod.methodName,methodRepoMethod.methodVersion,userInfo) { method =>
        withWdl(method) { wdl =>
          DBIO.successful(RequestComplete(StatusCodes.OK, MethodConfigResolver.toMethodConfiguration(wdl, methodRepoMethod)))
        }
      }
    }
  }

  def getMethodInputsOutputs( methodRepoMethod: MethodRepoMethod ): Future[PerRequestMessage] = {
    dataSource.inTransaction { dataAccess => 
      withMethod(methodRepoMethod.methodNamespace,methodRepoMethod.methodName,methodRepoMethod.methodVersion,userInfo) { method =>
        withWdl(method) { wdl =>
          DBIO.successful(RequestComplete(StatusCodes.OK, MethodConfigResolver.getMethodInputsOutputs(wdl)))
        }
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
      (dataAccess: DataAccess, workspaceContext: SlickWorkspaceContext, wdl: String, header: SubmissionValidationHeader, successes: Seq[SubmissionValidationEntityInputs], failures: Seq[SubmissionValidationEntityInputs]) =>

        val submissionId: String = UUID.randomUUID().toString

        val workflows = successes map { entityInputs =>
          Workflow(workflowId = None,
            status = WorkflowStatuses.Queued,
            statusLastChangedDate = DateTime.now,
            workflowEntity = Option(AttributeEntityReference(entityType = header.entityType, entityName = entityInputs.entityName)),
            inputResolutions = entityInputs.inputResolutions)
        }
        val workflowFailures = failures.map { entityInputs =>
          val errors = for (entityValue <- entityInputs.inputResolutions if entityValue.error.isDefined) yield (AttributeString(entityValue.error.get))
          WorkflowFailure(entityInputs.entityName, header.entityType, entityInputs.inputResolutions, errors)
        }

        val submission = Submission(submissionId = submissionId,
          submissionDate = DateTime.now(),
          submitter = RawlsUser(userInfo),
          methodConfigurationNamespace = submissionRequest.methodConfigurationNamespace,
          methodConfigurationName = submissionRequest.methodConfigurationName,
          submissionEntity = Option(AttributeEntityReference(entityType = submissionRequest.entityType, entityName = submissionRequest.entityName)),
          workflows = workflows,
          notstarted = workflowFailures,
          status = SubmissionStatuses.Submitted
        )

        dataAccess.submissionQuery.create(workspaceContext, submission) map { _ =>
          RequestComplete(StatusCodes.Created, SubmissionReport(submissionRequest, submission.submissionId, submission.submissionDate, userInfo.userEmail, submission.status, header, successes, failures))
        }
    }

    val credFuture = gcsDAO.getUserCredentials(RawlsUser(userInfo))

    submissionFuture.zip(credFuture) map {
      case (RequestComplete((StatusCodes.Created, submissionReport: SubmissionReport)), Some(credential)) =>
        submissionSupervisor ! SubmissionStarted(workspaceName, UUID.fromString(submissionReport.submissionId), credential)
        RequestComplete(StatusCodes.Created, submissionReport)

      case (somethingWrong, Some(_)) => somethingWrong // this is the case where something was not found in withSubmissionParameters
      case (_, None) => throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.InternalServerError, s"No refresh token found for ${userInfo.userEmail}"))
    }
  }

  def validateSubmission(workspaceName: WorkspaceName, submissionRequest: SubmissionRequest): Future[PerRequestMessage] =
    withSubmissionParameters(workspaceName,submissionRequest) {
      (dataAccess: DataAccess, workspaceContext: SlickWorkspaceContext, wdl: String, header: SubmissionValidationHeader, succeeded: Seq[SubmissionValidationEntityInputs], failed: Seq[SubmissionValidationEntityInputs]) =>
        DBIO.successful(RequestComplete(StatusCodes.OK, SubmissionValidationReport(submissionRequest, header, succeeded, failed)))
    }

  def getSubmissionStatus(workspaceName: WorkspaceName, submissionId: String) = {
    dataSource.inTransaction { dataAccess =>
      withWorkspaceContextAndPermissions(workspaceName, WorkspaceAccessLevels.Read, dataAccess) { workspaceContext =>
        withSubmission(workspaceContext, submissionId, dataAccess) { submission =>
          withRawlsUser(submission.submitter, dataAccess) { user =>
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
      dataAccess.submissionQuery.updateStatus(UUID.fromString(submission.submissionId), SubmissionStatuses.Aborting) flatMap { _ =>
        val aborts = DBIO.from(Future.traverse(submission.workflows.map(_.workflowId).collect { case Some(workflowId) => workflowId })(workflowId =>
          Future.successful(workflowId).zip(executionServiceDAO.abort(workflowId, userInfo))
        ))
  
        aborts.map { abortResults =>
  
          val failures = abortResults map { case (workflowId: String, result: Try[ExecutionServiceStatus]) =>
            (workflowId, result.recover {
              // Forbidden responses means that it is already done which is ok here
              case ure: UnsuccessfulResponseException if ure.response.status == StatusCodes.Forbidden => Success(ExecutionServiceStatus(workflowId, WorkflowStatuses.Aborted.toString))
            })
          } collect {
            case (workflowId: String, Failure(regret)) => (workflowId -> regret)
          }
  
          if (failures.isEmpty) {
            RequestComplete(StatusCodes.NoContent)
          } else {
            val causes = failures map { case (workflowId, throwable) =>
              ErrorReport(s"Unable to abort workflow ${workflowId}", executionServiceDAO.toErrorReport(throwable))
            }
            //if we fail to abort all workflows, we do not want to roll back the transaction. we've done all we can at this point
            RequestComplete(ErrorReport(StatusCodes.BadGateway, s"Unable to abort all workflows for submission ${submissionId}.",causes))
          }
        }
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
        withWorkflow(workspaceName, submissionId, workflowId, dataAccess) { workflow =>
          val outputFTs = toFutureTry(executionServiceDAO.outputs(workflowId, userInfo))
          val logFTs = toFutureTry(executionServiceDAO.logs(workflowId, userInfo))
          DBIO.from(outputFTs zip logFTs map {
            case (Success(outputs), Success(logs)) =>
              mergeWorkflowOutputs(outputs, logs, workflowId)
            case (Failure(outputsFailure), Success(logs)) =>
              throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.BadGateway, s"Unable to get outputs for ${submissionId}.", executionServiceDAO.toErrorReport(outputsFailure)))
            case (Success(outputs), Failure(logsFailure)) =>
              throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.BadGateway, s"Unable to get logs for ${submissionId}.", executionServiceDAO.toErrorReport(logsFailure)))
            case (Failure(outputsFailure), Failure(logsFailure)) =>
              throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.BadGateway, s"Unable to get outputs and unable to get logs for ${submissionId}.",
                Seq(executionServiceDAO.toErrorReport(outputsFailure),executionServiceDAO.toErrorReport(logsFailure))))
          })
        }
      }
    }
  }

  def workflowMetadata(workspaceName: WorkspaceName, submissionId: String, workflowId: String) = {
    dataSource.inTransaction { dataAccess =>
      withWorkspaceContextAndPermissions(workspaceName, WorkspaceAccessLevels.Read, dataAccess) { workspaceContext =>
        withWorkflow(workspaceName, submissionId, workflowId, dataAccess) { workflow =>
          DBIO.from(executionServiceDAO.callLevelMetadata(workflowId, userInfo).map(em => RequestComplete(StatusCodes.OK, em)))
        }
      }
    }
  }

  def workflowQueueStatus() = {
    dataSource.inTransaction { dataAccess =>
      dataAccess.workflowQuery.countWorkflowsByQueueStatus.map(RequestComplete(StatusCodes.OK, _))
    }
  }

  def listAllActiveSubmissions() = {
    dataSource.inTransaction { dataAccess =>
      dataAccess.submissionQuery.listAllActiveSubmissions().map(RequestComplete(StatusCodes.OK, _))
    }
  }

  def adminAbortSubmission(workspaceName: WorkspaceName, submissionId: String) = {
    asAdmin {
      dataSource.inTransaction { dataAccess =>
        withWorkspaceContext(workspaceName, dataAccess) { workspaceContext =>
          abortSubmission(workspaceContext, submissionId, dataAccess)
        }
      }
    }
  }

  def hasAllUserReadAccess(workspaceName: WorkspaceName): Future[PerRequestMessage] = {
    asAdmin {
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
    asAdmin {
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
    asAdmin {
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
    asAdmin {
      dataSource.inTransaction { dataAccess =>
        dataAccess.workspaceQuery.listAll.map(RequestComplete(StatusCodes.OK, _))
      }
    }
  }

  // this function is a bit naughty because it waits for database results
  // this is acceptable because it is a seldom used admin functions, not part of the mainstream
  // refactoring it to do the propper database action chaining is too much work at this time
  def getWorkspaceStatus(workspaceName: WorkspaceName, userSubjectId: Option[String]): Future[PerRequestMessage] = {
    def run[T](action: DataAccess => ReadWriteAction[T]): T = {
      Await.result(dataSource.inTransaction { dataAccess => action(dataAccess) }, 10 seconds)
    }
    
    asAdmin {
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
      val rawlsIntersectionGroupRefs = workspace.realmACLs
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

  // helper methods

  private def withNewWorkspaceContext(workspaceRequest: WorkspaceRequest, dataAccess: DataAccess)
                                     (op: (SlickWorkspaceContext) => ReadWriteAction[PerRequestMessage]): ReadWriteAction[PerRequestMessage] = {
    requireCreateWorkspaceAccess(workspaceRequest, dataAccess) {
      dataAccess.workspaceQuery.findByName(workspaceRequest.toWorkspaceName) flatMap {
        case Some(_) => DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.Conflict, s"Workspace ${workspaceRequest.namespace}/${workspaceRequest.name} already exists")))
        case None =>
          val workspaceId = UUID.randomUUID.toString
          DBIO.from(gcsDAO.setupWorkspace(userInfo, workspaceRequest.namespace, workspaceId, workspaceRequest.toWorkspaceName, workspaceRequest.realm)) flatMap { googleWorkspaceInfo =>
            val currentDate = DateTime.now

            val accessGroups = googleWorkspaceInfo.accessGroupsByLevel.map { case (a, g) => (a -> RawlsGroup.toRef(g))}
            val intersectionGroups = googleWorkspaceInfo.intersectionGroupsByLevel map { _.map { case (a, g) => (a -> RawlsGroup.toRef(g))} }

            val workspace = Workspace(
              namespace = workspaceRequest.namespace,
              name = workspaceRequest.name,
              realm = workspaceRequest.realm,
              workspaceId = workspaceId,
              bucketName = googleWorkspaceInfo.bucketName,
              createdDate = currentDate,
              lastModified = currentDate,
              createdBy = userInfo.userEmail,
              attributes = workspaceRequest.attributes,
              accessLevels = accessGroups,
              realmACLs = intersectionGroups getOrElse accessGroups
            )

            val groupInserts =
              googleWorkspaceInfo.accessGroupsByLevel.values.map(dataAccess.rawlsGroupQuery.save) ++
              googleWorkspaceInfo.intersectionGroupsByLevel.map(_.values.map(dataAccess.rawlsGroupQuery.save)).getOrElse(Seq.empty)

            DBIO.seq(groupInserts.toSeq:_*) andThen
              dataAccess.workspaceQuery.save(workspace).flatMap(ws => op(SlickWorkspaceContext(ws)))
          }
      }
    }
  }

  private def noSuchWorkspaceMessage(workspaceName: WorkspaceName) = s"${workspaceName} does not exist"
  private def accessDeniedMessage(workspaceName: WorkspaceName) = s"insufficient permissions to perform operation on ${workspaceName}"

  private def requireCreateWorkspaceAccess(workspaceRequest: WorkspaceRequest, dataAccess: DataAccess)(op: => ReadWriteAction[PerRequestMessage]): ReadWriteAction[PerRequestMessage] = {
    dataAccess.rawlsBillingProjectQuery.load(RawlsBillingProjectName(workspaceRequest.toWorkspaceName.namespace)) flatMap {
      case Some(billingProject) =>
        if (billingProject.users.contains(RawlsUser(userInfo))) {
          workspaceRequest.realm match {
            case Some(realm) => dataAccess.rawlsGroupQuery.loadGroupIfMember(realm, RawlsUser(userInfo)) flatMap {
              case None => DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.Forbidden, s"You cannot create a workspace in realm [${realm.groupName.value}] as you do not have access to it.")))
              case Some(_) => op
            }
            case None => op
          }
        } else {
          DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.Forbidden, s"You are not authorized to create a workspace in billing project ${workspaceRequest.toWorkspaceName.namespace}")))
        }
      case None => DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.BadRequest, s"billing project ${workspaceRequest.toWorkspaceName.namespace} not found")))
    }
  }

  private def withWorkspaceContextAndPermissions(workspaceName: WorkspaceName, accessLevel: WorkspaceAccessLevel, dataAccess: DataAccess)(op: (SlickWorkspaceContext) => ReadWriteAction[PerRequestMessage]): ReadWriteAction[PerRequestMessage] = {
    withWorkspaceContext(workspaceName, dataAccess) { workspaceContext =>
      requireAccess(workspaceContext.workspace, accessLevel, dataAccess) { op(workspaceContext) }
    }
  }

  private def withWorkspaceContext(workspaceName: WorkspaceName, dataAccess: DataAccess)(op: (SlickWorkspaceContext) => ReadWriteAction[PerRequestMessage]) = {
    dataAccess.workspaceQuery.findByName(workspaceName) flatMap {
      case None => DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.NotFound, noSuchWorkspaceMessage(workspaceName))))
      case Some(workspace) => op(SlickWorkspaceContext(workspace))
    }
  }

  private def requireAccess(workspace: Workspace, requiredLevel: WorkspaceAccessLevel, dataAccess: DataAccess)(codeBlock: => ReadWriteAction[PerRequestMessage]): ReadWriteAction[PerRequestMessage] = {
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

  private def requireOwnerIgnoreLock(workspace: Workspace, dataAccess: DataAccess)(op: => ReadWriteAction[PerRequestMessage]): ReadWriteAction[PerRequestMessage] = {
    requireAccess(workspace.copy(isLocked = false),WorkspaceAccessLevels.Owner, dataAccess)(op)
  }

  private def withEntity(workspaceContext: SlickWorkspaceContext, entityType: String, entityName: String, dataAccess: DataAccess)(op: (Entity) => ReadWriteAction[PerRequestMessage]): ReadWriteAction[PerRequestMessage] = {
    dataAccess.entityQuery.get(workspaceContext, entityType, entityName) flatMap {
      case None => DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.NotFound, s"${entityType} ${entityName} does not exist in ${workspaceContext}")))
      case Some(entity) => op(entity)
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
          case None => DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.NotFound, s"Submission with id ${submissionId} not found in workspace ${workspaceContext}")))
          case Some(submission) => op(submission)
        }
    }
  }

  private def withWorkflow(workspaceName: WorkspaceName, submissionId: String, workflowId: String, dataAccess: DataAccess)(op: (Workflow) => ReadWriteAction[PerRequestMessage]): ReadWriteAction[PerRequestMessage] = {
    dataAccess.workflowQuery.getByExternalId(workflowId, submissionId) flatMap {
      case None => DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.NotFound, s"Workflow with id ${workflowId} not found in submission ${submissionId} in workspace ${workspaceName.namespace}/${workspaceName.name}")))
      case Some(workflow) => op(workflow)
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
        SlickExpressionEvaluator.withNewExpressionEvaluator(dataAccess, workspaceContext, submissionRequest.entityType, submissionRequest.entityName) { evaluator =>
          evaluator.evalFinalEntity(workspaceContext, expression).asTry
        } flatMap { //gotta close out the expression evaluator to wipe the EXPREVAL_TEMP table
          case Failure(regret) => DBIO.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(regret, StatusCodes.BadRequest)))
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

  private def withSubmissionParameters(workspaceName: WorkspaceName, submissionRequest: SubmissionRequest)
   ( op: (DataAccess, SlickWorkspaceContext, String, SubmissionValidationHeader, Seq[SubmissionValidationEntityInputs], Seq[SubmissionValidationEntityInputs]) => ReadWriteAction[PerRequestMessage]): Future[PerRequestMessage] = {
    dataSource.inTransaction { dataAccess =>
      withWorkspaceContextAndPermissions(workspaceName, WorkspaceAccessLevels.Write, dataAccess) { workspaceContext =>
        withMethodConfig(workspaceContext, submissionRequest.methodConfigurationNamespace, submissionRequest.methodConfigurationName, dataAccess) { methodConfig =>
          withMethodInputs(methodConfig, userInfo) { (wdl,methodInputs) =>
            withSubmissionEntityRecs(submissionRequest, workspaceContext, methodConfig.rootEntityType, dataAccess) { jobEntityRecs =>

              //Parse out the entity -> results map to a tuple of (successful, failed) SubmissionValidationEntityInputs
              MethodConfigResolver.resolveInputsForEntities(workspaceContext, methodInputs, jobEntityRecs, dataAccess) flatMap { valuesByEntity =>
                valuesByEntity
                  .map({ case (entityName, values) => SubmissionValidationEntityInputs(entityName, values) })
                  .partition({ entityInputs => entityInputs.inputResolutions.forall(_.error.isEmpty) }) match {
                    case(succeeded, failed) =>
                      val methodConfigInputs = methodInputs.map { methodInput => SubmissionValidationInput(methodInput.workflowInput.fqn, methodInput.expression) }
                      val header = SubmissionValidationHeader(methodConfig.rootEntityType, methodConfigInputs)
                      op(dataAccess, workspaceContext, wdl, header, succeeded.toSeq, failed.toSeq)
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
