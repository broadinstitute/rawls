package org.broadinstitute.dsde.rawls.workspace

import java.util.UUID

import akka.actor.SupervisorStrategy.{Escalate, Stop}
import akka.actor._
import akka.pattern._
import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.jobexec.MethodConfigResolver
import org.broadinstitute.dsde.rawls.jobexec.MethodConfigResolver.MethodInput
import org.broadinstitute.dsde.rawls.jobexec.SubmissionSupervisor.SubmissionStarted
import org.broadinstitute.dsde.rawls.model.WorkspaceACLJsonSupport.WorkspaceACLFormat
import org.broadinstitute.dsde.rawls.model.WorkspaceAccessLevels.WorkspaceAccessLevel
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._
import org.broadinstitute.dsde.rawls.model.ExecutionJsonSupport.ActiveSubmissionFormat
import org.broadinstitute.dsde.rawls.model.ExecutionJsonSupport.ExecutionMetadataFormat
import org.broadinstitute.dsde.rawls.model.ExecutionJsonSupport.SubmissionFormat
import org.broadinstitute.dsde.rawls.model.ExecutionJsonSupport.SubmissionReportFormat
import org.broadinstitute.dsde.rawls.model.ExecutionJsonSupport.SubmissionValidationReportFormat
import org.broadinstitute.dsde.rawls.model.ExecutionJsonSupport.WorkflowOutputsFormat
import org.broadinstitute.dsde.rawls.model.ExecutionJsonSupport.ExecutionServiceValidationFormat
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.expressions._
import org.broadinstitute.dsde.rawls.webservice.PerRequest
import org.broadinstitute.dsde.rawls.webservice.PerRequest.{RequestCompleteWithLocation, PerRequestMessage, RequestComplete}
import AttributeUpdateOperations._
import org.broadinstitute.dsde.rawls.workspace.WorkspaceService._
import org.joda.time.DateTime
import spray.http
import spray.http.Uri
import spray.http.StatusCodes
import spray.httpx.SprayJsonSupport._
import spray.httpx.UnsuccessfulResponseException
import spray.json._

import scala.annotation.tailrec
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

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
  case class CloneWorkspace(sourceWorkspace: WorkspaceName, destWorkspace: WorkspaceName) extends WorkspaceServiceMessage
  case class GetACL(workspaceName: WorkspaceName) extends WorkspaceServiceMessage
  case class UpdateACL(workspaceName: WorkspaceName, aclUpdates: Seq[WorkspaceACLUpdate]) extends WorkspaceServiceMessage
  case class LockWorkspace(workspaceName: WorkspaceName) extends WorkspaceServiceMessage
  case class UnlockWorkspace(workspaceName: WorkspaceName) extends WorkspaceServiceMessage

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
  case class GetAndValidateMethodConfiguration(workspaceName: WorkspaceName, methodConfigurationNamespace: String, methodConfigurationName: String) extends WorkspaceServiceMessage

  case class ListSubmissions(workspaceName: WorkspaceName) extends WorkspaceServiceMessage
  case class CreateSubmission(workspaceName: WorkspaceName, submission: SubmissionRequest) extends WorkspaceServiceMessage
  case class ValidateSubmission(workspaceName: WorkspaceName, submission: SubmissionRequest) extends WorkspaceServiceMessage
  case class GetSubmissionStatus(workspaceName: WorkspaceName, submissionId: String) extends WorkspaceServiceMessage
  case class AbortSubmission(workspaceName: WorkspaceName, submissionId: String) extends WorkspaceServiceMessage
  case class GetWorkflowOutputs(workspaceName: WorkspaceName, submissionId: String, workflowId: String) extends WorkspaceServiceMessage
  case class GetWorkflowMetadata(workspaceName: WorkspaceName, submissionId: String, workflowId: String) extends WorkspaceServiceMessage

  case object ListAdmins extends WorkspaceServiceMessage
  case class IsAdmin(userId: String) extends WorkspaceServiceMessage
  case class AddAdmin(userId: String) extends WorkspaceServiceMessage
  case class DeleteAdmin(userId: String) extends WorkspaceServiceMessage
  case object ListAllActiveSubmissions extends WorkspaceServiceMessage
  case class AdminAbortSubmission(workspaceNamespace: String, workspaceName: String, submissionId: String) extends WorkspaceServiceMessage

  def props(workspaceServiceConstructor: UserInfo => WorkspaceService, userInfo: UserInfo): Props = {
    Props(workspaceServiceConstructor(userInfo))
  }

  def constructor(dataSource: DataSource, containerDAO: GraphContainerDAO, methodRepoDAO: MethodRepoDAO, executionServiceDAO: ExecutionServiceDAO, gcsDAO: GoogleServicesDAO, submissionSupervisor : ActorRef)(userInfo: UserInfo)(implicit executionContext: ExecutionContext) =
    new WorkspaceService(userInfo, dataSource, containerDAO, methodRepoDAO, executionServiceDAO, gcsDAO, submissionSupervisor)
}

class WorkspaceService(userInfo: UserInfo, dataSource: DataSource, containerDAO: GraphContainerDAO, methodRepoDAO: MethodRepoDAO, executionServiceDAO: ExecutionServiceDAO, gcsDAO: GoogleServicesDAO, submissionSupervisor : ActorRef)(implicit executionContext: ExecutionContext) extends Actor {
  override def receive = {
    case CreateWorkspace(workspace) => pipe(createWorkspace(workspace)) to context.parent
    case GetWorkspace(workspaceName) => pipe(getWorkspace(workspaceName)) to context.parent
    case DeleteWorkspace(workspaceName) => pipe(deleteWorkspace(workspaceName)) to context.parent
    case UpdateWorkspace(workspaceName, operations) => pipe(updateWorkspace(workspaceName, operations)) to context.parent
    case ListWorkspaces => pipe(listWorkspaces()) to context.parent
    case CloneWorkspace(sourceWorkspace, destWorkspace) => pipe(cloneWorkspace(sourceWorkspace, destWorkspace)) to context.parent
    case GetACL(workspaceName) => pipe(getACL(workspaceName)) to context.parent
    case UpdateACL(workspaceName, aclUpdates) => pipe(updateACL(workspaceName, aclUpdates)) to context.parent
    case LockWorkspace(workspaceName: WorkspaceName) => pipe(lockWorkspace(workspaceName)) to context.parent
    case UnlockWorkspace(workspaceName: WorkspaceName) => pipe(unlockWorkspace(workspaceName)) to context.parent

    case CreateEntity(workspaceName, entity) => pipe(createEntity(workspaceName, entity)) to context.parent
    case GetEntity(workspaceName, entityType, entityName) => pipe(getEntity(workspaceName, entityType, entityName)) to context.parent
    case UpdateEntity(workspaceName, entityType, entityName, operations) => pipe(updateEntity(workspaceName, entityType, entityName, operations)) to context.parent
    case DeleteEntity(workspaceName, entityType, entityName) => pipe(deleteEntity(workspaceName, entityType, entityName)) to context.parent
    case RenameEntity(workspaceName, entityType, entityName, newName) => pipe(renameEntity(workspaceName, entityType, entityName, newName)) to context.parent
    case EvaluateExpression(workspaceName, entityType, entityName, expression) => pipe(evaluateExpression(workspaceName, entityType, entityName, expression)) to context.parent
    case ListEntityTypes(workspaceName) => pipe(listEntityTypes(workspaceName)) to context.parent
    case ListEntities(workspaceName, entityType) => pipe(listEntities(workspaceName, entityType)) to context.parent
    case CopyEntities(entityCopyDefinition, uri: Uri) => pipe(copyEntities(entityCopyDefinition, uri)) to context.parent
    case BatchUpsertEntities(workspaceName, entityUpdates) => pipe(batchUpsertEntities(workspaceName, entityUpdates)) to context.parent
    case BatchUpdateEntities(workspaceName, entityUpdates) => pipe(batchUpdateEntities(workspaceName, entityUpdates)) to context.parent

    case CreateMethodConfiguration(workspaceName, methodConfiguration) => pipe(createMethodConfiguration(workspaceName, methodConfiguration)) to context.parent
    case RenameMethodConfiguration(workspaceName, methodConfigurationNamespace, methodConfigurationName, newName) => pipe(renameMethodConfiguration(workspaceName, methodConfigurationNamespace, methodConfigurationName, newName)) to context.parent
    case DeleteMethodConfiguration(workspaceName, methodConfigurationNamespace, methodConfigurationName) => pipe(deleteMethodConfiguration(workspaceName, methodConfigurationNamespace, methodConfigurationName)) to context.parent
    case GetMethodConfiguration(workspaceName, methodConfigurationNamespace, methodConfigurationName) => pipe(getMethodConfiguration(workspaceName, methodConfigurationNamespace, methodConfigurationName)) to context.parent
    case UpdateMethodConfiguration(workspaceName, methodConfiguration) => pipe(updateMethodConfiguration(workspaceName, methodConfiguration)) to context.parent
    case CopyMethodConfiguration(methodConfigNamePair) => pipe(copyMethodConfiguration(methodConfigNamePair)) to context.parent
    case CopyMethodConfigurationFromMethodRepo(query) => pipe(copyMethodConfigurationFromMethodRepo(query)) to context.parent
    case CopyMethodConfigurationToMethodRepo(query) => pipe(copyMethodConfigurationToMethodRepo(query)) to context.parent
    case ListMethodConfigurations(workspaceName) => pipe(listMethodConfigurations(workspaceName)) to context.parent
    case CreateMethodConfigurationTemplate( methodRepoMethod: MethodRepoMethod ) => pipe(createMethodConfigurationTemplate(methodRepoMethod)) to context.parent
    case GetAndValidateMethodConfiguration(workspaceName, methodConfigurationNamespace, methodConfigurationName) => pipe(getAndValidateMethodConfiguration(workspaceName, methodConfigurationNamespace, methodConfigurationName)) to context.parent

    case ListSubmissions(workspaceName) => pipe(listSubmissions(workspaceName)) to context.parent
    case CreateSubmission(workspaceName, submission) => pipe(createSubmission(workspaceName, submission)) to context.parent
    case ValidateSubmission(workspaceName, submission) => pipe(validateSubmission(workspaceName, submission)) to context.parent
    case GetSubmissionStatus(workspaceName, submissionId) => pipe(getSubmissionStatus(workspaceName, submissionId)) to context.parent
    case AbortSubmission(workspaceName, submissionId) => pipe(abortSubmission(workspaceName, submissionId)) to context.parent
    case GetWorkflowOutputs(workspaceName, submissionId, workflowId) => pipe(workflowOutputs(workspaceName, submissionId, workflowId)) to context.parent
    case GetWorkflowMetadata(workspaceName, submissionId, workflowId) => pipe(workflowMetadata(workspaceName, submissionId, workflowId)) to context.parent

    case ListAdmins => pipe(listAdmins()) to context.parent
    case IsAdmin(userId) => pipe(isAdmin(userId)) to context.parent
    case AddAdmin(userId) => pipe(addAdmin(userId)) to context.parent
    case DeleteAdmin(userId) => pipe(deleteAdmin(userId)) to context.parent
    case ListAllActiveSubmissions => pipe(listAllActiveSubmissions()) to context.parent
    case AdminAbortSubmission(workspaceNamespace,workspaceName,submissionId) => pipe(adminAbortSubmission(WorkspaceName(workspaceNamespace,workspaceName),submissionId)) to context.parent
  }

  def createWorkspace(workspaceRequest: WorkspaceRequest): Future[PerRequestMessage] =
    dataSource.inFutureTransaction(writeLocks=Set(workspaceRequest.toWorkspaceName)) { txn =>
      withNewWorkspaceContext(workspaceRequest, txn) { workspaceContext =>
        RequestCompleteWithLocation((StatusCodes.Created, workspaceContext.workspace), workspaceRequest.toWorkspaceName.path)
      }
    }

  def getWorkspace(workspaceName: WorkspaceName): Future[PerRequestMessage] =
    dataSource.inFutureTransaction(readLocks=Set(workspaceName)) { txn =>
      withWorkspaceContext(workspaceName, txn) { workspaceContext =>
        gcsDAO.getMaximumAccessLevel(userInfo.userEmail,workspaceContext.workspace.workspaceId) flatMap { accessLevel =>
          if (accessLevel < WorkspaceAccessLevels.Read)
            Future.successful(RequestComplete(ErrorReport(StatusCodes.NotFound, noSuchWorkspaceMessage(workspaceName))))
          else {
            gcsDAO.getOwners(workspaceContext.workspace.workspaceId) map { owners =>
              val response = WorkspaceListResponse(accessLevel,
                workspaceContext.workspace,
                getWorkspaceSubmissionStats(workspaceContext, txn),
                owners)
              RequestComplete(StatusCodes.OK, response)
            }
          }
        }
      }
    }

  def deleteWorkspace(workspaceName: WorkspaceName): Future[PerRequestMessage] =
    dataSource.inFutureTransaction(writeLocks=Set(workspaceName)) { txn =>
      withWorkspaceContextAndPermissions(workspaceName, WorkspaceAccessLevels.Owner, txn) { workspaceContext =>
        //Attempt to abort any running workflows so they don't write any more to the bucket.
        //Notice that we're kicking off Futures to do the aborts concurrently, but we never collect their results!
        //This is because there's nothing we can do if Cromwell fails, so we might as well move on and let the
        //ExecutionContext run the futures whenever
        containerDAO.submissionDAO.list(workspaceContext, txn).flatMap(_.workflows).toList collect {
          case wf if !wf.status.isDone => Future { executionServiceDAO.abort(wf.workflowId, userInfo) }
        }

        gcsDAO.deleteBucket(userInfo, workspaceContext.workspace.workspaceId).map { _ =>
          containerDAO.workspaceDAO.delete(workspaceName, txn)

          RequestComplete(StatusCodes.Accepted, s"Your Google bucket ${gcsDAO.getBucketName(workspaceContext.workspace.workspaceId)} will be deleted within 24h.")
        }
      }
    }

  def updateWorkspace(workspaceName: WorkspaceName, operations: Seq[AttributeUpdateOperation]): Future[PerRequestMessage] =
    dataSource.inFutureTransaction(writeLocks=Set(workspaceName)) { txn =>
      withWorkspaceContextAndPermissions(workspaceName, WorkspaceAccessLevels.Write, txn) { workspaceContext =>
        Future {
          try {
            val updatedWorkspace = applyOperationsToWorkspace(workspaceContext.workspace, operations)
            RequestComplete(StatusCodes.OK, containerDAO.workspaceDAO.save(updatedWorkspace, txn).workspace)
          } catch {
            case e: AttributeUpdateOperationException => {
              txn.setRollbackOnly()
              RequestComplete(ErrorReport(StatusCodes.BadRequest, s"Unable to update ${workspaceName}", ErrorReport(e)))
            }
          }
        }
      }
    }

  def listWorkspaces(): Future[PerRequestMessage] = {
    // this one is a little special:
    // 1 query to get the accessible workspace
    // then for each result in parallel 1 query to get the owners and 1 query to get details out of the db
    // last, join those parallel operations to a single result

    // 1 query to gcsDAO to list all the workspaces
    val permissionsPairs = dataSource.inTransaction() { txn =>
      containerDAO.authDAO.listWorkspaces(userInfo.userSubjectId, txn)
    }

    // Future.sequence will do everything within in parallel per workspace then join them all
    val eventualResponses = Future.sequence(permissionsPairs map { permissionsPair =>
      // query to get owners
      gcsDAO.getOwners(permissionsPair.workspaceId).zip(Future.successful(permissionsPair))
    } map { ownersAndPermissionsPairs =>
      for ((owners, permissionsPair) <- ownersAndPermissionsPairs) yield {
        // database query to get details
        dataSource.inTransaction() { txn =>
          containerDAO.workspaceDAO.findById(permissionsPair.workspaceId, txn) match {
            case Some(workspaceContext) =>
              Option(WorkspaceListResponse(permissionsPair.accessLevel,
                workspaceContext.workspace,
                getWorkspaceSubmissionStats(workspaceContext, txn),
                owners)
              )
            case None =>
              // this case will happen when permissions exist for workspaces that don't, use None here and ignore later
              None
          }
        }
      }
    })

    // collect the result
    eventualResponses map (r => RequestComplete(StatusCodes.OK, r.collect { case Some(x) => x }))
  }

  private def getWorkspaceSubmissionStats(workspaceContext: WorkspaceContext, txn: RawlsTransaction): WorkspaceSubmissionStats = {
    val submissions = containerDAO.submissionDAO.list(workspaceContext, txn)

    val workflowsOrderedByDateDesc = submissions.flatMap(_.workflows).toVector.sortWith { (first, second) =>
      first.statusLastChangedDate.isAfter(second.statusLastChangedDate)
    }

    WorkspaceSubmissionStats(
      lastSuccessDate = workflowsOrderedByDateDesc.find(_.status == WorkflowStatuses.Succeeded).map(_.statusLastChangedDate),
      lastFailureDate = workflowsOrderedByDateDesc.find(_.status == WorkflowStatuses.Failed).map(_.statusLastChangedDate),
      runningSubmissionsCount = submissions.count(_.status == SubmissionStatuses.Submitted)
    )
  }

  def cloneWorkspace(sourceWorkspaceName: WorkspaceName, destWorkspaceName: WorkspaceName): Future[PerRequestMessage] =
    dataSource.inFutureTransaction(readLocks=Set(sourceWorkspaceName), writeLocks=Set(destWorkspaceName)) { txn =>
      withWorkspaceContextAndPermissions(sourceWorkspaceName,WorkspaceAccessLevels.Read,txn) { sourceWorkspaceContext =>
        withNewWorkspaceContext(WorkspaceRequest(destWorkspaceName.namespace,destWorkspaceName.name,sourceWorkspaceContext.workspace.attributes),txn) { destWorkspaceContext =>
          containerDAO.entityDAO.cloneAllEntities(sourceWorkspaceContext, destWorkspaceContext, txn)
          // TODO add a method for cloning all method configs, instead of doing this
          containerDAO.methodConfigurationDAO.list(sourceWorkspaceContext, txn).foreach { methodConfig =>
            containerDAO.methodConfigurationDAO.save(destWorkspaceContext,
              containerDAO.methodConfigurationDAO.get(sourceWorkspaceContext, methodConfig.namespace, methodConfig.name, txn).get, txn)
          }
          RequestCompleteWithLocation((StatusCodes.Created, destWorkspaceContext.workspace), destWorkspaceName.path)
        }
      }
    }

  def getACL(workspaceName: WorkspaceName): Future[PerRequestMessage] =
    dataSource.inFutureTransaction(readLocks=Set(workspaceName)) { txn =>
      withWorkspaceContext(workspaceName, txn) { workspaceContext =>
        requireOwnerIgnoreLock(workspaceContext.workspace) {
          gcsDAO.getACL(workspaceContext.workspace.workspaceId) map { RequestComplete(StatusCodes.OK,_) }
        }
      }
    }

  def updateACL(workspaceName: WorkspaceName, aclUpdates: Seq[WorkspaceACLUpdate]): Future[PerRequestMessage] =
    dataSource.inFutureTransaction(writeLocks=Set(workspaceName)) { txn =>
      withWorkspaceContext(workspaceName, txn) { workspaceContext =>
        requireOwnerIgnoreLock(workspaceContext.workspace) {
          gcsDAO.updateACL(userInfo.userEmail, workspaceContext.workspace.workspaceId, aclUpdates).map( _ match {
            case None => RequestComplete(StatusCodes.OK)
            case Some(reports) => RequestComplete(ErrorReport(StatusCodes.Conflict,"Unable to alter some ACLs in $workspaceName",reports))
          } )
        }
      }
    }

  def lockWorkspace(workspaceName: WorkspaceName): Future[PerRequestMessage] =
    dataSource.inFutureTransaction(writeLocks=Set(workspaceName)) { txn =>
      withWorkspaceContext(workspaceName, txn) { workspaceContext =>
        requireOwnerIgnoreLock(workspaceContext.workspace) {
          Future {
            if ( !containerDAO.submissionDAO.list(workspaceContext,txn).forall(_.status.isDone) )
              RequestComplete(ErrorReport(StatusCodes.Conflict,s"There are running submissions in workspace $workspaceName, so it cannot be locked."))
            else {
              if (!workspaceContext.workspace.isLocked)
                containerDAO.workspaceDAO.save(workspaceContext.workspace.copy(isLocked = true), txn)
              RequestComplete(StatusCodes.NoContent)
            }
          }
        }
      }
    }

  def unlockWorkspace(workspaceName: WorkspaceName): Future[PerRequestMessage] =
    dataSource.inFutureTransaction(writeLocks=Set(workspaceName)) { txn =>
      withWorkspaceContext(workspaceName, txn) { workspaceContext =>
        requireOwnerIgnoreLock(workspaceContext.workspace) {
          Future {
            if ( workspaceContext.workspace.isLocked ) {
              containerDAO.workspaceDAO.save(workspaceContext.workspace.copy(isLocked = false), txn)
            }
            RequestComplete(StatusCodes.NoContent)
          }
        }
      }
    }

  def copyEntities(entityCopyDef: EntityCopyDefinition, uri: Uri): Future[PerRequestMessage] =
    dataSource.inFutureTransaction(readLocks=Set(entityCopyDef.sourceWorkspace), writeLocks=Set(entityCopyDef.destinationWorkspace)) { txn =>
      //NOTE: Order here is important. If the src and dest workspaces are the same, we need to get the write lock first, since
      //we can't upgrade a read lock to a write.
      withWorkspaceContextAndPermissions(entityCopyDef.destinationWorkspace, WorkspaceAccessLevels.Write, txn) { destWorkspaceContext =>
        withWorkspaceContextAndPermissions(entityCopyDef.sourceWorkspace, WorkspaceAccessLevels.Read, txn) { sourceWorkspaceContext =>
          Future {
            val entityNames = entityCopyDef.entityNames
            val entityType = entityCopyDef.entityType
            val conflicts = containerDAO.entityDAO.copyEntities(sourceWorkspaceContext, destWorkspaceContext, entityType, entityNames, txn)
            conflicts.size match {
              case 0 => {
                // get the entities that were copied into the destination workspace
                val entityCopies = containerDAO.entityDAO.list(destWorkspaceContext, entityType, txn).filter((e: Entity) => entityNames.contains(e.name)).toList
                RequestComplete(StatusCodes.Created, entityCopies)
              }
              case _ => {
                val basePath = s"/${destWorkspaceContext.workspace.namespace}/${destWorkspaceContext.workspace.name}/entities/"
                val conflictingUris = conflicts.map(conflict => ErrorReport(uri.copy(path = Uri.Path(basePath + s"${conflict.entityType}/${conflict.name}")).toString(),Seq.empty))
                RequestComplete(ErrorReport(StatusCodes.Conflict, "Unable to copy entities. Some entities already exist.", conflictingUris))
              }
            }
          }
        }
      }
    }

  def createEntity(workspaceName: WorkspaceName, entity: Entity): Future[PerRequestMessage] =
    dataSource.inFutureTransaction(writeLocks=Set(workspaceName)) { txn =>
      withWorkspaceContextAndPermissions(workspaceName, WorkspaceAccessLevels.Write, txn) { workspaceContext =>
        Future {
          containerDAO.entityDAO.get(workspaceContext, entity.entityType, entity.name, txn) match {
            case Some(_) => RequestComplete(ErrorReport(StatusCodes.Conflict, s"${entity.entityType} ${entity.name} already exists in ${workspaceName}"))
            case None => RequestCompleteWithLocation((StatusCodes.Created, containerDAO.entityDAO.save(workspaceContext, entity, txn)), entity.path(workspaceName))
          }
        }
      }
    }

  def batchUpdateEntities(workspaceName: WorkspaceName, entityUpdates: Seq[EntityUpdateDefinition]): Future[PerRequestMessage] =
    dataSource.inFutureTransaction(writeLocks=Set(workspaceName)) { txn =>
      withWorkspaceContextAndPermissions(workspaceName, WorkspaceAccessLevels.Write, txn) { workspaceContext =>
        Future {
          val results = entityUpdates.map { entityUpdate =>
            val entity = containerDAO.entityDAO.get(workspaceContext, entityUpdate.entityType, entityUpdate.name, txn)
            entity match {
              case Some(e) =>
                val trial = Try {
                  val updatedEntity = applyOperationsToEntity(e, entityUpdate.operations)
                  containerDAO.entityDAO.save(workspaceContext, updatedEntity, txn)
                }
                (entityUpdate, trial)
              case None => (entityUpdate, Failure(new RuntimeException("Entity does not exist")))
            }
          }
          val errorReports = results.collect{
            case (entityUpdate, Failure(regrets)) => ErrorReport(s"Could not update ${entityUpdate.entityType} ${entityUpdate.name}",ErrorReport(regrets))
          }
          if(errorReports.isEmpty) {
            RequestComplete(StatusCodes.NoContent)
          } else {
            txn.setRollbackOnly()
            RequestComplete(ErrorReport(StatusCodes.BadRequest, "Some entities could not be updated.",errorReports))
          }
        }
      }
    }

  def batchUpsertEntities(workspaceName: WorkspaceName, entityUpdates: Seq[EntityUpdateDefinition]): Future[PerRequestMessage] =
    dataSource.inFutureTransaction(writeLocks=Set(workspaceName)) { txn =>
      withWorkspaceContextAndPermissions(workspaceName, WorkspaceAccessLevels.Write, txn) { workspaceContext =>
        Future {
          val results = entityUpdates.map { entityUpdate =>
            val entity = containerDAO.entityDAO.get(workspaceContext, entityUpdate.entityType, entityUpdate.name, txn) match {
              case Some(e) => e
              case None => containerDAO.entityDAO.save(workspaceContext, Entity(entityUpdate.name, entityUpdate.entityType, Map.empty), txn)
            }
            val trial = Try {
              val updatedEntity = applyOperationsToEntity(entity, entityUpdate.operations)
              containerDAO.entityDAO.save(workspaceContext, updatedEntity, txn)
            }
            (entityUpdate, trial)
          }
          val errorReports = results.collect {
            case (entityUpdate, Failure(regrets)) => ErrorReport(s"Could not update ${entityUpdate.entityType} ${entityUpdate.name}",ErrorReport(regrets))
          }
          if (errorReports.isEmpty) {
            RequestComplete(StatusCodes.NoContent)
          } else {
            txn.setRollbackOnly()
            RequestComplete(ErrorReport(StatusCodes.BadRequest, "Some entities could not be upserted.", errorReports))
          }
        }
      }
    }

  def listEntityTypes(workspaceName: WorkspaceName): Future[PerRequestMessage] =
    dataSource.inFutureTransaction(readLocks=Set(workspaceName)) { txn =>
      withWorkspaceContextAndPermissions(workspaceName, WorkspaceAccessLevels.Read, txn) { workspaceContext =>
        Future.successful(RequestComplete(StatusCodes.OK, containerDAO.entityDAO.getEntityTypes(workspaceContext, txn)))
      }
    }

  def listEntities(workspaceName: WorkspaceName, entityType: String): Future[PerRequestMessage] =
    dataSource.inFutureTransaction(readLocks=Set(workspaceName)) { txn =>
      withWorkspaceContextAndPermissions(workspaceName, WorkspaceAccessLevels.Read, txn) { workspaceContext =>
        Future.successful(RequestComplete(StatusCodes.OK, containerDAO.entityDAO.list(workspaceContext, entityType, txn).toList))
      }
    }

  def getEntity(workspaceName: WorkspaceName, entityType: String, entityName: String): Future[PerRequestMessage] =
    dataSource.inFutureTransaction(readLocks=Set(workspaceName)) { txn =>
      withWorkspaceContextAndPermissions(workspaceName, WorkspaceAccessLevels.Read, txn) { workspaceContext =>
        withEntity(workspaceContext, entityType, entityName, txn) { entity =>
          Future.successful(PerRequest.RequestComplete(StatusCodes.OK, entity))
        }
      }
    }

  def updateEntity(workspaceName: WorkspaceName, entityType: String, entityName: String, operations: Seq[AttributeUpdateOperation]): Future[PerRequestMessage] =
    dataSource.inFutureTransaction(writeLocks=Set(workspaceName)) { txn =>
      withWorkspaceContextAndPermissions(workspaceName, WorkspaceAccessLevels.Write, txn) { workspaceContext =>
        withEntity(workspaceContext, entityType, entityName, txn) { entity =>
          Future {
            try {
              val updatedEntity = applyOperationsToEntity(entity, operations)
              RequestComplete(StatusCodes.OK, containerDAO.entityDAO.save(workspaceContext, updatedEntity, txn))
            } catch {
              case e: AttributeUpdateOperationException => {
                txn.setRollbackOnly()
                RequestComplete(ErrorReport(StatusCodes.BadRequest, s"Unable to update entity ${entityType}/${entityName} in ${workspaceName}", ErrorReport(e)))
              }
            }
          }
        }
      }
    }

  def deleteEntity(workspaceName: WorkspaceName, entityType: String, entityName: String): Future[PerRequestMessage] =
    dataSource.inFutureTransaction(writeLocks=Set(workspaceName)) { txn =>
      withWorkspaceContextAndPermissions(workspaceName, WorkspaceAccessLevels.Write, txn) { workspaceContext =>
        withEntity(workspaceContext, entityType, entityName, txn) { entity =>
          Future {
            containerDAO.entityDAO.delete(workspaceContext, entity.entityType, entity.name, txn)
            RequestComplete(StatusCodes.NoContent)
          }
        }
      }
    }

  def renameEntity(workspaceName: WorkspaceName, entityType: String, entityName: String, newName: String): Future[PerRequestMessage] =
    dataSource.inFutureTransaction(writeLocks=Set(workspaceName)) { txn =>
      withWorkspaceContextAndPermissions(workspaceName, WorkspaceAccessLevels.Write, txn) { workspaceContext =>
        withEntity(workspaceContext, entityType, entityName, txn) { entity =>
          Future {
            containerDAO.entityDAO.get(workspaceContext, entity.entityType, newName, txn) match {
              case None =>
                containerDAO.entityDAO.rename(workspaceContext, entity.entityType, entity.name, newName, txn)
                RequestComplete(StatusCodes.NoContent)
              case Some(_) => RequestComplete(ErrorReport(StatusCodes.Conflict, s"Destination ${entity.entityType} ${newName} already exists"))
            }
          }
        }
      }
    }

  def evaluateExpression(workspaceName: WorkspaceName, entityType: String, entityName: String, expression: String): Future[PerRequestMessage] =
    dataSource.inFutureTransaction(readLocks=Set(workspaceName)) { txn =>
      withWorkspaceContextAndPermissions(workspaceName, WorkspaceAccessLevels.Read, txn) { workspaceContext =>
        Future {
          txn withGraph { graph =>
            new ExpressionEvaluator(new ExpressionParser())
              .evalFinalAttribute(workspaceContext, entityType, entityName, expression) match {
              case Success(result) => RequestComplete(StatusCodes.OK, result)
              case Failure(regret) => {
                txn.setRollbackOnly()
                RequestComplete(ErrorReport(StatusCodes.BadRequest, "Unable to evaluate expression '${expression}' on ${entityType}/${entityName} in ${workspaceName}", ErrorReport(regret)))
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

  def saveAndValidateMCExpressions(workspaceContext: WorkspaceContext, methodConfiguration: MethodConfiguration, txn: RawlsTransaction): ValidatedMethodConfiguration = {
    containerDAO.methodConfigurationDAO.save(workspaceContext, methodConfiguration, txn)

    validateMCExpressions(methodConfiguration)
  }

  def validateMCExpressions(methodConfiguration: MethodConfiguration): ValidatedMethodConfiguration = {
    val parser = new ExpressionParser

    def parseAndPartition(m: Map[String, AttributeString], parseFunc:String => Try[ExpressionTypes.PipelineQuery] ) = {
      val parsed = m mapValues { attr => parseFunc(attr.value) }
      ( parsed collect { case (key, Success(_)) => key } toSeq,
        parsed collect { case (key, Failure(regret)) => (key, regret.getMessage) } )
    }
    val (successInputs, failedInputs) = parseAndPartition(methodConfiguration.inputs, parser.parseAttributeExpr)
    val (successOutputs, failedOutputs) = parseAndPartition(methodConfiguration.outputs, parser.parseOutputExpr)

    ValidatedMethodConfiguration(methodConfiguration, successInputs, failedInputs, successOutputs, failedOutputs)
  }

  def getAndValidateMethodConfiguration(workspaceName: WorkspaceName, methodConfigurationNamespace: String, methodConfigurationName: String): Future[PerRequestMessage] = {
    dataSource.inFutureTransaction(readLocks=Set(workspaceName)) { txn =>
      withWorkspaceContextAndPermissions(workspaceName, WorkspaceAccessLevels.Read, txn) { workspaceContext =>
        withMethodConfig(workspaceContext, methodConfigurationNamespace, methodConfigurationName, txn) { methodConfig =>
          Future { PerRequest.RequestComplete(StatusCodes.OK, validateMCExpressions(methodConfig)) }
        }
      }
    }
  }

  def createMethodConfiguration(workspaceName: WorkspaceName, methodConfiguration: MethodConfiguration): Future[PerRequestMessage] =
    dataSource.inFutureTransaction(writeLocks=Set(workspaceName)) { txn =>
      withWorkspaceContextAndPermissions(workspaceName, WorkspaceAccessLevels.Write, txn) { workspaceContext =>
        Future {
          containerDAO.methodConfigurationDAO.get(workspaceContext, methodConfiguration.namespace, methodConfiguration.name, txn) match {
            case Some(_) => RequestComplete(ErrorReport(StatusCodes.Conflict, s"${methodConfiguration.name} already exists in ${workspaceName}"))
            case None =>
              val validatedMethodConfiguration = saveAndValidateMCExpressions(workspaceContext, methodConfiguration, txn)
              RequestCompleteWithLocation((StatusCodes.Created, validatedMethodConfiguration), methodConfiguration.path(workspaceName))
          }
        }
      }
    }

  def deleteMethodConfiguration(workspaceName: WorkspaceName, methodConfigurationNamespace: String, methodConfigurationName: String): Future[PerRequestMessage] =
    dataSource.inFutureTransaction(writeLocks=Set(workspaceName)) { txn =>
      withWorkspaceContextAndPermissions(workspaceName, WorkspaceAccessLevels.Write, txn) { workspaceContext =>
        withMethodConfig(workspaceContext, methodConfigurationNamespace, methodConfigurationName, txn) { methodConfig =>
          Future {
            containerDAO.methodConfigurationDAO.delete(workspaceContext, methodConfigurationNamespace, methodConfigurationName, txn)
            RequestComplete(StatusCodes.NoContent)
          }
        }
      }
    }

  def renameMethodConfiguration(workspaceName: WorkspaceName, methodConfigurationNamespace: String, methodConfigurationName: String, newName: String): Future[PerRequestMessage] =
    dataSource.inFutureTransaction(writeLocks=Set(workspaceName)) { txn =>
      withWorkspaceContextAndPermissions(workspaceName, WorkspaceAccessLevels.Write, txn) { workspaceContext =>
        withMethodConfig(workspaceContext, methodConfigurationNamespace, methodConfigurationName, txn) { methodConfiguration =>
          Future {
            containerDAO.methodConfigurationDAO.get(workspaceContext, methodConfigurationNamespace, newName, txn) match {
              case None =>
                containerDAO.methodConfigurationDAO.rename(workspaceContext, methodConfigurationNamespace, methodConfigurationName, newName, txn)
                RequestComplete(StatusCodes.NoContent)
              case Some(_) => RequestComplete(ErrorReport(StatusCodes.Conflict, s"Destination ${newName} already exists"))
            }
          }
        }
      }
    }

  def updateMethodConfiguration(workspaceName: WorkspaceName, methodConfiguration: MethodConfiguration): Future[PerRequestMessage] =
    dataSource.inFutureTransaction(writeLocks=Set(workspaceName)) { txn =>
      withWorkspaceContextAndPermissions(workspaceName, WorkspaceAccessLevels.Write, txn) { workspaceContext =>
        Future {
          containerDAO.methodConfigurationDAO.get(workspaceContext, methodConfiguration.namespace, methodConfiguration.name, txn) match {
            case Some(_) => RequestComplete(StatusCodes.OK, saveAndValidateMCExpressions(workspaceContext, methodConfiguration, txn))
            case None => RequestComplete(ErrorReport(StatusCodes.NotFound,s"There is no method configuration named ${methodConfiguration.namespace}/${methodConfiguration.name} in ${workspaceName}."))
          }
        }
      }
    }

  def getMethodConfiguration(workspaceName: WorkspaceName, methodConfigurationNamespace: String, methodConfigurationName: String): Future[PerRequestMessage] =
    dataSource.inFutureTransaction(readLocks=Set(workspaceName)) { txn =>
      withWorkspaceContextAndPermissions(workspaceName, WorkspaceAccessLevels.Read, txn) { workspaceContext =>
        withMethodConfig(workspaceContext, methodConfigurationNamespace, methodConfigurationName, txn) { methodConfig =>
          Future.successful(PerRequest.RequestComplete(StatusCodes.OK, methodConfig))
        }
      }
    }

  def copyMethodConfiguration(mcnp: MethodConfigurationNamePair): Future[PerRequestMessage] = {
    dataSource.inFutureTransaction(readLocks=Set(mcnp.source.workspaceName), writeLocks=Set(mcnp.destination.workspaceName)) { txn =>
      withWorkspaceContextAndPermissions(mcnp.destination.workspaceName, WorkspaceAccessLevels.Write, txn) { destContext =>
        withWorkspaceContextAndPermissions(mcnp.source.workspaceName, WorkspaceAccessLevels.Read, txn) { sourceContext =>
          containerDAO.methodConfigurationDAO.get(sourceContext, mcnp.source.namespace, mcnp.source.name, txn) match {
            case None => Future.successful(RequestComplete(ErrorReport(StatusCodes.NotFound,s"There is no method configuration named ${mcnp.source.namespace}/${mcnp.source.name} in ${mcnp.source.workspaceName}.")))
            case Some(methodConfig) => saveCopiedMethodConfiguration(methodConfig, mcnp.destination, destContext, txn)
          }
        }
      }
    }
  }

  def copyMethodConfigurationFromMethodRepo(methodRepoQuery: MethodRepoConfigurationImport): Future[PerRequestMessage] =
    dataSource.inFutureTransaction(writeLocks=Set(methodRepoQuery.destination.workspaceName)) { txn =>
      withWorkspaceContextAndPermissions( methodRepoQuery.destination.workspaceName, WorkspaceAccessLevels.Write, txn ) { destContext =>
        methodRepoDAO.getMethodConfig(methodRepoQuery.methodRepoNamespace, methodRepoQuery.methodRepoName, methodRepoQuery.methodRepoSnapshotId, userInfo) flatMap { agoraEntityOption =>
          agoraEntityOption match {
            case None => Future.successful(RequestComplete(ErrorReport(StatusCodes.NotFound,s"There is no method configuration named ${methodRepoQuery.methodRepoNamespace}/${methodRepoQuery.methodRepoName}/${methodRepoQuery.methodRepoSnapshotId} in the repository.")))
            case Some(entity) =>
              try {
                // if JSON parsing fails, catch below
                val methodConfig = entity.payload.map(JsonParser(_).convertTo[MethodConfiguration])
                methodConfig match {
                  case Some(targetMethodConfig) => saveCopiedMethodConfiguration(targetMethodConfig, methodRepoQuery.destination, destContext, txn)
                  case None => Future.successful(RequestComplete(ErrorReport(StatusCodes.UnprocessableEntity, "Method Repo missing configuration payload")))
                }
              }
              catch {
                case e: Exception =>
                  Future.successful(RequestComplete(ErrorReport(StatusCodes.UnprocessableEntity, "Error parsing Method Repo response message.", ErrorReport(e))))
              }
          }
        }
      }
    }

  def copyMethodConfigurationToMethodRepo(methodRepoQuery: MethodRepoConfigurationExport): Future[PerRequestMessage] = {
    dataSource.inFutureTransaction(readLocks=Set(methodRepoQuery.source.workspaceName)) { txn =>
      withWorkspaceContextAndPermissions(methodRepoQuery.source.workspaceName, WorkspaceAccessLevels.Read, txn) { workspaceContext =>
        withMethodConfig(workspaceContext, methodRepoQuery.source.namespace, methodRepoQuery.source.name, txn) { methodConfig =>
          import org.broadinstitute.dsde.rawls.model.MethodRepoJsonSupport._
          methodRepoDAO.postMethodConfig(
            methodRepoQuery.methodRepoNamespace,
            methodRepoQuery.methodRepoName,
            methodConfig.copy(namespace = methodRepoQuery.methodRepoNamespace, name = methodRepoQuery.methodRepoName),
            userInfo) map { RequestComplete(StatusCodes.OK, _) }
        }
      }
    }
  }

  private def saveCopiedMethodConfiguration(methodConfig: MethodConfiguration, dest: MethodConfigurationName, destContext: WorkspaceContext, txn: RawlsTransaction) =
    Future {
      containerDAO.methodConfigurationDAO.get(destContext, dest.namespace, dest.name, txn) match {
        case Some(existingMethodConfig) => RequestComplete(ErrorReport(StatusCodes.Conflict, s"A method configuration named ${dest.namespace}/${dest.name} already exists in ${dest.workspaceName}"))
        case None =>
          val target = methodConfig.copy(name = dest.name, namespace = dest.namespace)
          val validatedTarget = saveAndValidateMCExpressions(destContext, target, txn)
          RequestCompleteWithLocation((StatusCodes.Created, validatedTarget), target.path(dest.workspaceName))
      }
    }

  def listMethodConfigurations(workspaceName: WorkspaceName): Future[PerRequestMessage] =
    dataSource.inFutureTransaction(readLocks=Set(workspaceName)) { txn =>
      withWorkspaceContextAndPermissions(workspaceName, WorkspaceAccessLevels.Read, txn) { workspaceContext =>
        // use toList below to eagerly iterate through the response from methodConfigurationDAO.list
        // to ensure it is evaluated within the transaction
        Future.successful(RequestComplete(StatusCodes.OK, containerDAO.methodConfigurationDAO.list(workspaceContext, txn).toList))
      }
    }

  def createMethodConfigurationTemplate( methodRepoMethod: MethodRepoMethod ): Future[PerRequestMessage] = {
    methodRepoDAO.getMethod(methodRepoMethod.methodNamespace,methodRepoMethod.methodName,methodRepoMethod.methodVersion,userInfo) map { method =>
      if (method.isEmpty) RequestComplete(ErrorReport(StatusCodes.NotFound, s"No method configuration named ${methodRepoMethod.methodNamespace}/${methodRepoMethod.methodName}/${methodRepoMethod.methodVersion} exists in the repository."))
      else if (method.get.payload.isEmpty) RequestComplete(ErrorReport(StatusCodes.BadRequest, "The method configuration named ${methodRepoMethod.methodNamespace}/${methodRepoMethod.methodName} has no payload."))
      else RequestComplete(StatusCodes.OK, MethodConfigResolver.toMethodConfiguration(method.get.payload.get, methodRepoMethod))
    }
  }

  /**
   * This is the function that would get called if we had a validate method config endpoint.
   */
  def validateMethodConfig(workspaceName: WorkspaceName,
    methodConfigurationNamespace: String, methodConfigurationName: String,
    entityType: String, entityName: String, userInfo: UserInfo): Future[PerRequestMessage] = {
      dataSource.inFutureTransaction(readLocks=Set(workspaceName)) { txn =>
        withWorkspaceContextAndPermissions(workspaceName, WorkspaceAccessLevels.Read, txn) { workspaceContext =>
          withMethodConfig(workspaceContext, methodConfigurationNamespace, methodConfigurationName, txn) { methodConfig =>
            withMethod(workspaceContext, methodConfig.methodRepoMethod.methodNamespace, methodConfig.methodRepoMethod.methodName, methodConfig.methodRepoMethod.methodVersion, userInfo) { method =>
              withEntity(workspaceContext, entityType, entityName, txn) { entity =>
                withWdl(method) { wdl =>
                  Future {
                    MethodConfigResolver.resolveInputsOrGatherErrors(workspaceContext, methodConfig, entity, wdl) match {
                      case Left(failures) => RequestComplete(StatusCodes.OK, failures)
                      case Right(unpacked) =>
                        val idation = executionServiceDAO.validateWorkflow(wdl, MethodConfigResolver.propertiesToWdlInputs(unpacked), userInfo)
                        RequestComplete(StatusCodes.OK, idation)
                    }
                  }
              }
            }
          }
        }
      }
    }
  }

  def listSubmissions(workspaceName: WorkspaceName): Future[PerRequestMessage] =
    dataSource.inFutureTransaction(readLocks=Set(workspaceName)) { txn =>
      withWorkspaceContextAndPermissions(workspaceName, WorkspaceAccessLevels.Read, txn) { workspaceContext =>
        Future.successful(RequestComplete(StatusCodes.OK, containerDAO.submissionDAO.list(workspaceContext, txn).toList))
      }
    }

  def createSubmission(workspaceName: WorkspaceName, submissionRequest: SubmissionRequest): Future[PerRequestMessage] =
    withSubmissionParameters(workspaceName,submissionRequest) {
      (txn: RawlsTransaction, workspaceContext: WorkspaceContext, wdl: String, header: SubmissionValidationHeader, successes: Seq[SubmissionValidationEntityInputs], failures: Seq[SubmissionValidationEntityInputs]) =>

        val submissionId: String = UUID.randomUUID().toString

        val submittedWorkflowsFuture = Future.sequence(successes map { entityInputs =>
          val methodProps = for ( (methodInput,entityValue) <- header.inputExpressions.zip(entityInputs.inputResolutions) if entityValue.value.isDefined ) yield( methodInput.wdlName -> entityValue.value.get )
          val execStatusFuture = executionServiceDAO.submitWorkflow(wdl, MethodConfigResolver.propertiesToWdlInputs(methodProps.toMap), workflowOptions(workspaceContext, submissionId), userInfo)
          execStatusFuture map (execStatus => Workflow(workflowId = execStatus.id, status = WorkflowStatuses.Submitted, statusLastChangedDate = DateTime.now, workflowEntity = AttributeEntityReference(entityType = header.entityType, entityName = entityInputs.entityName)))
        })

        submittedWorkflowsFuture map { submittedWorkflows =>
          val failedWorkflows = failures.map { entityInputs =>
            val errors = for( entityValue <- entityInputs.inputResolutions if entityValue.error.isDefined ) yield( AttributeString(entityValue.error.get) )
            WorkflowFailure(entityInputs.entityName,header.entityType,errors)
          }

          val submission = Submission(submissionId = submissionId,
            submissionDate = DateTime.now(),
            submitter = userInfo.userEmail,
            methodConfigurationNamespace = submissionRequest.methodConfigurationNamespace,
            methodConfigurationName = submissionRequest.methodConfigurationName,
            submissionEntity = AttributeEntityReference(entityType = submissionRequest.entityType, entityName = submissionRequest.entityName),
            workflows = submittedWorkflows,
            notstarted = failedWorkflows,
            status = if (submittedWorkflows.isEmpty) SubmissionStatuses.Done else SubmissionStatuses.Submitted
          )

          if (submission.status == SubmissionStatuses.Submitted) {
            submissionSupervisor ! SubmissionStarted(workspaceName, submission, userInfo)
          }

          containerDAO.submissionDAO.save(workspaceContext, submission, txn)
          val workflowReports = for ( (workflow,entityInputs) <- submittedWorkflows.zip(successes) )
            yield( WorkflowReport(workflow.workflowId,workflow.status,workflow.statusLastChangedDate,entityInputs.entityName,entityInputs.inputResolutions) )
          RequestComplete(StatusCodes.Created, SubmissionReport(submissionRequest,submission.submissionId,submission.submissionDate,submission.submitter,submission.status,header,workflowReports,failures))
        }
      }

  def validateSubmission(workspaceName: WorkspaceName, submissionRequest: SubmissionRequest): Future[PerRequestMessage] =
    withSubmissionParameters(workspaceName,submissionRequest) {
      (txn: RawlsTransaction, workspaceContext: WorkspaceContext, wdl: String, header: SubmissionValidationHeader, succeeded: Seq[SubmissionValidationEntityInputs], failed: Seq[SubmissionValidationEntityInputs]) =>
        Future.successful(RequestComplete(StatusCodes.OK, SubmissionValidationReport(submissionRequest, header, succeeded, failed)))
    }

  def getSubmissionStatus(workspaceName: WorkspaceName, submissionId: String) = {
    dataSource.inFutureTransaction(readLocks=Set(workspaceName)) { txn =>
      withWorkspaceContextAndPermissions(workspaceName, WorkspaceAccessLevels.Read, txn) { workspaceContext =>
        withSubmission(workspaceContext, submissionId, txn) { submission =>
          Future.successful(RequestComplete(StatusCodes.OK, submission))
        }
      }
    }
  }

  def abortSubmission(workspaceName: WorkspaceName, submissionId: String): Future[PerRequestMessage] = {
    dataSource.inFutureTransaction(writeLocks=Set(workspaceName)) { txn =>
      withWorkspaceContextAndPermissions(workspaceName, WorkspaceAccessLevels.Write, txn) { workspaceContext =>
        abortSubmission(workspaceContext, submissionId, txn)
      }
    }
  }

  private def abortSubmission(workspaceContext: WorkspaceContext, submissionId: String, txn: RawlsTransaction): Future[PerRequestMessage] = {
    withSubmission(workspaceContext, submissionId, txn) { submission =>
      val aborts = Future.traverse(submission.workflows)(wf =>
        Future.successful(wf.workflowId).zip(executionServiceDAO.abort(wf.workflowId, userInfo))
      )

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
          RequestComplete(ErrorReport(StatusCodes.BadGateway, s"Unable to abort all workflows for submission ${submissionId}.",causes))
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
    dataSource.inFutureTransaction(readLocks=Set(workspaceName)) { txn =>
      withWorkspaceContextAndPermissions(workspaceName, WorkspaceAccessLevels.Read, txn) { workspaceContext =>
        withSubmission(workspaceContext, submissionId, txn) { submission =>
          withWorkflow(workspaceName, submission, workflowId) { workflow =>
            val outputs = executionServiceDAO.outputs(workflowId, userInfo).map(Success(_)).recover{case t=>Failure(t)}
            val logs = executionServiceDAO.logs(workflowId, userInfo).map(Success(_)).recover{case t=>Failure(t)}
            outputs zip logs map { _ match {
              case (Success(outputs), Success(logs)) =>
                mergeWorkflowOutputs(outputs, logs, workflowId)
              case (Failure(outputsFailure), Success(logs)) =>
                RequestComplete(ErrorReport(StatusCodes.BadGateway, s"Unable to get outputs for ${submissionId}.", executionServiceDAO.toErrorReport(outputsFailure)))
              case (Success(outputs), Failure(logsFailure)) =>
                RequestComplete(ErrorReport(StatusCodes.BadGateway, s"Unable to get logs for ${submissionId}.", executionServiceDAO.toErrorReport(logsFailure)))
              case (Failure(outputsFailure), Failure(logsFailure)) =>
                RequestComplete(ErrorReport(StatusCodes.BadGateway, s"Unable to get outputs and unable to get logs for ${submissionId}.",
                  Seq(executionServiceDAO.toErrorReport(outputsFailure),executionServiceDAO.toErrorReport(logsFailure))))
              }
            }
          }
        }
      }
    }
  }

  def workflowMetadata(workspaceName: WorkspaceName, submissionId: String, workflowId: String) = {
    dataSource.inFutureTransaction(readLocks=Set(workspaceName)) { txn =>
      withWorkspaceContextAndPermissions(workspaceName, WorkspaceAccessLevels.Read, txn) { workspaceContext =>
        withSubmission(workspaceContext, submissionId, txn) { submission =>
          withWorkflow(workspaceName, submission, workflowId) { workflow =>
            executionServiceDAO.callLevelMetadata(workflowId, userInfo).map(em => RequestComplete(StatusCodes.OK, em))
          }
        }
      }
    }
  }

  def listAdmins() = {
    asAdmin {
      gcsDAO.listAdmins.map(RequestComplete(StatusCodes.OK, _)).recover{ case throwable =>
        RequestComplete(ErrorReport(StatusCodes.BadGateway,"Unable to list admins.",gcsDAO.toErrorReport(throwable)))
      }
    }
  }

  def isAdmin(userId: String) = {
    asAdmin {
      tryIsAdmin(userId) map { admin =>
        if (admin) RequestComplete(StatusCodes.NoContent)
        else RequestComplete(ErrorReport(StatusCodes.NotFound, s"User ${userId} is not an admin."))
      } recover { case throwable =>
        RequestComplete(ErrorReport(StatusCodes.BadGateway, s"Unable to determine whether ${userId} is an admin.", gcsDAO.toErrorReport(throwable)))
      }
    }
  }

  def addAdmin(userId: String) = {
    asAdmin {
      tryIsAdmin(userId) flatMap { admin =>
        if (admin) {
          Future.successful(RequestComplete(StatusCodes.NoContent))
        } else {
          gcsDAO.addAdmin(userId) map (_ => RequestComplete(StatusCodes.Created)) recover { case throwable =>
            RequestComplete(ErrorReport(StatusCodes.BadGateway, s"Unable to add ${userId} as an admin.", gcsDAO.toErrorReport(throwable)))
          }
        }
      } recover { case throwable =>
        RequestComplete(ErrorReport(StatusCodes.BadGateway, s"Unable to determine whether ${userId} is an admin.", gcsDAO.toErrorReport(throwable)))
      }
    }
  }

  def deleteAdmin(userId: String) = {
    asAdmin {
      tryIsAdmin(userId) flatMap { admin =>
        if (admin) {
          gcsDAO.deleteAdmin(userId) map (_ => RequestComplete(StatusCodes.NoContent)) recover { case throwable =>
            RequestComplete(ErrorReport(StatusCodes.BadGateway, s"Unable to delete ${userId} as an admin.", gcsDAO.toErrorReport(throwable)))
          }
        } else {
          Future.successful(RequestComplete(ErrorReport(StatusCodes.NotFound,s"${userId} is not an admin.")))
        }
      } recover { case throwable =>
        RequestComplete(ErrorReport(StatusCodes.BadGateway, s"Unable to determine whether ${userId} is an admin.", gcsDAO.toErrorReport(throwable)))
      }
    }
  }

  def listAllActiveSubmissions() = {
    asAdmin {
      dataSource.inFutureTransaction() { txn =>
        Future {
          RequestComplete(StatusCodes.OK, containerDAO.submissionDAO.listAllActiveSubmissions(txn).toList)
        }
      }
    }
  }

  def adminAbortSubmission(workspaceName: WorkspaceName, submissionId: String) = {
    asAdmin {
      dataSource.inFutureTransaction(writeLocks=Set(workspaceName)) { txn =>
        withWorkspaceContext(workspaceName, txn) { workspaceContext =>
          abortSubmission(workspaceContext, submissionId, txn)
        }
      }
    }
  }

  // helper methods

  private def withNewWorkspaceContext(workspaceRequest: WorkspaceRequest, txn: RawlsTransaction)
                                     (op: (WorkspaceContext) => PerRequestMessage): Future[PerRequestMessage] = {
    val workspaceName = workspaceRequest.toWorkspaceName
    containerDAO.workspaceDAO.loadContext(workspaceName, txn) match {
      case Some(_) => Future.successful(PerRequest.RequestComplete(ErrorReport(StatusCodes.Conflict, s"Workspace ${workspaceRequest.namespace}/${workspaceRequest.name} already exists")))
      case None =>
        val workspaceId = UUID.randomUUID.toString
        gcsDAO.createBucket(userInfo, workspaceRequest.namespace, workspaceId, workspaceName) map { _ =>
          val currentDate = DateTime.now
          val accessLevels = containerDAO.authDAO.createWorkspaceAccessGroups(workspaceName, userInfo, txn)

          val workspace = Workspace(workspaceRequest.namespace, workspaceRequest.name, workspaceId, gcsDAO.getBucketName(workspaceId), currentDate, currentDate, userInfo.userEmail, workspaceRequest.attributes, accessLevels)
          op(containerDAO.workspaceDAO.save(workspace, txn))
        }
    }
  }

  private def noSuchWorkspaceMessage(workspaceName: WorkspaceName) = s"${workspaceName} does not exist"
  private def accessDeniedMessage(workspaceName: WorkspaceName) = s"insufficient permissions to perform operation on ${workspaceName}"

  private def withWorkspaceContextAndPermissions(workspaceName: WorkspaceName, accessLevel: WorkspaceAccessLevel, txn: RawlsTransaction)(op: (WorkspaceContext) => Future[PerRequestMessage]): Future[PerRequestMessage] = {
    withWorkspaceContext(workspaceName, txn) { workspaceContext =>
      requireAccess(workspaceContext.workspace, accessLevel) { op(workspaceContext) }
    }
  }

  private def withWorkspaceContext(workspaceName: WorkspaceName, txn: RawlsTransaction)(op: (WorkspaceContext) => Future[PerRequestMessage]) = {
    assert( txn.readLocks.contains(workspaceName) || txn.writeLocks.contains(workspaceName),
            s"Attempting to use context on workspace $workspaceName but it's not read or write locked! Add it to inTransaction or inFutureTransaction")
    containerDAO.workspaceDAO.loadContext(workspaceName, txn) match {
      case None => Future.successful(RequestComplete(ErrorReport(StatusCodes.NotFound, noSuchWorkspaceMessage(workspaceName))))
      case Some(workspaceContext) => op(workspaceContext)
    }
  }

  private def requireAccess(workspace: Workspace, requiredLevel: WorkspaceAccessLevel)(codeBlock: => Future[PerRequestMessage]): Future[PerRequestMessage] = {
    gcsDAO.getMaximumAccessLevel(userInfo.userEmail, workspace.workspaceId) flatMap { userLevel =>
      if (userLevel >= requiredLevel) {
        if ( (requiredLevel > WorkspaceAccessLevels.Read) && workspace.isLocked )
          Future.successful(RequestComplete(ErrorReport(StatusCodes.Forbidden, s"The workspace ${workspace.toWorkspaceName} is locked.")))
        else codeBlock
      }
      else if (userLevel >= WorkspaceAccessLevels.Read) Future.successful(RequestComplete(ErrorReport(StatusCodes.Forbidden, accessDeniedMessage(workspace.toWorkspaceName))))
      else Future.successful(RequestComplete(ErrorReport(StatusCodes.NotFound, noSuchWorkspaceMessage(workspace.toWorkspaceName))))
    }
  }

  private def requireOwnerIgnoreLock(workspace: Workspace)(op: => Future[PerRequestMessage]): Future[PerRequestMessage] = {
    requireAccess(workspace.copy(isLocked = false),WorkspaceAccessLevels.Owner)(op)
  }

  private def tryIsAdmin(userId: String): Future[Boolean] = {
    gcsDAO.isAdmin(userId) transform( s => s, t => throw new RawlsException("Unable to query for admin status.", t))
  }

  private def asAdmin(op: => Future[PerRequestMessage]): Future[PerRequestMessage] = {

    tryIsAdmin(userInfo.userEmail) flatMap { isAdmin =>
      if (isAdmin) op else Future.successful(RequestComplete(ErrorReport(StatusCodes.Forbidden, "You must be an admin.")))
    }
  }

  private def withEntity(workspaceContext: WorkspaceContext, entityType: String, entityName: String, txn: RawlsTransaction)(op: (Entity) => Future[PerRequestMessage]): Future[PerRequestMessage] = {
    containerDAO.entityDAO.get(workspaceContext, entityType, entityName, txn) match {
      case None => Future.successful(RequestComplete(ErrorReport(StatusCodes.NotFound, s"${entityType} ${entityName} does not exist in ${workspaceContext}")))
      case Some(entity) => op(entity)
    }
  }

  private def withMethodConfig(workspaceContext: WorkspaceContext, methodConfigurationNamespace: String, methodConfigurationName: String, txn: RawlsTransaction)(op: (MethodConfiguration) => Future[PerRequestMessage]): Future[PerRequestMessage] = {
    containerDAO.methodConfigurationDAO.get(workspaceContext, methodConfigurationNamespace, methodConfigurationName, txn) match {
      case None => Future.successful(RequestComplete(ErrorReport(StatusCodes.NotFound, s"${methodConfigurationNamespace}/${methodConfigurationName} does not exist in ${workspaceContext}")))
      case Some(methodConfiguration) => op(methodConfiguration)
    }
  }

  private def withMethod(workspaceContext: WorkspaceContext, methodNamespace: String, methodName: String, methodVersion: Int, userInfo: UserInfo)(op: (AgoraEntity) => Future[PerRequestMessage]): Future[PerRequestMessage] = {
    // TODO add Method to model instead of exposing AgoraEntity?
    methodRepoDAO.getMethod(methodNamespace, methodName, methodVersion, userInfo) flatMap { agoraEntityOption => agoraEntityOption match {
      case None => Future.successful(RequestComplete(ErrorReport(StatusCodes.NotFound, s"Cannot get ${methodNamespace}/${methodName}/${methodVersion} from method repo.")))
      case Some(agoraEntity) => op(agoraEntity)
    }} recover { case throwable =>
        RequestComplete(ErrorReport(StatusCodes.BadGateway, s"Unable to query the method repo.",methodRepoDAO.toErrorReport(throwable)))
    }
  }

  private def withWdl(method: AgoraEntity)(op: (String) => Future[PerRequestMessage]): Future[PerRequestMessage] = {
    method.payload match {
      case None => Future.successful(RequestComplete(ErrorReport(StatusCodes.NotFound, "Can't get method's WDL from Method Repo: payload empty.")))
      case Some(wdl) => op(wdl)
    }
  }

  private def withSubmission(workspaceContext: WorkspaceContext, submissionId: String, txn: RawlsTransaction)(op: (Submission) => Future[PerRequestMessage]): Future[PerRequestMessage] = {
    containerDAO.submissionDAO.get(workspaceContext, submissionId, txn) match {
      case None => Future.successful(RequestComplete(ErrorReport(StatusCodes.NotFound, s"Submission with id ${submissionId} not found in workspace ${workspaceContext}")))
      case Some(submission) => op(submission)
    }
  }

  private def withWorkflow(workspaceName: WorkspaceName, submission: Submission, workflowId: String)(op: (Workflow) => Future[PerRequestMessage]): Future[PerRequestMessage] = {
    submission.workflows.find(wf => wf.workflowId == workflowId) match {
      case None => Future.successful(RequestComplete(ErrorReport(StatusCodes.NotFound, s"Workflow with id ${workflowId} not found in submission ${submission.submissionId} in workspace ${workspaceName.namespace}/${workspaceName.name}")))
      case Some(workflow) => op(workflow)
    }
  }

// TODO: not used?
//  private def submitWorkflow(workspaceContext: WorkspaceContext, methodConfig: MethodConfiguration, entity: Entity, wdl: String, submissionId: String, userInfo: UserInfo, txn: RawlsTransaction) : Either[WorkflowFailure, Workflow] = {
//    MethodConfigResolver.resolveInputsOrGatherErrors(workspaceContext, methodConfig, entity, wdl) match {
//      case Left(failures) => Left(WorkflowFailure(entityName = entity.name, entityType = entity.entityType, errors = failures.map(AttributeString(_))))
//      case Right(inputs) =>
//        val execStatus = executionServiceDAO.submitWorkflow(wdl, MethodConfigResolver.propertiesToWdlInputs(inputs), workflowOptions(workspaceContext, submissionId), userInfo)
//        Right(Workflow(workflowId = execStatus.id, status = WorkflowStatuses.Submitted, statusLastChangedDate = DateTime.now, workflowEntity = AttributeEntityReference(entityName = entity.name, entityType = entity.entityType)))
//    }
//  }

  private def workflowOptions(workspaceContext: WorkspaceContext, submissionId: String): Option[String] = {
    import ExecutionJsonSupport.ExecutionServiceWorkflowOptionsFormat // implicit format make toJson work below
    val bucketName = gcsDAO.getBucketName(workspaceContext.workspace.workspaceId)
    Option(ExecutionServiceWorkflowOptions(s"gs://${bucketName}/${submissionId}").toJson.toString)
  }

  private def withSubmissionEntities(submissionRequest: SubmissionRequest, workspaceContext: WorkspaceContext, rootEntityType: String, txn: RawlsTransaction)(op: (Seq[Entity]) => Future[PerRequestMessage]): Future[PerRequestMessage] = {
    //If there's an expression, evaluate it to get the list of entities to run this job on.
    //Otherwise, use the entity given in the submission.
    submissionRequest.expression match {
      case None =>
        if ( submissionRequest.entityType != rootEntityType )
          Future.successful(RequestComplete(ErrorReport(StatusCodes.BadRequest, s"Method configuration expects an entity of type ${rootEntityType}, but you gave us an entity of type ${submissionRequest.entityType}.")))
        else
          containerDAO.entityDAO.get(workspaceContext,submissionRequest.entityType,submissionRequest.entityName,txn) match {
            case None =>
              Future.successful(RequestComplete(ErrorReport(StatusCodes.NotFound, s"No entity of type ${submissionRequest.entityType} named ${submissionRequest.entityName} exists in this workspace.")))
            case Some(entity) =>
              op(Seq(entity))
          }
      case Some(expression) =>
        new ExpressionEvaluator(new ExpressionParser()).evalFinalEntity(workspaceContext, submissionRequest.entityType, submissionRequest.entityName, expression) match {
          case Failure(regret) =>
            Future.successful(RequestComplete(ErrorReport(regret, StatusCodes.BadRequest)))
          case Success(entities) =>
            if ( entities.isEmpty )
              Future.successful(RequestComplete(ErrorReport(StatusCodes.BadRequest, "No entities eligible for submission were found.")))
            else {
              val eligibleEntities = entities.filter(_.entityType == rootEntityType)
              if (eligibleEntities.isEmpty)
                Future.successful(RequestComplete(ErrorReport(StatusCodes.BadRequest, s"The expression in your SubmissionRequest matched only entities of the wrong type. (Expected type ${rootEntityType}.)")))
              else
                op(eligibleEntities)
            }
        }
    }
  }

  private def withMethodInputs(methodConfig: MethodConfiguration)(op: (String, Seq[MethodInput]) => Future[PerRequestMessage]): Future[PerRequestMessage] = {
    // TODO add Method to model instead of exposing AgoraEntity?
    val methodRepoMethod = methodConfig.methodRepoMethod
    methodRepoDAO.getMethod(methodRepoMethod.methodNamespace, methodRepoMethod.methodName, methodRepoMethod.methodVersion, userInfo) flatMap { _ match {
      case None => Future.successful(RequestComplete(ErrorReport(StatusCodes.NotFound, s"Cannot get ${methodRepoMethod.methodNamespace}/${methodRepoMethod.methodName}/${methodRepoMethod.methodVersion} from method repo.")))
      case Some(agoraEntity) => agoraEntity.payload match {
        case None => Future.successful(RequestComplete(ErrorReport(StatusCodes.NotFound, "Can't get method's WDL from Method Repo: payload empty.")))
        case Some(wdl) => Try(MethodConfigResolver.gatherInputs(methodConfig,wdl)) match {
          case Failure(exception) => Future.successful(RequestComplete(ErrorReport(exception,StatusCodes.BadRequest)))
          case Success(methodInputs) => op(wdl,methodInputs)
        }
      }
    }} recover { case throwable =>
      RequestComplete(ErrorReport(StatusCodes.BadGateway, s"Unable to query the method repo.",methodRepoDAO.toErrorReport(throwable)))
    }
  }

  private def withSubmissionParameters(workspaceName: WorkspaceName, submissionRequest: SubmissionRequest)
   ( op: (RawlsTransaction, WorkspaceContext, String, SubmissionValidationHeader, Seq[SubmissionValidationEntityInputs], Seq[SubmissionValidationEntityInputs]) => Future[PerRequestMessage]): Future[PerRequestMessage] = {
    dataSource.inFutureTransaction(writeLocks=Set(workspaceName)) { txn =>
      withWorkspaceContextAndPermissions(workspaceName, WorkspaceAccessLevels.Write, txn) { workspaceContext =>
        withMethodConfig(workspaceContext, submissionRequest.methodConfigurationNamespace, submissionRequest.methodConfigurationName, txn) { methodConfig =>
          withMethodInputs(methodConfig) { (wdl,methodInputs) =>
            withSubmissionEntities(submissionRequest, workspaceContext, methodConfig.rootEntityType, txn) { jobEntities =>
              val resolvedInputs = jobEntities map { entity => SubmissionValidationEntityInputs(entity.name, MethodConfigResolver.resolveInputs(workspaceContext,methodInputs,entity)) }
              val (succeeded, failed) = resolvedInputs partition { entityInputs => entityInputs.inputResolutions.forall(_.error.isEmpty) }
              val methodConfigInputs = methodInputs.map { methodInput => SubmissionValidationInput(methodInput.workflowInput.fqn, methodInput.expression) }
              val header = SubmissionValidationHeader(methodConfig.rootEntityType, methodConfigInputs)
              op(txn, workspaceContext, wdl, header, succeeded, failed)
            }
          }
        }
      }
    }
  }
}

class AttributeUpdateOperationException(message: String) extends RawlsException(message)
class AttributeNotFoundException(message: String) extends AttributeUpdateOperationException(message)
