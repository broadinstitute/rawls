package org.broadinstitute.dsde.rawls.workspace

import java.util.UUID

import akka.actor._
import akka.pattern._
import akka.util.Timeout
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.broadinstitute.dsde.rawls.{RawlsExceptionWithErrorReport, RawlsException}
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.jobexec.MethodConfigResolver
import org.broadinstitute.dsde.rawls.jobexec.MethodConfigResolver.MethodInput
import org.broadinstitute.dsde.rawls.jobexec.SubmissionSupervisor.SubmissionStarted
import org.broadinstitute.dsde.rawls.model.WorkspaceAccessLevels.WorkspaceAccessLevel
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._
import org.broadinstitute.dsde.rawls.expressions._
import org.broadinstitute.dsde.rawls.user.UserService
import org.broadinstitute.dsde.rawls.util.{FutureSupport,AdminSupport}
import org.broadinstitute.dsde.rawls.webservice.PerRequest
import org.broadinstitute.dsde.rawls.webservice.PerRequest.{RequestCompleteWithLocation, PerRequestMessage, RequestComplete}
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
import org.broadinstitute.dsde.rawls.model.ExecutionJsonSupport.{ActiveSubmissionFormat, ExecutionMetadataFormat, SubmissionStatusResponseFormat, SubmissionFormat, SubmissionReportFormat, SubmissionValidationReportFormat, WorkflowOutputsFormat, ExecutionServiceValidationFormat}

import scala.concurrent.duration._
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
  case class CreateSubmission(workspaceName: WorkspaceName, submission: SubmissionRequest) extends WorkspaceServiceMessage
  case class ValidateSubmission(workspaceName: WorkspaceName, submission: SubmissionRequest) extends WorkspaceServiceMessage
  case class GetSubmissionStatus(workspaceName: WorkspaceName, submissionId: String) extends WorkspaceServiceMessage
  case class AbortSubmission(workspaceName: WorkspaceName, submissionId: String) extends WorkspaceServiceMessage
  case class GetWorkflowOutputs(workspaceName: WorkspaceName, submissionId: String, workflowId: String) extends WorkspaceServiceMessage
  case class GetWorkflowMetadata(workspaceName: WorkspaceName, submissionId: String, workflowId: String) extends WorkspaceServiceMessage

  case object ListAllActiveSubmissions extends WorkspaceServiceMessage
  case class AdminAbortSubmission(workspaceNamespace: String, workspaceName: String, submissionId: String) extends WorkspaceServiceMessage

  case class HasAllUserReadAccess(workspaceName: WorkspaceName) extends WorkspaceServiceMessage
  case class GrantAllUserReadAccess(workspaceName: WorkspaceName) extends WorkspaceServiceMessage
  case class RevokeAllUserReadAccess(workspaceName: WorkspaceName) extends WorkspaceServiceMessage

  def props(workspaceServiceConstructor: UserInfo => WorkspaceService, userInfo: UserInfo): Props = {
    Props(workspaceServiceConstructor(userInfo))
  }

  def constructor(dataSource: DataSource, containerDAO: DbContainerDAO, methodRepoDAO: MethodRepoDAO, executionServiceDAO: ExecutionServiceDAO, gcsDAO: GoogleServicesDAO, submissionSupervisor: ActorRef, bucketDeletionMonitor: ActorRef, userServiceConstructor: UserInfo => UserService)(userInfo: UserInfo)(implicit executionContext: ExecutionContext) =
    new WorkspaceService(userInfo, dataSource, containerDAO, methodRepoDAO, executionServiceDAO, gcsDAO, submissionSupervisor, bucketDeletionMonitor, userServiceConstructor)
}

class WorkspaceService(protected val userInfo: UserInfo, dataSource: DataSource, containerDAO: DbContainerDAO, methodRepoDAO: MethodRepoDAO, executionServiceDAO: ExecutionServiceDAO, protected val gcsDAO: GoogleServicesDAO, submissionSupervisor: ActorRef, bucketDeletionMonitor: ActorRef, userServiceConstructor: UserInfo => UserService)(implicit protected val executionContext: ExecutionContext) extends Actor with AdminSupport with FutureSupport with LazyLogging {
  implicit val timeout = Timeout(5 minutes)

  override def receive = {
    case CreateWorkspace(workspace) => pipe(createWorkspace(workspace)) to sender
    case GetWorkspace(workspaceName) => pipe(getWorkspace(workspaceName)) to sender
    case DeleteWorkspace(workspaceName) => pipe(deleteWorkspace(workspaceName)) to sender
    case UpdateWorkspace(workspaceName, operations) => pipe(updateWorkspace(workspaceName, operations)) to sender
    case ListWorkspaces => pipe(listWorkspaces()) to sender
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
    case BatchUpsertEntities(workspaceName, entityUpdates) => pipe(batchUpsertEntities(workspaceName, entityUpdates)) to sender
    case BatchUpdateEntities(workspaceName, entityUpdates) => pipe(batchUpdateEntities(workspaceName, entityUpdates)) to sender

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
    case CreateSubmission(workspaceName, submission) => pipe(createSubmission(workspaceName, submission)) to sender
    case ValidateSubmission(workspaceName, submission) => pipe(validateSubmission(workspaceName, submission)) to sender
    case GetSubmissionStatus(workspaceName, submissionId) => pipe(getSubmissionStatus(workspaceName, submissionId)) to sender
    case AbortSubmission(workspaceName, submissionId) => pipe(abortSubmission(workspaceName, submissionId)) to sender
    case GetWorkflowOutputs(workspaceName, submissionId, workflowId) => pipe(workflowOutputs(workspaceName, submissionId, workflowId)) to sender
    case GetWorkflowMetadata(workspaceName, submissionId, workflowId) => pipe(workflowMetadata(workspaceName, submissionId, workflowId)) to sender

    case ListAllActiveSubmissions => pipe(listAllActiveSubmissions()) to sender
    case AdminAbortSubmission(workspaceNamespace,workspaceName,submissionId) => pipe(adminAbortSubmission(WorkspaceName(workspaceNamespace,workspaceName),submissionId)) to sender

    case HasAllUserReadAccess(workspaceName) => pipe(hasAllUserReadAccess(workspaceName)) to sender
    case GrantAllUserReadAccess(workspaceName) => pipe(grantAllUserReadAccess(workspaceName)) to sender
    case RevokeAllUserReadAccess(workspaceName) => pipe(revokeAllUserReadAccess(workspaceName)) to sender
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
        val accessLevel = containerDAO.authDAO.getMaximumAccessLevel(RawlsUser(userInfo), workspaceContext.workspace.workspaceId, txn)
        if (accessLevel < WorkspaceAccessLevels.Read)
          Future.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.NotFound, noSuchWorkspaceMessage(workspaceName))))
        else {
          val response = WorkspaceListResponse(accessLevel,
            workspaceContext.workspace,
            getWorkspaceSubmissionStats(workspaceContext, txn),
            getWorkspaceOwners(workspaceContext.workspace, txn))
          Future.successful(RequestComplete(StatusCodes.OK, response))
        }
      }
    }

  def getWorkspaceOwners(workspace: Workspace, txn: RawlsTransaction): Seq[String] = {
    val ownerGroup = containerDAO.authDAO.loadGroup(workspace.accessLevels(WorkspaceAccessLevels.Owner), txn).getOrElse(
      throw new RawlsException(s"Unable to load owners for workspace ${workspace.toWorkspaceName}"))
    val users = ownerGroup.users.map(u => containerDAO.authDAO.loadUser(u, txn).get.userEmail.value)
    val subGroups = ownerGroup.subGroups.map(g => containerDAO.authDAO.loadGroup(g, txn).get.groupEmail.value)

    (users++subGroups).toSeq
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

        val accessGroups = workspaceContext.workspace.accessLevels.values.map(containerDAO.authDAO.loadGroup(_, txn)).collect { case Some(g) => g }
        gcsDAO.deleteWorkspace(workspaceContext.workspace.bucketName, accessGroups.toSeq, bucketDeletionMonitor).map { _ =>
          containerDAO.authDAO.deleteWorkspaceAccessGroups(workspaceContext.workspace, txn)
          containerDAO.workspaceDAO.delete(workspaceName, txn)

          RequestComplete(StatusCodes.Accepted, s"Your Google bucket ${workspaceContext.workspace.bucketName} will be deleted within 24h.")
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
              throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.BadRequest, s"Unable to update ${workspaceName}", ErrorReport(e)))
            }
          }
        }
      }
    }

  def listWorkspaces(): Future[PerRequestMessage] =
    dataSource.inFutureTransaction() { txn =>
      Future {
        val permissionsPairs = containerDAO.authDAO.listWorkspaces(RawlsUser(userInfo), txn)

        val responses = permissionsPairs map { permissionsPair =>
          // database query to get details
          containerDAO.workspaceDAO.findById(permissionsPair.workspaceId, txn) match {
            case Some(workspaceContext) =>
              Option(WorkspaceListResponse(permissionsPair.accessLevel,
                workspaceContext.workspace,
                getWorkspaceSubmissionStats(workspaceContext, txn),
                getWorkspaceOwners(workspaceContext.workspace, txn))
              )
            case None =>
              // this case will happen when permissions exist for workspaces that don't, use None here and ignore later
              None
          }
        }

        RequestComplete(StatusCodes.OK, responses.collect { case Some(x) => x })
      }
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

  def cloneWorkspace(sourceWorkspaceName: WorkspaceName, destWorkspaceRequest: WorkspaceRequest): Future[PerRequestMessage] =
    dataSource.inFutureTransaction(readLocks=Set(sourceWorkspaceName), writeLocks=Set(destWorkspaceRequest.toWorkspaceName)) { txn =>
      withWorkspaceContextAndPermissions(sourceWorkspaceName,WorkspaceAccessLevels.Read,txn) { sourceWorkspaceContext =>
        withClonedRealm(sourceWorkspaceContext: WorkspaceContext, destWorkspaceRequest: WorkspaceRequest) { newRealm =>

          // add to or replace current attributes, on an individual basis
          val newAttrs = sourceWorkspaceContext.workspace.attributes ++ destWorkspaceRequest.attributes

          withNewWorkspaceContext(destWorkspaceRequest.copy(realm = newRealm, attributes = newAttrs), txn) { destWorkspaceContext =>
            containerDAO.entityDAO.cloneAllEntities(sourceWorkspaceContext, destWorkspaceContext, txn)
            // TODO add a method for cloning all method configs, instead of doing this
            containerDAO.methodConfigurationDAO.list(sourceWorkspaceContext, txn).foreach { methodConfig =>
              containerDAO.methodConfigurationDAO.save(destWorkspaceContext,
                containerDAO.methodConfigurationDAO.get(sourceWorkspaceContext, methodConfig.namespace, methodConfig.name, txn).get, txn)
            }
            RequestCompleteWithLocation((StatusCodes.Created, destWorkspaceContext.workspace), destWorkspaceRequest.toWorkspaceName.path)
          }
        }
      }
    }

  private def withClonedRealm(sourceWorkspaceContext: WorkspaceContext, destWorkspaceRequest: WorkspaceRequest)(op: (Option[RawlsGroupRef]) => Future[PerRequestMessage]): Future[PerRequestMessage] = {
    // if the source has a realm, the dest must also have that realm.  Otherwise, the caller chooses
    (sourceWorkspaceContext.workspace.realm, destWorkspaceRequest.realm) match {
      case (Some(sourceRealm), Some(destRealm)) if sourceRealm != destRealm =>
        val errorMsg = s"Source workspace ${sourceWorkspaceContext.workspace.briefName} has realm $sourceRealm; cannot change it to $destRealm when cloning"
        Future.failed(new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.UnprocessableEntity, errorMsg)))
      case (Some(sourceRealm), _) => op(Option(sourceRealm))
      case (None, destOpt) => op(destOpt)
    }
  }

  def withRawlsUser[T](userRef: RawlsUserRef, txn: RawlsTransaction)(op: (RawlsUser) => T): T = {
    containerDAO.authDAO.loadUser(userRef, txn) match {
      case Some(user) => op(user)
      case None => throw new RawlsException(s"Couldn't find user for userRef $userRef")
    }
  }

  def withRawlsGroup[T](groupRef: RawlsGroupRef, txn: RawlsTransaction)(op: (RawlsGroup) => T): T = {
    containerDAO.authDAO.loadGroup(groupRef, txn) match {
      case Some(group) => op(group)
      case None => throw new RawlsException(s"Couldn't find group for groupRef $groupRef")
    }
  }

  def getACL(workspaceName: WorkspaceName): Future[PerRequestMessage] =
    dataSource.inFutureTransaction(readLocks=Set(workspaceName)) { txn =>
      withWorkspaceContext(workspaceName, txn) { workspaceContext =>
        requireOwnerIgnoreLock(workspaceContext.workspace, txn) {
          //Pull the ACLs from the workspace. Sort the keys by level, so that higher access levels overwrite lower ones.
          //Build a map from user/group ID to associated access level.
          val aclList: Map[String, WorkspaceAccessLevel] = workspaceContext.workspace.accessLevels.toSeq.sortBy(_._1)
            .foldLeft(Map.empty[String, WorkspaceAccessLevel])({ case (currentMap, (level, groupRef)) =>
            //groupRef = group representing this access level for the workspace
            withRawlsGroup(groupRef, txn) { accessGroup =>

              //pairs of user emails -> this access level
              val userLevels = accessGroup.users.map {
                withRawlsUser(_, txn) { user =>
                  (user.userEmail.value, level)
                }
              }

              //pairs of group emails -> this access level
              val subgroupLevels = accessGroup.subGroups.map {
                withRawlsGroup(_, txn) { subGroup =>
                  (subGroup.groupEmail.value, level)
                }
              }

              //fold 'em into the map
              currentMap ++ userLevels ++ subgroupLevels
            }
          })
          Future.successful( RequestComplete(StatusCodes.OK, aclList) )
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
    dataSource.inFutureTransaction(writeLocks=Set(workspaceName)) { txn =>
      withWorkspaceContext(workspaceName, txn) { workspaceContext =>
        requireOwnerIgnoreLock(workspaceContext.workspace, txn) {

          //collapse the acl updates list so there are no dupe emails, and convert to RawlsGroup/RawlsUser instances
          val updateMap: Map[Either[RawlsUser, RawlsGroup], WorkspaceAccessLevel] = aclUpdates
            .map { case upd => (containerDAO.authDAO.loadFromEmail(upd.email, txn), upd.accessLevel) }
            .collect { case (Some(userauth), level) => userauth -> level }.toMap

          //make a list of all the refs we're going to update
          val allTheRefs: Set[UserAuthRef] = updateMap.map {
            case (Left(rawlsUser:RawlsUser), level) => RawlsUser.toRef(rawlsUser)
            case (Right(rawlsGroup:RawlsGroup), level) => RawlsGroup.toRef(rawlsGroup)
          }.toSet

          if (allTheRefs.contains(UserService.allUsersGroupRef)) {
            // UserService.allUsersGroupRef cannot be updated in this code path, there is an admin end point for that
            Future.successful(RequestComplete(ErrorReport(StatusCodes.BadRequest, s"Please contact an administrator to alter access to ${UserService.allUsersGroupRef.groupName}")))

          } else if (!updateMap.get(Left(RawlsUser(userInfo))).forall(_ == containerDAO.authDAO.getMaximumAccessLevel(RawlsUser(userInfo), workspaceContext.workspace.workspaceId, txn))) {
            // don't allow the user to change their own permissions but let it pass if they are in the list and their current access level
            // is the same as the new value
            Future.successful(RequestComplete(ErrorReport(StatusCodes.BadRequest, "You may not change your own permissions")))

          } else {
            val userServiceRef = context.actorOf(UserService.props(userServiceConstructor, userInfo))
            val groupsByLevel: Map[WorkspaceAccessLevel, Map[Either[RawlsUser, RawlsGroup], WorkspaceAccessLevel]] = updateMap.groupBy({ case (key, value) => value })

            // go through the access level groups on the workspace and update them
            // foldLeft to go through the futures in order instead of parallel to avoid concurrent modification exceptions
            workspaceContext.workspace.accessLevels.foldLeft(Future.successful(RequestComplete(StatusCodes.NoContent)).asInstanceOf[Future[PerRequestMessage]]) {
              case (priorFuture, (level, groupRef)) => priorFuture.flatMap {
                case RequestComplete(StatusCodes.NoContent) => withRawlsGroup(groupRef, txn) { group =>
                  //remove existing records for users and groups in the acl update list
                  val usersNotChanging = group.users.filter(userRef => !allTheRefs.contains(userRef))
                  val groupsNotChanging = group.subGroups.filter(groupRef => !allTheRefs.contains(groupRef))

                  //generate the list of new references
                  val newUsers = groupsByLevel.getOrElse(level, Map.empty).keys.collect({ case Left(ru) => RawlsUser.toRef(ru) })
                  val newgroups = groupsByLevel.getOrElse(level, Map.empty).keys.collect({ case Right(rg) => RawlsGroup.toRef(rg) })

                  userServiceRef ? UserService.OverwriteGroupMembers(group, RawlsGroupMemberList(
                    userSubjectIds = Option((usersNotChanging ++ newUsers).map(_.userSubjectId.value).toSeq),
                    subGroupNames = Option((groupsNotChanging ++ newgroups).map(_.groupName.value).toSeq)
                  ))
                }.asInstanceOf[Future[PerRequestMessage]]
                case _ => priorFuture
              }
            } map {
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

  def lockWorkspace(workspaceName: WorkspaceName): Future[PerRequestMessage] =
    dataSource.inFutureTransaction(writeLocks=Set(workspaceName)) { txn =>
      withWorkspaceContext(workspaceName, txn) { workspaceContext =>
        requireOwnerIgnoreLock(workspaceContext.workspace, txn) {
          Future {
            if ( !containerDAO.submissionDAO.list(workspaceContext,txn).forall(_.status.isDone) )
              throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.Conflict, s"There are running submissions in workspace $workspaceName, so it cannot be locked."))
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
        requireOwnerIgnoreLock(workspaceContext.workspace, txn) {
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
                throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.Conflict, "Unable to copy entities. Some entities already exist.", conflictingUris.toSeq))
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
            case Some(_) => throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.Conflict, s"${entity.entityType} ${entity.name} already exists in ${workspaceName}"))
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
            throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.BadRequest, "Some entities could not be updated.",errorReports))
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
            throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.BadRequest, "Some entities could not be upserted.", errorReports))
          }
        }
      }
    }

  def listEntityTypes(workspaceName: WorkspaceName): Future[PerRequestMessage] =
    dataSource.inFutureTransaction(readLocks=Set(workspaceName)) { txn =>
      withWorkspaceContextAndPermissions(workspaceName, WorkspaceAccessLevels.Read, txn) { workspaceContext =>
        Future.successful(RequestComplete(StatusCodes.OK, containerDAO.entityDAO.getEntityTypes(workspaceContext, txn).toSeq))
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
                throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.BadRequest, s"Unable to update entity ${entityType}/${entityName} in ${workspaceName}", ErrorReport(e)))
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
              case Some(_) => throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.Conflict, s"Destination ${entity.entityType} ${newName} already exists"))
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
                throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.BadRequest, "Unable to evaluate expression '${expression}' on ${entityType}/${entityName} in ${workspaceName}", ErrorReport(regret)))
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
            case Some(_) => throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.Conflict, s"${methodConfiguration.name} already exists in ${workspaceName}"))
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
              case Some(_) => throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.Conflict, s"Destination ${newName} already exists"))
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
            case None => throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.NotFound,s"There is no method configuration named ${methodConfiguration.namespace}/${methodConfiguration.name} in ${workspaceName}."))
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
            case None => Future.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.NotFound,s"There is no method configuration named ${mcnp.source.namespace}/${mcnp.source.name} in ${mcnp.source.workspaceName}.")))
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
            case None => Future.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.NotFound,s"There is no method configuration named ${methodRepoQuery.methodRepoNamespace}/${methodRepoQuery.methodRepoName}/${methodRepoQuery.methodRepoSnapshotId} in the repository.")))
            case Some(entity) =>
              try {
                // if JSON parsing fails, catch below
                val methodConfig = entity.payload.map(JsonParser(_).convertTo[MethodConfiguration])
                methodConfig match {
                  case Some(targetMethodConfig) => saveCopiedMethodConfiguration(targetMethodConfig, methodRepoQuery.destination, destContext, txn)
                  case None => Future.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.UnprocessableEntity, "Method Repo missing configuration payload")))
                }
              }
              catch {
                case e: Exception =>
                  Future.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.UnprocessableEntity, "Error parsing Method Repo response message.", ErrorReport(e))))
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
        case Some(existingMethodConfig) => throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.Conflict, s"A method configuration named ${dest.namespace}/${dest.name} already exists in ${dest.workspaceName}"))
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
    withMethod(methodRepoMethod.methodNamespace,methodRepoMethod.methodName,methodRepoMethod.methodVersion,userInfo) { method =>
      withWdl(method) { wdl =>
        Future {
          RequestComplete(StatusCodes.OK, MethodConfigResolver.toMethodConfiguration(wdl, methodRepoMethod))
        }
      }
    }
  }

  def getMethodInputsOutputs( methodRepoMethod: MethodRepoMethod ): Future[PerRequestMessage] = {
    withMethod(methodRepoMethod.methodNamespace,methodRepoMethod.methodName,methodRepoMethod.methodVersion,userInfo) { method =>
      withWdl(method) { wdl =>
        Future {
          RequestComplete(StatusCodes.OK, MethodConfigResolver.getMethodInputsOutputs(wdl))
        }
      }
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
            withMethod(methodConfig.methodRepoMethod.methodNamespace, methodConfig.methodRepoMethod.methodName, methodConfig.methodRepoMethod.methodVersion, userInfo) { method =>
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
        val statuses = containerDAO.submissionDAO.list(workspaceContext, txn) map { submission =>
          withRawlsUser(submission.submitter, txn) { user =>
            new SubmissionStatusResponse(submission, user)
          }
        }
        Future.successful(RequestComplete(StatusCodes.OK, statuses.toList))
      }
    }

  def createSubmission(workspaceName: WorkspaceName, submissionRequest: SubmissionRequest): Future[PerRequestMessage] = {
    val submissionFuture: Future[PerRequestMessage] = withSubmissionParameters(workspaceName, submissionRequest) {
      (txn: RawlsTransaction, workspaceContext: WorkspaceContext, wdl: String, header: SubmissionValidationHeader, successes: Seq[SubmissionValidationEntityInputs], failures: Seq[SubmissionValidationEntityInputs]) =>

        val submissionId: String = UUID.randomUUID().toString
        val workflowOptionsFuture = buildWorkflowOptions(workspaceContext, submissionId)
        val submittedWorkflowsFuture = workflowOptionsFuture.flatMap(workflowOptions => Future.sequence(successes map { entityInputs =>
          Try {
            val methodProps = for ((methodInput, entityValue) <- header.inputExpressions.zip(entityInputs.inputResolutions) if entityValue.value.isDefined) yield (methodInput.wdlName -> entityValue.value.get)
            val execStatusFuture = executionServiceDAO.submitWorkflow(wdl, MethodConfigResolver.propertiesToWdlInputs(methodProps.toMap), workflowOptions, userInfo)
            execStatusFuture map {
              execStatus => Workflow(workflowId = execStatus.id, status = WorkflowStatuses.Submitted, statusLastChangedDate = DateTime.now, workflowEntity = Option(AttributeEntityReference(entityType = header.entityType, entityName = entityInputs.entityName)), inputResolutions = entityInputs.inputResolutions)
            } recover {
              case t: Exception => {
                val error = "Unable to submit workflow when creating submission"
                logger.error(error, t)
                WorkflowFailure(entityInputs.entityName, header.entityType, entityInputs.inputResolutions, Seq(AttributeString(error), AttributeString(t.getMessage)))
              }
            }
          } match {
            case Success(result) => result
            case Failure(t) => {
              val error = "Unable to process workflow when creating submission"
              logger.error(error, t)
              Future.successful(WorkflowFailure(entityInputs.entityName, header.entityType, entityInputs.inputResolutions, Seq(AttributeString(error), AttributeString(t.getMessage))))
            }
          }
        }))

        submittedWorkflowsFuture map { submittedWorkflows =>
          val succeededWorkflowSubmissions = submittedWorkflows.collect {
            case w:Workflow => w
          }
          val failedWorkflowSubmissions = submittedWorkflows.collect {
            case w:WorkflowFailure => w
          }
          val failedWorkflows = failures.map { entityInputs =>
            val errors = for (entityValue <- entityInputs.inputResolutions if entityValue.error.isDefined) yield (AttributeString(entityValue.error.get))
            WorkflowFailure(entityInputs.entityName, header.entityType, entityInputs.inputResolutions, errors)
          } ++ failedWorkflowSubmissions

          val submission = Submission(submissionId = submissionId,
            submissionDate = DateTime.now(),
            submitter = RawlsUser(userInfo),
            methodConfigurationNamespace = submissionRequest.methodConfigurationNamespace,
            methodConfigurationName = submissionRequest.methodConfigurationName,
            submissionEntity = Option(AttributeEntityReference(entityType = submissionRequest.entityType, entityName = submissionRequest.entityName)),
            workflows = succeededWorkflowSubmissions,
            notstarted = failedWorkflows,
            status = if (succeededWorkflowSubmissions.isEmpty) SubmissionStatuses.Done else SubmissionStatuses.Submitted
          )

          containerDAO.submissionDAO.save(workspaceContext, submission, txn)
          val workflowReports = succeededWorkflowSubmissions.map { workflow =>
            WorkflowReport(workflow.workflowId, workflow.status, workflow.statusLastChangedDate, workflow.workflowEntity.map(_.entityName).getOrElse("*deleted*"), workflow.inputResolutions)
          }

          RequestComplete(StatusCodes.Created, SubmissionReport(submissionRequest, submission.submissionId, submission.submissionDate, userInfo.userEmail, submission.status, header, workflowReports, failures))
        }
    }

    val credFuture = gcsDAO.getUserCredentials(RawlsUser(userInfo))

    submissionFuture.zip(credFuture) map {
      case (RequestComplete((StatusCodes.Created, submissionReport: SubmissionReport)), Some(credential)) =>
        if (submissionReport.status == SubmissionStatuses.Submitted) {
          submissionSupervisor ! SubmissionStarted(workspaceName, submissionReport.submissionId, credential)
        }
        RequestComplete(StatusCodes.Created, submissionReport)

      case (somethingWrong, Some(_)) => somethingWrong // this is the case where something was not found in withSubmissionParameters
      case (_, None) => throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.InternalServerError, s"No refresh token found for ${userInfo.userEmail}"))
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
          withRawlsUser(submission.submitter, txn) { user =>
            Future.successful(RequestComplete(StatusCodes.OK, new SubmissionStatusResponse(submission, user)))
          }
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
      containerDAO.submissionDAO.update(workspaceContext, submission.copy(status = SubmissionStatuses.Aborting), txn)
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
          //if we fail to abort all workflows, we do not want to roll back the transaction. we've done all we can at this point
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
                throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.BadGateway, s"Unable to get outputs for ${submissionId}.", executionServiceDAO.toErrorReport(outputsFailure)))
              case (Success(outputs), Failure(logsFailure)) =>
                throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.BadGateway, s"Unable to get logs for ${submissionId}.", executionServiceDAO.toErrorReport(logsFailure)))
              case (Failure(outputsFailure), Failure(logsFailure)) =>
                throw new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.BadGateway, s"Unable to get outputs and unable to get logs for ${submissionId}.",
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

  def hasAllUserReadAccess(workspaceName: WorkspaceName): Future[PerRequestMessage] = {
    asAdmin {
      dataSource.inFutureTransaction(readLocks=Set(workspaceName)) { txn =>
        withWorkspaceContext(workspaceName, txn) { workspaceContext =>
          val readerGroup = containerDAO.authDAO.loadGroup(workspaceContext.workspace.accessLevels(WorkspaceAccessLevels.Read), txn).get

          if (readerGroup.subGroups.contains(UserService.allUsersGroupRef)) {
            Future.successful(RequestComplete(StatusCodes.NoContent))
          } else {
            Future.successful(RequestComplete(StatusCodes.NotFound))
          }
        }
      }
    }
  }

  def grantAllUserReadAccess(workspaceName: WorkspaceName): Future[PerRequestMessage] = {
    asAdmin {
      dataSource.inFutureTransaction(readLocks=Set(workspaceName)) { txn =>
        withWorkspaceContext(workspaceName, txn) { workspaceContext =>
          val userServiceRef = context.actorOf(UserService.props(userServiceConstructor, userInfo))
          (userServiceRef ? UserService.AddGroupMembers(
            workspaceContext.workspace.accessLevels(WorkspaceAccessLevels.Read),
            RawlsGroupMemberList(subGroupNames = Option(Seq(UserService.allUsersGroupRef.groupName.value))))).asInstanceOf[Future[PerRequestMessage]]
        } map {
          case RequestComplete(StatusCodes.OK) => RequestComplete(StatusCodes.Created)
          case otherwise => otherwise
        }
      }
    }
  }

  def revokeAllUserReadAccess(workspaceName: WorkspaceName): Future[PerRequestMessage] = {
    asAdmin {
      dataSource.inFutureTransaction(readLocks=Set(workspaceName)) { txn =>
        withWorkspaceContext(workspaceName, txn) { workspaceContext =>
          val userServiceRef = context.actorOf(UserService.props(userServiceConstructor, userInfo))
          (userServiceRef ? UserService.RemoveGroupMembers(
            workspaceContext.workspace.accessLevels(WorkspaceAccessLevels.Read),
            RawlsGroupMemberList(subGroupNames = Option(Seq(UserService.allUsersGroupRef.groupName.value))))).asInstanceOf[Future[PerRequestMessage]]
        }
      }
    }
  }


  def getWorkspaceStatus(workspaceName: WorkspaceName, userSubjectId: Option[String]): Future[PerRequestMessage] = {
    asAdmin {
      dataSource.inFutureTransaction(readLocks = Set(workspaceName)) { txn =>
        withWorkspaceContext(workspaceName, txn) { workspaceContext =>
          val STATUS_FOUND = "FOUND"
          val STATUS_NOT_FOUND = "NOT_FOUND"
          val STATUS_CAN_WRITE = "USER_CAN_WRITE"
          val STATUS_CANNOT_WRITE = "USER_CANNOT_WRITE"
          val STATUS_NA = "NOT_AVAILABLE"

          val bucketName = workspaceContext.workspace.bucketName
          val rawlsGroupRefs = workspaceContext.workspace.accessLevels
          val googleGroupRefs = rawlsGroupRefs map { case (accessLevel, groupRef) =>
            accessLevel -> containerDAO.authDAO.loadGroup(groupRef, txn)
          }

          val userRef = userSubjectId.flatMap(id =>
            containerDAO.authDAO.loadUser(RawlsUserRef(RawlsUserSubjectId(id)), txn))

          val userStatus = userRef match {
            case Some(user) => "FIRECLOUD_USER: " + user.userSubjectId.value -> STATUS_FOUND
            case None => userSubjectId match {
              case Some(id) => "FIRECLOUD_USER: " + id -> STATUS_NOT_FOUND
              case None => "FIRECLOUD_USER: None Supplied" -> STATUS_NA
            }
          }

          val rawlsGroupStatuses = rawlsGroupRefs map { case (_, groupRef) =>
            containerDAO.authDAO.loadGroup(groupRef, txn) match {
              case Some(group) => "WORKSPACE_GROUP: " + group.groupName.value -> STATUS_FOUND
              case None => "WORKSPACE_GROUP: " + groupRef.groupName.value -> STATUS_NOT_FOUND
            }
          }

          val googleGroupStatuses = googleGroupRefs map { case (_, groupRef) =>
            val groupEmail = groupRef.get.groupEmail.value
            toFutureTry(gcsDAO.getGoogleGroup(groupEmail).map(_ match {
              case Some(_) => "GOOGLE_GROUP: " + groupEmail -> STATUS_FOUND
              case None => "GOOGLE_GROUP: " + groupEmail -> STATUS_NOT_FOUND
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
                if(status) "FIRECLOUD_USER_PROXY: " + bucketName -> STATUS_FOUND
                else "FIRECLOUD_USER_PROXY: " + bucketName -> STATUS_NOT_FOUND
              })
            }
            case (_, _) => Future(Try("FIRECLOUD_USER_PROXY: " + bucketName -> STATUS_NA))
          }

          val userAccessLevel = userStatus match {
            case (_, STATUS_FOUND) =>
              "WORKSPACE_USER_ACCESS_LEVEL" -> containerDAO.authDAO.getMaximumAccessLevel(userRef.get, workspaceContext.workspace.workspaceId, txn).toString
            case (_, _) => "WORKSPACE_USER_ACCESS_LEVEL" -> STATUS_NA
          }

          val googleAccessLevel = userStatus match {
            case (_, STATUS_FOUND) => {
              val accessLevel = containerDAO.authDAO.getMaximumAccessLevel(userRef.get, workspaceContext.workspace.workspaceId, txn)
              if(accessLevel >= WorkspaceAccessLevels.Read) {
                val groupEmail = containerDAO.authDAO.loadGroup(workspaceContext.workspace.accessLevels.get(accessLevel).get, txn).get.groupEmail.value
                toFutureTry(gcsDAO.isEmailInGoogleGroup(gcsDAO.toProxyFromUser(userRef.get.userSubjectId), groupEmail).map { status =>
                  if(status) "GOOGLE_USER_ACCESS_LEVEL: " + groupEmail -> STATUS_FOUND
                  else "GOOGLE_USER_ACCESS_LEVEL: " + groupEmail -> STATUS_NOT_FOUND
                })
              }
              else Future(Try("GOOGLE_USER_ACCESS_LEVEL" -> WorkspaceAccessLevels.NoAccess.toString))
            }
            case (_, _) => Future(Try("GOOGLE_USER_ACCESS_LEVEL" -> STATUS_NA))
          }

          Future.sequence(googleGroupStatuses++Seq(bucketStatus,bucketWriteStatus,userProxyStatus,googleAccessLevel)).map { tries =>
            val statuses = tries.collect { case Success(s) => s }.toSeq
            RequestComplete(WorkspaceStatus(workspaceName, (rawlsGroupStatuses++statuses++Seq(userStatus,userAccessLevel)).toMap))
          }
        }
      }
    }
  }

  // helper methods

  private def withNewWorkspaceContext(workspaceRequest: WorkspaceRequest, txn: RawlsTransaction)
                                     (op: (WorkspaceContext) => PerRequestMessage): Future[PerRequestMessage] = {
    val workspaceName = workspaceRequest.toWorkspaceName
    requireCreateWorkspaceAccess(workspaceName, txn) {
      containerDAO.workspaceDAO.loadContext(workspaceName, txn) match {
        case Some(_) => Future.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.Conflict, s"Workspace ${workspaceRequest.namespace}/${workspaceRequest.name} already exists")))
        case None =>
          val workspaceId = UUID.randomUUID.toString
          gcsDAO.setupWorkspace(userInfo, workspaceRequest.namespace, workspaceId, workspaceName) map { googleWorkspaceInfo =>
            val currentDate = DateTime.now
            googleWorkspaceInfo.groupsByAccessLevel.values.foreach(containerDAO.authDAO.saveGroup(_, txn))

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
              accessLevels = googleWorkspaceInfo.groupsByAccessLevel.map { case (a, g) => (a -> RawlsGroup.toRef(g))})

            op(containerDAO.workspaceDAO.save(workspace, txn))
          }
      }
    }
  }

  private def noSuchWorkspaceMessage(workspaceName: WorkspaceName) = s"${workspaceName} does not exist"
  private def accessDeniedMessage(workspaceName: WorkspaceName) = s"insufficient permissions to perform operation on ${workspaceName}"

  private def requireCreateWorkspaceAccess(workspaceName: WorkspaceName, txn: RawlsTransaction)(op: => Future[PerRequestMessage]): Future[PerRequestMessage] = {
    containerDAO.billingDAO.loadProject(RawlsBillingProjectName(workspaceName.namespace), txn) match {
      case Some(billingProject) =>
        if (billingProject.users.contains(RawlsUser(userInfo))) {
          op
        } else {
          Future.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.Forbidden, s"You are not authorized to create a workspace in billing project ${workspaceName.namespace}")))
        }
      case None => Future.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.BadRequest, s"billing project ${workspaceName.namespace} not found")))
    }
  }

  private def withWorkspaceContextAndPermissions(workspaceName: WorkspaceName, accessLevel: WorkspaceAccessLevel, txn: RawlsTransaction)(op: (WorkspaceContext) => Future[PerRequestMessage]): Future[PerRequestMessage] = {
    withWorkspaceContext(workspaceName, txn) { workspaceContext =>
      requireAccess(workspaceContext.workspace, accessLevel, txn) { op(workspaceContext) }
    }
  }

  private def withWorkspaceContext(workspaceName: WorkspaceName, txn: RawlsTransaction)(op: (WorkspaceContext) => Future[PerRequestMessage]) = {
    assert( txn.readLocks.contains(workspaceName) || txn.writeLocks.contains(workspaceName),
            s"Attempting to use context on workspace $workspaceName but it's not read or write locked! Add it to inTransaction or inFutureTransaction")
    containerDAO.workspaceDAO.loadContext(workspaceName, txn) match {
      case None => Future.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.NotFound, noSuchWorkspaceMessage(workspaceName))))
      case Some(workspaceContext) => op(workspaceContext)
    }
  }

  private def requireAccess(workspace: Workspace, requiredLevel: WorkspaceAccessLevel, txn: RawlsTransaction)(codeBlock: => Future[PerRequestMessage]): Future[PerRequestMessage] = {
    val userLevel = containerDAO.authDAO.getMaximumAccessLevel(RawlsUser(userInfo), workspace.workspaceId, txn)
    if (userLevel >= requiredLevel) {
      if ( (requiredLevel > WorkspaceAccessLevels.Read) && workspace.isLocked )
        Future.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.Forbidden, s"The workspace ${workspace.toWorkspaceName} is locked.")))
      else codeBlock
    }
    else if (userLevel >= WorkspaceAccessLevels.Read) Future.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.Forbidden, accessDeniedMessage(workspace.toWorkspaceName))))
    else Future.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.NotFound, noSuchWorkspaceMessage(workspace.toWorkspaceName))))
  }

  private def requireOwnerIgnoreLock(workspace: Workspace, txn: RawlsTransaction)(op: => Future[PerRequestMessage]): Future[PerRequestMessage] = {
    requireAccess(workspace.copy(isLocked = false),WorkspaceAccessLevels.Owner, txn)(op)
  }

  private def withEntity(workspaceContext: WorkspaceContext, entityType: String, entityName: String, txn: RawlsTransaction)(op: (Entity) => Future[PerRequestMessage]): Future[PerRequestMessage] = {
    containerDAO.entityDAO.get(workspaceContext, entityType, entityName, txn) match {
      case None => Future.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.NotFound, s"${entityType} ${entityName} does not exist in ${workspaceContext}")))
      case Some(entity) => op(entity)
    }
  }

  private def withMethodConfig(workspaceContext: WorkspaceContext, methodConfigurationNamespace: String, methodConfigurationName: String, txn: RawlsTransaction)(op: (MethodConfiguration) => Future[PerRequestMessage]): Future[PerRequestMessage] = {
    containerDAO.methodConfigurationDAO.get(workspaceContext, methodConfigurationNamespace, methodConfigurationName, txn) match {
      case None => Future.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.NotFound, s"${methodConfigurationNamespace}/${methodConfigurationName} does not exist in ${workspaceContext}")))
      case Some(methodConfiguration) => op(methodConfiguration)
    }
  }

  private def withMethod(methodNamespace: String, methodName: String, methodVersion: Int, userInfo: UserInfo)(op: (AgoraEntity) => Future[PerRequestMessage]): Future[PerRequestMessage] = {
    toFutureTry(methodRepoDAO.getMethod(methodNamespace, methodName, methodVersion, userInfo)) flatMap { agoraEntityOption => agoraEntityOption match {
      case Success(None) => Future.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.NotFound, s"Cannot get ${methodNamespace}/${methodName}/${methodVersion} from method repo.")))
      case Success(Some(agoraEntity)) => op(agoraEntity)
      case Failure(throwable) => Future.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.BadGateway, s"Unable to query the method repo.",methodRepoDAO.toErrorReport(throwable))))
    }}
  }

  private def withWdl(method: AgoraEntity)(op: (String) => Future[PerRequestMessage]): Future[PerRequestMessage] = {
    method.payload match {
      case None => Future.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.NotFound, "Can't get method's WDL from Method Repo: payload empty.")))
      case Some(wdl) => op(wdl)
    }
  }

  private def withSubmission(workspaceContext: WorkspaceContext, submissionId: String, txn: RawlsTransaction)(op: (Submission) => Future[PerRequestMessage]): Future[PerRequestMessage] = {
    containerDAO.submissionDAO.get(workspaceContext, submissionId, txn) match {
      case None => Future.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.NotFound, s"Submission with id ${submissionId} not found in workspace ${workspaceContext}")))
      case Some(submission) => op(submission)
    }
  }

  private def withWorkflow(workspaceName: WorkspaceName, submission: Submission, workflowId: String)(op: (Workflow) => Future[PerRequestMessage]): Future[PerRequestMessage] = {
    submission.workflows.find(wf => wf.workflowId == workflowId) match {
      case None => Future.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.NotFound, s"Workflow with id ${workflowId} not found in submission ${submission.submissionId} in workspace ${workspaceName.namespace}/${workspaceName.name}")))
      case Some(workflow) => op(workflow)
    }
  }

  private def buildWorkflowOptions(workspaceContext: WorkspaceContext, submissionId: String): Future[Option[String]] = {
    val bucketName = workspaceContext.workspace.bucketName
    val billingProjectName = RawlsBillingProjectName(workspaceContext.workspace.namespace)

    // note: executes in a Future
    gcsDAO.getToken(RawlsUser(userInfo)) map { optToken =>
      val refreshToken = optToken getOrElse {
        throw new RawlsException(s"Refresh token missing for user ${userInfo.userEmail}")
      }

      val billingProject = dataSource.inTransaction() { txn =>
        containerDAO.billingDAO.loadProject(billingProjectName, txn) getOrElse {
          throw new RawlsException(s"Billing Project with name ${billingProjectName.value} not found")
        }
      }

      import ExecutionJsonSupport.ExecutionServiceWorkflowOptionsFormat

      Option(ExecutionServiceWorkflowOptions(
        s"gs://${bucketName}/${submissionId}",
        workspaceContext.workspace.namespace,
        userInfo.userEmail,
        refreshToken,
        billingProject.cromwellAuthBucketUrl
      ).toJson.toString)
    }
  }

  private def withSubmissionEntities(submissionRequest: SubmissionRequest, workspaceContext: WorkspaceContext, rootEntityType: String, txn: RawlsTransaction)(op: (Seq[Entity]) => Future[PerRequestMessage]): Future[PerRequestMessage] = {
    //If there's an expression, evaluate it to get the list of entities to run this job on.
    //Otherwise, use the entity given in the submission.
    submissionRequest.expression match {
      case None =>
        if ( submissionRequest.entityType != rootEntityType )
          Future.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.BadRequest, s"Method configuration expects an entity of type ${rootEntityType}, but you gave us an entity of type ${submissionRequest.entityType}.")))
        else
          containerDAO.entityDAO.get(workspaceContext,submissionRequest.entityType,submissionRequest.entityName,txn) match {
            case None =>
              Future.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.NotFound, s"No entity of type ${submissionRequest.entityType} named ${submissionRequest.entityName} exists in this workspace.")))
            case Some(entity) =>
              op(Seq(entity))
          }
      case Some(expression) =>
        new ExpressionEvaluator(new ExpressionParser()).evalFinalEntity(workspaceContext, submissionRequest.entityType, submissionRequest.entityName, expression) match {
          case Failure(regret) =>
            Future.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(regret, StatusCodes.BadRequest)))
          case Success(entities) =>
            if ( entities.isEmpty )
              Future.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.BadRequest, "No entities eligible for submission were found.")))
            else {
              val eligibleEntities = entities.filter(_.entityType == rootEntityType)
              if (eligibleEntities.isEmpty)
                Future.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.BadRequest, s"The expression in your SubmissionRequest matched only entities of the wrong type. (Expected type ${rootEntityType}.)")))
              else
                op(eligibleEntities)
            }
        }
    }
  }

  private def withMethodInputs(methodConfig: MethodConfiguration)(op: (String, Seq[MethodInput]) => Future[PerRequestMessage]): Future[PerRequestMessage] = {
    // TODO add Method to model instead of exposing AgoraEntity?
    val methodRepoMethod = methodConfig.methodRepoMethod
    toFutureTry(methodRepoDAO.getMethod(methodRepoMethod.methodNamespace, methodRepoMethod.methodName, methodRepoMethod.methodVersion, userInfo)) flatMap { _ match {
      case Success(None) => Future.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.NotFound, s"Cannot get ${methodRepoMethod.methodNamespace}/${methodRepoMethod.methodName}/${methodRepoMethod.methodVersion} from method repo.")))
      case Success(Some(agoraEntity)) => agoraEntity.payload match {
        case None => Future.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.NotFound, "Can't get method's WDL from Method Repo: payload empty.")))
        case Some(wdl) => Try(MethodConfigResolver.gatherInputs(methodConfig,wdl)) match {
          case Failure(exception) => Future.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(exception,StatusCodes.BadRequest)))
          case Success(methodInputs) => op(wdl,methodInputs)
        }
      }
      case Failure(throwable) => Future.failed(new RawlsExceptionWithErrorReport(errorReport = ErrorReport(StatusCodes.BadGateway, s"Unable to query the method repo.",methodRepoDAO.toErrorReport(throwable))))
    }}
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
