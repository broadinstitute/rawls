package org.broadinstitute.dsde.rawls.workspace

import java.util.UUID

import akka.actor.{ActorRef, Actor, Props}
import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.jobexec.MethodConfigResolver
import org.broadinstitute.dsde.rawls.jobexec.SubmissionSupervisor.SubmissionStarted
import org.broadinstitute.dsde.rawls.model.GCSAccessLevel.GCSAccessLevel
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._
import org.broadinstitute.dsde.rawls.model.ExecutionJsonSupport.SubmissionFormat
import org.broadinstitute.dsde.rawls.model.BucketAccessControlJsonSupport.BucketAccessControlsFormat
import org.broadinstitute.dsde.rawls.dataaccess.{MethodConfigurationDAO, EntityDAO, WorkspaceDAO}
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.model.AttributeConversions
import org.broadinstitute.dsde.rawls.expressions._
import org.broadinstitute.dsde.rawls.webservice.PerRequest
import org.broadinstitute.dsde.rawls.webservice.PerRequest.{RequestCompleteWithHeaders, PerRequestMessage, RequestComplete}
import AttributeUpdateOperations._
import org.broadinstitute.dsde.rawls.workspace.WorkspaceService._
import org.joda.time.DateTime
import spray.http
import spray.http.Uri
import spray.http.HttpHeaders.Location
import spray.http.{StatusCodes, HttpCookie}
import spray.httpx.SprayJsonSupport._
import spray.json._

import scala.util.{Failure, Success, Try}

/**
 * Created by dvoet on 4/27/15.
 */

object WorkspaceService {
  sealed trait WorkspaceServiceMessage
  case class RegisterUser(callbackPath: String) extends WorkspaceServiceMessage
  case class CompleteUserRegistration( authCode: String, state: String, callbackPath: String) extends WorkspaceServiceMessage

  case class CreateWorkspace(workspace: WorkspaceRequest) extends WorkspaceServiceMessage
  case class GetWorkspace(workspaceName: WorkspaceName) extends WorkspaceServiceMessage
  case class UpdateWorkspace(workspaceName: WorkspaceName, operations: Seq[AttributeUpdateOperation]) extends WorkspaceServiceMessage
  case object ListWorkspaces extends WorkspaceServiceMessage
  case class CloneWorkspace(sourceWorkspace: WorkspaceName, destWorkspace: WorkspaceName) extends WorkspaceServiceMessage
  case class GetACL(workspaceName: WorkspaceName) extends WorkspaceServiceMessage
  case class PutACL(workspaceName: WorkspaceName, acl: String) extends WorkspaceServiceMessage

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
  case class CopyMethodConfigurationFromMethodRepo(query: MethodRepoConfigurationQuery) extends WorkspaceServiceMessage
  case class ListMethodConfigurations(workspaceName: WorkspaceName) extends WorkspaceServiceMessage

  case class CreateSubmission(workspaceName: WorkspaceName, submission: SubmissionRequest) extends WorkspaceServiceMessage
  case class GetSubmissionStatus(workspaceName: WorkspaceName, submissionId: String) extends WorkspaceServiceMessage

  def props(workspaceServiceConstructor: UserInfo => WorkspaceService, userInfo: UserInfo): Props = {
    Props(workspaceServiceConstructor(userInfo))
  }

  def constructor(dataSource: DataSource, workspaceDAO: WorkspaceDAO, entityDAO: EntityDAO, methodConfigurationDAO: MethodConfigurationDAO, methodRepoDAO: MethodRepoDAO, executionServiceDAO: ExecutionServiceDAO, gcsDAO: GoogleCloudStorageDAO, submissionSupervisor : ActorRef, submissionDAO: SubmissionDAO)(userInfo: UserInfo) =
    new WorkspaceService(userInfo, dataSource, workspaceDAO, entityDAO, methodConfigurationDAO, methodRepoDAO, executionServiceDAO, gcsDAO, submissionSupervisor, submissionDAO)
}

class WorkspaceService(userInfo: UserInfo, dataSource: DataSource, workspaceDAO: WorkspaceDAO, entityDAO: EntityDAO, methodConfigurationDAO: MethodConfigurationDAO, methodRepoDAO: MethodRepoDAO, executionServiceDAO: ExecutionServiceDAO, gcsDAO: GoogleCloudStorageDAO, submissionSupervisor : ActorRef, submissionDAO: SubmissionDAO) extends Actor {

  override def receive = {
    case RegisterUser(callbackPath) => context.parent ! registerUser(callbackPath)
    case CompleteUserRegistration(authCode, state, callbackPath) => context.parent ! completeUserRegistration(authCode,state,callbackPath)

    case CreateWorkspace(workspace) => context.parent ! createWorkspace(workspace)
    case GetWorkspace(workspaceName) => context.parent ! getWorkspace(workspaceName)
    case UpdateWorkspace(workspaceName, operations) => context.parent ! updateWorkspace(workspaceName, operations)
    case ListWorkspaces => context.parent ! listWorkspaces(dataSource)
    case CloneWorkspace(sourceWorkspace, destWorkspace) => context.parent ! cloneWorkspace(sourceWorkspace, destWorkspace)
    case GetACL(workspaceName) => context.parent ! getACL(workspaceName)
    case PutACL(workspaceName, acl) => context.parent ! putACL(workspaceName, acl)

    case CreateEntity(workspaceName, entity) => context.parent ! createEntity(workspaceName, entity)
    case GetEntity(workspaceName, entityType, entityName) => context.parent ! getEntity(workspaceName, entityType, entityName)
    case UpdateEntity(workspaceName, entityType, entityName, operations) => context.parent ! updateEntity(workspaceName, entityType, entityName, operations)
    case DeleteEntity(workspaceName, entityType, entityName) => context.parent ! deleteEntity(workspaceName, entityType, entityName)
    case RenameEntity(workspaceName, entityType, entityName, newName) => context.parent ! renameEntity(workspaceName, entityType, entityName, newName)
    case EvaluateExpression(workspaceName, entityType, entityName, expression) => context.parent ! evaluateExpression(workspaceName, entityType, entityName, expression)
    case ListEntityTypes(workspaceName) => context.parent ! listEntityTypes(workspaceName)
    case ListEntities(workspaceName, entityType) => context.parent ! listEntities(workspaceName, entityType)
    case CopyEntities(entityCopyDefinition, uri:Uri) => context.parent ! copyEntities(entityCopyDefinition, uri)
    case BatchUpsertEntities(workspaceName, entityUpdates) => context.parent ! batchUpsertEntities(workspaceName, entityUpdates)
    case BatchUpdateEntities(workspaceName, entityUpdates) => context.parent ! batchUpdateEntities(workspaceName, entityUpdates)

    case CreateMethodConfiguration(workspaceName, methodConfiguration) => context.parent ! createMethodConfiguration(workspaceName, methodConfiguration)
    case RenameMethodConfiguration(workspaceName, methodConfigurationNamespace, methodConfigurationName, newName) => context.parent ! renameMethodConfiguration(workspaceName, methodConfigurationNamespace, methodConfigurationName, newName)
    case DeleteMethodConfiguration(workspaceName, methodConfigurationNamespace, methodConfigurationName) => context.parent ! deleteMethodConfiguration(workspaceName, methodConfigurationNamespace, methodConfigurationName)
    case GetMethodConfiguration(workspaceName, methodConfigurationNamespace, methodConfigurationName) => context.parent ! getMethodConfiguration(workspaceName, methodConfigurationNamespace, methodConfigurationName)
    case UpdateMethodConfiguration(workspaceName, methodConfiguration) => context.parent ! updateMethodConfiguration(workspaceName, methodConfiguration)
    case CopyMethodConfiguration(methodConfigNamePair) => context.parent ! copyMethodConfiguration(methodConfigNamePair)
    case CopyMethodConfigurationFromMethodRepo(query) => context.parent ! copyMethodConfigurationFromMethodRepo(query)
    case ListMethodConfigurations(workspaceName) => context.parent ! listMethodConfigurations(workspaceName)

    case CreateSubmission(workspaceName, submission) => context.parent ! createSubmission(workspaceName, submission)
    case GetSubmissionStatus(workspaceName, submissionId) => context.parent ! getSubmissionStatus(workspaceName, submissionId)
  }

  def registerUser(callbackPath: String): PerRequestMessage = {
    RequestCompleteWithHeaders(StatusCodes.SeeOther,Location(gcsDAO.getGoogleRedirectURI(userInfo.userId, callbackPath)))
  }

  def completeUserRegistration(authCode: String, state: String, callbackPath: String): PerRequestMessage = {
    gcsDAO.storeUser(userInfo.userId,authCode,state,callbackPath)
    RequestComplete(StatusCodes.Created)
  }

  private def createBucketName(workspaceName: String) = s"${workspaceName}-${UUID.randomUUID}"

  def createWorkspace(workspaceRequest: WorkspaceRequest): PerRequestMessage =
    dataSource inTransaction { txn =>
      workspaceDAO.load(workspaceRequest.namespace, workspaceRequest.name, txn) match {
        case Some(_) =>
          PerRequest.RequestComplete(StatusCodes.Conflict, s"Workspace ${workspaceRequest.namespace}/${workspaceRequest.name} already exists")
        case None =>
          val bucketName = createBucketName(workspaceRequest.name)
          Try( gcsDAO.createBucket(userInfo.userId,workspaceRequest.namespace, bucketName) ) match {
            case Failure(err) => RequestComplete(StatusCodes.Forbidden, s"Unable to create bucket for ${workspaceRequest.namespace}/${workspaceRequest.name}: " + err.getMessage)
            case Success(_) =>
              val workspace = Workspace(workspaceRequest.namespace, workspaceRequest.name, bucketName, DateTime.now, userInfo.userId, workspaceRequest.attributes)
              workspaceDAO.save(workspace, txn)
              PerRequest.RequestComplete((StatusCodes.Created, workspace))
          }
      }
    }

  def getWorkspace(workspaceName: WorkspaceName): PerRequestMessage =
    dataSource inTransaction { txn =>
      withWorkspace(workspaceName, txn) { workspace =>
        requireAccess(GCSAccessLevel.Read, workspace, txn) {
          RequestComplete(workspace)
        }
      }
    }

  def updateWorkspace(workspaceName: WorkspaceName, operations: Seq[AttributeUpdateOperation]): PerRequestMessage =
    dataSource inTransaction { txn =>
      withWorkspace(workspaceName, txn) { workspace =>
        requireAccess(GCSAccessLevel.Write, workspace, txn) {
          try {
            val updatedWorkspace = applyOperationsToWorkspace(workspace, operations)
            RequestComplete(workspaceDAO.save(updatedWorkspace, txn))
          } catch {
            case e: AttributeUpdateOperationException => RequestComplete(http.StatusCodes.BadRequest, s"in ${workspaceName}, ${e.getMessage}")
          }
        }
      }
    }

  def listWorkspaces(dataSource: DataSource): PerRequestMessage =
    dataSource inTransaction { txn =>
      RequestComplete(workspaceDAO.list(txn))
    }

  def cloneWorkspace(sourceWorkspace: WorkspaceName, destWorkspace: WorkspaceName): PerRequestMessage =
    dataSource inTransaction { txn =>
      val originalWorkspace = workspaceDAO.load(sourceWorkspace.namespace, sourceWorkspace.name, txn)
      val copyWorkspace = workspaceDAO.load(destWorkspace.namespace, destWorkspace.name, txn)

      (originalWorkspace, copyWorkspace) match {
        case (Some(ws), None) => {
          val bucketName = createBucketName(destWorkspace.namespace)
          Try( gcsDAO.createBucket(userInfo.userId, destWorkspace.namespace, bucketName) ) match {
            case Failure(err) => RequestComplete(StatusCodes.Forbidden,s"Unable to create bucket for ${destWorkspace}: "+err.getMessage)
            case Success(_) =>
              val newWorkspace = Workspace(destWorkspace.namespace, destWorkspace.name, bucketName, DateTime.now, userInfo.userId, ws.attributes)
              workspaceDAO.save(newWorkspace, txn)
              entityDAO.cloneAllEntities(ws.namespace, newWorkspace.namespace, ws.name, newWorkspace.name, txn)
              methodConfigurationDAO.list(ws.namespace, ws.name, txn).foreach { methodConfig =>
                methodConfigurationDAO.save(newWorkspace.namespace, newWorkspace.name, methodConfigurationDAO.get(ws.namespace, ws.name, methodConfig.namespace, methodConfig.name, txn).get, txn)
              }
              RequestComplete((StatusCodes.Created, newWorkspace))
          }
        }
        case (None, _) => RequestComplete(StatusCodes.NotFound, s"Source workspace ${sourceWorkspace} not found")
        case (_, Some(_)) => RequestComplete(StatusCodes.Conflict, s"Destination workspace ${destWorkspace} already exists")
      }
    }

  def getACL(workspaceName: WorkspaceName): PerRequestMessage =
    dataSource inTransaction { txn =>
      withWorkspace(workspaceName, txn) { workspace =>
        requireAccess(GCSAccessLevel.Owner, workspace, txn) {
          Try(gcsDAO.getACL(userInfo.userId,workspace.bucketName)) match {
            case Success(acl) => RequestComplete(StatusCodes.OK, acl)
            case Failure(err) => RequestComplete(StatusCodes.Forbidden, s"Can't retrieve ACL for workspace ${workspaceName}: " + err.getMessage())
          }
        }
      }
    }

  def putACL(workspaceName: WorkspaceName, acl: String): PerRequestMessage =
    dataSource inTransaction { txn =>
      withWorkspace(workspaceName, txn) { workspace =>
        requireAccess(GCSAccessLevel.Owner, workspace, txn) {
          Try(gcsDAO.putACL(userInfo.userId,workspace.bucketName, acl)) match {
            case Failure(err) => RequestComplete(StatusCodes.Forbidden, s"Can't set ACL for workspace ${workspaceName}: " + err.getMessage())
            case _ => RequestComplete(StatusCodes.OK)
          }
        }
      }
    }

  def copyEntities(entityCopyDef: EntityCopyDefinition, uri: Uri): PerRequestMessage =
    dataSource inTransaction { txn =>
      val sourceWorkspace = entityCopyDef.sourceWorkspace

      requireAccess(GCSAccessLevel.Read, sourceWorkspace, txn) {
        val destWorkspace = entityCopyDef.destinationWorkspace

        requireAccess(GCSAccessLevel.Write, destWorkspace, txn) {
          val entityNames = entityCopyDef.entityNames
          val entityType = entityCopyDef.entityType

          val conflicts = entityDAO.copyEntities(destWorkspace.namespace, destWorkspace.name, sourceWorkspace.namespace, sourceWorkspace.name, entityType, entityNames, txn)

          conflicts.size match {
            case 0 => {
              // get the entities that were copied into the destination workspace
              val entityCopies = entityDAO.list(destWorkspace.namespace, destWorkspace.name, entityType, txn).filter((e: Entity) => entityNames.contains(e.name)).toList
              RequestComplete(StatusCodes.Created, entityCopies)
            }
            case _ => {
              val basePath = s"/${destWorkspace.namespace}/${destWorkspace.name}/entities/"
              val conflictUris = conflicts.map(conflict => uri.copy(path = Uri.Path(basePath + s"${conflict.entityType}/${conflict.name}")).toString())
              val conflictingEntities = ConflictingEntities(conflictUris)
              RequestComplete(StatusCodes.Conflict, conflictingEntities)
            }
          }
        }
      }
    }

  def createEntity(workspaceName: WorkspaceName, entity: Entity): PerRequestMessage =
    dataSource inTransaction { txn =>
      withWorkspace(workspaceName, txn) { workspace =>
        requireAccess(GCSAccessLevel.Write, workspace, txn) {
          entityDAO.get(workspaceName.namespace, workspaceName.name, entity.entityType, entity.name, txn) match {
            case Some(_) => RequestComplete(StatusCodes.Conflict, s"${entity.entityType} ${entity.name} already exists in ${workspaceName}")
            case None => RequestComplete(StatusCodes.Created, entityDAO.save(workspaceName.namespace, workspaceName.name, entity, txn))
          }
        }
      }
    }

  def batchUpdateEntities(workspaceName: WorkspaceName, entityUpdates: Seq[EntityUpdateDefinition]): PerRequestMessage =
    dataSource inTransaction { txn =>
      withWorkspace(workspaceName, txn) { workspace =>
        requireAccess(GCSAccessLevel.Write, workspace, txn) {
          val results = entityUpdates.map { entityUpdate =>
            val entity = entityDAO.get(workspaceName.namespace, workspaceName.name, entityUpdate.entityType, entityUpdate.name, txn)

            entity match {
              case Some(e) =>
                val trial = Try {
                  val updatedEntity = applyOperationsToEntity(e, entityUpdate.operations)
                  entityDAO.save(workspaceName.namespace, workspaceName.name, updatedEntity, txn)
                }
                (entityUpdate, trial)
              case None => (entityUpdate, Failure(new RuntimeException("Entity does not exist")))
            }
          }
          val errorMessages = results.collect{
            case (entityUpdate, Failure(regrets)) => s"Could not update ${entityUpdate.entityType} ${entityUpdate.name} : ${regrets.getMessage}"
          }
          if(errorMessages.isEmpty) {
            RequestComplete(StatusCodes.NoContent)
          } else {
            dataSource.rollbackOnly.set(true)
            RequestComplete(StatusCodes.BadRequest, errorMessages)
          }
        }
      }
    }

  def batchUpsertEntities(workspaceName: WorkspaceName, entityUpdates: Seq[EntityUpdateDefinition]): PerRequestMessage =
    dataSource inTransaction { txn =>
      withWorkspace(workspaceName, txn) { workspace =>
        requireAccess(GCSAccessLevel.Write, workspace, txn) {
          val results = entityUpdates.map { entityUpdate =>
            val entity = entityDAO.get(workspaceName.namespace, workspaceName.name, entityUpdate.entityType, entityUpdate.name, txn) match {
              case Some(e) => e
              case None => entityDAO.save(workspaceName.namespace, workspaceName.name, Entity(entityUpdate.name, entityUpdate.entityType, Map.empty, workspace.toWorkspaceName), txn)
            }
            val trial = Try {
              val updatedEntity = applyOperationsToEntity(entity, entityUpdate.operations)
              entityDAO.save(workspaceName.namespace, workspaceName.name, updatedEntity, txn)
            }
            (entityUpdate, trial)
          }
          val errorMessages = results.collect {
            case (entityUpdate, Failure(regrets)) => s"Could not update ${entityUpdate.entityType} ${entityUpdate.name} : ${regrets.getMessage}"
          }
          if (errorMessages.isEmpty) {
            RequestComplete(StatusCodes.NoContent)
          } else {
            dataSource.rollbackOnly.set(true)
            RequestComplete(StatusCodes.BadRequest, errorMessages)
          }
        }
      }
    }

  def listEntityTypes(workspaceName: WorkspaceName): PerRequestMessage =
    dataSource inTransaction { txn =>
      withWorkspace(workspaceName, txn) { workspace =>
        requireAccess(GCSAccessLevel.Read, workspace, txn) {
          RequestComplete(entityDAO.getEntityTypes(workspaceName.namespace, workspaceName.name, txn))
        }
      }
    }

  def listEntities(workspaceName: WorkspaceName, entityType: String): PerRequestMessage =
    dataSource inTransaction { txn =>
      withWorkspace(workspaceName, txn) { workspace =>
        requireAccess(GCSAccessLevel.Read, workspace, txn) {
          RequestComplete(entityDAO.list(workspaceName.namespace, workspaceName.name, entityType, txn).toList)
        }
      }
    }

  def getEntity(workspaceName: WorkspaceName, entityType: String, entityName: String): PerRequestMessage =
    dataSource inTransaction { txn =>
      withWorkspace(workspaceName, txn) { workspace =>
        requireAccess(GCSAccessLevel.Read, workspace, txn) {
          withEntity(workspace, entityType, entityName, txn) { entity =>
            PerRequest.RequestComplete(entity)
          }
        }
      }
    }

  def updateEntity(workspaceName: WorkspaceName, entityType: String, entityName: String, operations: Seq[AttributeUpdateOperation]): PerRequestMessage =
    dataSource inTransaction { txn =>
      withWorkspace(workspaceName, txn) { workspace =>
        requireAccess(GCSAccessLevel.Write, workspace, txn) {
          withEntity(workspace, entityType, entityName, txn) { entity =>
            try {
              val updatedEntity = applyOperationsToEntity(entity, operations)
              RequestComplete(entityDAO.save(workspaceName.namespace, workspaceName.name, updatedEntity, txn))
            } catch {
              case e: AttributeUpdateOperationException => RequestComplete(http.StatusCodes.BadRequest, s"in ${workspaceName}, ${e.getMessage}")
            }
          }
        }
      }
    }

  def deleteEntity(workspaceName: WorkspaceName, entityType: String, entityName: String): PerRequestMessage =
    dataSource inTransaction { txn =>
      withWorkspace(workspaceName, txn) { workspace =>
        requireAccess(GCSAccessLevel.Write, workspace, txn) {
          withEntity(workspace, entityType, entityName, txn) { entity =>
            entityDAO.delete(workspace.namespace, workspace.name, entity.entityType, entity.name, txn)
            RequestComplete(http.StatusCodes.NoContent)
          }
        }
      }
    }

  def renameEntity(workspaceName: WorkspaceName, entityType: String, entityName: String, newName: String): PerRequestMessage =
    dataSource inTransaction { txn =>
      withWorkspace(workspaceName, txn) { workspace =>
        requireAccess(GCSAccessLevel.Write, workspace, txn) {
          withEntity(workspace, entityType, entityName, txn) { entity =>
            entityDAO.get(workspace.namespace, workspace.name, entity.entityType, newName, txn) match {
              case None =>
                entityDAO.rename(workspace.namespace, workspace.name, entity.entityType, entity.name, newName, txn)
                RequestComplete(http.StatusCodes.NoContent)
              case Some(_) => RequestComplete(StatusCodes.Conflict, s"Destination ${entity.entityType} ${newName} already exists")
            }
          }
        }
      }
    }

  def evaluateExpression(workspaceName: WorkspaceName, entityType: String, entityName: String, expression: String): PerRequestMessage =
    dataSource inTransaction { txn =>
      requireAccess(GCSAccessLevel.Read, workspaceName, txn) {
        txn withGraph { graph =>
          new ExpressionEvaluator(graph, new ExpressionParser())
            .evalFinalAttribute(workspaceName.namespace, workspaceName.name, entityType, entityName, expression) match {
              case Success(result) => RequestComplete(http.StatusCodes.OK, result)
              case Failure(regret) => RequestComplete(http.StatusCodes.BadRequest, regret.getMessage)
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

            case Some(_) => throw new AttributeUpdateOperationException(s"$attributeListName of ${attributable.path} is not a list")
          }

        case RemoveListMember(attributeListName, removeMember) =>
          startingAttributes.get(attributeListName) match {
            case Some(l: AttributeValueList) =>
              startingAttributes + (attributeListName -> AttributeValueList(l.list.filterNot(_ == removeMember)))
            case Some(l: AttributeEntityReferenceList) =>
              startingAttributes + (attributeListName -> AttributeEntityReferenceList(l.list.filterNot(_ == removeMember)))
            case None => throw new AttributeNotFoundException(s"$attributeListName of ${attributable.path} does not exist")
            case Some(_) => throw new AttributeUpdateOperationException(s"$attributeListName of ${attributable.path} is not a list")
          }
      }
    }
  }

  private def noSuchWorkspaceMessage( workspaceName: WorkspaceName ) = s"${workspaceName} does not exist"

  private def withWorkspace(workspaceName: WorkspaceName, txn: RawlsTransaction)(op: (Workspace) => PerRequestMessage): PerRequestMessage = {
    workspaceDAO.load(workspaceName.namespace, workspaceName.name, txn) match {
      case None => RequestComplete(http.StatusCodes.NotFound, noSuchWorkspaceMessage(workspaceName))
      case Some(workspace) => op(workspace)
    }
  }

  private def withEntity(workspace: Workspace, entityType: String, entityName: String, txn: RawlsTransaction)(op: (Entity) => PerRequestMessage): PerRequestMessage = {
    entityDAO.get(workspace.namespace, workspace.name, entityType, entityName, txn) match {
      case None => RequestComplete(http.StatusCodes.NotFound, s"${entityType} ${entityName} does not exist in ${workspace.namespace}/${workspace.name}")
      case Some(entity) => op(entity)
    }
  }

  private def withMethodConfig(workspace: Workspace, methodConfigurationNamespace: String, methodConfigurationName: String, txn: RawlsTransaction)(op: (MethodConfiguration) => PerRequestMessage): PerRequestMessage = {
    methodConfigurationDAO.get(workspace.namespace, workspace.name, methodConfigurationNamespace, methodConfigurationName, txn) match {
      case None => RequestComplete(http.StatusCodes.NotFound, s"${methodConfigurationNamespace}/${methodConfigurationName} does not exist in ${workspace.namespace}/${workspace.name}")
      case Some(methodConfiguration) => op(methodConfiguration)
    }
  }

  private def withMethod(workspace: Workspace, methodNamespace: String, methodName: String, methodVersion: String, authCookie: HttpCookie)(op: (AgoraEntity) => PerRequestMessage): PerRequestMessage = {
    // TODO add Method to model instead of exposing AgoraEntity?
    methodRepoDAO.getMethod(methodNamespace, methodName, methodVersion, authCookie) match {
      case None => RequestComplete(http.StatusCodes.NotFound, s"Cannot get ${methodNamespace}/${methodName}/${methodVersion} from method repo.")
      case Some(agoraEntity) => op(agoraEntity)
    }
  }

  private def withWdl(method: AgoraEntity)(op: (String) => PerRequestMessage): PerRequestMessage = {
    method.payload match {
      case None => RequestComplete(StatusCodes.NotFound, "Can't get method's WDL from Method Repo: payload empty.")
      case Some(wdl) => op(wdl)
    }
  }

  private def withSubmission(workspace: Workspace, submissionId: String, txn: RawlsTransaction)(op: (Submission) => PerRequestMessage): PerRequestMessage = {
    submissionDAO.get(workspace.namespace, workspace.name, submissionId, txn) match {
      case None => RequestComplete(StatusCodes.NotFound, s"Submission with id ${submissionId} not found in workspace ${workspace.namespace}/${workspace.name}")
      case Some(submission) => op(submission)
    }
  }

  def createMethodConfiguration(workspaceName: WorkspaceName, methodConfiguration: MethodConfiguration): PerRequestMessage =
    dataSource inTransaction { txn =>
      withWorkspace(workspaceName, txn) { workspace =>
        requireAccess(GCSAccessLevel.Write, workspace, txn) {
          methodConfigurationDAO.get(workspace.namespace, workspace.name, methodConfiguration.namespace, methodConfiguration.name, txn) match {
            case Some(_) => RequestComplete(StatusCodes.Conflict, s"${methodConfiguration.name} already exists in ${workspaceName}")
            case None => RequestComplete(StatusCodes.Created, methodConfigurationDAO.save(workspaceName.namespace, workspaceName.name, methodConfiguration, txn))
          }
        }
      }
    }

  def deleteMethodConfiguration(workspaceName: WorkspaceName, methodConfigurationNamespace: String, methodConfigurationName: String): PerRequestMessage =
    dataSource inTransaction { txn =>
      withWorkspace(workspaceName, txn) { workspace =>
        requireAccess(GCSAccessLevel.Write, workspace, txn) {
          withMethodConfig(workspace, methodConfigurationNamespace, methodConfigurationName, txn) { methodConfig =>
            methodConfigurationDAO.delete(workspace.namespace, workspace.name, methodConfigurationNamespace, methodConfigurationName, txn)
            RequestComplete(http.StatusCodes.NoContent)
          }
        }
      }
    }

  def renameMethodConfiguration(workspaceName: WorkspaceName, methodConfigurationNamespace: String, methodConfigurationName: String, newName: String): PerRequestMessage =
    dataSource inTransaction { txn =>
      withWorkspace(workspaceName, txn) { workspace =>
        requireAccess(GCSAccessLevel.Write, workspace, txn) {
          withMethodConfig(workspace, methodConfigurationNamespace, methodConfigurationName, txn) { methodConfiguration =>
            methodConfigurationDAO.get(workspace.namespace, workspace.name, methodConfigurationNamespace, newName, txn) match {
              case None =>
                methodConfigurationDAO.rename(workspace.namespace, workspace.name, methodConfigurationNamespace, methodConfigurationName, newName, txn)
                RequestComplete(http.StatusCodes.NoContent)
              case Some(_) => RequestComplete(StatusCodes.Conflict, s"Destination ${newName} already exists")
            }
          }
        }
      }
    }

  def updateMethodConfiguration(workspaceName: WorkspaceName, methodConfiguration: MethodConfiguration): PerRequestMessage =
    dataSource inTransaction { txn =>
      withWorkspace(workspaceName, txn) { workspace =>
        requireAccess(GCSAccessLevel.Write, workspaceName, txn) {
          methodConfigurationDAO.get(workspace.namespace, workspace.name, methodConfiguration.namespace, methodConfiguration.name, txn) match {
            case Some(_) =>
              methodConfigurationDAO.save(workspaceName.namespace, workspaceName.name, methodConfiguration, txn)
              RequestComplete(StatusCodes.OK)
            case None => RequestComplete(StatusCodes.NotFound)
          }
        }
      }
    }

  def getMethodConfiguration(workspaceName: WorkspaceName, methodConfigurationNamespace: String, methodConfigurationName: String): PerRequestMessage =
    dataSource inTransaction { txn =>
      withWorkspace(workspaceName, txn) { workspace =>
        requireAccess(GCSAccessLevel.Read, workspace, txn) {
          withMethodConfig(workspace, methodConfigurationNamespace, methodConfigurationName, txn) { methodConfig =>
            PerRequest.RequestComplete(methodConfig)
          }
        }
      }
    }

  def copyMethodConfiguration(mcnp: MethodConfigurationNamePair): PerRequestMessage =
    dataSource inTransaction { txn =>
      withWorkspace(mcnp.source.workspaceName, txn) { srcWorkspace =>
        requireAccess(GCSAccessLevel.Read, srcWorkspace, txn) {
          methodConfigurationDAO.get(mcnp.source.workspaceName.namespace, mcnp.source.workspaceName.name, mcnp.source.namespace, mcnp.source.name, txn) match {
            case None => RequestComplete(StatusCodes.NotFound)
            case Some(methodConfig) => saveCopiedMethodConfiguration(methodConfig, mcnp.destination, txn)
          }
        }
      }
    }

  def copyMethodConfigurationFromMethodRepo(methodRepoQuery: MethodRepoConfigurationQuery): PerRequestMessage =
    dataSource inTransaction { txn =>
      methodRepoDAO.getMethodConfig(methodRepoQuery.methodRepoNamespace, methodRepoQuery.methodRepoName, methodRepoQuery.methodRepoSnapshotId, userInfo.authCookie) match {
        case None => RequestComplete(StatusCodes.NotFound)
        case Some(entity) =>
          try {
            // if JSON parsing fails, catch below
            val methodConfig = entity.payload.map(JsonParser(_).convertTo[MethodConfiguration])
            methodConfig match {
              case Some(targetMethodConfig) => saveCopiedMethodConfiguration(targetMethodConfig, methodRepoQuery.destination, txn)
              case None => RequestComplete(StatusCodes.UnprocessableEntity, "Method Repo missing configuration payload")
            }
          }
          catch { case e: Exception =>
            val message = "Error parsing Method Repo response: " + e.getMessage
            RequestComplete(StatusCodes.UnprocessableEntity, message)
          }
      }
    }

  private def saveCopiedMethodConfiguration(methodConfig: MethodConfiguration, dest: MethodConfigurationName, txn: RawlsTransaction) =
    withWorkspace(dest.workspaceName, txn) { destWorkspace =>
      requireAccess(GCSAccessLevel.Write, destWorkspace, txn) {
        methodConfigurationDAO.get(dest.workspaceName.namespace, dest.workspaceName.name, dest.namespace, dest.name, txn) match {
          case Some(existingMethodConfig) => RequestComplete(StatusCodes.Conflict, existingMethodConfig)
          case None =>
            val target = methodConfig.copy(name = dest.name, namespace = dest.namespace, workspaceName = dest.workspaceName)
            val targetMethodConfig = methodConfigurationDAO.save(target.workspaceName.namespace, target.workspaceName.name, target, txn)
            RequestComplete(StatusCodes.Created, targetMethodConfig)
        }
      }
    }

  def listMethodConfigurations(workspaceName: WorkspaceName): PerRequestMessage =
    dataSource inTransaction { txn =>
      requireAccess(GCSAccessLevel.Read, workspaceName, txn) {
        // use toList below to eagerly iterate through the response from methodConfigurationDAO.list
        // to ensure it is evaluated within the transaction
        RequestComplete(methodConfigurationDAO.list(workspaceName.namespace, workspaceName.name, txn).toList)
      }
    }

  /**
   * This is the function that would get called if we had a validate method config endpoint.
   */
  def validateMethodConfig(workspaceName: WorkspaceName,
    methodConfigurationNamespace: String, methodConfigurationName: String,
    entityType: String, entityName: String, authCookie: HttpCookie): PerRequestMessage = {
      dataSource inTransaction { txn =>
        withWorkspace(workspaceName, txn) { workspace =>
          withMethodConfig(workspace, methodConfigurationNamespace, methodConfigurationName, txn) { methodConfig =>
            withEntity(workspace, entityType, entityName, txn) { entity =>
              withMethod(workspace, methodConfig.methodRepoMethod.methodNamespace, methodConfig.methodRepoMethod.methodName, methodConfig.methodRepoMethod.methodVersion, authCookie) { method =>
                withWdl(method) { wdl =>
                  // TODO should we return OK even if there are validation errors?
                  RequestComplete(StatusCodes.OK, MethodConfigResolver.getValidationErrors(methodConfig, entity, wdl, txn))
                }
              }
            }
          }
        }
      }
    }

  private def submitWorkflow(workspace: WorkspaceName, methodConfig: MethodConfiguration, entity: Entity, wdl: String, authCookie: HttpCookie, txn: RawlsTransaction) : Either[WorkflowFailure, Workflow] = {
    val inputs = MethodConfigResolver.resolveInputs(methodConfig, entity, wdl, txn)
    if ( inputs.forall(  _._2.isSuccess ) ) {
      val execStatus = executionServiceDAO.submitWorkflow(wdl, MethodConfigResolver.propertiesToWdlInputs( inputs map { case (key, value) => (key, value.get) } ), authCookie)
      Right(Workflow(workspaceName = workspace, workflowId = execStatus.id, status = WorkflowStatuses.Submitted, statusLastChangedDate = DateTime.now, workflowEntity = AttributeEntityReference(entityName = entity.name, entityType = entity.entityType) ))
    } else {
      Left(WorkflowFailure( workspaceName = workspace, entityName = entity.name, entityType = entity.entityType, errors = (inputs collect { case (key, Failure(regret)) => AttributeString(regret.getMessage) }).toSeq ))
    }
  }

  def createSubmission(workspaceName: WorkspaceName, submission: SubmissionRequest): PerRequestMessage =
    dataSource inTransaction { txn =>
      txn withGraph { graph =>
        withWorkspace(workspaceName, txn) { workspace =>
          requireAccess(GCSAccessLevel.Write, workspace, txn) {
            withMethodConfig(workspace, submission.methodConfigurationNamespace, submission.methodConfigurationName, txn) { methodConfig =>
              withEntity(workspace, submission.entityType, submission.entityName, txn) { entity =>
                withMethod(workspace, methodConfig.methodRepoMethod.methodNamespace, methodConfig.methodRepoMethod.methodName, methodConfig.methodRepoMethod.methodVersion, userInfo.authCookie) { agoraEntity =>
                  withWdl(agoraEntity) { wdl =>

                    //If there's an expression, evaluate it to get the list of entities to run this job on.
                    //Otherwise, use the entity given in the submission.
                    val jobEntities: Seq[Entity] = submission.expression match {
                      case Some(expr) =>
                        new ExpressionEvaluator(graph, new ExpressionParser()).evalFinalEntity(workspaceName.namespace, workspaceName.name, submission.entityType, submission.entityName, expr) match {
                          case Success(ents) => ents
                          case Failure(regret) => return RequestComplete(StatusCodes.BadRequest, "Expression evaluation failed: " + regret.getMessage())
                        }
                      case None => List(entity)
                    }

                    //Verify the type of all job entities matches the type of the method configuration.
                    if (!jobEntities.forall(_.entityType == methodConfig.rootEntityType)) {
                      return RequestComplete(StatusCodes.BadRequest, "Entities " + jobEntities.filter(_.entityType != methodConfig.rootEntityType).map(e => e.entityType + ":" + e.name) +
                        "are not the same type as the method configuration requires")
                    }

                    //Attempt to resolve method inputs and submit the workflows to Cromwell, and build the submission status accordingly.
                    val submittedWorkflows = jobEntities.map(e => submitWorkflow(workspaceName, methodConfig, e, wdl, userInfo.authCookie, txn))
                    val newSubmission = Submission(submissionId = UUID.randomUUID().toString,
                      submissionDate = DateTime.now(),
                      workspaceName = workspaceName,
                      methodConfigurationNamespace = methodConfig.namespace,
                      methodConfigurationName = methodConfig.name,
                      submissionEntity = AttributeEntityReference( entityType = submission.entityType, entityName = submission.entityName ),
                      workflows = submittedWorkflows collect { case Right(e) => e },
                      notstarted = submittedWorkflows collect { case Left(e) => e },
                      status = if (submittedWorkflows.forall(_.isLeft)) SubmissionStatuses.Done else SubmissionStatuses.Submitted)

                    if (newSubmission.status == SubmissionStatuses.Submitted) {
                      submissionSupervisor ! SubmissionStarted(newSubmission, userInfo.authCookie)
                    }

                    RequestComplete(StatusCodes.Created, newSubmission)
                  }
                }
              }
            }
          }
        }
      }
    }

  def getSubmissionStatus(workspaceName: WorkspaceName, submissionId: String) = {
    dataSource inTransaction { txn =>
      withWorkspace(workspaceName, txn) { workspace =>
        requireAccess(GCSAccessLevel.Read, workspace, txn) {
          withSubmission(workspace, submissionId, txn) { submission =>
            RequestComplete(submission)
          }
        }
      }
    }
  }

  def requireAccess(requiredLevel: GCSAccessLevel, workspace: Workspace, txn: RawlsTransaction)(codeBlock: => PerRequestMessage): PerRequestMessage = {
    val acls = JsonParser(gcsDAO.getACL(userInfo.userId, workspace.bucketName)).convertTo[BucketAccessControls]
    if (acls.maximumAccessLevel >= requiredLevel)
      codeBlock
    else
      RequestComplete(http.StatusCodes.NotFound, noSuchWorkspaceMessage(workspace.toWorkspaceName))
  }

  def requireAccess(requiredLevel: GCSAccessLevel, workspaceName: WorkspaceName, txn: RawlsTransaction)(codeBlock: => PerRequestMessage): PerRequestMessage = {
    withWorkspace(workspaceName, txn) { workspace =>
      requireAccess(requiredLevel, workspace, txn)(codeBlock)
    }
  }

}

class AttributeUpdateOperationException(message: String) extends RawlsException(message)
class AttributeNotFoundException(message: String) extends AttributeUpdateOperationException(message)
