package org.broadinstitute.dsde.rawls.workspace

import java.util.UUID

import akka.actor.{ActorRef, Actor, Props}
import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.jobexec.MethodConfigResolver
import org.broadinstitute.dsde.rawls.jobexec.SubmissionSupervisor.SubmissionStarted
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._
import org.broadinstitute.dsde.rawls.model.ExecutionJsonSupport.SubmissionFormat
import org.broadinstitute.dsde.rawls.model.GoogleBucketACLJsonSupport.BucketAccessControlsFormat
import org.broadinstitute.dsde.rawls.dataaccess.{MethodConfigurationDAO, EntityDAO, WorkspaceDAO}
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.expressions._
import org.broadinstitute.dsde.rawls.webservice.PerRequest
import org.broadinstitute.dsde.rawls.webservice.PerRequest.{RequestCompleteWithHeaders, PerRequestMessage, RequestComplete}
import org.broadinstitute.dsde.rawls.workspace.AttributeUpdateOperations._
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
  case class RegisterUser(userId: String, callbackPath: String) extends WorkspaceServiceMessage
  case class CompleteUserRegistration( userId: String, authCode: String, state: String, callbackPath: String) extends WorkspaceServiceMessage

  case class CreateWorkspace(userId: String, workspace: WorkspaceRequest) extends WorkspaceServiceMessage
  case class GetWorkspace(workspaceNamespace: String, workspaceName: String) extends WorkspaceServiceMessage
  case class UpdateWorkspace(workspaceNamespace: String, workspaceName: String, operations: Seq[AttributeUpdateOperation]) extends WorkspaceServiceMessage
  case object ListWorkspaces extends WorkspaceServiceMessage
  case class CloneWorkspace(userId: String, sourceNamespace:String, sourceWorkspace:String, destNamespace:String, destWorkspace:String) extends WorkspaceServiceMessage
  case class GetACL(userId: String, workspaceNamespace: String, workspaceName: String) extends WorkspaceServiceMessage
  case class PutACL(userId: String, workspaceNamespace: String, workspaceName: String, acl: String) extends WorkspaceServiceMessage
  case class CreateEntity(workspaceNamespace: String, workspaceName: String, entity: Entity) extends WorkspaceServiceMessage
  case class GetEntity(workspaceNamespace: String, workspaceName: String, entityType: String, entityName: String) extends WorkspaceServiceMessage
  case class UpdateEntity(workspaceNamespace: String, workspaceName: String, entityType: String, entityName: String, operations: Seq[AttributeUpdateOperation]) extends WorkspaceServiceMessage
  case class DeleteEntity(workspaceNamespace: String, workspaceName: String, entityType: String, entityName: String) extends WorkspaceServiceMessage
  case class RenameEntity(workspaceNamespace: String, workspaceName: String, entityType: String, entityName: String, newName: String) extends WorkspaceServiceMessage
  case class EvaluateExpression(workspaceNamespace: String, workspaceName: String, entityType: String, entityName: String, expression: String) extends WorkspaceServiceMessage
  case class ListEntityTypes(workspaceNamespace: String, workspaceName: String) extends WorkspaceServiceMessage
  case class ListEntities(workspaceNamespace: String, workspaceName: String, entityType: String) extends WorkspaceServiceMessage

  case class CreateMethodConfiguration(workspaceNamespace: String, workspaceName: String, methodConfiguration: MethodConfiguration) extends WorkspaceServiceMessage
  case class GetMethodConfiguration(workspaceNamespace: String, workspaceName: String, methodConfigurationNamespace: String, methodConfigurationName: String) extends WorkspaceServiceMessage
  case class UpdateMethodConfiguration(workspaceNamespace: String, workspaceName: String, methodConfiguration: MethodConfiguration) extends WorkspaceServiceMessage
  case class DeleteMethodConfiguration(workspaceNamespace: String, workspaceName: String, methodConfigurationNamespace: String, methodConfigurationName: String) extends WorkspaceServiceMessage
  case class RenameMethodConfiguration(workspaceNamespace: String, workspaceName: String, methodConfigurationNamespace: String, methodConfigurationName: String, newName: String) extends WorkspaceServiceMessage
  case class CopyMethodConfiguration(methodConfigNamePair: MethodConfigurationNamePair) extends WorkspaceServiceMessage
  case class CopyEntities(entityCopyDefinition: EntityCopyDefinition, uri:Uri) extends WorkspaceServiceMessage
  case class CopyMethodConfigurationFromMethodRepo(query: MethodRepoConfigurationQuery, authCookie: HttpCookie) extends WorkspaceServiceMessage
  case class ListMethodConfigurations(workspaceNamespace: String, workspaceName: String) extends WorkspaceServiceMessage

  case class CreateSubmission(workspaceName: WorkspaceName, submission: SubmissionRequest, authCookie: HttpCookie) extends WorkspaceServiceMessage
  case class GetSubmissionStatus(workspaceName: WorkspaceName, submissionId: String) extends WorkspaceServiceMessage

  def props(workspaceServiceConstructor: () => WorkspaceService): Props = {
    Props(workspaceServiceConstructor())
  }

  def constructor(dataSource: DataSource, workspaceDAO: WorkspaceDAO, entityDAO: EntityDAO, methodConfigurationDAO: MethodConfigurationDAO, methodRepoDAO: MethodRepoDAO, executionServiceDAO: ExecutionServiceDAO, gcsDAO: GoogleCloudStorageDAO, submissionSupervisor : ActorRef, submissionDAO: SubmissionDAO) = () =>
    new WorkspaceService(dataSource, workspaceDAO, entityDAO, methodConfigurationDAO, methodRepoDAO, executionServiceDAO, gcsDAO, submissionSupervisor, submissionDAO)
}

class WorkspaceService(dataSource: DataSource, workspaceDAO: WorkspaceDAO, entityDAO: EntityDAO, methodConfigurationDAO: MethodConfigurationDAO, methodRepoDAO: MethodRepoDAO, executionServiceDAO: ExecutionServiceDAO, gcsDAO: GoogleCloudStorageDAO, submissionSupervisor : ActorRef, submissionDAO: SubmissionDAO) extends Actor {

  override def receive = {
    case RegisterUser(userId, callbackPath) => context.parent ! registerUser(userId, callbackPath)
    case CompleteUserRegistration(userId, authCode, state, callbackPath) => context.parent ! completeUserRegistration(userId,authCode,state,callbackPath)

    case CreateWorkspace(userId,workspace) => context.parent ! createWorkspace(userId,workspace)
    case GetWorkspace(workspaceNamespace, workspaceName) => context.parent ! getWorkspace(workspaceNamespace, workspaceName)
    case UpdateWorkspace(workspaceNamespace, workspaceName, operations) => context.parent ! updateWorkspace(workspaceNamespace, workspaceName, operations)
    case ListWorkspaces => context.parent ! listWorkspaces(dataSource)
    case CloneWorkspace(userId, sourceNamespace, sourceWorkspace, destNamespace, destWorkspace) => context.parent ! cloneWorkspace(userId, sourceNamespace, sourceWorkspace, destNamespace, destWorkspace)
    case GetACL(userId, workspaceNamespace, workspaceName) => context.parent ! getACL(userId,workspaceNamespace,workspaceName)
    case PutACL(userId, workspaceNamespace, workspaceName, acl) => context.parent ! putACL(userId,workspaceNamespace,workspaceName,acl)

    case CreateEntity(workspaceNamespace, workspaceName, entity) => context.parent ! createEntity(workspaceNamespace, workspaceName, entity)
    case GetEntity(workspaceNamespace, workspaceName, entityType, entityName) => context.parent ! getEntity(workspaceNamespace, workspaceName, entityType, entityName)
    case UpdateEntity(workspaceNamespace, workspaceName, entityType, entityName, operations) => context.parent ! updateEntity(workspaceNamespace, workspaceName, entityType, entityName, operations)
    case DeleteEntity(workspaceNamespace, workspaceName, entityType, entityName) => context.parent ! deleteEntity(workspaceNamespace, workspaceName, entityType, entityName)
    case RenameEntity(workspaceNamespace, workspaceName, entityType, entityName, newName) => context.parent ! renameEntity(workspaceNamespace, workspaceName, entityType, entityName, newName)
    case EvaluateExpression(workspaceNamespace, workspaceName, entityType, entityName, expression) => context.parent ! evaluateExpression(workspaceNamespace, workspaceName, entityType, entityName, expression)
    case ListEntityTypes(workspaceNamespace, workspaceName) => context.parent ! listEntityTypes(workspaceNamespace, workspaceName)
    case ListEntities(workspaceNamespace, workspaceName, entityType) => context.parent ! listEntities(workspaceNamespace, workspaceName, entityType)

    case CreateMethodConfiguration(workspaceNamespace, workspaceName, methodConfiguration) => context.parent ! createMethodConfiguration(workspaceNamespace, workspaceName, methodConfiguration)
    case RenameMethodConfiguration(workspaceNamespace, workspaceName, methodConfigurationNamespace, methodConfigurationName, newName) => context.parent ! renameMethodConfiguration(workspaceNamespace, workspaceName, methodConfigurationNamespace, methodConfigurationName, newName)
    case DeleteMethodConfiguration(workspaceNamespace, workspaceName, methodConfigurationNamespace, methodConfigurationName) => context.parent ! deleteMethodConfiguration(workspaceNamespace, workspaceName, methodConfigurationNamespace, methodConfigurationName)
    case GetMethodConfiguration(workspaceNamespace, workspaceName, methodConfigurationNamespace, methodConfigurationName) => context.parent ! getMethodConfiguration(workspaceNamespace, workspaceName, methodConfigurationNamespace, methodConfigurationName)
    case UpdateMethodConfiguration(workspaceNamespace, workspaceName, methodConfiguration) => context.parent ! updateMethodConfiguration(workspaceNamespace, workspaceName, methodConfiguration)
    case CopyMethodConfiguration(methodConfigNamePair) => context.parent ! copyMethodConfiguration(methodConfigNamePair)
    case CopyMethodConfigurationFromMethodRepo(query, authCookie) => context.parent ! copyMethodConfigurationFromMethodRepo(query, authCookie)
    case ListMethodConfigurations(workspaceNamespace, workspaceName) => context.parent ! listMethodConfigurations(workspaceNamespace, workspaceName)
    case CopyEntities(entityCopyDefinition, uri:Uri) => context.parent ! copyEntities(entityCopyDefinition, uri)

    case CreateSubmission(workspaceName, submission, authCookie) => context.parent ! createSubmission(workspaceName, submission, authCookie)
    case GetSubmissionStatus(workspaceName, submissionId) => context.parent ! getSubmissionStatus(workspaceName, submissionId)
  }

  def registerUser(userId: String, callbackPath: String): PerRequestMessage = {
    RequestCompleteWithHeaders(StatusCodes.SeeOther,Location(gcsDAO.getGoogleRedirectURI(userId,callbackPath)))
  }

  def completeUserRegistration(userId: String, authCode: String, state: String, callbackPath: String): PerRequestMessage = {
    gcsDAO.storeUser(userId,authCode,state,callbackPath)
    RequestComplete(StatusCodes.Created)
  }

  private def createBucketName(workspaceName: String) = s"${workspaceName}-${UUID.randomUUID}"

  def createWorkspace(userId: String, workspaceRequest: WorkspaceRequest): PerRequestMessage =
    dataSource inTransaction { txn =>
      workspaceDAO.load(workspaceRequest.namespace, workspaceRequest.name, txn) match {
        case Some(_) =>
          PerRequest.RequestComplete(StatusCodes.Conflict, s"Workspace ${workspaceRequest.namespace}/${workspaceRequest.name} already exists")
        case None =>
          val bucketName = createBucketName(workspaceRequest.name)
          Try( gcsDAO.createBucket(userId,workspaceRequest.namespace, bucketName) ) match {
            case Failure(err) => RequestComplete(StatusCodes.Forbidden,s"Unable to create bucket for ${workspaceRequest.namespace}/${workspaceRequest.name}: "+err.getMessage)
            case Success(_) =>
              val workspace = Workspace(workspaceRequest.namespace,workspaceRequest.name,bucketName,DateTime.now,userId,workspaceRequest.attributes)
              workspaceDAO.save(workspace, txn)
              PerRequest.RequestComplete((StatusCodes.Created, workspace))
          }
      }
    }

  def getWorkspace(workspaceNamespace: String, workspaceName: String): PerRequestMessage =
    dataSource inTransaction { txn =>
      withWorkspace(workspaceNamespace, workspaceName, txn) {
        RequestComplete(_)
      }
    }

  def updateWorkspace(workspaceNamespace: String, workspaceName: String, operations: Seq[AttributeUpdateOperation]): PerRequestMessage =
    dataSource inTransaction { txn =>
      withWorkspace(workspaceNamespace, workspaceName, txn) { workspace =>
        try {
          val updatedWorkspace = applyOperationsToWorkspace(workspace, operations)
          RequestComplete(workspaceDAO.save(updatedWorkspace, txn))
        } catch {
          case e: AttributeUpdateOperationException => RequestComplete(http.StatusCodes.BadRequest, s"in $workspaceNamespace/$workspaceName, ${e.getMessage}")
        }
      }
    }

  def listWorkspaces(dataSource: DataSource): PerRequestMessage =
    dataSource inTransaction { txn =>
      RequestComplete(workspaceDAO.list(txn))
    }

  def cloneWorkspace(userId: String, sourceNamespace: String, sourceWorkspace: String, destNamespace: String, destWorkspace: String): PerRequestMessage =
    dataSource inTransaction { txn =>
      val originalWorkspace = workspaceDAO.load(sourceNamespace, sourceWorkspace, txn)
      val copyWorkspace = workspaceDAO.load(destNamespace, destWorkspace, txn)

      (originalWorkspace, copyWorkspace) match {
        case (Some(ws), None) => {
          val bucketName = createBucketName(destWorkspace)
          Try( gcsDAO.createBucket(userId,destNamespace, bucketName) ) match {
            case Failure(err) => RequestComplete(StatusCodes.Forbidden,s"Unable to create bucket for ${destNamespace}/${destWorkspace}: "+err.getMessage)
            case Success(_) =>
              val newWorkspace = Workspace(destNamespace,destWorkspace,bucketName,DateTime.now,userId,ws.attributes)
              workspaceDAO.save(newWorkspace, txn)
              entityDAO.cloneAllEntities(ws.namespace, newWorkspace.namespace, ws.name, newWorkspace.name, txn)
              methodConfigurationDAO.list(ws.namespace, ws.name, txn).foreach { methodConfig =>
                methodConfigurationDAO.save(newWorkspace.namespace, newWorkspace.name, methodConfigurationDAO.get(ws.namespace, ws.name, methodConfig.namespace, methodConfig.name, txn).get, txn)
              }
              RequestComplete((StatusCodes.Created, newWorkspace))
          }
        }
        case (None, _) => RequestComplete(StatusCodes.NotFound, "Source workspace " + sourceNamespace + "/" + sourceWorkspace + " not found")
        case (_, Some(_)) => RequestComplete(StatusCodes.Conflict, "Destination workspace " + destNamespace + "/" + destWorkspace + " already exists")
      }
    }

  // TODO: should I stay or should I go (move to a different class)?
  //
  // STAY: needs dataSource, workspaceDAO, and gcsDAO which are available here
  // GO: does not directly support an endpoint or fit the PerRequestMessage paradigm

  def getACLObject(userId: String, workspaceNamespace: String, workspaceName: String): Option[BucketAccessControls] =
    dataSource inTransaction { txn =>
      workspaceDAO.load(workspaceNamespace, workspaceName, txn) map { ws =>
        JsonParser(gcsDAO.getACL(userId, ws.bucketName)).convertTo[BucketAccessControls]
      }
    }

  def getACL(userId: String, workspaceNamespace: String, workspaceName: String): PerRequestMessage =
    dataSource inTransaction { txn =>
      withWorkspace(workspaceNamespace,workspaceName,txn) { workspace =>
        Try( gcsDAO.getACL(userId,workspace.bucketName) ) match {
          case Success(acl) => RequestComplete(StatusCodes.OK,acl)
          case Failure(err) => RequestComplete(StatusCodes.Forbidden,"Can't retrieve ACL for workspace ${workspaceNamespace}/${workspaceName}: "+err.getMessage())
        }
      }
    }

  def putACL(userId: String, workspaceNamespace: String, workspaceName: String, acl: String): PerRequestMessage =
    dataSource inTransaction { txn =>
      withWorkspace(workspaceNamespace,workspaceName,txn) { workspace =>
        Try( gcsDAO.putACL(userId,workspace.bucketName,acl) ) match {
          case Failure(err) =>  RequestComplete(StatusCodes.Forbidden,"Can't set ACL for workspace ${workspaceNamespace}/${workspaceName}: "+err.getMessage())
          case _ => RequestComplete(StatusCodes.OK)
        }
      }
    }

  def copyEntities(entityCopyDef: EntityCopyDefinition, uri: Uri): PerRequestMessage =
    dataSource inTransaction { txn =>
      val destNamespace = entityCopyDef.destinationWorkspace.namespace
      val destWorkspace = entityCopyDef.destinationWorkspace.name

      val sourceNamespace = entityCopyDef.sourceWorkspace.namespace
      val sourceWorkspace = entityCopyDef.sourceWorkspace.name

      val entityNames = entityCopyDef.entityNames
      val entityType = entityCopyDef.entityType

      val conflicts = entityDAO.copyEntities(destNamespace, destWorkspace, sourceNamespace, sourceWorkspace, entityType, entityNames, txn)

      conflicts.size match {
        case 0 => {
          // get the entities that were copied into the destination workspace
          val entityCopies = entityDAO.list(destNamespace, destWorkspace, entityType, txn).filter((e: Entity) => entityNames.contains(e.name)).toList
          RequestComplete(StatusCodes.Created, entityCopies)
        }
        case _ => {
          val basePath = "/" + destNamespace + "/" + destWorkspace + "/entities/"
          val conflictUris = conflicts.map(conflict => uri.copy(path = Uri.Path(basePath + conflict.entityType + "/" + conflict.name)).toString())
          val conflictingEntities = ConflictingEntities(conflictUris)
          RequestComplete(StatusCodes.Conflict, conflictingEntities)
        }
      }
    }

  def createEntity(workspaceNamespace: String, workspaceName: String, entity: Entity): PerRequestMessage =
    dataSource inTransaction { txn =>
      withWorkspace(workspaceNamespace, workspaceName, txn) { workspace =>
        entityDAO.get(workspaceNamespace, workspaceName, entity.entityType, entity.name, txn) match {
          case Some(_) => RequestComplete(StatusCodes.Conflict, s"${entity.entityType} ${entity.name} already exists in $workspaceNamespace/$workspaceName")
          case None => RequestComplete(StatusCodes.Created, entityDAO.save(workspaceNamespace, workspaceName, entity, txn))
        }
      }
    }

  def listEntityTypes(workspaceNamespace: String, workspaceName: String): PerRequestMessage =
    dataSource inTransaction { txn =>
      withWorkspace(workspaceNamespace, workspaceName, txn) { workspace =>
        RequestComplete(entityDAO.getEntityTypes(workspaceNamespace, workspaceName, txn))
      }
    }

  def listEntities(workspaceNamespace: String, workspaceName: String, entityType: String): PerRequestMessage =
    dataSource inTransaction { txn =>
      withWorkspace(workspaceNamespace, workspaceName, txn) { workspace =>
        RequestComplete(entityDAO.list(workspaceNamespace, workspaceName, entityType, txn).toList)
      }
    }

  def getEntity(workspaceNamespace: String, workspaceName: String, entityType: String, entityName: String): PerRequestMessage =
    dataSource inTransaction { txn =>
      withWorkspace(workspaceNamespace, workspaceName, txn) { workspace =>
        withEntity(workspace, entityType, entityName, txn) { entity =>
          PerRequest.RequestComplete(entity)
        }
      }
    }

  def updateEntity(workspaceNamespace: String, workspaceName: String, entityType: String, entityName: String, operations: Seq[AttributeUpdateOperation]): PerRequestMessage =
    dataSource inTransaction { txn =>
      withWorkspace(workspaceNamespace, workspaceName, txn) { workspace =>
        withEntity(workspace, entityType, entityName, txn) { entity =>
          try {
            val updatedEntity = applyOperationsToEntity(entity, operations)
            RequestComplete(entityDAO.save(workspaceNamespace, workspaceName, updatedEntity, txn))
          } catch {
            case e: AttributeUpdateOperationException => RequestComplete(http.StatusCodes.BadRequest, s"in $workspaceNamespace/$workspaceName, ${e.getMessage}")
          }
        }
      }
    }

  def deleteEntity(workspaceNamespace: String, workspaceName: String, entityType: String, entityName: String): PerRequestMessage =
    dataSource inTransaction { txn =>
      withWorkspace(workspaceNamespace, workspaceName, txn) { workspace =>
        withEntity(workspace, entityType, entityName, txn) { entity =>
          entityDAO.delete(workspace.namespace, workspace.name, entity.entityType, entity.name, txn)
          RequestComplete(http.StatusCodes.NoContent)
        }
      }
    }

  def renameEntity(workspaceNamespace: String, workspaceName: String, entityType: String, entityName: String, newName: String): PerRequestMessage =
    dataSource inTransaction { txn =>
      withWorkspace(workspaceNamespace, workspaceName, txn) { workspace =>
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

  def evaluateExpression(workspaceNamespace: String, workspaceName: String, entityType: String, entityName: String, expression: String): PerRequestMessage =
    dataSource inTransaction { txn =>
      txn withGraph { graph =>
        new ExpressionEvaluator( graph, new ExpressionParser() ).evalFinalAttribute(workspaceNamespace, workspaceName, entityType, entityName, expression) match {
          case Success(result) => RequestComplete(http.StatusCodes.OK, result)
          case Failure(regret) => RequestComplete(http.StatusCodes.BadRequest, regret.getMessage)
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
            case Some(l: AttributeValueList) =>
              newMember match {
                case newMember: AttributeValue =>
                  startingAttributes + (attributeListName -> AttributeValueList(l.list :+ newMember))
                case _ => throw new AttributeUpdateOperationException("Cannot add non-value to list of values.")
              }

            case Some(l: AttributeReferenceList) =>
              newMember match {
                case newMember: AttributeReferenceSingle =>
                  startingAttributes + (attributeListName -> AttributeReferenceList(l.list :+ newMember))
                case _ => throw new AttributeUpdateOperationException("Cannot add non-reference to list of references.")
              }

            case None =>
              newMember match {
                case newMember: AttributeValue =>
                  startingAttributes + (attributeListName -> AttributeValueList(Seq(newMember)))
                case newMember: AttributeReferenceSingle =>
                  startingAttributes + (attributeListName -> AttributeReferenceList(Seq(newMember)))
                case _ => throw new AttributeUpdateOperationException("Cannot create list with that type.")
              }

            case Some(_) => throw new AttributeUpdateOperationException(s"$attributeListName of ${attributable.path} is not a list")
          }

        case RemoveListMember(attributeListName, removeMember) =>
          startingAttributes.get(attributeListName) match {
            case Some(l: AttributeValueList) =>
              startingAttributes + (attributeListName -> AttributeValueList(l.list.filterNot(_ == removeMember)))
            case Some(l: AttributeReferenceList) =>
              startingAttributes + (attributeListName -> AttributeReferenceList(l.list.filterNot(_ == removeMember)))
            case None => throw new AttributeNotFoundException(s"$attributeListName of ${attributable.path} does not exist")
            case Some(_) => throw new AttributeUpdateOperationException(s"$attributeListName of ${attributable.path} is not a list")
          }
      }
    }
  }

  private def withWorkspace(workspaceNamespace: String, workspaceName: String, txn: RawlsTransaction)(op: (Workspace) => PerRequestMessage): PerRequestMessage = {
    workspaceDAO.load(workspaceNamespace, workspaceName, txn) match {
      case None => RequestComplete(http.StatusCodes.NotFound, s"$workspaceNamespace/$workspaceName does not exist")
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

  def createMethodConfiguration(workspaceNamespace: String, workspaceName: String, methodConfiguration: MethodConfiguration): PerRequestMessage =
    dataSource inTransaction { txn =>
      withWorkspace(workspaceNamespace, workspaceName, txn) { workspace =>
        methodConfigurationDAO.get(workspace.namespace, workspace.name, methodConfiguration.namespace, methodConfiguration.name, txn) match {
          case Some(_) => RequestComplete(StatusCodes.Conflict, s"${methodConfiguration.name} already exists in $workspaceNamespace/$workspaceName")
          case None => RequestComplete(StatusCodes.Created, methodConfigurationDAO.save(workspaceNamespace, workspaceName, methodConfiguration, txn))
        }
      }
    }

  def deleteMethodConfiguration(workspaceNamespace: String, workspaceName: String, methodConfigurationNamespace: String, methodConfigurationName: String): PerRequestMessage =
    dataSource inTransaction { txn =>
      withWorkspace(workspaceNamespace, workspaceName, txn) { workspace =>
        withMethodConfig(workspace, methodConfigurationNamespace, methodConfigurationName, txn) { methodConfig =>
          methodConfigurationDAO.delete(workspace.namespace, workspace.name, methodConfigurationNamespace, methodConfigurationName, txn)
          RequestComplete(http.StatusCodes.NoContent)
        }
      }
    }

  def renameMethodConfiguration(workspaceNamespace: String, workspaceName: String, methodConfigurationNamespace: String, methodConfigurationName: String, newName: String): PerRequestMessage =
    dataSource inTransaction { txn =>
      withWorkspace(workspaceNamespace, workspaceName, txn) { workspace =>
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

  def updateMethodConfiguration(workspaceNamespace: String, workspaceName: String, methodConfiguration: MethodConfiguration): PerRequestMessage =
    dataSource inTransaction { txn =>
      withWorkspace(workspaceNamespace, workspaceName, txn) { workspace =>
        methodConfigurationDAO.get(workspace.namespace, workspace.name, methodConfiguration.namespace, methodConfiguration.name, txn) match {
          case Some(_) =>
            methodConfigurationDAO.save(workspaceNamespace, workspaceName, methodConfiguration, txn)
            RequestComplete(StatusCodes.OK)
          case None => RequestComplete(StatusCodes.NotFound)
        }
      }
    }

  def getMethodConfiguration(workspaceNamespace: String, workspaceName: String, methodConfigurationNamespace: String, methodConfigurationName: String): PerRequestMessage =
    dataSource inTransaction { txn =>
      withWorkspace(workspaceNamespace, workspaceName, txn) { workspace =>
        withMethodConfig(workspace, methodConfigurationNamespace, methodConfigurationName, txn) { methodConfig =>
          PerRequest.RequestComplete(methodConfig)
        }
      }
    }

  def copyMethodConfiguration(mcnp: MethodConfigurationNamePair): PerRequestMessage =
    dataSource inTransaction { txn =>
      withWorkspace(mcnp.source.workspaceName.namespace, mcnp.source.workspaceName.name, txn) { srcWorkspace =>
        methodConfigurationDAO.get(mcnp.source.workspaceName.namespace, mcnp.source.workspaceName.name, mcnp.source.namespace, mcnp.source.name, txn) match {
          case None => RequestComplete(StatusCodes.NotFound)
          case Some(methodConfig) => saveCopiedMethodConfiguration(methodConfig, mcnp.destination, txn)
        }
      }
    }

  def copyMethodConfigurationFromMethodRepo(methodRepoQuery: MethodRepoConfigurationQuery, authCookie: HttpCookie): PerRequestMessage =
    dataSource inTransaction { txn =>
      methodRepoDAO.getMethodConfig(methodRepoQuery.methodRepoNamespace, methodRepoQuery.methodRepoName, methodRepoQuery.methodRepoSnapshotId, authCookie) match {
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
    withWorkspace(dest.workspaceName.namespace, dest.workspaceName.name, txn) { destWorkspace =>
      methodConfigurationDAO.get(dest.workspaceName.namespace, dest.workspaceName.name, dest.namespace, dest.name, txn) match {
        case Some(existingMethodConfig) => RequestComplete(StatusCodes.Conflict, existingMethodConfig)
        case None =>
          val target = methodConfig.copy(name = dest.name, namespace = dest.namespace, workspaceName = dest.workspaceName)
          val targetMethodConfig = methodConfigurationDAO.save(target.workspaceName.namespace, target.workspaceName.name, target, txn)
          RequestComplete(StatusCodes.Created, targetMethodConfig)
      }
    }

  def listMethodConfigurations(workspaceNamespace: String, workspaceName: String): PerRequestMessage =
    dataSource inTransaction { txn =>
      // use toList below to eagerly iterate through the response from methodConfigurationDAO.list
      // to ensure it is evaluated within the transaction
      RequestComplete(methodConfigurationDAO.list(workspaceNamespace, workspaceName, txn).toList)
    }

  /**
   * This is the function that would get called if we had a validate method config endpoint.
   */
  def validateMethodConfig(workspaceNamespace: String, workspaceName: String,
    methodConfigurationNamespace: String, methodConfigurationName: String,
    entityType: String, entityName: String, authCookie: HttpCookie): PerRequestMessage = {
      dataSource inTransaction { txn =>
        withWorkspace(workspaceNamespace, workspaceName, txn) { workspace =>
          withMethodConfig(workspace, methodConfigurationNamespace, methodConfigurationName, txn) { methodConfig =>
            withEntity(workspace, entityType, entityName, txn) { entity =>
              withMethod(workspace, methodConfig.methodNamespace, methodConfig.methodName, methodConfig.methodVersion, authCookie) { method =>
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
      Right(Workflow( workspaceNamespace = workspace.namespace, workspaceName = workspace.name, id = execStatus.id, status = WorkflowStatuses.Submitted, statusLastChangedDate = DateTime.now, entityName = entity.name, entityType = entity.entityType ))
    } else {
      Left(WorkflowFailure( workspaceNamespace = workspace.namespace, workspaceName = workspace.name, entityName = entity.name, entityType = entity.entityType, errors = (inputs collect { case (key, Failure(regret)) => regret.getMessage }).toSeq ))
    }
  }

  def createSubmission(workspaceName: WorkspaceName, submission: SubmissionRequest, authCookie: HttpCookie): PerRequestMessage =
    dataSource inTransaction { txn =>
      txn withGraph { graph =>
      withWorkspace(workspaceName.namespace, workspaceName.name, txn) { workspace =>
        withMethodConfig(workspace, submission.methodConfigurationNamespace, submission.methodConfigurationName, txn) { methodConfig =>
          withEntity(workspace, submission.entityType, submission.entityName, txn) { entity =>
            withMethod(workspace, methodConfig.methodNamespace, methodConfig.methodName, methodConfig.methodVersion, authCookie) { agoraEntity =>
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
                val submittedWorkflows = jobEntities.map( e => submitWorkflow(workspaceName, methodConfig, e, wdl, authCookie, txn))
                val newSubmission = Submission(id = UUID.randomUUID().toString,
                  submissionDate = DateTime.now(),
                  workspaceNamespace = workspaceName.namespace,
                  workspaceName = workspaceName.name,
                  methodConfigurationNamespace = methodConfig.namespace,
                  methodConfigurationName = methodConfig.name,
                  entityType = submission.entityType,
                  entityName = submission.entityName,
                  workflows = submittedWorkflows collect { case Right(e) => e },
                  notstarted = submittedWorkflows collect { case Left(e) => e },
                  status = if (submittedWorkflows.forall( _.isLeft )) SubmissionStatuses.Done else SubmissionStatuses.Submitted )

                if( newSubmission.status == SubmissionStatuses.Submitted ) {
                  submissionSupervisor ! SubmissionStarted(newSubmission, authCookie)
                }

                RequestComplete(StatusCodes.Created, newSubmission)
                }
              }
            }
          }
        }
      }
    }

  def getSubmissionStatus(workspaceName: WorkspaceName, submissionId: String) = {
    dataSource inTransaction { txn =>
      withWorkspace(workspaceName.namespace, workspaceName.name, txn) { workspace =>
        withSubmission(workspace, submissionId, txn) { submission =>
          RequestComplete(submission)
        }
      }
    }
  }
}

object AttributeUpdateOperations {
  sealed trait AttributeUpdateOperation
  case class AddUpdateAttribute(attributeName: String, addUpdateAttribute: Attribute) extends AttributeUpdateOperation
  case class RemoveAttribute(attributeName: String) extends AttributeUpdateOperation
  case class AddListMember(attributeListName: String, newMember: Attribute) extends AttributeUpdateOperation
  case class RemoveListMember(attributeListName: String, removeMember: Attribute) extends AttributeUpdateOperation

  private val AddUpdateAttributeFormat = jsonFormat2(AddUpdateAttribute)
  private val RemoveAttributeFormat = jsonFormat1(RemoveAttribute)
  private val AddListMemberFormat = jsonFormat2(AddListMember)
  private val RemoveListMemberFormat = jsonFormat2(RemoveListMember)

  implicit object AttributeUpdateOperationFormat extends RootJsonFormat[AttributeUpdateOperation] {

    override def write(obj: AttributeUpdateOperation): JsValue = {
      val json = obj match {
        case x: AddUpdateAttribute => AddUpdateAttributeFormat.write(x)
        case x: RemoveAttribute => RemoveAttributeFormat.write(x)
        case x: AddListMember => AddListMemberFormat.write(x)
        case x: RemoveListMember => RemoveListMemberFormat.write(x)
      }

      JsObject(json.asJsObject.fields + ("op" -> JsString(obj.getClass.getSimpleName)))
    }

    override def read(json: JsValue) : AttributeUpdateOperation = json match {
      case JsObject(fields) =>
        val op = fields.getOrElse("op", throw new DeserializationException("missing op property"))
        op match {
          case JsString("AddUpdateAttribute") => AddUpdateAttributeFormat.read(json)
          case JsString("RemoveAttribute") => RemoveAttributeFormat.read(json)
          case JsString("AddListMember") => AddListMemberFormat.read(json)
          case JsString("RemoveListMember") => RemoveListMemberFormat.read(json)
          case x => throw new DeserializationException("unrecognized op: " + x)
        }

      case _ => throw new DeserializationException("unexpected json type")
    }
  }
}

class AttributeUpdateOperationException(message: String) extends RawlsException(message)
class AttributeNotFoundException(message: String) extends AttributeUpdateOperationException(message)
