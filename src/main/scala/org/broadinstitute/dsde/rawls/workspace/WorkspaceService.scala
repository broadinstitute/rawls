package org.broadinstitute.dsde.rawls.workspace

import akka.actor.{Props, Actor}
import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.dataaccess.{EntityDAO, WorkspaceDAO}
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.webservice.PerRequest.{RequestComplete, PerRequestMessage}
import org.broadinstitute.dsde.rawls.workspace.EntityUpdateOperations._
import org.broadinstitute.dsde.rawls.workspace.WorkspaceService._
import org.broadinstitute.dsde.rawls.webservice.PerRequest
import spray.http
import spray.http.{Uri, HttpHeaders, StatusCodes}
import org.joda.time.DateTime
import spray.httpx.SprayJsonSupport._
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._
import spray.json._

import scala.util.{Failure, Success, Try}

/**
 * Created by dvoet on 4/27/15.
 */

object WorkspaceService {
  sealed trait WorkspaceServiceMessage
  case class SaveWorkspace(workspace: Workspace) extends WorkspaceServiceMessage
  case object ListWorkspaces extends WorkspaceServiceMessage
  case class CloneWorkspace(sourceNamespace:String, sourceWorkspace:String, destNamespace:String, destWorkspace:String) extends WorkspaceServiceMessage

  case class CreateEntity(workspaceNamespace: String, workspaceName: String, entity: Entity) extends WorkspaceServiceMessage
  case class GetEntity(workspaceNamespace: String, workspaceName: String, entityType: String, entityName: String) extends WorkspaceServiceMessage
  case class UpdateEntity(workspaceNamespace: String, workspaceName: String, entityType: String, entityName: String, operations: Seq[EntityUpdateOperation]) extends WorkspaceServiceMessage
  case class DeleteEntity(workspaceNamespace: String, workspaceName: String, entityType: String, entityName: String) extends WorkspaceServiceMessage
  case class RenameEntity(workspaceNamespace: String, workspaceName: String, entityType: String, entityName: String, newName: String) extends WorkspaceServiceMessage


  def props(workspaceServiceConstructor: () => WorkspaceService): Props = {
    Props(workspaceServiceConstructor())
  }

  def constructor(workspaceDAO: WorkspaceDAO, entityDAO: EntityDAO) = () => new WorkspaceService(workspaceDAO, entityDAO)
}

class WorkspaceService(workspaceDAO: WorkspaceDAO, entityDAO: EntityDAO) extends Actor {
  override def receive = {
    case SaveWorkspace(workspace) => context.parent ! saveWorkspace(workspace)
    case ListWorkspaces => context.parent ! listWorkspaces()
    case CloneWorkspace(sourceNamespace, sourceWorkspace, destNamespace, destWorkspace) => context.parent ! cloneWorkspace(sourceNamespace, sourceWorkspace, destNamespace, destWorkspace)
    case CreateEntity(workspaceNamespace, workspaceName, entity) => context.parent ! createEntity(workspaceNamespace, workspaceName, entity)
    case GetEntity(workspaceNamespace, workspaceName, entityType, entityName) => context.parent ! getEntity(workspaceNamespace, workspaceName, entityType, entityName)
    case UpdateEntity(workspaceNamespace, workspaceName, entityType, entityName, operations) => context.parent ! updateEntity(workspaceNamespace, workspaceName, entityType, entityName, operations)
    case DeleteEntity(workspaceNamespace, workspaceName, entityType, entityName) => context.parent ! deleteEntity(workspaceNamespace, workspaceName, entityType, entityName)
    case RenameEntity(workspaceNamespace, workspaceName, entityType, entityName, newName) => context.parent ! renameEntity(workspaceNamespace, workspaceName, entityType, entityName, newName)
  }

  def saveWorkspace(workspace: Workspace): PerRequestMessage = {
//    workspaceDAO.load(workspace.namespace, workspace.name) match {
//      case Some(_) => context.parent ! PerRequest.RequestComplete(StatusCodes.Conflict, s"Workspace ${workspace.namespace}/${workspace.name} already exists")
//      case None =>
//        workspaceDAO.save(workspace)
//        context.parent ! PerRequest.RequestComplete((StatusCodes.Created, workspace))
//    }

    workspaceDAO.save(workspace)
    RequestComplete((StatusCodes.Created, workspace))
  }

  def listWorkspaces(): PerRequestMessage = {
    RequestComplete(workspaceDAO.list())
  }

  def cloneWorkspace(sourceNamespace:String, sourceWorkspace:String, destNamespace:String, destWorkspace:String): PerRequestMessage = {
    val originalWorkspace = workspaceDAO.load(sourceNamespace, sourceWorkspace)
    val copyWorkspace = workspaceDAO.load(destNamespace, destWorkspace)
    (originalWorkspace, copyWorkspace) match {
      case ( Some(ws), None ) => {
        val newWorkspace = ws.copy(namespace = destNamespace, name = destWorkspace, createdDate = DateTime.now)
        workspaceDAO.save( newWorkspace )
        RequestComplete((StatusCodes.Created, newWorkspace))
      }
      case ( None, _ ) => RequestComplete(StatusCodes.NotFound, "Source workspace " + sourceNamespace + "/" + sourceWorkspace + " not found")
      case ( _, Some(_) ) => RequestComplete(StatusCodes.Conflict, "Destination workspace " + destNamespace + "/" + destWorkspace + " already exists")
    }
  }

  def createEntity(workspaceNamespace: String, workspaceName: String, entity: Entity): PerRequestMessage =
    withWorkspace(workspaceNamespace, workspaceName) { workspace =>
      entityDAO.get(workspaceNamespace, workspaceName, entity.entityType, entity.name) match {
        case Some(_) => RequestComplete(StatusCodes.Conflict, s"${entity.entityType} ${entity.name} already exists in $workspaceNamespace/$workspaceName")
        case None => RequestComplete(StatusCodes.Created, entityDAO.save(workspaceNamespace, workspaceName, entity))
      }
    }

  def getEntity(workspaceNamespace: String, workspaceName: String, entityType: String, entityName: String): PerRequestMessage =
    withWorkspace(workspaceNamespace, workspaceName) { workspace =>
      withEntity(workspace, entityType, entityName) { entity =>
        PerRequest.RequestComplete(entity)
      }
    }

  def updateEntity(workspaceNamespace: String, workspaceName: String, entityType: String, entityName: String, operations: Seq[EntityUpdateOperation]): PerRequestMessage =
    withWorkspace(workspaceNamespace, workspaceName) { workspace =>
      withEntity(workspace, entityType, entityName) { entity =>
        try {
          val updatedEntity = applyOperationsToEntity(entity, operations)
          RequestComplete(entityDAO.save(workspaceNamespace, workspaceName, updatedEntity))
        } catch {
          case e: AttributeUpdateOperationException => RequestComplete(http.StatusCodes.BadRequest, s"in $workspaceNamespace/$workspaceName, ${e.getMessage}")
        }
      }
    }

  def deleteEntity(workspaceNamespace: String, workspaceName: String, entityType: String, entityName: String): PerRequestMessage =
    withWorkspace(workspaceNamespace, workspaceName) { workspace =>
      withEntity(workspace, entityType, entityName) { entity =>
        entityDAO.delete(workspace.namespace, workspace.name, entity.entityType, entity.name)
        RequestComplete(http.StatusCodes.NoContent)
      }
    }

  def renameEntity(workspaceNamespace: String, workspaceName: String, entityType: String, entityName: String, newName: String): PerRequestMessage =
    withWorkspace(workspaceNamespace, workspaceName) { workspace =>
      withEntity(workspace, entityType, entityName) { entity =>
        entityDAO.get(workspace.namespace, workspace.name, entity.entityType, newName) match {
          case None =>
            entityDAO.rename(workspace.namespace, workspace.name, entity.entityType, entity.name, newName)
            RequestComplete(http.StatusCodes.NoContent)
          case Some(_) => RequestComplete(StatusCodes.Conflict, s"Destination ${entity.entityType} ${newName} already exists")
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
  def applyOperationsToEntity(entity: Entity, operations: Seq[EntityUpdateOperation]): Entity = {
    operations.foldLeft(entity) { (currentEntity, operation) =>
      operation match {
        case AddUpdateAttribute(attributeName, attribute) => currentEntity.copy(attributes = currentEntity.attributes + (attributeName -> attribute))

        case RemoveAttribute(attributeName) => currentEntity.copy(attributes = currentEntity.attributes - attributeName)

        case AddListMember(attributeListName, newMember) =>
          currentEntity.attributes.get(attributeListName) match {
            case Some(l: AttributeList) =>
              currentEntity.copy(attributes = currentEntity.attributes + (attributeListName -> AttributeList(l.value :+ newMember)))
            case None => currentEntity.copy(attributes = currentEntity.attributes + (attributeListName -> AttributeList(Seq(newMember))))

            case Some(_) => throw new AttributeUpdateOperationException(s"$attributeListName of ${entity.entityType} ${entity.name} is not a list")
          }

        case RemoveListMember(attributeListName, removeMember) =>
          currentEntity.attributes.get(attributeListName) match {
            case Some(l: AttributeList) =>
              currentEntity.copy(attributes = currentEntity.attributes + (attributeListName -> AttributeList(l.value.filterNot(_ == removeMember))))

            case None => throw new AttributeNotFoundException(s"$attributeListName of ${entity.entityType} ${entity.name} does not exists")
            case Some(_) => throw new AttributeUpdateOperationException(s"$attributeListName of ${entity.entityType} ${entity.name} is not a list")
          }
      }
    }
  }

  private def withWorkspace(workspaceNamespace: String, workspaceName: String)(op: (WorkspaceShort) => PerRequestMessage): PerRequestMessage = {
    workspaceDAO.loadShort(workspaceNamespace, workspaceName) match {
      case None => RequestComplete(http.StatusCodes.NotFound, s"$workspaceNamespace/$workspaceName does not exist")
      case Some(workspace) => op(workspace)
    }
  }

  private def withEntity(workspace: WorkspaceShort, entityType: String, entityName: String)(op: (Entity) => PerRequestMessage): PerRequestMessage = {
    entityDAO.get(workspace.namespace, workspace.name, entityType, entityName) match {
      case None => RequestComplete(http.StatusCodes.NotFound, s"${entityType} ${entityName} does not exists in ${workspace.namespace}/${workspace.name}")
      case Some(entity) => op(entity)
    }
  }
}

object EntityUpdateOperations {
  sealed trait EntityUpdateOperation
  case class AddUpdateAttribute(attributeName: String, addUpdateAttribute: Attribute) extends EntityUpdateOperation
  case class RemoveAttribute(attributeName: String) extends EntityUpdateOperation
  case class AddListMember(attributeListName: String, newMember: Attribute) extends EntityUpdateOperation
  case class RemoveListMember(attributeListName: String, removeMember: Attribute) extends EntityUpdateOperation

  private val AddUpdateAttributeFormat = jsonFormat2(AddUpdateAttribute)
  private val RemoveAttributeFormat = jsonFormat1(RemoveAttribute)
  private val AddListMemberFormat = jsonFormat2(AddListMember)
  private val RemoveListMemberFormat = jsonFormat2(RemoveListMember)

  implicit object EntityUpdateOperationFormat extends RootJsonFormat[EntityUpdateOperation] {

    override def write(obj: EntityUpdateOperation): JsValue = {
      val json = obj match {
        case x: AddUpdateAttribute => AddUpdateAttributeFormat.write(x)
        case x: RemoveAttribute => RemoveAttributeFormat.write(x)
        case x: AddListMember => AddListMemberFormat.write(x)
        case x: RemoveListMember => RemoveListMemberFormat.write(x)
      }

      JsObject(json.asJsObject.fields + ("op" -> JsString(obj.getClass.getSimpleName)))
    }

    override def read(json: JsValue) : EntityUpdateOperation = json match {
      case JsObject(fields) =>
        val op = fields.getOrElse("op", throw new DeserializationException("missing op property"))
        op match {
          case JsString("AddUpdateAttribute") => AddUpdateAttributeFormat.read(json)
          case JsString("RemoveAttribute") => RemoveAttributeFormat.read(json)
          case JsString("AddListMemberFormat") => AddListMemberFormat.read(json)
          case JsString("RemoveListMember") => RemoveListMemberFormat.read(json)
          case x => throw new DeserializationException("unrecognized op: " + x)
        }

      case _ => throw new DeserializationException("unexpected json type")
    }
  }
}

class AttributeUpdateOperationException(message: String) extends RawlsException(message)
class AttributeNotFoundException(message: String) extends AttributeUpdateOperationException(message)
