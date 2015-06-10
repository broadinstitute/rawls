package org.broadinstitute.dsde.rawls.workspace

import akka.actor.{Actor, Props}
import org.broadinstitute.dsde.rawls.RawlsException
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._
import org.broadinstitute.dsde.rawls.dataaccess.{MethodConfigurationDAO, EntityDAO, WorkspaceDAO}
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.webservice.PerRequest
import org.broadinstitute.dsde.rawls.webservice.PerRequest.{PerRequestMessage, RequestComplete}
import org.broadinstitute.dsde.rawls.workspace.AttributeUpdateOperations._
import org.broadinstitute.dsde.rawls.workspace.WorkspaceService._
import org.joda.time.DateTime
import spray.http
import spray.http.StatusCodes
import spray.httpx.SprayJsonSupport._
import spray.json._

/**
 * Created by dvoet on 4/27/15.
 */

object WorkspaceService {
  sealed trait WorkspaceServiceMessage
  case class SaveWorkspace(workspace: Workspace) extends WorkspaceServiceMessage
  case class GetWorkspace(workspaceNamespace: String, workspaceName: String) extends WorkspaceServiceMessage
  case class UpdateWorkspace(workspaceNamespace: String, workspaceName: String, operations: Seq[AttributeUpdateOperation]) extends WorkspaceServiceMessage
  case object ListWorkspaces extends WorkspaceServiceMessage
  case class CloneWorkspace(sourceNamespace:String, sourceWorkspace:String, destNamespace:String, destWorkspace:String) extends WorkspaceServiceMessage

  case class CreateEntity(workspaceNamespace: String, workspaceName: String, entity: Entity) extends WorkspaceServiceMessage
  case class GetEntity(workspaceNamespace: String, workspaceName: String, entityType: String, entityName: String) extends WorkspaceServiceMessage
  case class UpdateEntity(workspaceNamespace: String, workspaceName: String, entityType: String, entityName: String, operations: Seq[AttributeUpdateOperation]) extends WorkspaceServiceMessage
  case class DeleteEntity(workspaceNamespace: String, workspaceName: String, entityType: String, entityName: String) extends WorkspaceServiceMessage
  case class RenameEntity(workspaceNamespace: String, workspaceName: String, entityType: String, entityName: String, newName: String) extends WorkspaceServiceMessage
  case class ListEntityTypes(workspaceNamespace: String, workspaceName: String) extends WorkspaceServiceMessage
  case class ListEntities(workspaceNamespace: String, workspaceName: String, entityType: String) extends WorkspaceServiceMessage

  case class CreateMethodConfiguration(workspaceNamespace: String, workspaceName: String, methodConfiguration: MethodConfiguration) extends WorkspaceServiceMessage
  case class GetMethodConfiguration(workspaceNamespace: String, workspaceName: String, methodConfigurationNamespace: String, methodConfigurationName: String) extends WorkspaceServiceMessage
  case class UpdateMethodConfiguration(workspaceNamespace: String, workspaceName: String, methodConfiguration: MethodConfiguration) extends WorkspaceServiceMessage
  case class DeleteMethodConfiguration(workspaceNamespace: String, workspaceName: String, methodConfigurationNamespace: String, methodConfigurationName: String) extends WorkspaceServiceMessage
  case class RenameMethodConfiguration(workspaceNamespace: String, workspaceName: String, methodConfigurationNamespace: String, methodConfigurationName: String, newName: String) extends WorkspaceServiceMessage
  case class CopyMethodConfiguration(methodConfigNamePair: MethodConfigurationNamePair) extends WorkspaceServiceMessage
  case class ListMethodConfigurations(workspaceNamespace: String, workspaceName: String) extends WorkspaceServiceMessage

  def props(workspaceServiceConstructor: () => WorkspaceService): Props = {
    Props(workspaceServiceConstructor())
  }

  def constructor(dataSource: DataSource, workspaceDAO: WorkspaceDAO, entityDAO: EntityDAO, methodConfigurationDAO: MethodConfigurationDAO) = () => new WorkspaceService(dataSource, workspaceDAO, entityDAO, methodConfigurationDAO)
}

class WorkspaceService(dataSource: DataSource, workspaceDAO: WorkspaceDAO, entityDAO: EntityDAO, methodConfigurationDAO: MethodConfigurationDAO) extends Actor {


  override def receive = {
    case SaveWorkspace(workspace) => context.parent ! saveWorkspace(workspace)
    case GetWorkspace(workspaceNamespace, workspaceName) => context.parent ! getWorkspace(workspaceNamespace, workspaceName)
    case UpdateWorkspace(workspaceNamespace, workspaceName, operations) => context.parent ! updateWorkspace(workspaceNamespace, workspaceName, operations)
    case ListWorkspaces => context.parent ! listWorkspaces(dataSource)
    case CloneWorkspace(sourceNamespace, sourceWorkspace, destNamespace, destWorkspace) => context.parent ! cloneWorkspace(sourceNamespace, sourceWorkspace, destNamespace, destWorkspace)

    case CreateEntity(workspaceNamespace, workspaceName, entity) => context.parent ! createEntity(workspaceNamespace, workspaceName, entity)
    case GetEntity(workspaceNamespace, workspaceName, entityType, entityName) => context.parent ! getEntity(workspaceNamespace, workspaceName, entityType, entityName)
    case UpdateEntity(workspaceNamespace, workspaceName, entityType, entityName, operations) => context.parent ! updateEntity(workspaceNamespace, workspaceName, entityType, entityName, operations)
    case DeleteEntity(workspaceNamespace, workspaceName, entityType, entityName) => context.parent ! deleteEntity(workspaceNamespace, workspaceName, entityType, entityName)
    case RenameEntity(workspaceNamespace, workspaceName, entityType, entityName, newName) => context.parent ! renameEntity(workspaceNamespace, workspaceName, entityType, entityName, newName)
    case ListEntityTypes(workspaceNamespace, workspaceName) => context.parent ! listEntityTypes(workspaceNamespace, workspaceName)
    case ListEntities(workspaceNamespace, workspaceName, entityType) => context.parent ! listEntities(workspaceNamespace, workspaceName, entityType)

    case CreateMethodConfiguration(workspaceNamespace, workspaceName, methodConfiguration) => context.parent ! createMethodConfiguration(workspaceNamespace, workspaceName, methodConfiguration)
    case RenameMethodConfiguration(workspaceNamespace, workspaceName, methodConfigurationNamespace, methodConfigurationName, newName) => context.parent ! renameMethodConfiguration(workspaceNamespace, workspaceName, methodConfigurationNamespace, methodConfigurationName, newName)
    case DeleteMethodConfiguration(workspaceNamespace, workspaceName, methodConfigurationNamespace, methodConfigurationName) => context.parent ! deleteMethodConfiguration(workspaceNamespace, workspaceName, methodConfigurationNamespace, methodConfigurationName)
    case GetMethodConfiguration(workspaceNamespace, workspaceName, methodConfigurationNamespace, methodConfigurationName) => context.parent ! getMethodConfiguration(workspaceNamespace, workspaceName, methodConfigurationNamespace, methodConfigurationName)
    case UpdateMethodConfiguration(workspaceNamespace, workspaceName, methodConfiguration) => context.parent ! updateMethodConfiguration(workspaceNamespace, workspaceName, methodConfiguration)
    case CopyMethodConfiguration(methodConfigNamePair) => context.parent ! copyMethodConfiguration(methodConfigNamePair)
    case ListMethodConfigurations(workspaceNamespace, workspaceName) => context.parent ! listMethodConfigurations(workspaceNamespace, workspaceName)
  }

  def saveWorkspace(workspace: Workspace): PerRequestMessage =
    dataSource inTransaction { txn =>
    workspaceDAO.load(workspace.namespace, workspace.name, txn) match {
      case Some(_) =>
        PerRequest.RequestComplete(StatusCodes.Conflict, s"Workspace ${workspace.namespace}/${workspace.name} already exists")
      case None =>
        workspaceDAO.save(workspace, txn)
        PerRequest.RequestComplete((StatusCodes.Created, workspace))
    }
  }

  def getWorkspace(workspaceNamespace: String, workspaceName: String): PerRequestMessage =
    dataSource inTransaction { txn =>
      withWorkspace(workspaceNamespace, workspaceName, txn) { RequestComplete(_) }
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

  def cloneWorkspace(sourceNamespace:String, sourceWorkspace:String, destNamespace:String, destWorkspace:String): PerRequestMessage =
    dataSource inTransaction { txn =>
      val originalWorkspace = workspaceDAO.load(sourceNamespace, sourceWorkspace, txn)
      val copyWorkspace = workspaceDAO.load(destNamespace, destWorkspace, txn)
      (originalWorkspace, copyWorkspace) match {
        case ( Some(ws), None ) => {
          val newWorkspace = ws.copy(namespace = destNamespace, name = destWorkspace, createdDate = DateTime.now)
          workspaceDAO.save(newWorkspace, txn)
          entityDAO.cloneAllEntities(ws.namespace, newWorkspace.namespace, ws.name, newWorkspace.name, txn)
          methodConfigurationDAO.list(ws.namespace, ws.name, txn).foreach { methodConfig =>
            methodConfigurationDAO.save(newWorkspace.namespace, newWorkspace.name, methodConfigurationDAO.get(ws.namespace, ws.name, methodConfig.namespace, methodConfig.name, txn).get, txn)
          }
          RequestComplete((StatusCodes.Created, newWorkspace))
        }
        case ( None, _ ) => RequestComplete(StatusCodes.NotFound, "Source workspace " + sourceNamespace + "/" + sourceWorkspace + " not found")
        case ( _, Some(_) ) => RequestComplete(StatusCodes.Conflict, "Destination workspace " + destNamespace + "/" + destWorkspace + " already exists")
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
        RequestComplete(entityDAO.list(workspaceNamespace, workspaceName, entityType, txn).toIterable)
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
            case None => throw new AttributeNotFoundException(s"$attributeListName of ${attributable.path} does not exists")
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
      case None => RequestComplete(http.StatusCodes.NotFound, s"${entityType} ${entityName} does not exists in ${workspace.namespace}/${workspace.name}")
      case Some(entity) => op(entity)
    }
  }

  private def withMethodConfig(workspace: Workspace, methodConfigurationNamespace: String, methodConfigurationName: String, txn: RawlsTransaction)(op: (MethodConfiguration) => PerRequestMessage): PerRequestMessage = {
    methodConfigurationDAO.get(workspace.namespace, workspace.name, methodConfigurationNamespace, methodConfigurationName, txn) match {
      case None => RequestComplete(http.StatusCodes.NotFound, s"${methodConfigurationNamespace}/${methodConfigurationName} does not exists in ${workspace.namespace}/${workspace.name}")
      case Some(methodConfiguration) => op(methodConfiguration)
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
      withWorkspace(workspaceNamespace, workspaceName,txn) { workspace =>
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
          case Some(methodConfig) =>
            withWorkspace(mcnp.destination.workspaceName.namespace, mcnp.destination.workspaceName.name, txn) { destWorkspace =>
              methodConfigurationDAO.get(mcnp.destination.workspaceName.namespace, mcnp.destination.workspaceName.name, mcnp.destination.namespace, mcnp.destination.name, txn) match {
                case Some(existingMethodConfig) => RequestComplete(StatusCodes.Conflict,existingMethodConfig)
                case None =>
                  val target = methodConfig.copy(name = mcnp.destination.name, namespace = mcnp.destination.namespace, workspaceName = mcnp.destination.workspaceName)
                  val targetMethodConfig = methodConfigurationDAO.save(target.workspaceName.namespace, target.workspaceName.name, target, txn)
                  RequestComplete(StatusCodes.Created, targetMethodConfig)
            }
          }
        }
      }
    }

  def listMethodConfigurations(workspaceNamespace: String, workspaceName: String): PerRequestMessage =
    dataSource inTransaction { txn =>
      // use toList below to eagerly iterate through the response from methodConfigurationDAO.list
      // to ensure it is evaluated within the transaction
      RequestComplete(methodConfigurationDAO.list(workspaceNamespace, workspaceName, txn).toList)
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
