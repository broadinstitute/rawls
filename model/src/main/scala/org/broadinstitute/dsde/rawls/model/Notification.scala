package org.broadinstitute.dsde.rawls.model

import spray.json.DefaultJsonProtocol._
import spray.json._

object Notifications {

  sealed trait Notification

  case class ActivationNotification(recipientUserId: String) extends Notification
  case class WorkspaceAddedNotification(recipientUserId: String, accessLevel: String, workspaceNamespace: String, workspaceName: String, workspaceOwnerEmail: String) extends Notification
  case class WorkspaceRemovedNotification(recipientUserId: String, accessLevel: String, workspaceNamespace: String, workspaceName: String, workspaceOwnerEmail: String) extends Notification
  case class WorkspaceInvitedNotification(recipientUserEmail: String, originEmail: String) extends Notification

  private val ActivationNotificationFormat = jsonFormat1(ActivationNotification)
  private val WorkspaceAddedNotificationFormat = jsonFormat5(WorkspaceAddedNotification)
  private val WorkspaceRemovedNotificationFormat = jsonFormat5(WorkspaceRemovedNotification)
  private val WorkspaceInvitedNotificationFormat = jsonFormat2(WorkspaceInvitedNotification)

  implicit object NotificationFormat extends RootJsonFormat[Notification] {

    private val notificationTypeAttribute = "notificationType"

    override def write(obj: Notification): JsValue = {
      val json = obj match {
        case x: ActivationNotification => ActivationNotificationFormat.write(x)
        case x: WorkspaceAddedNotification => WorkspaceAddedNotificationFormat.write(x)
        case x: WorkspaceRemovedNotification => WorkspaceRemovedNotificationFormat.write(x)
        case x: WorkspaceInvitedNotification => WorkspaceInvitedNotificationFormat.write(x)
      }

      JsObject(json.asJsObject.fields + (notificationTypeAttribute -> JsString(obj.getClass.getSimpleName)))
    }

    override def read(json: JsValue) : Notification = json match {
      case JsObject(fields) =>
        val notificationType = fields.getOrElse(notificationTypeAttribute, throw new DeserializationException(s"missing $notificationTypeAttribute property"))
        notificationType match {
          case JsString("ActivationNotification") => ActivationNotificationFormat.read(json)
          case JsString("WorkspaceAddedNotification") => WorkspaceAddedNotificationFormat.read(json)
          case JsString("WorkspaceRemovedNotification") => WorkspaceRemovedNotificationFormat.read(json)
          case JsString("WorkspaceInvitedNotification") => WorkspaceInvitedNotificationFormat.read(json)
          case x => throw new DeserializationException(s"unrecognized $notificationTypeAttribute: $x")
        }

      case _ => throw new DeserializationException("unexpected json type")
    }
  }
}