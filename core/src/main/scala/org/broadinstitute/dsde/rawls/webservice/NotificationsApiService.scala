package org.broadinstitute.dsde.rawls.webservice

import org.broadinstitute.dsde.rawls.model.{WorkspaceName, Notifications}
import org.broadinstitute.dsde.rawls.model.Notifications.{WorkspaceNotificationType, WorkspaceNotification, NotificationType}
import spray.routing.HttpService
import spray.httpx.SprayJsonSupport._
import spray.json.DefaultJsonProtocol._

/**
 * Created by dvoet on 3/28/17.
 */
trait NotificationsApiService extends HttpService {
  val notificationsRoutes = pathPrefix("notifications") {
    path("workspace" / Segment / Segment) { (namespace, name) =>
      get {
        val workspaceName = WorkspaceName(namespace, name)

        val workspaceNotificationTypes = Notifications.allNotificationTypes.values.collect {
          case nt: WorkspaceNotificationType[_] if nt.workspaceNotification && !nt.alwaysOn => nt
        }

        complete {
          workspaceNotificationTypes.map(nt => Map(
            "notificationKey" -> nt.workspaceKey(workspaceName),
            "description" -> nt.description))
        }
      }
    } ~
    path("general") {
      get {
        val workspaceNotificationTypes = Notifications.allNotificationTypes.values.collect {
          case nt: NotificationType[_] if !nt.workspaceNotification && !nt.alwaysOn => nt
        }

        complete {
          workspaceNotificationTypes.map(nt => Map(
            "notificationKey" -> nt.baseKey,
            "description" -> nt.description))
        }
      }
    }
  }
}
