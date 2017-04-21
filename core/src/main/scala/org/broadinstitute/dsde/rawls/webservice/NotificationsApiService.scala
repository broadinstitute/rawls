package org.broadinstitute.dsde.rawls.webservice

import org.broadinstitute.dsde.rawls.model.{Notifications, UserInfo, WorkspaceName}
import org.broadinstitute.dsde.rawls.model.Notifications.{NotificationType, WorkspaceNotification, WorkspaceNotificationType}
import org.broadinstitute.dsde.rawls.workspace.WorkspaceService
import org.broadinstitute.dsde.rawls.openam.UserInfoDirectives
import spray.routing.HttpService
import spray.httpx.SprayJsonSupport._
import spray.json.DefaultJsonProtocol._

import scala.concurrent.ExecutionContext
/**
 * Created by dvoet on 3/28/17.
 */
trait NotificationsApiService extends HttpService with PerRequestCreator with UserInfoDirectives {
  implicit val executionContext: ExecutionContext

  val workspaceServiceConstructor: UserInfo => WorkspaceService
  //this route does not have the notifications prefix
  val sendChangeNotificationRoute = requireUserInfo() { userInfo =>
    path("workspace" / Segment / Segment / "sendChangeNotification") { (namespace, name) =>
      post {
        requestContext => perRequest(requestContext, WorkspaceService.props(workspaceServiceConstructor, userInfo),
          WorkspaceService.SendChangeNotification(WorkspaceName(namespace, name)))
      }
    }
  }
  val notificationsRoutes = pathPrefix("notifications") {
    path("workspace" / Segment / Segment) { (namespace, name) =>
      get {
        val workspaceName = WorkspaceName(namespace, name)

        val workspaceNotificationTypes = Notifications.allNotificationTypes.values.collect {
          case nt: WorkspaceNotificationType[_] if nt.workspaceNotification => nt
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
          case nt: NotificationType[_] if !nt.workspaceNotification => nt
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
