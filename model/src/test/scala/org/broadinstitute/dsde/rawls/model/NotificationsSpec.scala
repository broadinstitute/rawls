package org.broadinstitute.dsde.rawls.model

import org.scalatest.{Matchers, FlatSpec}
import spray.json.JsString

/**
 * Created by dvoet on 3/28/17.
 */
class NotificationsSpec extends FlatSpec with Matchers {
  "Notifications" should "have types" in {
    assert(!Notifications.allNotificationTypes.isEmpty)
  }

  val testsNotificationsByType = Map(
    "ActivationNotification" -> Notifications.ActivationNotification("asdf"),
    "WorkspaceAddedNotification" -> Notifications.WorkspaceAddedNotification("asdf", "user", WorkspaceName("namespace", "name"), "foo@bar.com"),
    "WorkspaceRemovedNotification" -> Notifications.WorkspaceRemovedNotification("asdf", "user", WorkspaceName("namespace", "name"), "foo@bar.com"),
    "WorkspaceInvitedNotification" -> Notifications.WorkspaceInvitedNotification("asdf", "foo@bar.com")
  )

  Notifications.allNotificationTypes.foreach { case (notificationTypeString, notificationType) =>
    it should s"write/read json for $notificationTypeString" in {
      val testsNotification = testsNotificationsByType(notificationTypeString)

      val json = Notifications.NotificationFormat.write(testsNotification)
      assertResult(Seq(JsString(notificationTypeString))) {
        json.asJsObject.getFields("notificationType")
      }
      
      assertResult(testsNotification) {
        Notifications.NotificationFormat.read(json)
      }
    }
  }


}
