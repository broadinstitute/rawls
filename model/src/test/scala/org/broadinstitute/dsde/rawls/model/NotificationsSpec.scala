package org.broadinstitute.dsde.rawls.model

import spray.json.JsString
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * Created by dvoet on 3/28/17.
 */
class NotificationsSpec extends AnyFlatSpec with Matchers {
  "Notifications" should "have types" in {
    assert(!Notifications.allNotificationTypes.isEmpty)
  }

  val testsNotificationsByType = Map(
    "ActivationNotification" -> Notifications.ActivationNotification(RawlsUserSubjectId("123456789876543212346")),
    "WorkspaceAddedNotification" -> Notifications.WorkspaceAddedNotification(RawlsUserSubjectId("123456789876543212346"), "READER", WorkspaceName("namespace", "name"), RawlsUserSubjectId("123456789876543212347")),
    "WorkspaceRemovedNotification" -> Notifications.WorkspaceRemovedNotification(RawlsUserSubjectId("123456789876543212346"), "READER", WorkspaceName("namespace", "name"), RawlsUserSubjectId("123456789876543212347")),
    "WorkspaceInvitedNotification" -> Notifications.WorkspaceInvitedNotification(RawlsUserEmail("foo@bar.com"), RawlsUserSubjectId("123456789876543212347"), WorkspaceName("namespace", "name"), "test-bucket-name"),
    "WorkspaceChangedNotification" -> Notifications.WorkspaceChangedNotification(RawlsUserSubjectId("123456789876543212346"), WorkspaceName("namespace", "name")),
    "GroupAccessRequestNotification" -> Notifications.GroupAccessRequestNotification(RawlsUserSubjectId("123456789876543212346"), "my-group", Set(RawlsUserSubjectId("223456789876543212346")), RawlsUserSubjectId("123456789876543212347"))
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
