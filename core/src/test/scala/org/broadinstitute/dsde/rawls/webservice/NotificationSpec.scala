package org.broadinstitute.dsde.rawls.webservice

import akka.testkit.TestKit
import org.broadinstitute.dsde.rawls.dataaccess.{MockGoogleServicesDAO, SlickDataSource}
import org.broadinstitute.dsde.rawls.google.MockGooglePubSubDAO
import org.broadinstitute.dsde.rawls.model.Notifications._
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.openam.MockUserInfoDirectives
import spray.http.{StatusCode, StatusCodes}
import scala.concurrent.duration._
import spray.json.DefaultJsonProtocol._
import WorkspaceACLJsonSupport._

import scala.concurrent.{Await, ExecutionContext}

/**
 * Created by dvoet on 3/3/17.
 */
class NotificationSpec extends ApiServiceSpec {

  case class TestApiService(dataSource: SlickDataSource, user: String, gcsDAO: MockGoogleServicesDAO, gpsDAO: MockGooglePubSubDAO)(implicit val executionContext: ExecutionContext) extends ApiServices with MockUserInfoDirectives

  def withApiServices[T](dataSource: SlickDataSource, user: String = testData.userOwner.userEmail.value)(testCode: TestApiService => T): T = {
    val apiService = new TestApiService(dataSource, user, new MockGoogleServicesDAO("test"), new MockGooglePubSubDAO)
    try {
      testCode(apiService)
    } finally {
      apiService.cleanupSupervisor
    }
  }

  def withTestDataApiServices[T](testCode: TestApiService => T): T = {
    withDefaultTestDatabase { dataSource: SlickDataSource =>
      withApiServices(dataSource)(testCode)
    }
  }


  "Notification" should "be sent for invitation" in withTestDataApiServices { services =>
    val user = RawlsUser(RawlsUserSubjectId("obamaiscool"), RawlsUserEmail("obama@whitehouse.gov"))

    //add ACL
    Patch(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/acl?inviteUsersNotFound=true", httpJson(Seq(WorkspaceACLUpdate(user.userEmail.value, WorkspaceAccessLevels.Write, None)))) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK, response.entity.asString) { status }
      }

    TestKit.awaitCond(services.gpsDAO.messageLog.contains(s"${services.notificationTopic}|${NotificationFormat.write(WorkspaceInvitedNotification("obama@whitehouse.gov", userInfo.userEmail)).compactPrint}"), 10 seconds)
  }

  it should "be sent for add and remove from workspace" in withTestDataApiServices { services =>
    val user = RawlsUser(RawlsUserSubjectId("obamaiscool"), RawlsUserEmail("obama@whitehouse.gov"))
    runAndWait(rawlsUserQuery.save(user))

    //add ACL
    Patch(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/acl", httpJson(Seq(WorkspaceACLUpdate(user.userEmail.value, WorkspaceAccessLevels.Write, None)))) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK, response.entity.asString) { status }
      }

    TestKit.awaitCond(services.gpsDAO.messageLog.contains(s"${services.notificationTopic}|${NotificationFormat.write(WorkspaceAddedNotification(user.userSubjectId.value, WorkspaceAccessLevels.Write.toString, testData.workspace.toWorkspaceName, userInfo.userEmail)).compactPrint}"), 10 seconds)

    //remove ACL
    Patch(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/acl", httpJson(Seq(WorkspaceACLUpdate(user.userEmail.value, WorkspaceAccessLevels.NoAccess, None)))) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK, response.entity.asString) { status }
      }

    TestKit.awaitCond(services.gpsDAO.messageLog.contains(s"${services.notificationTopic}|${NotificationFormat.write(WorkspaceRemovedNotification(user.userSubjectId.value, WorkspaceAccessLevels.NoAccess.toString, testData.workspace.toWorkspaceName, userInfo.userEmail)).compactPrint}"), 10 seconds)
  }

  it should "be sent for activation" in withEmptyTestDatabase { dataSource: SlickDataSource => withApiServices(dataSource) { services =>
    val user = RawlsUser(RawlsUserSubjectId("123456789876543212345"), RawlsUserEmail("owner-access"))

    Post("/user") ~>
      sealRoute(services.createUserRoute) ~>
      check {
        assertResult(StatusCodes.Created) {
          status
        }
      }

    TestKit.awaitCond(services.gpsDAO.messageLog.contains(s"${services.notificationTopic}|${NotificationFormat.write(ActivationNotification(user.userSubjectId.value)).compactPrint}"), 10 seconds)

  } }
}
