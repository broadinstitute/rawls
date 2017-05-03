package org.broadinstitute.dsde.rawls.webservice

import akka.testkit.TestKit
import org.broadinstitute.dsde.rawls.dataaccess.{MockGoogleServicesDAO, SlickDataSource}
import org.broadinstitute.dsde.rawls.google.MockGooglePubSubDAO
import org.broadinstitute.dsde.rawls.model.Notifications._
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.openam.{MockUserInfoDirectives, UserInfoDirectives}
import spray.http.{OAuth2BearerToken, StatusCode, StatusCodes}

import scala.concurrent.duration._
import spray.json.DefaultJsonProtocol._
import WorkspaceACLJsonSupport._
import org.broadinstitute.dsde.vault.common.util.ImplicitMagnet
import spray.routing.Directive1

import scala.concurrent.{Await, ExecutionContext}

/**
 * Created by dvoet on 3/3/17.
 */
class NotificationSpec extends ApiServiceSpec {

  trait MockUserInfoDirectivesWithUser extends UserInfoDirectives {
  val user: String
  def requireUserInfo(magnet: ImplicitMagnet[ExecutionContext]): Directive1[UserInfo] = {
    // just return the cookie text as the common name
    user match {
      case testData.userProjectOwner.userEmail.value => provide(UserInfo(user, OAuth2BearerToken("token"), 123, testData.userProjectOwner.userSubjectId.value))
      case testData.userOwner.userEmail.value => provide(UserInfo(user, OAuth2BearerToken("token"), 123, testData.userOwner.userSubjectId.value))
      case testData.userWriter.userEmail.value => provide(UserInfo(user, OAuth2BearerToken("token"), 123, testData.userWriter.userSubjectId.value))
      case testData.userReader.userEmail.value => provide(UserInfo(user, OAuth2BearerToken("token"), 123, testData.userReader.userSubjectId.value))
      case "no-access" => provide(UserInfo(user, OAuth2BearerToken("token"), 123, "123456789876543212348"))
      case _ => provide(UserInfo(user, OAuth2BearerToken("token"), 123, "123456789876543212349"))
    }
  }
}
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

  def withTestDataApiServicesAndUser[T](user: String)(testCode: TestApiService => T): T = {
    withDefaultTestDatabase { dataSource: SlickDataSource =>
      withApiServices(dataSource, user) { services =>
        testData.createWorkspaceGoogleGroups(services.gcsDAO)
        testCode(services)
      }
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
  // Send Change Notification for Workspace require WRITE access. Accept if OWNER or WRITE; Reject if READ or NO ACCESS
  it should "allow an owner to send change notifications" in withTestDataApiServicesAndUser(testData.userOwner.userEmail.value) { services =>
    Post(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/sendChangeNotification", httpJsonEmpty) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
      }
    TestKit.awaitCond(services.gpsDAO.messageLog.contains(s"${services.notificationTopic}|${NotificationFormat.write(WorkspaceChangedNotification(testData.userProjectOwner.userSubjectId.value, testData.workspace.toWorkspaceName)).compactPrint}"), 10 seconds)
    TestKit.awaitCond(services.gpsDAO.messageLog.contains(s"${services.notificationTopic}|${NotificationFormat.write(WorkspaceChangedNotification(testData.userOwner.userSubjectId.value, testData.workspace.toWorkspaceName)).compactPrint}"), 10 seconds)
    TestKit.awaitCond(services.gpsDAO.messageLog.contains(s"${services.notificationTopic}|${NotificationFormat.write(WorkspaceChangedNotification(testData.userWriter.userSubjectId.value, testData.workspace.toWorkspaceName)).compactPrint}"), 10 seconds)
    TestKit.awaitCond(services.gpsDAO.messageLog.contains(s"${services.notificationTopic}|${NotificationFormat.write(WorkspaceChangedNotification(testData.userReader.userSubjectId.value, testData.workspace.toWorkspaceName)).compactPrint}"), 10 seconds)
  }

 it should "allow user with write-access to send change notifications" in withTestDataApiServicesAndUser(testData.userWriter.userEmail.value) { services =>
    Post(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/sendChangeNotification", httpJsonEmpty) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK){
          status
        }
      }
  }

  it should "not allow user with read-access to send change notifications" in withTestDataApiServicesAndUser(testData.userReader.userEmail.value) { services =>
    Post(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/sendChangeNotification", httpJsonEmpty) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.Forbidden) {
          status
        }
      }
  }

  it should "not allow user with no-access to send change notifications" in withTestDataApiServicesAndUser("no-access") { services =>
    Post(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/sendChangeNotification", httpJsonEmpty) ~>
      sealRoute(services.workspaceRoutes) ~>
      check{
        assertResult(StatusCodes.NotFound) {
          status
        }
      }
  }

}
