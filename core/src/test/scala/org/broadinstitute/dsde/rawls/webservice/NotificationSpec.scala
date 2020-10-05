package org.broadinstitute.dsde.rawls.webservice

import akka.testkit.TestKit
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.google.MockGooglePubSubDAO
import org.broadinstitute.dsde.rawls.model.Notifications._
import org.broadinstitute.dsde.rawls.model._
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Route.{seal => sealRoute}

import scala.concurrent.duration._
import spray.json.DefaultJsonProtocol._
import WorkspaceACLJsonSupport._
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import org.broadinstitute.dsde.rawls.mock.CustomizableMockSamDAO
import org.broadinstitute.dsde.rawls.model
import org.broadinstitute.dsde.rawls.openam.MockUserInfoDirectivesWithUser
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.mockito.ArgumentMatchers

import scala.concurrent.{ExecutionContext, Future}
import scala.language.postfixOps
import org.mockito.Mockito._
import org.mockito.ArgumentMatchers._


/**
 * Created by dvoet on 3/3/17.
 */
class NotificationSpec extends ApiServiceSpec {

  case class TestApiService(dataSource: SlickDataSource, user: RawlsUser, gcsDAO: MockGoogleServicesDAO, gpsDAO: MockGooglePubSubDAO)(implicit override val executionContext: ExecutionContext) extends ApiServices with MockUserInfoDirectivesWithUser {
    override val samDAO: CustomizableMockSamDAO = new CustomizableMockSamDAO(dataSource)
  }

  case class TestApiServiceMokitoSam(dataSource: SlickDataSource, user: RawlsUser, gcsDAO: MockGoogleServicesDAO, gpsDAO: MockGooglePubSubDAO)(implicit override val executionContext: ExecutionContext) extends ApiServices with MockUserInfoDirectivesWithUser {
    override val samDAO: SamDAO = mock[SamDAO](RETURNS_SMART_NULLS)
  }

  def withApiServices[T](dataSource: SlickDataSource, user: RawlsUser = testData.userOwner)(testCode: TestApiService => T): T = {
    val apiService = new TestApiService(dataSource, user, new MockGoogleServicesDAO("test"), new MockGooglePubSubDAO)
    try {
      testCode(apiService)
    } finally {
      apiService.cleanupSupervisor
    }
  }

  def withApiServicesMockitoSam[T](dataSource: SlickDataSource, user: RawlsUser = testData.userOwner)(testCode: TestApiServiceMokitoSam => T): T = {
    val apiService = new TestApiServiceMokitoSam(dataSource, user, new MockGoogleServicesDAO("test"), new MockGooglePubSubDAO)
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

  def withTestDataApiServicesAndUser[T](user: RawlsUser)(testCode: TestApiService => T): T = {
    withDefaultTestDatabase { dataSource: SlickDataSource =>
      withApiServices(dataSource, user) { services =>
        testCode(services)
      }
    }
  }

  def withTestDataApiServicesAndUserAndMockitoSam[T](user: RawlsUser)(testCode: TestApiServiceMokitoSam => T): T = {
    withDefaultTestDatabase { dataSource: SlickDataSource =>
      withApiServicesMockitoSam(dataSource, user) { services =>
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
        assertResult(StatusCodes.OK, responseAs[String]) { status }
      }

    services.samDAO.invitedUsers.keySet should contain(user.userEmail.value)
    TestKit.awaitCond(services.gpsDAO.messageLog.contains(s"${services.notificationTopic}|${NotificationFormat.write(WorkspaceInvitedNotification(RawlsUserEmail("obama@whitehouse.gov"), userInfo.userSubjectId, testData.workspace.toWorkspaceName, testData.workspace.bucketName)).compactPrint}"), 30 seconds)
  }

  it should "be sent for add to workspace" in withTestDataApiServices { services =>
    val user = model.UserInfo(RawlsUserEmail("obama@whitehouse.gov"), OAuth2BearerToken(""), 0, RawlsUserSubjectId("obamaiscool"))
    services.samDAO.registerUser(user)

    //add ACL
    Patch(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/acl", httpJson(Seq(WorkspaceACLUpdate(user.userEmail.value, WorkspaceAccessLevels.Write, None)))) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK) { status }
      }

    TestKit.awaitCond(services.gpsDAO.messageLog.contains(s"${services.notificationTopic}|${NotificationFormat.write(WorkspaceAddedNotification(user.userSubjectId, WorkspaceAccessLevels.Write.toString, testData.workspace.toWorkspaceName, userInfo.userSubjectId)).compactPrint}"), 30 seconds)
  }

  it should "be sent for remove from workspace" in withTestDataApiServices { services =>
    val user = model.UserInfo(RawlsUserEmail("obama@whitehouse.gov"), OAuth2BearerToken(""), 0, RawlsUserSubjectId("obamaiscool"))
    services.samDAO.registerUser(user)
    services.samDAO.overwritePolicy(SamResourceTypeNames.workspace, testData.workspace.workspaceId, SamWorkspacePolicyNames.writer, SamPolicy(Set(WorkbenchEmail(user.userEmail.value)), Set.empty, Set.empty), userInfo)

    //remove ACL
    Patch(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/acl", httpJson(Seq(WorkspaceACLUpdate(user.userEmail.value, WorkspaceAccessLevels.NoAccess, None)))) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK, responseAs[String]) { status }
      }

    TestKit.awaitCond(services.gpsDAO.messageLog.contains(s"${services.notificationTopic}|${NotificationFormat.write(WorkspaceRemovedNotification(user.userSubjectId, WorkspaceAccessLevels.NoAccess.toString, testData.workspace.toWorkspaceName, userInfo.userSubjectId)).compactPrint}"), 30 seconds)
  }

  it should "be sent on sendChangeNotification" in withTestDataApiServicesAndUserAndMockitoSam(testData.userOwner) { services =>
    when(services.samDAO.userHasAction(
      ArgumentMatchers.eq(SamResourceTypeNames.workspace),
      ArgumentMatchers.eq(testData.workspace.workspaceId),
      ArgumentMatchers.eq(SamWorkspaceActions.own),
      any[UserInfo]
    )).thenReturn(Future.successful(true))

    when(services.samDAO.listAllResourceMemberIds(
      ArgumentMatchers.eq(SamResourceTypeNames.workspace),
      ArgumentMatchers.eq(testData.workspace.workspaceId),
      any[UserInfo]
    )).thenReturn(Future.successful(Set(UserIdInfo("1", "", None), UserIdInfo("1", "", Some("11")), UserIdInfo("1", "", Some("22")))))

    Post(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/sendChangeNotification") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK, responseAs[String]) {
          status
        }
        assertResult("2") {
          responseAs[String]
        }
      }
    TestKit.awaitCond(services.gpsDAO.messageLog.contains(s"${services.notificationTopic}|${NotificationFormat.write(WorkspaceChangedNotification(RawlsUserSubjectId("11"), testData.workspace.toWorkspaceName)).compactPrint}"), 30 seconds)
    TestKit.awaitCond(services.gpsDAO.messageLog.contains(s"${services.notificationTopic}|${NotificationFormat.write(WorkspaceChangedNotification(RawlsUserSubjectId("22"), testData.workspace.toWorkspaceName)).compactPrint}"), 30 seconds)
  }

  it should "403 on sendChangeNotification for non owner" in withTestDataApiServicesAndUserAndMockitoSam(testData.userOwner) { services =>
    when(services.samDAO.userHasAction(
      ArgumentMatchers.eq(SamResourceTypeNames.workspace),
      ArgumentMatchers.eq(testData.workspace.workspaceId),
      ArgumentMatchers.eq(SamWorkspaceActions.read),
      any[UserInfo]
    )).thenReturn(Future.successful(true))

    when(services.samDAO.userHasAction(
      ArgumentMatchers.eq(SamResourceTypeNames.workspace),
      ArgumentMatchers.eq(testData.workspace.workspaceId),
      ArgumentMatchers.eq(SamWorkspaceActions.own),
      any[UserInfo]
    )).thenReturn(Future.successful(false))

    Post(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/sendChangeNotification") ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.Forbidden, responseAs[String]) {
          status
        }
      }
  }
}
