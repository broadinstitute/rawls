package org.broadinstitute.dsde.rawls.webservice

import akka.testkit.TestKit
import org.broadinstitute.dsde.rawls.dataaccess.{MockGoogleServicesDAO, SlickDataSource}
import org.broadinstitute.dsde.rawls.google.MockGooglePubSubDAO
import org.broadinstitute.dsde.rawls.model.Notifications._
import org.broadinstitute.dsde.rawls.model._
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Route.{seal => sealRoute}

import scala.concurrent.duration._
import spray.json.DefaultJsonProtocol._
import WorkspaceACLJsonSupport._
import org.broadinstitute.dsde.rawls.openam.MockUserInfoDirectivesWithUser

import scala.concurrent.{Await, ExecutionContext}

/**
 * Created by dvoet on 3/3/17.
 */
class NotificationSpec extends ApiServiceSpec {


  case class TestApiService(dataSource: SlickDataSource, user: RawlsUser, gcsDAO: MockGoogleServicesDAO, gpsDAO: MockGooglePubSubDAO)(implicit override val executionContext: ExecutionContext) extends ApiServices with MockUserInfoDirectivesWithUser

  def withApiServices[T](dataSource: SlickDataSource, user: RawlsUser = testData.userOwner)(testCode: TestApiService => T): T = {
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

  def withTestDataApiServicesAndUser[T](user: RawlsUser)(testCode: TestApiService => T): T = {
    withDefaultTestDatabase { dataSource: SlickDataSource =>
      withApiServices(dataSource, user) { services =>
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
        assertResult(StatusCodes.OK) { status }
      }

    TestKit.awaitCond(services.gpsDAO.messageLog.contains(s"${services.notificationTopic}|${NotificationFormat.write(WorkspaceInvitedNotification(RawlsUserEmail("obama@whitehouse.gov"), userInfo.userSubjectId, testData.workspace.toWorkspaceName, testData.workspace.bucketName)).compactPrint}"), 30 seconds)
  }

  it should "be sent for add and remove from workspace" in withTestDataApiServices { services =>
    val user = RawlsUser(RawlsUserSubjectId("obamaiscool"), RawlsUserEmail("obama@whitehouse.gov"))
//    runAndWait(DBIO.from(samDataSaver.createUser(user)))

    //add ACL
    Patch(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/acl", httpJson(Seq(WorkspaceACLUpdate(user.userEmail.value, WorkspaceAccessLevels.Write, None)))) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK) { status }
      }

    TestKit.awaitCond(services.gpsDAO.messageLog.contains(s"${services.notificationTopic}|${NotificationFormat.write(WorkspaceAddedNotification(user.userSubjectId, WorkspaceAccessLevels.Write.toString, testData.workspace.toWorkspaceName, userInfo.userSubjectId)).compactPrint}"), 30 seconds)

    //remove ACL
    Patch(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/acl", httpJson(Seq(WorkspaceACLUpdate(user.userEmail.value, WorkspaceAccessLevels.NoAccess, None)))) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK) { status }
      }

    TestKit.awaitCond(services.gpsDAO.messageLog.contains(s"${services.notificationTopic}|${NotificationFormat.write(WorkspaceRemovedNotification(user.userSubjectId, WorkspaceAccessLevels.NoAccess.toString, testData.workspace.toWorkspaceName, userInfo.userSubjectId)).compactPrint}"), 30 seconds)
  }

  // Send Change Notification for Workspace require WRITE access. Accept if OWNER or WRITE; Reject if READ or NO ACCESS
  it should "allow an owner to send change notifications" in withTestDataApiServicesAndUser(testData.userOwner) { services =>
    Post(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/sendChangeNotification", httpJsonEmpty) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
      }
    TestKit.awaitCond(services.gpsDAO.messageLog.contains(s"${services.notificationTopic}|${NotificationFormat.write(WorkspaceChangedNotification(testData.userProjectOwner.userSubjectId, testData.workspace.toWorkspaceName)).compactPrint}"), 30 seconds)
    TestKit.awaitCond(services.gpsDAO.messageLog.contains(s"${services.notificationTopic}|${NotificationFormat.write(WorkspaceChangedNotification(testData.userOwner.userSubjectId, testData.workspace.toWorkspaceName)).compactPrint}"), 30 seconds)
    TestKit.awaitCond(services.gpsDAO.messageLog.contains(s"${services.notificationTopic}|${NotificationFormat.write(WorkspaceChangedNotification(testData.userWriter.userSubjectId, testData.workspace.toWorkspaceName)).compactPrint}"), 30 seconds)
    TestKit.awaitCond(services.gpsDAO.messageLog.contains(s"${services.notificationTopic}|${NotificationFormat.write(WorkspaceChangedNotification(testData.userReader.userSubjectId, testData.workspace.toWorkspaceName)).compactPrint}"), 30 seconds)
  }

 it should "allow user with write-access to send change notifications" in withTestDataApiServicesAndUser(testData.userWriter) { services =>
    Post(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/sendChangeNotification", httpJsonEmpty) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK){
          status
        }
      }
  }

  it should "not allow user with read-access to send change notifications" in withTestDataApiServicesAndUser(testData.userReader) { services =>
    Post(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/sendChangeNotification", httpJsonEmpty) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.Forbidden) {
          status
        }
      }
  }

  it should "not allow user with no-access to send change notifications" in withTestDataApiServicesAndUser(RawlsUser(RawlsUserSubjectId("no-access"), RawlsUserEmail("obamaiscool"))) { services =>
    Post(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/sendChangeNotification", httpJsonEmpty) ~>
      sealRoute(services.workspaceRoutes) ~>
      check{
        assertResult(StatusCodes.NotFound) {
          status
        }
      }
  }

}
