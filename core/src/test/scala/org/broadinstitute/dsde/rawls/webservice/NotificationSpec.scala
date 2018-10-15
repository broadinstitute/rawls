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
import org.broadinstitute.dsde.rawls.mock.MockSamDAO
import org.broadinstitute.dsde.rawls.openam.MockUserInfoDirectivesWithUser

import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}

/**
 * Created by dvoet on 3/3/17.
 */
class NotificationSpec extends ApiServiceSpec {

  class NotificationSpecMockSamDAO(dataSource: SlickDataSource) extends MockSamDAO(slickDataSource) {
    val userEmails = new TrieMap[String, String]()
    val invitedUsers = new TrieMap[String, String]()
    val policies = new TrieMap[(SamResourceTypeNames.SamResourceTypeName, String), Set[SamPolicyWithNameAndEmail]]()


    def addUser(user: RawlsUser) = userEmails.put(user.userEmail.value, user.userSubjectId.value)

    override def getUserIdInfo(userEmail: String, userInfo: UserInfo): Future[Either[Unit, Option[UserIdInfo]]] = {
      val result = userEmails.get(userEmail).map { id => UserIdInfo(id, userEmail, Option(id)) }
      Future.successful(result match {
        case Some(_) => Right(result)
        case None => Left(())
      })
    }

    override def inviteUser(userEmail: String, userInfo: UserInfo): Future[Unit] = {
      Future.successful(invitedUsers.put(userEmail, userEmail))
    }

    override def listPoliciesForResource(resourceTypeName: SamResourceTypeNames.SamResourceTypeName, resourceId: String, userInfo: UserInfo): Future[Set[SamPolicyWithNameAndEmail]] = {
      policies.get((resourceTypeName, resourceId)) match {
        case Some(policies) => Future.successful(policies)
        case None => super.listPoliciesForResource(resourceTypeName, resourceId, userInfo)
      }
    }
  }

  case class TestApiService(dataSource: SlickDataSource, user: RawlsUser, gcsDAO: MockGoogleServicesDAO, gpsDAO: MockGooglePubSubDAO)(implicit override val executionContext: ExecutionContext) extends ApiServices with MockUserInfoDirectivesWithUser {
    override val samDAO: NotificationSpecMockSamDAO = new NotificationSpecMockSamDAO(dataSource)
  }

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
        assertResult(StatusCodes.OK, responseAs[String]) { status }
      }

    services.samDAO.invitedUsers.keySet should contain(user.userEmail.value)
    TestKit.awaitCond(services.gpsDAO.messageLog.contains(s"${services.notificationTopic}|${NotificationFormat.write(WorkspaceInvitedNotification(RawlsUserEmail("obama@whitehouse.gov"), userInfo.userSubjectId, testData.workspace.toWorkspaceName, testData.workspace.bucketName)).compactPrint}"), 30 seconds)
  }

  it should "be sent for add to workspace" in withTestDataApiServices { services =>
    val user = RawlsUser(RawlsUserSubjectId("obamaiscool"), RawlsUserEmail("obama@whitehouse.gov"))
    services.samDAO.addUser(user)

    //add ACL
    Patch(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/acl", httpJson(Seq(WorkspaceACLUpdate(user.userEmail.value, WorkspaceAccessLevels.Write, None)))) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK) { status }
      }

    TestKit.awaitCond(services.gpsDAO.messageLog.contains(s"${services.notificationTopic}|${NotificationFormat.write(WorkspaceAddedNotification(user.userSubjectId, WorkspaceAccessLevels.Write.toString, testData.workspace.toWorkspaceName, userInfo.userSubjectId)).compactPrint}"), 30 seconds)
  }

  it should "be sent for remove from workspace" in withTestDataApiServices { services =>
    val user = RawlsUser(RawlsUserSubjectId("obamaiscool"), RawlsUserEmail("obama@whitehouse.gov"))
    services.samDAO.addUser(user)
    services.samDAO.policies.put((SamResourceTypeNames.workspace, testData.workspace.workspaceId), Set(SamPolicyWithNameAndEmail(SamWorkspacePolicyNames.writer.value, SamPolicy(Set(user.userEmail.value), Set.empty, Set.empty), "")))

    //remove ACL
    Patch(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/acl", httpJson(Seq(WorkspaceACLUpdate(user.userEmail.value, WorkspaceAccessLevels.NoAccess, None)))) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK, responseAs[String]) { status }
      }

    TestKit.awaitCond(services.gpsDAO.messageLog.contains(s"${services.notificationTopic}|${NotificationFormat.write(WorkspaceRemovedNotification(user.userSubjectId, WorkspaceAccessLevels.NoAccess.toString, testData.workspace.toWorkspaceName, userInfo.userSubjectId)).compactPrint}"), 30 seconds)
  }

  it should "sendChangeNotification api should exist" in withTestDataApiServicesAndUser(testData.userOwner) { services =>
    Post(s"/workspaces/${testData.workspace.namespace}/${testData.workspace.name}/sendChangeNotification", httpJsonEmpty) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
      }
  }
}
