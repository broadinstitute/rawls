package org.broadinstitute.dsde.rawls.webservice

import java.util.UUID

import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.google.MockGooglePubSubDAO
import org.broadinstitute.dsde.rawls.model.ExecutionJsonSupport.{ActiveSubmissionFormat, WorkflowQueueStatusByUserResponseFormat}
import org.broadinstitute.dsde.rawls.model.UserAuthJsonSupport.{RawlsGroupMemberListFormat, SyncReportFormat, RawlsBillingProjectTransferFormat, UserInfoFormat}
import org.broadinstitute.dsde.rawls.model.UserJsonSupport.{UserListFormat, UserStatusFormat}
import org.broadinstitute.dsde.rawls.model.UserModelJsonSupport.RawlsGroupRefFormat
import org.broadinstitute.dsde.rawls.model.WorkspaceAccessLevels.ProjectOwner
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport.{AttributeReferenceFormat, WorkspaceFormat, WorkspaceListResponseFormat, WorkspaceStatusFormat}
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.openam.MockUserInfoDirectives
import org.broadinstitute.dsde.rawls.user.UserService
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import spray.json.DefaultJsonProtocol._
import spray.json._
import akka.http.scaladsl.server.Route.{seal => sealRoute}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext}

/**
 * Created by tsharpe on 9/28/15.
 */
class AdminApiServiceSpec extends ApiServiceSpec {

  case class TestApiService(dataSource: SlickDataSource, gcsDAO: MockGoogleServicesDAO, gpsDAO: MockGooglePubSubDAO)(implicit override val executionContext: ExecutionContext) extends ApiServices with MockUserInfoDirectives

  def withApiServices[T](dataSource: SlickDataSource)(testCode: TestApiService =>  T): T = {
    val apiService = new TestApiService(dataSource, new MockGoogleServicesDAO("test"), new MockGooglePubSubDAO)
    try {
      testCode(apiService)
    } finally {
      apiService.cleanupSupervisor
    }
  }

  def withTestDataApiServices[T](testCode: TestApiService =>  T): T = {
    withDefaultTestDatabase { dataSource: SlickDataSource =>
      withApiServices(dataSource)(testCode)
    }
  }

  def withConstantTestDataApiServices[T](testCode: TestApiService =>  T): T = {
    withConstantTestDatabase { dataSource: SlickDataSource =>
      withApiServices(dataSource)(testCode)
    }
  }

  def getBillingProject(dataSource: SlickDataSource, project: RawlsBillingProject) = runAndWait(rawlsBillingProjectQuery.load(project.projectName))

  def loadUser(user: RawlsUser) = runAndWait(rawlsUserQuery.load(user))

  def assertUserMissing(services: TestApiService, user: RawlsUser): Unit = {
    assert {
      loadUser(user).isEmpty
    }
    assert {
      val group = runAndWait(rawlsGroupQuery.load(UserService.allUsersGroupRef))
      group.isEmpty || ! group.get.users.contains(user)
    }
  }

  def assertUserExists(services: TestApiService, user: RawlsUser): Unit = {
    assert {
      loadUser(user).nonEmpty
    }
    val group = runAndWait(rawlsGroupQuery.load(UserService.allUsersGroupRef))
    assert {
      group.isDefined
    }
    assert {
      group.get.users.contains(user)
    }
  }

  "AdminApi" should "return 200 when listing active submissions" in withConstantTestDataApiServices { services =>
    val expected = Seq(
      ActiveSubmission(constantData.workspace.namespace, constantData.workspace.name, constantData.submissionNoWorkflows),
      ActiveSubmission(constantData.workspace.namespace, constantData.workspace.name, constantData.submission1),
      ActiveSubmission(constantData.workspace.namespace, constantData.workspace.name, constantData.submission2))

    withStatsD {
      Get("/admin/submissions") ~>
        sealRoute(instrumentRequest { services.adminRoutes }) ~>
        check {
          assertResult(StatusCodes.OK) {
            status
          }
          assertSameElements(expected, responseAs[Seq[ActiveSubmission]])
        }
    } { capturedMetrics =>
      val expected = expectedHttpRequestMetrics("get", "admin.submissions", StatusCodes.OK.intValue, 1)
      assertSubsetOf(expected, capturedMetrics)
    }
  }

  val project = "some-project"
  val bucket = "some-bucket"

  it should "return 201 when registering a billing project for the 1st time, and 500 for the 2nd" in withTestDataApiServices { services =>
    Post(s"/admin/project/registration", httpJson(RawlsBillingProjectTransfer(project, bucket, userInfo.userEmail.value, userInfo.accessToken.value))) ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.Created) {
          status
        }
      }

    Post(s"/admin/project/registration", httpJson(RawlsBillingProjectTransfer(project, bucket, userInfo.userEmail.value, userInfo.accessToken.value))) ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.InternalServerError) {
          status
        }
      }
  }

  it should "return 204 when unregistering a billing project" in withTestDataApiServices { services =>
    val projectName = "unregistered-bp"

    Post(s"/admin/project/registration", httpJson(RawlsBillingProjectTransfer(projectName, bucket, userInfo.userEmail.value, userInfo.accessToken.value))) ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.Created) {
          status
        }
      }

    Delete(s"/admin/project/registration/$projectName", httpJson(Map("newOwnerEmail" -> userInfo.userEmail.value, "newOwnerToken" -> userInfo.accessToken.token))) ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.NoContent) {
          status
        }
      }
  }

  it should "return 200 when listing active submissions on deleted entities" in withConstantTestDataApiServices { services =>
    Post(s"${constantData.workspace.path}/entities/delete", httpJson(EntityDeleteRequest(constantData.indiv1))) ~>
      sealRoute(services.entityRoutes) ~>
      check {
        assertResult(StatusCodes.NoContent) {
          status
        }
      }

    Get(s"/admin/submissions") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }

        val resp = responseAs[Array[ActiveSubmission]]

        // entity name will be modified a la DriverComponent.renameForHiding

        val responseEntityNames = resp.map(_.submission).map(_.submissionEntity).map(_.get.entityName).toSet
        assertResult(1)(responseEntityNames.size)
        assert(responseEntityNames.head.contains(constantData.indiv1.name + "_"))

        // check that the response contains the same submissions, with only entity names changed

        val expected = Seq(
          ActiveSubmission(constantData.workspace.namespace, constantData.workspace.name, constantData.submissionNoWorkflows),
          ActiveSubmission(constantData.workspace.namespace, constantData.workspace.name, constantData.submission1),
          ActiveSubmission(constantData.workspace.namespace, constantData.workspace.name, constantData.submission2))

        def withNewEntityNames(in: Seq[ActiveSubmission]): Seq[ActiveSubmission] = {
          in.map { as =>
            as.copy(submission = as.submission.copy(submissionEntity = Some(as.submission.submissionEntity.get.copy(entityName = "newName"))))
          }
        }

        assertSameElements(withNewEntityNames(expected), withNewEntityNames(resp))
      }
  }

  it should "return 204 when aborting an active submission" in withTestDataApiServices { services =>
    Delete(s"/admin/submissions/${testData.wsName.namespace}/${testData.wsName.name}/${testData.submissionTerminateTest.submissionId}") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.NoContent) { status }
      }
  }

  it should "return 404 when aborting a bogus active submission" in withTestDataApiServices { services =>
    Delete(s"/admin/submissions/${testData.wsName.namespace}/${testData.wsName.name}/fake") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound) { status }
      }
  }

  it should "return 200 when adding a library curator" in withTestDataApiServices { services =>
    val testUser = "foo@bar.com"
    Put(s"/admin/user/role/curator/${testUser}") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.OK) { status }
      }
  }

  it should "return 200 when removing a library curator" in withTestDataApiServices { services =>
    val testUser = "foo@bar.com"
    Put(s"/admin/user/role/curator/${testUser}") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.OK) { status }
      }
    Delete(s"/admin/user/role/curator/${testUser}") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.OK) { status }
      }
  }

  it should "get, grant, revoke all user read access to workspace" in withTestDataApiServices { services =>
    runAndWait(rawlsGroupQuery.save(RawlsGroup(UserService.allUsersGroupRef.groupName, RawlsGroupEmail(services.gcsDAO.toGoogleGroupName(UserService.allUsersGroupRef.groupName)), Set.empty[RawlsUserRef], Set.empty[RawlsGroupRef])))

    testData.workspace.accessLevels.values.foreach(services.gcsDAO.createGoogleGroup)

    Get(s"/admin/allUserReadAccess/${testData.workspace.namespace}/${testData.workspace.name}") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound) { status }
      }

    services.gpsDAO.messageLog.clear()
    Put(s"/admin/allUserReadAccess/${testData.workspace.namespace}/${testData.workspace.name}") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.NoContent) { status }
      }
    Get(s"/admin/allUserReadAccess/${testData.workspace.namespace}/${testData.workspace.name}") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.NoContent) { status }
      }

    val group = runAndWait(rawlsGroupQuery.load(testData.workspace.accessLevels(WorkspaceAccessLevels.Read))).get
    assert(services.gpsDAO.receivedMessage(services.googleGroupSyncTopic, RawlsGroup.toRef(group).toJson.compactPrint, 1))

    Get(testData.workspace.path) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertWorkspaceModifiedDate(status, responseAs[WorkspaceListResponse].workspace)
      }

    Delete(s"/admin/allUserReadAccess/${testData.workspace.namespace}/${testData.workspace.name}") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.NoContent) { status }
      }
    Get(s"/admin/allUserReadAccess/${testData.workspace.namespace}/${testData.workspace.name}") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound) { status }
      }
    assert(services.gpsDAO.receivedMessage(services.googleGroupSyncTopic, RawlsGroup.toRef(group).toJson.compactPrint, 2))

    Get(testData.workspace.path) ~>
      sealRoute(services.workspaceRoutes) ~>
      check {
        assertWorkspaceModifiedDate(status, responseAs[WorkspaceListResponse].workspace)
      }

  }

  it should "sync group membership" in withTestDataApiServices { services =>
    val inGoogleGroup = RawlsGroup(
      RawlsGroupName("google"),
      RawlsGroupEmail(services.gcsDAO.toGoogleGroupName(RawlsGroupName("google"))),
      Set.empty[RawlsUserRef],
      Set.empty[RawlsGroupRef])
    val inBothGroup = RawlsGroup(
      RawlsGroupName("both"),
      RawlsGroupEmail(services.gcsDAO.toGoogleGroupName(RawlsGroupName("both"))),
      Set.empty[RawlsUserRef],
      Set.empty[RawlsGroupRef])
    val inDbGroup = RawlsGroup(
      RawlsGroupName("db"),
      RawlsGroupEmail(services.gcsDAO.toGoogleGroupName(RawlsGroupName("db"))),
      Set.empty[RawlsUserRef],
      Set.empty[RawlsGroupRef])

    val inGoogleUser = RawlsUser(RawlsUserSubjectId("google"), RawlsUserEmail("google@fc.org"))
    val inBothUser = RawlsUser(RawlsUserSubjectId("both"), RawlsUserEmail("both@fc.org"))
    val inDbUser = RawlsUser(RawlsUserSubjectId("db"), RawlsUserEmail("db@fc.org"))
    val unknownUserInGoogle = RawlsUser(RawlsUserSubjectId("unknown"), RawlsUserEmail("unknown@fc.org"))

    val topGroup = RawlsGroup(
      RawlsGroupName("synctest"),
      RawlsGroupEmail(services.gcsDAO.toGoogleGroupName(RawlsGroupName("synctest"))),
      Set[RawlsUserRef](inBothUser, inDbUser),
      Set[RawlsGroupRef](inBothGroup, inDbGroup))

    Await.result(services.gcsDAO.createGoogleGroup(topGroup), Duration.Inf)
    Await.result(services.gcsDAO.addMemberToGoogleGroup(topGroup, Right(inGoogleGroup)), Duration.Inf)
    Await.result(services.gcsDAO.addMemberToGoogleGroup(topGroup, Right(inBothGroup)), Duration.Inf)
    Await.result(services.gcsDAO.addMemberToGoogleGroup(topGroup, Left(inGoogleUser)), Duration.Inf)
    Await.result(services.gcsDAO.addMemberToGoogleGroup(topGroup, Left(inBothUser)), Duration.Inf)
    Await.result(services.gcsDAO.addMemberToGoogleGroup(topGroup, Left(unknownUserInGoogle)), Duration.Inf)

    runAndWait(rawlsUserQuery.createUser(inGoogleUser))
    runAndWait(rawlsUserQuery.createUser(inBothUser))
    runAndWait(rawlsUserQuery.createUser(inDbUser))

    runAndWait(rawlsGroupQuery.save(inGoogleGroup))
    runAndWait(rawlsGroupQuery.save(inBothGroup))
    runAndWait(rawlsGroupQuery.save(inDbGroup))

    runAndWait(rawlsGroupQuery.save(topGroup))

    Post(s"/admin/groups/${topGroup.groupName.value}/sync") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.OK) { status }

        val expected = Seq(
          SyncReportItem("added", inDbUser.userEmail.value, None),
          SyncReportItem("added", inDbGroup.groupEmail.value, None),
          SyncReportItem("removed", inGoogleUser.userEmail.value, None),
          SyncReportItem("removed", inGoogleGroup.groupEmail.value, None),
          SyncReportItem("removed", unknownUserInGoogle.userEmail.value, None)
        )
        assertSameElements(expected, responseAs[SyncReport].items)
      }
  }

  it should "get the status of a workspace" in withTestDataApiServices { services =>
    val testUser = RawlsUser(RawlsUserSubjectId("123456789876543212345"), RawlsUserEmail("owner-access"))

    runAndWait(rawlsUserQuery.createUser(testUser))

    Get(s"/admin/validate/${testData.workspace.namespace}/${testData.workspace.name}?userSubjectId=${testUser.userSubjectId.value}") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
        val responseStatus = responseAs[WorkspaceStatus]
        assertResult(WorkspaceName(testData.workspace.namespace, testData.workspace.name)) {
          responseStatus.workspaceName
        }

        val expected = Map("GOOGLE_BUCKET_WRITE: aBucket" -> "USER_CAN_WRITE",
            "WORKSPACE_ACCESS_GROUP: owner@myNamespace@billing-project" -> "FOUND",
            "WORKSPACE_ACCESS_GROUP: myNamespace-myWorkspace-OWNER" -> "FOUND",
            "FIRECLOUD_USER_PROXY: aBucket" -> "NOT_FOUND",
            "WORKSPACE_USER_ACCESS_LEVEL" -> "PROJECT_OWNER",
            "GOOGLE_ACCESS_GROUP: GROUP_owner@myNamespace@billing-project@dev.firecloud.org" -> "FOUND",
            "GOOGLE_ACCESS_GROUP: myNamespace-myWorkspace-OWNER@example.com" -> "FOUND",
            "GOOGLE_ACCESS_GROUP: myNamespace-myWorkspace-WRITER@example.com" -> "FOUND",
            "GOOGLE_ACCESS_GROUP: myNamespace-myWorkspace-READER@example.com" -> "FOUND",
            "GOOGLE_BUCKET: aBucket" -> "FOUND",
            "GOOGLE_USER_ACCESS_LEVEL: GROUP_owner@myNamespace@billing-project@dev.firecloud.org" -> "FOUND",
            "FIRECLOUD_USER: 123456789876543212345" -> "FOUND",
            "WORKSPACE_ACCESS_GROUP: myNamespace-myWorkspace-WRITER" -> "FOUND",
            "WORKSPACE_ACCESS_GROUP: myNamespace-myWorkspace-READER" -> "FOUND",
            "WORKSPACE_INTERSECTION_GROUP: myNamespace-myWorkspace-READER" -> "FOUND",
            "WORKSPACE_INTERSECTION_GROUP: myNamespace-myWorkspace-WRITER" -> "FOUND",
            "WORKSPACE_INTERSECTION_GROUP: myNamespace-myWorkspace-OWNER" -> "FOUND",
            "WORKSPACE_INTERSECTION_GROUP: owner@myNamespace@billing-project" -> "FOUND",
            "GOOGLE_INTERSECTION_GROUP: GROUP_owner@myNamespace@billing-project@dev.firecloud.org" -> "FOUND",
            "GOOGLE_INTERSECTION_GROUP: myNamespace-myWorkspace-OWNER@example.com" -> "FOUND",
            "GOOGLE_INTERSECTION_GROUP: myNamespace-myWorkspace-WRITER@example.com" -> "FOUND",
            "GOOGLE_INTERSECTION_GROUP: myNamespace-myWorkspace-READER@example.com" -> "FOUND"
          )
        assertSameElements(expected, responseStatus.statuses)
      }
  }

  it should "return 200 when listing all workspaces" in withTestDataApiServices { services =>
    Get(s"/admin/workspaces") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.OK) { status }
        // TODO: why is this result returned out of order?
        sortAndAssertWorkspaceResult(testData.allWorkspaces) { responseAs[Seq[Workspace]] }
      }
  }

  it should "return 200 when getting workspaces by a string attribute" in withConstantTestDataApiServices { services =>
    Get(s"/admin/workspaces?attributeName=string&valueString=yep%2C%20it's%20a%20string") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.OK) { status }
        assertWorkspaceResult(Seq(constantData.workspace)) { responseAs[Seq[Workspace]] }
      }
  }

  it should "return 200 when getting workspaces by a numeric attribute" in withConstantTestDataApiServices { services =>
    Get(s"/admin/workspaces?attributeName=number&valueNumber=10") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.OK) { status }
        assertWorkspaceResult(Seq(constantData.workspace)) { responseAs[Seq[Workspace]] }
      }
  }

  it should "return 200 when getting workspaces by a boolean attribute" in withTestDataApiServices { services =>
    Get(s"/admin/workspaces?attributeName=library%3Apublished&valueBoolean=true") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.OK) { status }
        assertWorkspaceResult(Seq(testData.workspacePublished)) { responseAs[Seq[Workspace]] }
      }
  }

  it should "delete a workspace" in withTestDataApiServices { services =>
    Delete(s"/admin/workspaces/${testData.workspace.namespace}/${testData.workspace.name}") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.Accepted) {
          status
        }
      }
    assertResult(None) {
      runAndWait(workspaceQuery.findByName(testData.workspace.toWorkspaceName))
    }
  }

  it should "delete workspace groups when deleting a workspace" in withTestDataApiServices { services =>
    val workspaceGroupRefs = (testData.workspace.accessLevels.values.toSet ++ testData.workspace.authDomainACLs.values) - testData.workspace.accessLevels(ProjectOwner)
    workspaceGroupRefs foreach { case groupRef =>
      assertResult(Option(groupRef)) {
        runAndWait(rawlsGroupQuery.load(groupRef)) map RawlsGroup.toRef
      }
    }

    Delete(s"/admin/workspaces/${testData.workspace.namespace}/${testData.workspace.name}") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.Accepted) {
          status
        }
      }

    workspaceGroupRefs foreach { case groupRef =>
      assertResult(None) {
        runAndWait(rawlsGroupQuery.load(groupRef))
      }
    }

  }

  it should "return 200 when querying firecloud statistics with valid dates" in withTestDataApiServices { services =>
    Get("/admin/statistics?startDate=2010-10-10&endDate=2011-10-10") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
      }
  }

  it should "return 400 when querying firecloud statistics with invalid (equal) dates" in withTestDataApiServices { services =>
    Get("/admin/statistics?startDate=2010-10-10&endDate=2010-10-10") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.BadRequest) {
          status
        }
      }
  }

  it should "return 400 when querying firecloud statistics with invalid dates" in withTestDataApiServices { services =>
    Get("/admin/statistics?startDate=2011-10-10&endDate=2010-10-10") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.BadRequest) {
          status
        }
      }
  }

  it should "return 500 when querying firecloud statistics illformed dates" in withTestDataApiServices { services =>
    Get("/admin/statistics?startDate=foo&endDate=bar") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.InternalServerError) {
          status
        }
      }
  }

  it should "get queue status by user" in withConstantTestDataApiServices { services =>
    import driver.api._

    // Create a new test user and some new submissions
    val testUserEmail = "testUser"
    val testSubjectId = "0001"
    val testUserStatusCounts = Map(WorkflowStatuses.Submitted -> 1, WorkflowStatuses.Running -> 10, WorkflowStatuses.Aborting -> 100)
    withWorkspaceContext(constantData.workspace) { ctx =>
      val testUser = RawlsUser(UserInfo(RawlsUserEmail(testUserEmail), OAuth2BearerToken("token"), 123, RawlsUserSubjectId(testSubjectId)))
      runAndWait(rawlsUserQuery.createUser(testUser))
      val inputResolutionsList = Seq(SubmissionValidationValue(Option(
        AttributeValueList(Seq(AttributeString("elem1"), AttributeString("elem2"), AttributeString("elem3")))), Option("message3"), "test_input_name3"))
      testUserStatusCounts.flatMap { case (st, count) =>
        for (_ <- 0 until count) yield {
          createTestSubmission(constantData.workspace, constantData.methodConfig, constantData.sset1, testUser,
            Seq(constantData.sset1), Map(constantData.sset1 -> inputResolutionsList),
            Seq.empty, Map.empty, st)
        }
      }.foreach { sub =>
        runAndWait(submissionQuery.create(ctx, sub))
      }
    }

    Get("/admin/submissions/queueStatusByUser") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
        val workflowRecs = runAndWait(workflowQuery.result)
        val groupedWorkflowRecs = workflowRecs.groupBy(_.status)
          .filterKeys((WorkflowStatuses.queuedStatuses ++ WorkflowStatuses.runningStatuses).map(_.toString).contains)
          .mapValues(_.size)

        val testUserWorkflows = (testSubjectId -> testUserStatusCounts.map { case (k, v) => k.toString -> v })

        // userOwner workflow counts should be equal to all workflows in the system except for testUser's workflows.
        val userOwnerWorkflows = (constantData.userOwner.userSubjectId.value ->
          groupedWorkflowRecs.map { case (k, v) =>
            k -> (v - testUserStatusCounts.getOrElse(WorkflowStatuses.withName(k), 0))
          }.filter(_._2 > 0))

        val expectedResponse = WorkflowQueueStatusByUserResponse(
          groupedWorkflowRecs,
          Map(userOwnerWorkflows, testUserWorkflows),
          services.maxActiveWorkflowsTotal,
          services.maxActiveWorkflowsPerUser)
        assertResult(expectedResponse) {
          responseAs[WorkflowQueueStatusByUserResponse]
        }
      }
  }
}
