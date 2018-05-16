package org.broadinstitute.dsde.rawls.webservice

import java.util.UUID

import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.dataaccess.slick.{RawlsBillingProjectOperationRecord, TestData}
import org.broadinstitute.dsde.rawls.google.MockGooglePubSubDAO
import org.broadinstitute.dsde.rawls.model
import org.broadinstitute.dsde.rawls.model.ManagedRoles.ManagedRole
import org.broadinstitute.dsde.rawls.model.Notifications.{ActivationNotification, NotificationFormat}
import org.broadinstitute.dsde.rawls.model.UserJsonSupport._
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.monitor.CreatingBillingProjectMonitor
import org.broadinstitute.dsde.rawls.monitor.CreatingBillingProjectMonitor.CheckDone
import org.broadinstitute.dsde.rawls.openam.MockUserInfoDirectives
import org.broadinstitute.dsde.rawls.user.UserService
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import spray.json.DefaultJsonProtocol._
import akka.http.scaladsl.server.Route.{seal => sealRoute}
import akka.http.scaladsl.unmarshalling.Unmarshal

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Try}

/**
 * Created by dvoet on 4/24/15.
 */
class UserApiServiceSpec extends ApiServiceSpec {
  case class TestApiService(dataSource: SlickDataSource, user: RawlsUser, gcsDAO: MockGoogleServicesDAO, gpsDAO: MockGooglePubSubDAO)(implicit val executionContext: ExecutionContext) extends ApiServices with MockUserInfoDirectives {
    override def userInfo =  UserInfo(user.userEmail, OAuth2BearerToken("token"), 0, user.userSubjectId)
  }

  def withApiServices[T](dataSource: SlickDataSource, user: RawlsUser = RawlsUser(userInfo))(testCode: TestApiService => T): T = {
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

  def userFromId(subjectId: String) =
    RawlsUser(RawlsUserSubjectId(subjectId), RawlsUserEmail("dummy@example.com"))

  def loadUser(user: RawlsUser) = runAndWait(rawlsUserQuery.load(user))


  "UserApi" should "put token and get date" in withTestDataApiServices { services =>
    Put("/user/refreshToken", httpJson(UserRefreshToken("gobblegobble"))) ~>
      sealRoute(services.userRoutes) ~>
      check { assertResult(StatusCodes.Created) {status} }

    Get("/user/refreshTokenDate") ~>
      sealRoute(services.userRoutes) ~>
      check { assertResult(StatusCodes.OK) {status} }
  }

  it should "get 404 when token is not set" in withTestDataApiServices { services =>
    Get("/user/refreshTokenDate") ~>
      sealRoute(services.userRoutes) ~>
      check { assertResult(StatusCodes.NotFound) {status} }
  }

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
    assert {
      val group = runAndWait(rawlsGroupQuery.load(UserService.allUsersGroupRef))
      group.isDefined && group.get.users.contains(user)
    }
  }

  it should "fully create a user and grant them pending access to a workspace" in withMinimalTestDatabase { dataSource: SlickDataSource =>
    withApiServices(dataSource) { services =>
      runAndWait(dataSource.dataAccess.workspaceQuery.saveInvite(java.util.UUID.fromString(minimalTestData.workspace.workspaceId), testData.userReader.userSubjectId.value, WorkspaceACLUpdate(testData.userOwner.userEmail.value, WorkspaceAccessLevels.Read, None)))
      runAndWait(dataSource.dataAccess.workspaceQuery.saveInvite(java.util.UUID.fromString(minimalTestData.workspace2.workspaceId), testData.userReader.userSubjectId.value, WorkspaceACLUpdate(testData.userOwner.userEmail.value, WorkspaceAccessLevels.Write, None)))

      val startingInvites = runAndWait(services.dataSource.dataAccess.workspaceQuery.findWorkspaceInvitesForUser(testData.userOwner.userEmail))

      runAndWait(dataSource.dataAccess.rawlsUserQuery.createUser(testData.userOwner))

      withStatsD {
        Post("/user") ~> services.sealedInstrumentedRoutes ~>
          check {
            assertResult(StatusCodes.Created) {
              status
            }
          }

        import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport.WorkspaceListResponseFormat

        Get(s"/workspaces") ~> services.sealedInstrumentedRoutes ~>
          check {
            assertResult(Some(WorkspaceAccessLevels.Read)) {
              responseAs[Array[WorkspaceListResponse]].find(r => r.workspace.toWorkspaceName == minimalTestData.workspace.toWorkspaceName).map(_.accessLevel)
            }
            assertResult(Some(WorkspaceAccessLevels.Write)) {
              responseAs[Array[WorkspaceListResponse]].find(r => r.workspace.toWorkspaceName == minimalTestData.workspace2.toWorkspaceName).map(_.accessLevel)
            }
          }
      } { capturedMetrics =>
        val expected = expectedHttpRequestMetrics("post", "user", StatusCodes.Created.intValue, 1) ++
          expectedHttpRequestMetrics("get", "workspaces", StatusCodes.OK.intValue, 1)
        assertSubsetOf(expected, capturedMetrics)
      }

      val leftoverInvites = runAndWait(services.dataSource.dataAccess.workspaceQuery.findWorkspaceInvitesForUser(testData.userOwner.userEmail))
      assert(leftoverInvites.size == 0)

    }
  }

  it should "get a user's valid billing project membership" in withTestDataApiServices { services =>
    val membership = RawlsBillingProjectMembership(testData.billingProject.projectName, ProjectRoles.Owner, CreationStatuses.Ready)
    Get(s"/user/billing/${membership.projectName.value}") ~>
      sealRoute(services.userRoutes) ~>
      check {
        assertResult(StatusCodes.OK) { status }
        import org.broadinstitute.dsde.rawls.model.UserAuthJsonSupport.RawlsBillingProjectMembershipFormat
        assertResult(membership) { responseAs[RawlsBillingProjectMembership] }
      }
  }

  it should "fail to get an invalid billing project membership" in withTestDataApiServices { services =>
    Get("/user/billing/not-found-project-name") ~>
      sealRoute(services.userRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound) { status }
      }
  }

  it should "list a user's billing projects ordered a-z" in withTestDataApiServices { services =>
      Get("/user/billing") ~>
        sealRoute(services.userRoutes) ~>
        check {
          assertResult(StatusCodes.OK) {
            status
          }

          import org.broadinstitute.dsde.rawls.model.UserAuthJsonSupport.RawlsBillingProjectMembershipFormat
          assertResult(
            List(RawlsBillingProjectMembership(testData.testProject1.projectName, ProjectRoles.User, CreationStatuses.Ready),
              RawlsBillingProjectMembership(testData.billingProject.projectName, ProjectRoles.Owner, CreationStatuses.Ready))) {
            responseAs[List[RawlsBillingProjectMembership]]
          }

          assertResult(responseAs[List[RawlsBillingProjectMembership]].head)(RawlsBillingProjectMembership(testData.testProject1.projectName, ProjectRoles.User, CreationStatuses.Ready))
          assertResult(responseAs[List[RawlsBillingProjectMembership]].last)(RawlsBillingProjectMembership(testData.billingProject.projectName, ProjectRoles.Owner, CreationStatuses.Ready))
        }
    }

  private val project1Groups = generateBillingGroups(RawlsBillingProjectName("project1"), Map.empty, Map.empty)

  it should "create a billing project" in withEmptyTestDatabase { dataSource: SlickDataSource =>
    withApiServices(dataSource) { services =>

      // first add the project and user to the DB

      val billingUser = testData.userOwner
      val project1 = RawlsBillingProject(RawlsBillingProjectName("project1"), "mockBucketUrl", CreationStatuses.Ready, None, None)

      runAndWait(rawlsUserQuery.createUser(billingUser))

      val createRequest = CreateRawlsBillingProjectFullRequest(project1.projectName, services.gcsDAO.accessibleBillingAccountName)

      import UserAuthJsonSupport.CreateRawlsBillingProjectFullRequestFormat

      Post(s"/billing", httpJson(createRequest)) ~>
        sealRoute(services.billingRoutes) ~>
        check {
          assertResult(StatusCodes.Created) {
            status
          }
        }

      // need to manually create the owner group because the test sam dao does not actually talk to ldap
      Await.result(samDataSaver.savePolicyGroups(project1Groups.values.flatten, SamResourceTypeNames.billingProject.value, project1.projectName.value), Duration.Inf)

      Get("/user/billing") ~>
        sealRoute(services.userRoutes) ~>
        check {
          assertResult(StatusCodes.OK) {
            status
          }
          assertResult(List(RawlsBillingProjectMembership(project1.projectName, ProjectRoles.Owner, CreationStatuses.Creating))) {
            import org.broadinstitute.dsde.rawls.model.UserAuthJsonSupport.RawlsBillingProjectMembershipFormat
            responseAs[List[RawlsBillingProjectMembership]]
          }
        }

      assertResult(1) {
        runAndWait(rawlsBillingProjectQuery.loadOperationsForProjects(Seq(project1.projectName))).size
      }

      val billingProjectMonitor = new CreatingBillingProjectMonitor {
        override val datasource: SlickDataSource = services.dataSource
        override val projectTemplate: ProjectTemplate = ProjectTemplate(Map.empty, Seq("foo", "bar", "baz"))
        override val gcsDAO = new MockGoogleServicesDAO("foo")
        override val samDAO = new HttpSamDAO(mockServer.mockServerBaseUrl, gcsDAO.getBucketServiceAccountCredential)
      }

      assertResult(CheckDone(1)) { Await.result(billingProjectMonitor.checkCreatingProjects(), Duration.Inf) }

      assertResult(1) {
        runAndWait(rawlsBillingProjectQuery.loadOperationsForProjects(Seq(project1.projectName))).count(_.done)
      }

      assertResult(4) {
        runAndWait(rawlsBillingProjectQuery.loadOperationsForProjects(Seq(project1.projectName))).size
      }

      Get("/user/billing") ~>
        sealRoute(services.userRoutes) ~>
        check {
          assertResult(StatusCodes.OK) {
            status
          }
          assertResult(List(RawlsBillingProjectMembership(project1.projectName, ProjectRoles.Owner, CreationStatuses.Creating))) {
            import org.broadinstitute.dsde.rawls.model.UserAuthJsonSupport.RawlsBillingProjectMembershipFormat
            responseAs[List[RawlsBillingProjectMembership]]
          }
        }

      assertResult(CheckDone(0)) { Await.result(billingProjectMonitor.checkCreatingProjects(), Duration.Inf) }

      assertResult(4) {
        runAndWait(rawlsBillingProjectQuery.loadOperationsForProjects(Seq(project1.projectName))).count(_.done)
      }

      Get("/user/billing") ~>
        sealRoute(services.userRoutes) ~>
        check {
          assertResult(StatusCodes.OK) {
            status
          }
          assertResult(List(RawlsBillingProjectMembership(project1.projectName, ProjectRoles.Owner, CreationStatuses.Ready))) {
            import org.broadinstitute.dsde.rawls.model.UserAuthJsonSupport.RawlsBillingProjectMembershipFormat
            responseAs[List[RawlsBillingProjectMembership]]
          }
        }

    }
  }

  it should "handle operation errors creating a billing project" in {
    testWithPollingError { rawlsBillingProjectOperation =>
      Future.successful(rawlsBillingProjectOperation.copy(done = true, errorMessage = Option("this failed")))
    }
  }

  it should "handle polling errors creating a billing project" in {
    testWithPollingError { _ =>
      Future.failed(new RuntimeException("foo"))
    }
  }

  private def testWithPollingError(failureMode: RawlsBillingProjectOperationRecord => Future[RawlsBillingProjectOperationRecord]) = withEmptyTestDatabase { dataSource: SlickDataSource =>
    withApiServices(dataSource) { services =>

      // first add the project and user to the DB

      val billingUser = testData.userOwner
      val project1 = RawlsBillingProject(RawlsBillingProjectName("project1"), "mockBucketUrl", CreationStatuses.Ready, None, None)

      runAndWait(rawlsUserQuery.createUser(billingUser))

      val createRequest = CreateRawlsBillingProjectFullRequest(project1.projectName, services.gcsDAO.accessibleBillingAccountName)

      import UserAuthJsonSupport.CreateRawlsBillingProjectFullRequestFormat

      Post(s"/billing", httpJson(createRequest)) ~>
        sealRoute(services.billingRoutes) ~>
        check {
          assertResult(StatusCodes.Created) {
            status
          }
        }

      // need to manually create the owner group because the test sam dao does not actually talk to ldap
      Await.result(samDataSaver.savePolicyGroups(project1Groups.values.flatten, SamResourceTypeNames.billingProject.value, project1.projectName.value), Duration.Inf)

      Get("/user/billing") ~>
        sealRoute(services.userRoutes) ~>
        check {
          assertResult(StatusCodes.OK) {
            status
          }
          assertResult(List(RawlsBillingProjectMembership(project1.projectName, ProjectRoles.Owner, CreationStatuses.Creating))) {
            import org.broadinstitute.dsde.rawls.model.UserAuthJsonSupport.RawlsBillingProjectMembershipFormat
            responseAs[List[RawlsBillingProjectMembership]]
          }
        }

      assertResult(1) {
        runAndWait(rawlsBillingProjectQuery.loadOperationsForProjects(Seq(project1.projectName))).size
      }

      val billingProjectMonitor = new CreatingBillingProjectMonitor {
        override val datasource: SlickDataSource = services.dataSource
        override val projectTemplate: ProjectTemplate = ProjectTemplate(Map.empty, Seq("foo", "bar", "baz"))
        override val gcsDAO = new MockGoogleServicesDAO("foo") {
          override def pollOperation(rawlsBillingProjectOperation: RawlsBillingProjectOperationRecord): Future[RawlsBillingProjectOperationRecord] = failureMode(rawlsBillingProjectOperation)
        }
        override val samDAO = new HttpSamDAO(mockServer.mockServerBaseUrl, gcsDAO.getBucketServiceAccountCredential)
      }

      assertResult(CheckDone(0)) { Await.result(billingProjectMonitor.checkCreatingProjects(), Duration.Inf) }

      assertResult(1) {
        runAndWait(rawlsBillingProjectQuery.loadOperationsForProjects(Seq(project1.projectName))).count(_.done)
      }

      assertResult(1) {
        runAndWait(rawlsBillingProjectQuery.loadOperationsForProjects(Seq(project1.projectName))).size
      }

      Get("/user/billing") ~>
        sealRoute(services.userRoutes) ~>
        check {
          assertResult(StatusCodes.OK) {
            status
          }
          import org.broadinstitute.dsde.rawls.model.UserAuthJsonSupport.RawlsBillingProjectMembershipFormat
          val memberships = responseAs[Seq[RawlsBillingProjectMembership]]
          assert(memberships.forall(_.creationStatus == CreationStatuses.Error))
          assert(memberships.forall(_.message.isDefined))
        }
    }
  }

  it should "handle errors setting up a billing project" in withEmptyTestDatabase { dataSource: SlickDataSource =>
    withApiServices(dataSource) { services =>

      // first add the project and user to the DB

      val billingUser = testData.userOwner
      val project1 = RawlsBillingProject(RawlsBillingProjectName("project1"), "mockBucketUrl", CreationStatuses.Ready, None, None)

      runAndWait(rawlsUserQuery.createUser(billingUser))

      val createRequest = CreateRawlsBillingProjectFullRequest(project1.projectName, services.gcsDAO.accessibleBillingAccountName)

      import UserAuthJsonSupport.CreateRawlsBillingProjectFullRequestFormat

      Post(s"/billing", httpJson(createRequest)) ~>
        sealRoute(services.billingRoutes) ~>
        check {
          assertResult(StatusCodes.Created) {
            status
          }
        }

      // need to manually create the owner group because the test sam dao does not actually talk to ldap
      Await.result(samDataSaver.savePolicyGroups(project1Groups.values.flatten, SamResourceTypeNames.billingProject.value, project1.projectName.value), Duration.Inf)

      Get("/user/billing") ~>
        sealRoute(services.userRoutes) ~>
        check {
          assertResult(StatusCodes.OK) {
            status
          }
          assertResult(List(RawlsBillingProjectMembership(project1.projectName, ProjectRoles.Owner, CreationStatuses.Creating))) {
            import org.broadinstitute.dsde.rawls.model.UserAuthJsonSupport.RawlsBillingProjectMembershipFormat
            responseAs[List[RawlsBillingProjectMembership]]
          }
        }

      assertResult(1) {
        runAndWait(rawlsBillingProjectQuery.loadOperationsForProjects(Seq(project1.projectName))).size
      }

      val billingProjectMonitor = new CreatingBillingProjectMonitor {
        override val datasource: SlickDataSource = services.dataSource
        override val projectTemplate: ProjectTemplate = ProjectTemplate(Map.empty, Seq("foo", "bar", "baz"))
        override val gcsDAO = new MockGoogleServicesDAO("foo") {
          override def pollOperation(rawlsBillingProjectOperation: RawlsBillingProjectOperationRecord): Future[RawlsBillingProjectOperationRecord] = {
            if (rawlsBillingProjectOperation.operationName == projectTemplate.services(1)) {
              Future.successful(rawlsBillingProjectOperation.copy(done = true, errorMessage = Option("this failed")))
            } else {
              super.pollOperation(rawlsBillingProjectOperation)
            }
          }
        }
        override val samDAO = new HttpSamDAO(mockServer.mockServerBaseUrl, gcsDAO.getBucketServiceAccountCredential)
      }

      assertResult(CheckDone(1)) { Await.result(billingProjectMonitor.checkCreatingProjects(), Duration.Inf) }

      assertResult(1) {
        runAndWait(rawlsBillingProjectQuery.loadOperationsForProjects(Seq(project1.projectName))).count(_.done)
      }

      assertResult(4) {
        runAndWait(rawlsBillingProjectQuery.loadOperationsForProjects(Seq(project1.projectName))).size
      }

      Get("/user/billing") ~>
        sealRoute(services.userRoutes) ~>
        check {
          assertResult(StatusCodes.OK) {
            status
          }
          assertResult(List(RawlsBillingProjectMembership(project1.projectName, ProjectRoles.Owner, CreationStatuses.Creating))) {
            import org.broadinstitute.dsde.rawls.model.UserAuthJsonSupport.RawlsBillingProjectMembershipFormat
            responseAs[List[RawlsBillingProjectMembership]]
          }
        }

      assertResult(CheckDone(0)) { Await.result(billingProjectMonitor.checkCreatingProjects(), Duration.Inf) }

      assertResult(4) {
        runAndWait(rawlsBillingProjectQuery.loadOperationsForProjects(Seq(project1.projectName))).count(_.done)
      }

      Get("/user/billing") ~>
        sealRoute(services.userRoutes) ~>
        check {
          assertResult(StatusCodes.OK) {
            status
          }
          assertResult(List(RawlsBillingProjectMembership(project1.projectName, ProjectRoles.Owner, CreationStatuses.Error, Option(s"[Failure enabling api ${billingProjectMonitor.projectTemplate.services(1)}: this failed]")))) {
            import org.broadinstitute.dsde.rawls.model.UserAuthJsonSupport.RawlsBillingProjectMembershipFormat
            responseAs[List[RawlsBillingProjectMembership]]
          }
        }

    }
  }

  it should "return 200 when adding a user to a billing project that the caller owns" in withTestDataApiServices { services =>
    val project1 = RawlsBillingProject(RawlsBillingProjectName("project1"), "mockBucketUrl", CreationStatuses.Ready, None, None)
    val createRequest = CreateRawlsBillingProjectFullRequest(project1.projectName, services.gcsDAO.accessibleBillingAccountName)

    import UserAuthJsonSupport.CreateRawlsBillingProjectFullRequestFormat

    Post(s"/billing", httpJson(createRequest)) ~>
      sealRoute(services.billingRoutes) ~>
      check {
        assertResult(StatusCodes.Created) {
          status
        }
      }

    Await.result(samDataSaver.savePolicyGroups(project1Groups.values.flatten, SamResourceTypeNames.billingProject.value, project1.projectName.value), Duration.Inf)
    Await.result(services.gcsDAO.beginProjectSetup(project1, null), Duration.Inf)

    Put(s"/billing/${project1.projectName.value}/user/${testData.userWriter.userEmail.value}") ~>
      sealRoute(services.billingRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
      }
  }

  it should "return 403 when trying to add a user to a billing project that the caller does not own (but has access to)" in withTestDataApiServices { services =>
    Put(s"/billing/not_an_owner/user/${testData.userWriter.userEmail.value}") ~>
      sealRoute(services.billingRoutes) ~>
      check {
        assertResult(StatusCodes.Forbidden) {
          status
        }
      }
  }

  it should "return 200 when adding a group to a billing project that the caller owns" in withTestDataApiServices { services =>
    val project1 = RawlsBillingProject(RawlsBillingProjectName("project1"), "mockBucketUrl", CreationStatuses.Ready, None, None)
    val createRequest = CreateRawlsBillingProjectFullRequest(project1.projectName, services.gcsDAO.accessibleBillingAccountName)

    import UserAuthJsonSupport.CreateRawlsBillingProjectFullRequestFormat

    Post(s"/billing", httpJson(createRequest)) ~>
      sealRoute(services.billingRoutes) ~>
      check {
        assertResult(StatusCodes.Created) {
          status
        }
      }

    // need to manually create the owner group because the test sam dao does not actually talk to ldap
    Await.result(samDataSaver.savePolicyGroups(project1Groups.values.flatten, SamResourceTypeNames.billingProject.value, project1.projectName.value), Duration.Inf)

    Await.result(services.gcsDAO.beginProjectSetup(project1, null), Duration.Inf)

    Put(s"/billing/${project1.projectName.value}/user/${testData.dbGapAuthorizedUsersGroup.membersGroup.groupEmail.value}") ~>
      sealRoute(services.billingRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
      }
  }

  it should "return 200 when removing a user from a billing project that the caller owns" in withTestDataApiServices { services =>
    val project1 = RawlsBillingProject(RawlsBillingProjectName("project1"), "mockBucketUrl", CreationStatuses.Ready, None, None)
    val createRequest = CreateRawlsBillingProjectFullRequest(project1.projectName, services.gcsDAO.accessibleBillingAccountName)

    import UserAuthJsonSupport.CreateRawlsBillingProjectFullRequestFormat

    Post(s"/billing", httpJson(createRequest)) ~>
      sealRoute(services.billingRoutes) ~>
      check {
        assertResult(StatusCodes.Created) {
          status
        }
      }

    // need to manually create the owner group because the test sam dao does not actually talk to ldap
    Await.result(samDataSaver.savePolicyGroups(project1Groups.values.flatten, SamResourceTypeNames.billingProject.value, project1.projectName.value), Duration.Inf)

    Await.result(services.gcsDAO.beginProjectSetup(project1, null), Duration.Inf)

    Put(s"/billing/${project1.projectName.value}/user/${testData.userWriter.userEmail.value}") ~>
      sealRoute(services.billingRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
      }

    Delete(s"/billing/${project1.projectName.value}/user/${testData.userWriter.userEmail.value}") ~>
      sealRoute(services.billingRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
      }
  }

  it should "return 403 when a non-owner tries to alter project permissions" in withTestDataApiServices { services =>
    Put(s"/billing/no_access/user/${testData.userWriter.userEmail.value}") ~>
      sealRoute(services.billingRoutes) ~>
      check {
        assertResult(StatusCodes.Forbidden) {
          status
        }
      }
  }

  it should "get details of a group a user is a member of" in withTestDataApiServices { services =>
    val group3 = RawlsGroup(RawlsGroupName("testgroupname3"), RawlsGroupEmail("testgroupname3@foo.bar"), Set[RawlsUserRef](RawlsUser(userInfo)), Set.empty[RawlsGroupRef])
    val group2 = RawlsGroup(RawlsGroupName("testgroupname2"), RawlsGroupEmail("testgroupname2@foo.bar"), Set.empty[RawlsUserRef], Set[RawlsGroupRef](group3))
    val group1 = RawlsGroup(RawlsGroupName("testgroupname1"), RawlsGroupEmail("testgroupname1@foo.bar"), Set.empty[RawlsUserRef], Set[RawlsGroupRef](group2))

    runAndWait(rawlsGroupQuery.save(group3))
    runAndWait(rawlsGroupQuery.save(group2))
    runAndWait(rawlsGroupQuery.save(group1))

    import org.broadinstitute.dsde.rawls.model.UserModelJsonSupport._
    withStatsD {
      Get(s"/user/group/${group3.groupName.value}") ~> services.sealedInstrumentedRoutes ~>
        check {
          assertResult(StatusCodes.OK) {
            status
          }
          assertResult(group3.toRawlsGroupShort) {
            responseAs[RawlsGroupShort]
          }
        }
      Get(s"/user/group/${group2.groupName.value}") ~> services.sealedInstrumentedRoutes ~>
        check {
          assertResult(StatusCodes.OK) {
            status
          }
          assertResult(group2.toRawlsGroupShort) {
            responseAs[RawlsGroupShort]
          }
        }
      Get(s"/user/group/${group1.groupName.value}") ~> services.sealedInstrumentedRoutes ~>
        check {
          assertResult(StatusCodes.OK) {
            status
          }
          assertResult(group1.toRawlsGroupShort) {
            responseAs[RawlsGroupShort]
          }
        }
    } { capturedMetrics =>
      val expected = Set(group1, group2, group3) flatMap { g => expectedHttpRequestMetrics("get", s"user.group.redacted", StatusCodes.OK.intValue, 3) }
      assertSubsetOf(expected, capturedMetrics)
    }
  }

  it should "not get details of a group a user is not a member of" in withTestDataApiServices { services =>
    val group3 = RawlsGroup(RawlsGroupName("testgroupname3"), RawlsGroupEmail("testgroupname3@foo.bar"), Set[RawlsUserRef](RawlsUser(userInfo)), Set.empty[RawlsGroupRef])
    val group2 = RawlsGroup(RawlsGroupName("testgroupname2"), RawlsGroupEmail("testgroupname2@foo.bar"), Set.empty[RawlsUserRef], Set.empty[RawlsGroupRef])
    val group1 = RawlsGroup(RawlsGroupName("testgroupname1"), RawlsGroupEmail("testgroupname1@foo.bar"), Set.empty[RawlsUserRef], Set[RawlsGroupRef](group2))

    runAndWait(rawlsGroupQuery.save(group3))
    runAndWait(rawlsGroupQuery.save(group2))
    runAndWait(rawlsGroupQuery.save(group1))

    import org.broadinstitute.dsde.rawls.model.UserModelJsonSupport._
    Get(s"/user/group/${group3.groupName.value}") ~>
      sealRoute(services.userRoutes) ~>
      check {
        assertResult(StatusCodes.OK) { status }
        assertResult(group3.toRawlsGroupShort) { responseAs[RawlsGroupShort] }
      }
    Get(s"/user/group/${group2.groupName.value}") ~>
      sealRoute(services.userRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound, Await.result(Unmarshal(responseEntity).to[String], Duration.Inf)) { status }
      }
    Get(s"/user/group/${group1.groupName.value}") ~>
      sealRoute(services.userRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound) { status }
      }
  }

  it should "return groups for a user" in withTestDataApiServices { services =>
    val group3 = RawlsGroup(RawlsGroupName("testgroupname3"), RawlsGroupEmail("testgroupname3@foo.bar"), Set[RawlsUserRef](RawlsUser(userInfo)), Set.empty[RawlsGroupRef])
    val group2 = RawlsGroup(RawlsGroupName("testgroupname2"), RawlsGroupEmail("testgroupname2@foo.bar"), Set.empty[RawlsUserRef], Set.empty[RawlsGroupRef])
    val group1 = RawlsGroup(RawlsGroupName("testgroupname1"), RawlsGroupEmail("testgroupname1@foo.bar"), Set.empty[RawlsUserRef], Set[RawlsGroupRef](group3))

    runAndWait(rawlsGroupQuery.save(group3))
    runAndWait(rawlsGroupQuery.save(group2))
    runAndWait(rawlsGroupQuery.save(group1))

    import org.broadinstitute.dsde.rawls.model.UserModelJsonSupport._
    Get("/user/groups") ~>
      sealRoute(services.userRoutes) ~>
      check {
        assertResult(StatusCodes.OK) { status }
        val expected = Seq(group3.groupName.value, group1.groupName.value)
        assertResult(expected) { responseAs[Seq[String]].intersect(expected) }
      }
  }

  it should "get not details of a group that does not exist" in withTestDataApiServices { services =>
    Get("/user/group/blarg") ~>
      sealRoute(services.userRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
      }
  }

  it should "return OK for a user who is an admin" in withTestDataApiServices { services =>
    Get("/user/role/admin") ~>
      sealRoute(services.userRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
      }
  }

  it should "return Not Found for a user who is not an admin" in withTestDataApiServices { services =>
    assertResult(()) {Await.result(services.gcsDAO.removeAdmin(services.user.userEmail.value), Duration.Inf)}
    Get("/user/role/admin") ~>
      sealRoute(services.userRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
      }
  }

  it should "return OK for a user who is a curator" in withTestDataApiServices { services =>
    Get("/user/role/curator") ~>
      sealRoute(services.userRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
      }
  }

  it should "return Not Found for a user who is not a curator" in withTestDataApiServices { services =>
    Delete(s"/admin/user/role/curator/owner-access") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.OK) { status }
      }
    Get("/user/role/curator") ~>
      sealRoute(services.userRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
      }
  }



  it should "201 create a group" in withUsersTestDataApiServices() { services =>
    val testGroupName = "testGroup"
    createManagedGroup(services, testGroupName)

    val testGroupRef = ManagedGroupRef(RawlsGroupName(testGroupName))

    val testGroup = runAndWait(managedGroupQuery.load(testGroupRef)).getOrElse(fail("group not found"))
    assertResult(Set(RawlsUser.toRef(usersTestData.userOwner))) { testGroup.adminsGroup.users }
    assertResult(Set(RawlsGroup.toRef(testGroup.adminsGroup))) { testGroup.membersGroup.subGroups }
    assert(testGroup.adminsGroup.subGroups.isEmpty)
    assert(testGroup.membersGroup.users.isEmpty)
  }

  it should "400 when creating a group with a name longer than 50 characters" in withUsersTestDataApiServices() { services =>
    val testGroupName = "111111111122222222223333333333444444444455555555556666666666" //60 characters

    Post(s"/groups/$testGroupName") ~>
      sealRoute(services.userRoutes) ~>
      check {
        assertResult(StatusCodes.BadRequest) {
          status
        }
      }
  }

  it should "400 when creating a group with invalid characters in the name" in withUsersTestDataApiServices() { services =>
    val testGroupName = "(*#&$(*&#$@"

    Post(s"/groups/$testGroupName") ~>
      sealRoute(services.userRoutes) ~>
      check {
        assertResult(StatusCodes.BadRequest) {
          status
        }
      }
  }

  it should "409 creating an existing group" in withUsersTestDataApiServices() { services =>
    val testGroupName = "testGroup"
    createManagedGroup(services, testGroupName)
    createManagedGroup(services, testGroupName, StatusCodes.Conflict)
  }

  it should "204 delete group" in withUsersTestDataApiServices() { services =>
    val testGroupName = "testGroup"
    createManagedGroup(services, testGroupName)
    val managedGroup = runAndWait(managedGroupQuery.load(ManagedGroupRef(RawlsGroupName(testGroupName)))).get

    Delete(s"/groups/$testGroupName") ~>
      sealRoute(services.userRoutes) ~>
      check {
        assertResult(StatusCodes.NoContent) {
          status
        }
      }

    assertResult(None) { runAndWait(managedGroupQuery.load(managedGroup)) }
    assertResult(None) { runAndWait(rawlsGroupQuery.load(managedGroup.adminsGroup)) }
    assertResult(None) { runAndWait(rawlsGroupQuery.load(managedGroup.membersGroup)) }

    assert(!services.gcsDAO.googleGroups.contains(managedGroup.adminsGroup.groupEmail.value))
    assert(!services.gcsDAO.googleGroups.contains(managedGroup.membersGroup.groupEmail.value))
  }

  it should "409 deleting a group in use" in withUsersTestDataApiServices() { services =>
    val testGroupName = "testGroup"
    createManagedGroup(services, testGroupName)
    val managedGroup = runAndWait(managedGroupQuery.load(ManagedGroupRef(RawlsGroupName(testGroupName)))).get

    val otherGroupName = "othergroup"
    createManagedGroup(services, otherGroupName)
    val otherGroup = runAndWait(managedGroupQuery.load(ManagedGroupRef(RawlsGroupName(otherGroupName)))).get

    // update othergroup to reference the test group
    runAndWait(rawlsGroupQuery.save(otherGroup.membersGroup.copy(subGroups = Set(managedGroup.membersGroup))))

    Delete(s"/groups/$testGroupName") ~>
      sealRoute(services.userRoutes) ~>
      check {
        assertResult(StatusCodes.Conflict) {
          status
        }
      }

    assertResult(Some(managedGroup)) { runAndWait(managedGroupQuery.load(managedGroup)) }
    assertResult(Some(managedGroup.adminsGroup)) { runAndWait(rawlsGroupQuery.load(managedGroup.adminsGroup)) }
    assertResult(Some(managedGroup.membersGroup)) { runAndWait(rawlsGroupQuery.load(managedGroup.membersGroup)) }

    assert(services.gcsDAO.googleGroups.contains(managedGroup.adminsGroup.groupEmail.value))
    assert(services.gcsDAO.googleGroups.contains(managedGroup.membersGroup.groupEmail.value))
  }

  it should "allow users to request access to groups" in withUsersTestDataApiServices(testData.userReader) { services =>
    Post(s"/groups/my-test-group", httpJsonEmpty) ~>
      sealRoute(services.userRoutes) ~>
      check {
        assertResult(StatusCodes.Created){
          status
        }
      }
    Post(s"/groups/my-test-group/requestAccess", httpJsonEmpty) ~>
      sealRoute(services.userRoutes) ~>
      check {
        assertResult(StatusCodes.NoContent){
          status
        }
      }
  }

  it should "200 list groups for user - no groups" in withUsersTestDataApiServices(usersTestData.userNoAccess) { services =>
    import org.broadinstitute.dsde.rawls.model.UserModelJsonSupport.ManagedGroupAccessFormat
    Get("/groups") ~>
      sealRoute(services.userRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
        assertResult(Seq.empty) {
          responseAs[Seq[ManagedGroupAccess]]
        }
      }
  }

  it should "200 list groups for user - some user, some owner, some both" in withCustomTestDatabase(usersTestData) { dataSource: SlickDataSource =>
    val ownerOnlyGroupName = "owner-only"
    val userOnlyGroupName = "user-only"
    val bothGroupName = "both"

    withApiServices(dataSource, usersTestData.userOwner) { services =>
      createManagedGroup(services, ownerOnlyGroupName)
      val managedGroup = runAndWait(managedGroupQuery.load(ManagedGroupRef(RawlsGroupName(ownerOnlyGroupName)))).get
      // owners automatically added as users - undo that for this test
      removeUser(services, ownerOnlyGroupName, ManagedRoles.Member, managedGroup.adminsGroup.groupEmail.value)
      addUser(services, ownerOnlyGroupName, ManagedRoles.Admin, usersTestData.userUser.userEmail.value)

      createManagedGroup(services, userOnlyGroupName)
      addUser(services, userOnlyGroupName, ManagedRoles.Member, usersTestData.userUser.userEmail.value)

      createManagedGroup(services, bothGroupName)
      addUser(services, bothGroupName, ManagedRoles.Admin, usersTestData.userUser.userEmail.value)
    }

    withApiServices(dataSource, usersTestData.userUser) { services =>
      import org.broadinstitute.dsde.rawls.model.UserModelJsonSupport.ManagedGroupAccessResponseFormat
      Get("/groups") ~>
        sealRoute(services.userRoutes) ~>
        check {
          assertResult(StatusCodes.OK) {
            status
          }
          responseAs[Seq[ManagedGroupAccessResponse]] should contain theSameElementsAs Seq(
            ManagedGroupAccessResponse(RawlsGroupName(ownerOnlyGroupName), RawlsGroupEmail(services.gcsDAO.toGoogleGroupName(RawlsGroupName(ownerOnlyGroupName))), ManagedRoles.Admin),
            ManagedGroupAccessResponse(RawlsGroupName(userOnlyGroupName), RawlsGroupEmail(services.gcsDAO.toGoogleGroupName(RawlsGroupName(userOnlyGroupName))), ManagedRoles.Member),
            ManagedGroupAccessResponse(RawlsGroupName(bothGroupName), RawlsGroupEmail(services.gcsDAO.toGoogleGroupName(RawlsGroupName(bothGroupName))), ManagedRoles.Admin)
          )
        }
    }
  }

  Seq((Option(ManagedRoles.Member), StatusCodes.Forbidden), (Option(ManagedRoles.Admin), StatusCodes.OK), (None, StatusCodes.NotFound)).foreach { case (roleOption, expectedStatus) =>
    it should s"${expectedStatus.toString} get a group as ${roleOption.map(_.toString).getOrElse("nobody")}" in withCustomTestDatabase(usersTestData) { dataSource: SlickDataSource =>
      import org.broadinstitute.dsde.rawls.model.UserModelJsonSupport.ManagedGroupWithMembersFormat

      val testGroupName = "testGroup"

      withApiServices(dataSource, usersTestData.userOwner) { services =>
        createManagedGroup(services, testGroupName)
        roleOption.foreach(role => addUser(services, testGroupName, role, usersTestData.userUser.userEmail.value))
      }
      withApiServices(dataSource, usersTestData.userUser) { services =>
        withStatsD {
          Get(s"/groups/$testGroupName") ~> services.sealedInstrumentedRoutes ~>
            check {
              assertResult(expectedStatus) {
                status
              }
              if (status.isSuccess) {
                val managedGroup = runAndWait(managedGroupQuery.load(ManagedGroupRef(RawlsGroupName(testGroupName)))).get

                assertResult(ManagedGroupWithMembers(managedGroup.membersGroup.toRawlsGroupShort,
                  managedGroup.adminsGroup.toRawlsGroupShort,
                  Seq.empty,
                  Seq(usersTestData.userOwner.userEmail.value, usersTestData.userUser.userEmail.value))) {

                  responseAs[ManagedGroupWithMembers]
                }
              }
            }
        } { capturedMetrics =>
          val expected = expectedHttpRequestMetrics("get", "groups.redacted", expectedStatus.intValue, 1)
          assertSubsetOf(expected, capturedMetrics)
        }
      }
    }
  }

  Seq((Option(ManagedRoles.Member), StatusCodes.Forbidden), (Option(ManagedRoles.Admin), StatusCodes.NoContent), (None, StatusCodes.NotFound)).foreach { case (roleOption, expectedStatus) =>
    ManagedRoles.all.foreach { roleToAddRemove =>
      it should s"${expectedStatus.toString} add ${roleToAddRemove} to group as ${roleOption.map(_.toString).getOrElse("nobody")}" in withCustomTestDatabase(usersTestData) { dataSource: SlickDataSource =>
        val testGroupName = "testGroup"

        withApiServices(dataSource, usersTestData.userOwner) { services =>
          createManagedGroup(services, testGroupName)
          roleOption.foreach(role => addUser(services, testGroupName, role, usersTestData.userUser.userEmail.value))
        }
        withApiServices(dataSource, usersTestData.userUser) { services =>
          addUser(services, testGroupName, roleToAddRemove, usersTestData.userNoAccess.userEmail.value, expectedStatus)
          val resultGroup = runAndWait(managedGroupQuery.load(ManagedGroupRef(RawlsGroupName(testGroupName)))).get
          assertResult(expectedStatus.isSuccess) {
            val group = roleToAddRemove match {
              case ManagedRoles.Admin => resultGroup.adminsGroup
              case ManagedRoles.Member => resultGroup.membersGroup
            }
            group.users.contains(RawlsUser.toRef(usersTestData.userNoAccess))
          }
        }
      }

      it should s"${expectedStatus.toString} remove ${roleToAddRemove} from group as ${roleOption.map(_.toString).getOrElse("nobody")}" in withCustomTestDatabase(usersTestData) { dataSource: SlickDataSource =>
        val testGroupName = "testGroup"

        withApiServices(dataSource, usersTestData.userOwner) { services =>
          createManagedGroup(services, testGroupName)
          addUser(services, testGroupName, roleToAddRemove, usersTestData.userNoAccess.userEmail.value)
          roleOption.foreach(role => addUser(services, testGroupName, role, usersTestData.userUser.userEmail.value))
        }
        withApiServices(dataSource, usersTestData.userUser) { services =>
          removeUser(services, testGroupName, roleToAddRemove, usersTestData.userNoAccess.userEmail.value, expectedStatus)
          val resultGroup = runAndWait(managedGroupQuery.load(ManagedGroupRef(RawlsGroupName(testGroupName)))).get
          assertResult(expectedStatus.isSuccess) {
            val group = roleToAddRemove match {
              case ManagedRoles.Admin => resultGroup.adminsGroup
              case ManagedRoles.Member => resultGroup.membersGroup
            }
            !group.users.contains(RawlsUser.toRef(usersTestData.userNoAccess))
          }
        }
      }
    }
  }

  it should "200 adding a user that already exists" in withUsersTestDataApiServices() { services =>
    val testGroupName = "testGroup"

    createManagedGroup(services, testGroupName)
    addUser(services, testGroupName, ManagedRoles.Member, usersTestData.userUser.userEmail.value)
    addUser(services, testGroupName, ManagedRoles.Member, usersTestData.userUser.userEmail.value)
  }

  it should "200 removing a user that does not exists" in withUsersTestDataApiServices() { services =>
    val testGroupName = "testGroup"

    createManagedGroup(services, testGroupName)
    removeUser(services, testGroupName, ManagedRoles.Member, usersTestData.userNoAccess.userEmail.value)

  }

  it should "400 remove self from owner" in withUsersTestDataApiServices(usersTestData.userOwner) { services =>
    val testGroupName = "testGroup"

    createManagedGroup(services, testGroupName)
    removeUser(services, testGroupName, ManagedRoles.Admin, usersTestData.userOwner.userEmail.value, StatusCodes.BadRequest)
  }

  def createManagedGroup(services: TestApiService, testGroupName: String, expectedStatusCode: StatusCode = StatusCodes.Created): Unit = {
    Post(s"/groups/$testGroupName") ~>
      sealRoute(services.userRoutes) ~>
      check {
        assertResult(expectedStatusCode) {
          status
        }
        if (status.isSuccess) {
          val usersGroupShort = RawlsGroupShort(RawlsGroupName(testGroupName), RawlsGroupEmail(services.gcsDAO.toGoogleGroupName(RawlsGroupName(testGroupName))))
          val ownersGroupShort = RawlsGroupShort(RawlsGroupName(testGroupName + "-owners"), RawlsGroupEmail(services.gcsDAO.toGoogleGroupName(RawlsGroupName(testGroupName + "-owners"))))
          import UserModelJsonSupport.ManagedGroupWithMembersFormat
          assertResult(ManagedGroupWithMembers(usersGroupShort, ownersGroupShort, Seq(ownersGroupShort.groupEmail.value), Seq(services.userInfo.userEmail.value))) {
            responseAs[ManagedGroupWithMembers]
          }
        }
      }
  }

  def addUser(services: TestApiService, group: String, role: ManagedRole, email: String, expectedStatusCode: StatusCode = StatusCodes.NoContent): Unit = {
    Put(s"/groups/$group/${role.toString}/$email") ~>
      sealRoute(services.userRoutes) ~>
      check {
        assertResult(expectedStatusCode) {
          status
        }
      }
  }

  def removeUser(services: TestApiService, group: String, role: ManagedRole, email: String, expectedStatusCode: StatusCode = StatusCodes.NoContent): Unit = {
    Delete(s"/groups/$group/${role.toString}/$email") ~>
      sealRoute(services.userRoutes) ~>
      check {
        assertResult(expectedStatusCode) {
          status
        }
      }
  }

  def withUsersTestDataApiServices[T](user: RawlsUser = usersTestData.userOwner)(testCode: TestApiService => T): T = {
    withCustomTestDatabase(usersTestData) { dataSource: SlickDataSource =>
      withApiServices(dataSource, user)(testCode)
    }
  }

  val usersTestData = new TestData {
    import driver.api._

    val userOwner = RawlsUser(userInfo)
    val userUser = RawlsUser(UserInfo(RawlsUserEmail("user"), OAuth2BearerToken("token"), 123, RawlsUserSubjectId("123456789876543212346")))
    val userNoAccess = RawlsUser(UserInfo(RawlsUserEmail("no-access"), OAuth2BearerToken("token"), 123, RawlsUserSubjectId("123456789876543212347")))

    override def save() = {
      DBIO.seq(
        rawlsUserQuery.createUser(userOwner),
        rawlsUserQuery.createUser(userUser),
        rawlsUserQuery.createUser(userNoAccess)
      )
    }
  }
}
