package org.broadinstitute.dsde.rawls.webservice

import java.util.UUID

import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.dataaccess.slick.{TestData, RawlsBillingProjectOperationRecord}
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
import spray.http._
import spray.json.DefaultJsonProtocol._

import scala.concurrent.duration.Duration
import scala.concurrent.{Future, Await, ExecutionContext}
import scala.util.{Failure, Try}

/**
 * Created by dvoet on 4/24/15.
 */
class UserApiServiceSpec extends ApiServiceSpec {
  case class TestApiService(dataSource: SlickDataSource, user: RawlsUser, gcsDAO: MockGoogleServicesDAO, gpsDAO: MockGooglePubSubDAO)(implicit val executionContext: ExecutionContext) extends ApiServices with MockUserInfoDirectives {
    override def userInfo =  UserInfo(user.userEmail.value, OAuth2BearerToken("token"), 0, user.userSubjectId.value)
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

    assert {
      !services.gcsDAO.containsProxyGroup(user)
    }
    assert {
      !services.directoryDAO.exists(user.userSubjectId)
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

    assert {
      services.gcsDAO.containsProxyGroup(user)
    }
    assert {
      services.directoryDAO.exists(user.userSubjectId)
    }
  }

  it should "create a DB user, user proxy group, ldap entry, and add them to all users group, and enable them" in withEmptyTestDatabase { dataSource: SlickDataSource =>
    withApiServices(dataSource) { services =>

      // values from MockUserInfoDirectives
      val user = RawlsUser(RawlsUserSubjectId("123456789876543212345"), RawlsUserEmail("owner-access"))

      assertUserMissing(services, user)

      Post("/user") ~>
        sealRoute(services.createUserRoute) ~>
        check {
          assertResult(StatusCodes.Created) {
            status
          }
        }

      assertUserExists(services, user)

      Get(s"/admin/user/${user.userSubjectId.value}") ~>
        sealRoute(services.adminRoutes) ~>
        check {
          assertResult(StatusCodes.OK) {
            status
          }
          assertResult(UserStatus(user, Map("google" -> true, "ldap" -> true, "allUsersGroup" -> true))) {
            responseAs[UserStatus]
          }
        }
    }
  }

  it should "fully create a user and grant them pending access to a workspace" in withMinimalTestDatabase { dataSource: SlickDataSource =>
    withApiServices(dataSource) { services =>

      // values from MockUserInfoDirectives
      val user = RawlsUser(RawlsUserSubjectId("123456789876543212345"), RawlsUserEmail("owner-access"))

      assertUserMissing(services, user)

      runAndWait(dataSource.dataAccess.workspaceQuery.saveInvite(java.util.UUID.fromString(minimalTestData.workspace.workspaceId), minimalTestData.userReader.userSubjectId.value, WorkspaceACLUpdate("owner-access", WorkspaceAccessLevels.Read, None)))
      runAndWait(dataSource.dataAccess.workspaceQuery.saveInvite(java.util.UUID.fromString(minimalTestData.workspace2.workspaceId), minimalTestData.userReader.userSubjectId.value, WorkspaceACLUpdate("owner-access", WorkspaceAccessLevels.Write, None)))

      Post("/user") ~>
        sealRoute(services.createUserRoute) ~>
        check {
          assertResult(StatusCodes.Created) {
            status
          }
        }

      assertUserExists(services, user)

      import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport.WorkspaceListResponseFormat

      Get(s"/workspaces") ~>
        sealRoute(services.workspaceRoutes) ~>
        check {
          assertResult(Some(WorkspaceAccessLevels.Read)) {
            responseAs[Array[WorkspaceListResponse]].find(r => r.workspace.toWorkspaceName == minimalTestData.workspace.toWorkspaceName).map(_.accessLevel)
          }
          assertResult(Some(WorkspaceAccessLevels.Write)) {
            responseAs[Array[WorkspaceListResponse]].find(r => r.workspace.toWorkspaceName == minimalTestData.workspace2.toWorkspaceName).map(_.accessLevel)
          }
        }

      val leftoverInvites = runAndWait(dataSource.dataAccess.workspaceQuery.findWorkspaceInvitesForUser(user.userEmail))
      assert(leftoverInvites.size == 0)

    }
  }

  it should "get a users own status" in withTestDataApiServices { services =>
    Get("/user") ~>
      sealRoute(services.getUserStatusRoute) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
        assertResult(UserStatus(testData.userOwner, Map("google" -> false, "ldap" -> false, "allUsersGroup" -> false))) {
          responseAs[UserStatus]
        }
      }
  }

  it should "list a user's billing projects" in withTestDataApiServices { services =>
      Get("/user/billing") ~>
        sealRoute(services.userRoutes) ~>
        check {
          assertResult(StatusCodes.OK) {
            status
          }

          assertResult(Set(RawlsBillingProjectMembership(testData.billingProject.projectName, ProjectRoles.Owner, CreationStatuses.Ready), RawlsBillingProjectMembership(testData.testProject1.projectName, ProjectRoles.User, CreationStatuses.Ready))) {
            import org.broadinstitute.dsde.rawls.model.UserAuthJsonSupport.RawlsBillingProjectMembershipFormat
            responseAs[Seq[RawlsBillingProjectMembership]].toSet
          }
        }
    }

  it should "create a billing project" in withEmptyTestDatabase { dataSource: SlickDataSource =>
    withApiServices(dataSource) { services =>

      // first add the project and user to the DB

      val billingUser = testData.userOwner
      val project1 = RawlsBillingProject(RawlsBillingProjectName("project1"), generateBillingGroups(RawlsBillingProjectName("project1"), Map.empty, Map.empty), "mockBucketUrl", CreationStatuses.Ready, None, None)

      runAndWait(rawlsUserQuery.save(billingUser))

      val createRequest = CreateRawlsBillingProjectFullRequest(project1.projectName, services.gcsDAO.accessibleBillingAccountName)

      import UserAuthJsonSupport.CreateRawlsBillingProjectFullRequestFormat

      Post(s"/billing", httpJson(createRequest)) ~>
        sealRoute(services.billingRoutes) ~>
        check {
          assertResult(StatusCodes.Created) {
            status
          }
        }
      Get("/user/billing") ~>
        sealRoute(services.userRoutes) ~>
        check {
          assertResult(StatusCodes.OK) {
            status
          }
          assertResult(Set(RawlsBillingProjectMembership(project1.projectName, ProjectRoles.Owner, CreationStatuses.Creating))) {
            import org.broadinstitute.dsde.rawls.model.UserAuthJsonSupport.RawlsBillingProjectMembershipFormat
            responseAs[Seq[RawlsBillingProjectMembership]].toSet
          }
        }

      assertResult(1) {
        runAndWait(rawlsBillingProjectQuery.loadOperationsForProjects(Seq(project1.projectName))).size
      }

      val billingProjectMonitor = new CreatingBillingProjectMonitor {
        override val datasource: SlickDataSource = services.dataSource
        override val projectTemplate: ProjectTemplate = ProjectTemplate(Map.empty, Seq("foo", "bar", "baz"))
        override val gcsDAO = new MockGoogleServicesDAO("foo")
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
          assertResult(Set(RawlsBillingProjectMembership(project1.projectName, ProjectRoles.Owner, CreationStatuses.Creating))) {
            import org.broadinstitute.dsde.rawls.model.UserAuthJsonSupport.RawlsBillingProjectMembershipFormat
            responseAs[Seq[RawlsBillingProjectMembership]].toSet
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
          assertResult(Set(RawlsBillingProjectMembership(project1.projectName, ProjectRoles.Owner, CreationStatuses.Ready))) {
            import org.broadinstitute.dsde.rawls.model.UserAuthJsonSupport.RawlsBillingProjectMembershipFormat
            responseAs[Seq[RawlsBillingProjectMembership]].toSet
          }
        }

    }
  }

  it should "handle errors creating a billing project" in withEmptyTestDatabase { dataSource: SlickDataSource =>
    withApiServices(dataSource) { services =>

      // first add the project and user to the DB

      val billingUser = testData.userOwner
      val project1 = RawlsBillingProject(RawlsBillingProjectName("project1"), generateBillingGroups(RawlsBillingProjectName("project1"), Map.empty, Map.empty), "mockBucketUrl", CreationStatuses.Ready, None, None)

      runAndWait(rawlsUserQuery.save(billingUser))

      val createRequest = CreateRawlsBillingProjectFullRequest(project1.projectName, services.gcsDAO.accessibleBillingAccountName)

      import UserAuthJsonSupport.CreateRawlsBillingProjectFullRequestFormat

      Post(s"/billing", httpJson(createRequest)) ~>
        sealRoute(services.billingRoutes) ~>
        check {
          assertResult(StatusCodes.Created) {
            status
          }
        }
      Get("/user/billing") ~>
        sealRoute(services.userRoutes) ~>
        check {
          assertResult(StatusCodes.OK) {
            status
          }
          assertResult(Set(RawlsBillingProjectMembership(project1.projectName, ProjectRoles.Owner, CreationStatuses.Creating))) {
            import org.broadinstitute.dsde.rawls.model.UserAuthJsonSupport.RawlsBillingProjectMembershipFormat
            responseAs[Seq[RawlsBillingProjectMembership]].toSet
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
            Future.successful(rawlsBillingProjectOperation.copy(done = true, errorMessage = Option("this failed")))
          }
        }
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
          assertResult(Set(RawlsBillingProjectMembership(project1.projectName, ProjectRoles.Owner, CreationStatuses.Error, Option("this failed")))) {
            import org.broadinstitute.dsde.rawls.model.UserAuthJsonSupport.RawlsBillingProjectMembershipFormat
            responseAs[Seq[RawlsBillingProjectMembership]].toSet
          }
        }
    }
  }

  it should "handle errors setting up a billing project" in withEmptyTestDatabase { dataSource: SlickDataSource =>
    withApiServices(dataSource) { services =>

      // first add the project and user to the DB

      val billingUser = testData.userOwner
      val project1 = RawlsBillingProject(RawlsBillingProjectName("project1"), generateBillingGroups(RawlsBillingProjectName("project1"), Map.empty, Map.empty), "mockBucketUrl", CreationStatuses.Ready, None, None)

      runAndWait(rawlsUserQuery.save(billingUser))

      val createRequest = CreateRawlsBillingProjectFullRequest(project1.projectName, services.gcsDAO.accessibleBillingAccountName)

      import UserAuthJsonSupport.CreateRawlsBillingProjectFullRequestFormat

      Post(s"/billing", httpJson(createRequest)) ~>
        sealRoute(services.billingRoutes) ~>
        check {
          assertResult(StatusCodes.Created) {
            status
          }
        }
      Get("/user/billing") ~>
        sealRoute(services.userRoutes) ~>
        check {
          assertResult(StatusCodes.OK) {
            status
          }
          assertResult(Set(RawlsBillingProjectMembership(project1.projectName, ProjectRoles.Owner, CreationStatuses.Creating))) {
            import org.broadinstitute.dsde.rawls.model.UserAuthJsonSupport.RawlsBillingProjectMembershipFormat
            responseAs[Seq[RawlsBillingProjectMembership]].toSet
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
          assertResult(Set(RawlsBillingProjectMembership(project1.projectName, ProjectRoles.Owner, CreationStatuses.Creating))) {
            import org.broadinstitute.dsde.rawls.model.UserAuthJsonSupport.RawlsBillingProjectMembershipFormat
            responseAs[Seq[RawlsBillingProjectMembership]].toSet
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
          assertResult(Set(RawlsBillingProjectMembership(project1.projectName, ProjectRoles.Owner, CreationStatuses.Error, Option(s"[Failure enabling api ${billingProjectMonitor.projectTemplate.services(1)}: this failed]")))) {
            import org.broadinstitute.dsde.rawls.model.UserAuthJsonSupport.RawlsBillingProjectMembershipFormat
            responseAs[Seq[RawlsBillingProjectMembership]].toSet
          }
        }

    }
  }

  it should "return 200 when adding a user to a billing project that the caller owns" in withTestDataApiServices { services =>
    val project1 = RawlsBillingProject(RawlsBillingProjectName("project1"), generateBillingGroups(RawlsBillingProjectName("project1"), Map.empty, Map.empty), "mockBucketUrl", CreationStatuses.Ready, None, None)
    val createRequest = CreateRawlsBillingProjectFullRequest(project1.projectName, services.gcsDAO.accessibleBillingAccountName)

    import UserAuthJsonSupport.CreateRawlsBillingProjectFullRequestFormat

    Post(s"/billing", httpJson(createRequest)) ~>
      sealRoute(services.billingRoutes) ~>
      check {
        assertResult(StatusCodes.Created) {
          status
        }
      }

    Await.result(services.gcsDAO.beginProjectSetup(project1, null, Map.empty), Duration.Inf)

    Put(s"/billing/${project1.projectName.value}/user/${testData.userWriter.userEmail.value}") ~>
      sealRoute(services.billingRoutes) ~>
      check {
        assertResult(StatusCodes.OK, response.entity.asString) {
          status
        }
        assert {
          val loadedProject = runAndWait(rawlsBillingProjectQuery.load(project1.projectName)).get
          loadedProject.groups(ProjectRoles.User).users.contains(testData.userWriter) && !loadedProject.groups(ProjectRoles.Owner).users.contains(testData.userWriter)
        }
      }
  }

  it should "return 200 when removing a user from a billing project that the caller owns" in withTestDataApiServices { services =>
    val project1 = RawlsBillingProject(RawlsBillingProjectName("project1"), generateBillingGroups(RawlsBillingProjectName("project1"), Map.empty, Map.empty), "mockBucketUrl", CreationStatuses.Ready, None, None)
    val createRequest = CreateRawlsBillingProjectFullRequest(project1.projectName, services.gcsDAO.accessibleBillingAccountName)

    import UserAuthJsonSupport.CreateRawlsBillingProjectFullRequestFormat

    Post(s"/billing", httpJson(createRequest)) ~>
      sealRoute(services.billingRoutes) ~>
      check {
        assertResult(StatusCodes.Created) {
          status
        }
      }

    Await.result(services.gcsDAO.beginProjectSetup(project1, null, Map.empty), Duration.Inf)

    Put(s"/billing/${project1.projectName.value}/user/${testData.userWriter.userEmail.value}") ~>
      sealRoute(services.billingRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
        assert {
          val loadedProject = runAndWait(rawlsBillingProjectQuery.load(project1.projectName)).get
          loadedProject.groups(ProjectRoles.User).users.contains(testData.userWriter) && !loadedProject.groups(ProjectRoles.Owner).users.contains(testData.userWriter)
        }
      }

    Delete(s"/billing/${project1.projectName.value}/user/${testData.userWriter.userEmail.value}") ~>
      sealRoute(services.billingRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
        assert {
          val loadedProject = runAndWait(rawlsBillingProjectQuery.load(project1.projectName)).get
          !loadedProject.groups(ProjectRoles.User).users.contains(testData.userWriter) && !loadedProject.groups(ProjectRoles.Owner).users.contains(testData.userWriter)
        }
      }
  }

  it should "return 403 when a non-owner tries to alter project permissions" in withTestDataApiServices { services =>
    val project1 = RawlsBillingProject(RawlsBillingProjectName("project1"), generateBillingGroups(RawlsBillingProjectName("project1"), Map.empty, Map.empty), "mockBucketUrl", CreationStatuses.Ready, None, None)
    val createRequest = CreateRawlsBillingProjectFullRequest(project1.projectName, services.gcsDAO.accessibleBillingAccountName)

    import UserAuthJsonSupport.CreateRawlsBillingProjectFullRequestFormat

    Post(s"/billing", httpJson(createRequest)) ~>
      sealRoute(services.billingRoutes) ~>
      check {
        assertResult(StatusCodes.Created) {
          status
        }
      }

    Await.result(services.gcsDAO.beginProjectSetup(project1, null, Map.empty), Duration.Inf)

    Delete(s"/admin/billing/${project1.projectName.value}/owner/${testData.userOwner.userEmail.value}") ~>
      sealRoute(services.adminRoutes) ~>
      check {
        assertResult(StatusCodes.OK, response.entity.asString) {
          status
        }
        assert {
          val loadedProject = runAndWait(rawlsBillingProjectQuery.load(project1.projectName)).get
          !loadedProject.groups(ProjectRoles.User).users.contains(testData.userOwner) && !loadedProject.groups(ProjectRoles.Owner).users.contains(testData.userOwner)
        }
      }

    Put(s"/billing/${project1.projectName.value}/user/${testData.userWriter.userEmail.value}") ~>
      sealRoute(services.billingRoutes) ~>
      check {
        assertResult(StatusCodes.Forbidden, response.entity.asString) {
          status
        }
        assert {
          val loadedProject = runAndWait(rawlsBillingProjectQuery.load(project1.projectName)).get
          !loadedProject.groups(ProjectRoles.User).users.contains(testData.userWriter) && !loadedProject.groups(ProjectRoles.Owner).users.contains(testData.userWriter)
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
    Get(s"/user/group/${group3.groupName.value}") ~>
      sealRoute(services.userRoutes) ~>
      check {
        assertResult(StatusCodes.OK) { status }
        assertResult(group3.toRawlsGroupShort) { responseAs[RawlsGroupShort] }
      }
    Get(s"/user/group/${group2.groupName.value}") ~>
      sealRoute(services.userRoutes) ~>
      check {
        assertResult(StatusCodes.OK) { status }
        assertResult(group2.toRawlsGroupShort) { responseAs[RawlsGroupShort] }
      }
    Get(s"/user/group/${group1.groupName.value}") ~>
      sealRoute(services.userRoutes) ~>
      check {
        assertResult(StatusCodes.OK) { status }
        assertResult(group1.toRawlsGroupShort) { responseAs[RawlsGroupShort] }
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
        assertResult(StatusCodes.NotFound) { status }
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
    assertResult(Set(RawlsUser.toRef(usersTestData.userOwner))) { testGroup.ownersGroup.users }
    assertResult(Set(RawlsGroup.toRef(testGroup.ownersGroup))) { testGroup.usersGroup.subGroups }
    assert(testGroup.ownersGroup.subGroups.isEmpty)
    assert(testGroup.usersGroup.users.isEmpty)
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
    assertResult(None) { runAndWait(rawlsGroupQuery.load(managedGroup.ownersGroup)) }
    assertResult(None) { runAndWait(rawlsGroupQuery.load(managedGroup.usersGroup)) }

    assert(!services.gcsDAO.googleGroups.contains(managedGroup.ownersGroup.groupEmail.value))
    assert(!services.gcsDAO.googleGroups.contains(managedGroup.usersGroup.groupEmail.value))
  }

  it should "409 deleting a group in use" in withUsersTestDataApiServices() { services =>
    val testGroupName = "testGroup"
    createManagedGroup(services, testGroupName)
    val managedGroup = runAndWait(managedGroupQuery.load(ManagedGroupRef(RawlsGroupName(testGroupName)))).get

    val otherGroupName = "othergroup"
    createManagedGroup(services, otherGroupName)
    val otherGroup = runAndWait(managedGroupQuery.load(ManagedGroupRef(RawlsGroupName(otherGroupName)))).get

    // update othergroup to reference the test group
    runAndWait(rawlsGroupQuery.save(otherGroup.usersGroup.copy(subGroups = Set(managedGroup.usersGroup))))

    Delete(s"/groups/$testGroupName") ~>
      sealRoute(services.userRoutes) ~>
      check {
        assertResult(StatusCodes.Conflict) {
          status
        }
      }

    assertResult(Some(managedGroup)) { runAndWait(managedGroupQuery.load(managedGroup)) }
    assertResult(Some(managedGroup.ownersGroup)) { runAndWait(rawlsGroupQuery.load(managedGroup.ownersGroup)) }
    assertResult(Some(managedGroup.usersGroup)) { runAndWait(rawlsGroupQuery.load(managedGroup.usersGroup)) }

    assert(services.gcsDAO.googleGroups.contains(managedGroup.ownersGroup.groupEmail.value))
    assert(services.gcsDAO.googleGroups.contains(managedGroup.usersGroup.groupEmail.value))
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
      removeUser(services, ownerOnlyGroupName, ManagedRoles.User, managedGroup.ownersGroup.groupEmail.value)
      addUser(services, ownerOnlyGroupName, ManagedRoles.Owner, usersTestData.userUser.userEmail.value)

      createManagedGroup(services, userOnlyGroupName)
      addUser(services, userOnlyGroupName, ManagedRoles.User, usersTestData.userUser.userEmail.value)

      createManagedGroup(services, bothGroupName)
      addUser(services, bothGroupName, ManagedRoles.Owner, usersTestData.userUser.userEmail.value)
    }

    withApiServices(dataSource, usersTestData.userUser) { services =>
      import org.broadinstitute.dsde.rawls.model.UserModelJsonSupport.ManagedGroupAccessFormat
      Get("/groups") ~>
        sealRoute(services.userRoutes) ~>
        check {
          assertResult(StatusCodes.OK) {
            status
          }
          responseAs[Seq[ManagedGroupAccess]] should contain theSameElementsAs Seq(
            ManagedGroupAccess(ManagedGroupRef(RawlsGroupName(ownerOnlyGroupName)), ManagedRoles.Owner),
            ManagedGroupAccess(ManagedGroupRef(RawlsGroupName(userOnlyGroupName)), ManagedRoles.User),
            ManagedGroupAccess(ManagedGroupRef(RawlsGroupName(bothGroupName)), ManagedRoles.Owner),
            ManagedGroupAccess(ManagedGroupRef(RawlsGroupName(bothGroupName)), ManagedRoles.User)
          )
        }
    }
  }

  Seq((Option(ManagedRoles.User), StatusCodes.Forbidden), (Option(ManagedRoles.Owner), StatusCodes.OK), (None, StatusCodes.NotFound)).foreach { case (roleOption, expectedStatus) =>
    it should s"${expectedStatus.toString} get a group as ${roleOption.map(_.toString).getOrElse("nobody")}" in withCustomTestDatabase(usersTestData) { dataSource: SlickDataSource =>
      import org.broadinstitute.dsde.rawls.model.UserModelJsonSupport.ManagedGroupWithMembersFormat

      val testGroupName = "testGroup"

      withApiServices(dataSource, usersTestData.userOwner) { services =>
        createManagedGroup(services, testGroupName)
        roleOption.foreach(role => addUser(services, testGroupName, role, usersTestData.userUser.userEmail.value))
      }
      withApiServices(dataSource, usersTestData.userUser) { services =>
        Get(s"/groups/$testGroupName") ~>
          sealRoute(services.userRoutes) ~>
          check {
            assertResult(expectedStatus) {
              status
            }
            if (status.isSuccess) {
              val managedGroup = runAndWait(managedGroupQuery.load(ManagedGroupRef(RawlsGroupName(testGroupName)))).get

              assertResult(ManagedGroupWithMembers(managedGroup.usersGroup.toRawlsGroupShort,
                managedGroup.ownersGroup.toRawlsGroupShort,
                Seq(managedGroup.ownersGroup.groupEmail.value),
                Seq(usersTestData.userOwner.userEmail.value, usersTestData.userUser.userEmail.value))) {

                responseAs[ManagedGroupWithMembers]
              }
            }
          }
      }
    }
  }

  Seq((Option(ManagedRoles.User), StatusCodes.Forbidden), (Option(ManagedRoles.Owner), StatusCodes.NoContent), (None, StatusCodes.NotFound)).foreach { case (roleOption, expectedStatus) =>
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
              case ManagedRoles.Owner => resultGroup.ownersGroup
              case ManagedRoles.User => resultGroup.usersGroup
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
              case ManagedRoles.Owner => resultGroup.ownersGroup
              case ManagedRoles.User => resultGroup.usersGroup
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
    addUser(services, testGroupName, ManagedRoles.User, usersTestData.userUser.userEmail.value)
    addUser(services, testGroupName, ManagedRoles.User, usersTestData.userUser.userEmail.value)
  }

  it should "200 removing a user that does not exists" in withUsersTestDataApiServices() { services =>
    val testGroupName = "testGroup"

    createManagedGroup(services, testGroupName)
    removeUser(services, testGroupName, ManagedRoles.User, usersTestData.userNoAccess.userEmail.value)

  }

  it should "400 remove self from owner" in withUsersTestDataApiServices(usersTestData.userOwner) { services =>
    val testGroupName = "testGroup"

    createManagedGroup(services, testGroupName)
    removeUser(services, testGroupName, ManagedRoles.Owner, usersTestData.userOwner.userEmail.value, StatusCodes.BadRequest)
  }

  def createManagedGroup(services: TestApiService, testGroupName: String, expectedStatusCode: StatusCode = StatusCodes.Created): Unit = {
    Post(s"/groups/$testGroupName") ~>
      sealRoute(services.userRoutes) ~>
      check {
        assertResult(expectedStatusCode, response.entity.asString) {
          status
        }
        if (status.isSuccess) {
          val usersGroupShort = RawlsGroupShort(RawlsGroupName(testGroupName), RawlsGroupEmail(services.gcsDAO.toGoogleGroupName(RawlsGroupName(testGroupName))))
          val ownersGroupShort = RawlsGroupShort(RawlsGroupName(testGroupName + "-owners"), RawlsGroupEmail(services.gcsDAO.toGoogleGroupName(RawlsGroupName(testGroupName + "-owners"))))
          import UserModelJsonSupport.ManagedGroupWithMembersFormat
          assertResult(ManagedGroupWithMembers(usersGroupShort, ownersGroupShort, Seq(ownersGroupShort.groupEmail.value), Seq(services.userInfo.userEmail))) {
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
    val userUser = RawlsUser(UserInfo("user", OAuth2BearerToken("token"), 123, "123456789876543212346"))
    val userNoAccess = RawlsUser(UserInfo("no-access", OAuth2BearerToken("token"), 123, "123456789876543212347"))

    override def save() = {
      DBIO.seq(
        rawlsUserQuery.save(userOwner),
        rawlsUserQuery.save(userUser),
        rawlsUserQuery.save(userNoAccess)
      )
    }
  }
}
