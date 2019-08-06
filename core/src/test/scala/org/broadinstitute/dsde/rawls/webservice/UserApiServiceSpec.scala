package org.broadinstitute.dsde.rawls.webservice

import java.util.UUID

import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.dataaccess.slick.{RawlsBillingProjectOperationRecord, TestData}
import org.broadinstitute.dsde.rawls.google.{GooglePubSubDAO, MockGooglePubSubDAO}
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
import com.typesafe.config.{Config, ConfigFactory}
import org.broadinstitute.dsde.rawls.mock.MockSamDAO
import org.broadinstitute.dsde.workbench.model.WorkbenchProjectLocation

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

  val testConf = ConfigFactory.load()

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

  it should "get a valid billing project status" in withTestDataApiServices { services =>
    val projectStatus = RawlsBillingProjectStatus(testData.billingProject.projectName, CreationStatuses.Ready)
    Get(s"/user/billing/${projectStatus.projectName.value}") ~>
      sealRoute(services.userRoutes) ~>
      check {
        assertResult(StatusCodes.OK) { status }
        import org.broadinstitute.dsde.rawls.model.UserAuthJsonSupport.RawlsBillingProjectStatusFormat
        assertResult(projectStatus) { responseAs[RawlsBillingProjectStatus] }
      }
  }

  it should "fail to get an invalid billing project status" in withTestDataApiServices { services =>
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
          responseAs[List[RawlsBillingProjectMembership]] should contain theSameElementsInOrderAs List(
              RawlsBillingProjectMembership(testData.testProject1.projectName, ProjectRoles.Owner, CreationStatuses.Ready, Some(WorkbenchProjectLocation.US.name)),
              RawlsBillingProjectMembership(testData.billingProject.projectName, ProjectRoles.Owner, CreationStatuses.Ready, Some(WorkbenchProjectLocation.US.name)),
              RawlsBillingProjectMembership(testData.testProject2.projectName, ProjectRoles.Owner, CreationStatuses.Ready, Some(WorkbenchProjectLocation.US.name)),
              RawlsBillingProjectMembership(testData.testProject3.projectName, ProjectRoles.Owner, CreationStatuses.Ready, Some(WorkbenchProjectLocation.US.name)))
        }
    }

  it should "create a billing project" in withEmptyTestDatabase { dataSource: SlickDataSource =>
    withApiServices(dataSource) { services =>

      // first add the project and user to the DB

      val billingUser = testData.userOwner
      val project1 = RawlsBillingProject(RawlsBillingProjectName("project1"), "mockBucketUrl", CreationStatuses.Ready, None, Some(WorkbenchProjectLocation.US), None)

      val createRequest = CreateRawlsBillingProjectFullRequest(project1.projectName, services.gcsDAO.accessibleBillingAccountName, WorkbenchProjectLocation.US.name, None, None, None)

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
          assertResult(List(RawlsBillingProjectMembership(project1.projectName, ProjectRoles.Owner, CreationStatuses.Creating, Some(WorkbenchProjectLocation.US.name)))) {
            import org.broadinstitute.dsde.rawls.model.UserAuthJsonSupport.RawlsBillingProjectMembershipFormat
            responseAs[List[RawlsBillingProjectMembership]]
          }
        }

      assertResult(1) {
        runAndWait(rawlsBillingProjectQuery.loadOperationsForProjects(Seq(project1.projectName), GoogleOperationNames.DeploymentManagerCreateProject)).size
      }

      val billingProjectMonitor = new CreatingBillingProjectMonitor {
        override val datasource: SlickDataSource = services.dataSource
        override val projectTemplate: ProjectTemplate = ProjectTemplate(Seq(), Seq())
        override val gcsDAO = new MockGoogleServicesDAO("foo")
        override val samDAO = new MockSamDAO(dataSource)
        override val requesterPaysRole: String = "requesterPaysRole"
        override val executionContext: ExecutionContext = services.executionContext
      }

      assertResult(CheckDone(1)) { Await.result(billingProjectMonitor.checkCreatingProjects(), Duration.Inf) }

      assertResult(1) {
        runAndWait(rawlsBillingProjectQuery.loadOperationsForProjects(Seq(project1.projectName), GoogleOperationNames.DeploymentManagerCreateProject)).count(_.done)
      }

      Get("/user/billing") ~>
        sealRoute(services.userRoutes) ~>
        check {
          assertResult(StatusCodes.OK) {
            status
          }
          assertResult(List(RawlsBillingProjectMembership(project1.projectName, ProjectRoles.Owner, CreationStatuses.Ready, Some(WorkbenchProjectLocation.US.name)))) {
            import org.broadinstitute.dsde.rawls.model.UserAuthJsonSupport.RawlsBillingProjectMembershipFormat
            responseAs[List[RawlsBillingProjectMembership]]
          }
        }

    }
  }


  it should "handle operation errors creating a billing project" in {
    testWithPollingError { _ =>
      Future.successful(OperationStatus(done = true, errorMessage = Option("this failed")))
    }
  }

  it should "handle polling errors creating a billing project" in {
    testWithPollingError { _ =>
      Future.failed(new RuntimeException("foo"))
    }
  }

  private def testWithPollingError(failureMode: OperationId => Future[OperationStatus]) = withEmptyTestDatabase { dataSource: SlickDataSource =>
    withApiServices(dataSource) { services =>

      // first add the project and user to the DB

      val billingUser = testData.userOwner
      val project1 = RawlsBillingProject(RawlsBillingProjectName("project1"), "mockBucketUrl", CreationStatuses.Ready, None, Some(WorkbenchProjectLocation.US), None)

      val createRequest = CreateRawlsBillingProjectFullRequest(project1.projectName, services.gcsDAO.accessibleBillingAccountName, WorkbenchProjectLocation.US.name, None, None, None)

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
          assertResult(List(RawlsBillingProjectMembership(project1.projectName, ProjectRoles.Owner, CreationStatuses.Creating, Some(WorkbenchProjectLocation.US.name)))) {
            import org.broadinstitute.dsde.rawls.model.UserAuthJsonSupport.RawlsBillingProjectMembershipFormat
            responseAs[List[RawlsBillingProjectMembership]]
          }
        }

      assertResult(1) {
        runAndWait(rawlsBillingProjectQuery.loadOperationsForProjects(Seq(project1.projectName), GoogleOperationNames.DeploymentManagerCreateProject)).size
      }

      val billingProjectMonitor = new CreatingBillingProjectMonitor {
        override val datasource: SlickDataSource = services.dataSource
        override val projectTemplate: ProjectTemplate = ProjectTemplate(Seq(), Seq())
        override val gcsDAO = new MockGoogleServicesDAO("foo") {
          override def pollOperation(operationId: OperationId): Future[OperationStatus] = failureMode(operationId)
        }
        override val samDAO = new MockSamDAO(dataSource)
        override val requesterPaysRole: String = "requesterPaysRole"
        override val executionContext: ExecutionContext = services.executionContext
      }

      assertResult(CheckDone(1)) { Await.result(billingProjectMonitor.checkCreatingProjects(), Duration.Inf) }

      assertResult(1) {
        runAndWait(rawlsBillingProjectQuery.loadOperationsForProjects(Seq(project1.projectName), GoogleOperationNames.DeploymentManagerCreateProject)).count(_.done)
      }

      assertResult(1) {
        runAndWait(rawlsBillingProjectQuery.loadOperationsForProjects(Seq(project1.projectName), GoogleOperationNames.DeploymentManagerCreateProject)).size
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
      val project1 = RawlsBillingProject(RawlsBillingProjectName("project1"), "mockBucketUrl", CreationStatuses.Ready, None, Some(WorkbenchProjectLocation.US), None)

      val createRequest = CreateRawlsBillingProjectFullRequest(project1.projectName, services.gcsDAO.accessibleBillingAccountName, WorkbenchProjectLocation.US.name, None, None, None)

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
          assertResult(List(RawlsBillingProjectMembership(project1.projectName, ProjectRoles.Owner, CreationStatuses.Creating, Some(WorkbenchProjectLocation.US.name)))) {
            import org.broadinstitute.dsde.rawls.model.UserAuthJsonSupport.RawlsBillingProjectMembershipFormat
            responseAs[List[RawlsBillingProjectMembership]]
          }
        }

      assertResult(1) {
        runAndWait(rawlsBillingProjectQuery.loadOperationsForProjects(Seq(project1.projectName), GoogleOperationNames.DeploymentManagerCreateProject)).size
      }

      val billingProjectMonitor = new CreatingBillingProjectMonitor {
        override val datasource: SlickDataSource = services.dataSource
        override val projectTemplate: ProjectTemplate = ProjectTemplate(Seq(), Seq())
        override val gcsDAO = new MockGoogleServicesDAO("foo") {
          override def pollOperation(operationId: OperationId): Future[OperationStatus] = {
            if (operationId.apiType == GoogleApiTypes.DeploymentManagerApi) {
              Future.successful(OperationStatus(done = true, errorMessage = Option("this failed")))
            } else {
              super.pollOperation(operationId)
            }
          }
        }
        override val samDAO = new MockSamDAO(dataSource)
        override val requesterPaysRole: String = "requesterPaysRole"
        override val executionContext: ExecutionContext = services.executionContext

      }

      assertResult(CheckDone(1)) { Await.result(billingProjectMonitor.checkCreatingProjects(), Duration.Inf) }

      assertResult(1) {
        runAndWait(rawlsBillingProjectQuery.loadOperationsForProjects(Seq(project1.projectName), GoogleOperationNames.DeploymentManagerCreateProject)).count(_.done)
      }

      assertResult(1) {
        runAndWait(rawlsBillingProjectQuery.loadOperationsForProjects(Seq(project1.projectName), GoogleOperationNames.DeploymentManagerCreateProject)).size
      }

      Get("/user/billing") ~>
        sealRoute(services.userRoutes) ~>
        check {
          assertResult(StatusCodes.OK) {
            status
          }
          assertResult(List(RawlsBillingProjectMembership(project1.projectName, ProjectRoles.Owner, CreationStatuses.Error, Option(s"project ${project1.projectName.value} creation finished with errors: this failed")))) {
            import org.broadinstitute.dsde.rawls.model.UserAuthJsonSupport.RawlsBillingProjectMembershipFormat
            responseAs[List[RawlsBillingProjectMembership]]
          }
        }

    }
  }

  it should "return 200 when adding a user to a billing project that the caller owns" in withTestDataApiServices { services =>
    val project1 = RawlsBillingProject(RawlsBillingProjectName("project1"), "mockBucketUrl", CreationStatuses.Ready, None, Some(WorkbenchProjectLocation.US), None)
    val createRequest = CreateRawlsBillingProjectFullRequest(project1.projectName, services.gcsDAO.accessibleBillingAccountName, WorkbenchProjectLocation.US.name, None, None, None)

    import UserAuthJsonSupport.CreateRawlsBillingProjectFullRequestFormat

    Post(s"/billing", httpJson(createRequest)) ~>
      sealRoute(services.billingRoutes) ~>
      check {
        assertResult(StatusCodes.Created) {
          status
        }
      }

    check {
      false
      //was Await.result(services.gcsDAO.beginProjectSetup(project1, null), Duration.Inf)
    }

    Put(s"/billing/${project1.projectName.value}/user/${testData.userWriter.userEmail.value}") ~>
      sealRoute(services.billingRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
      }
  }

  it should "return 200 when removing a user from a billing project that the caller owns" in withTestDataApiServices { services =>
    val project1 = RawlsBillingProject(RawlsBillingProjectName("project1"), "mockBucketUrl", CreationStatuses.Ready, None, Some(WorkbenchProjectLocation.US), None)
    val createRequest = CreateRawlsBillingProjectFullRequest(project1.projectName, services.gcsDAO.accessibleBillingAccountName, WorkbenchProjectLocation.US.name, None, None, None)

    import UserAuthJsonSupport.CreateRawlsBillingProjectFullRequestFormat

    Post(s"/billing", httpJson(createRequest)) ~>
      sealRoute(services.billingRoutes) ~>
      check {
        assertResult(StatusCodes.Created) {
          status
        }
      }

    check {
      false
      //was Await.result(services.gcsDAO.beginProjectSetup(project1, null), Duration.Inf)
    }

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
}
