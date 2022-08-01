package org.broadinstitute.dsde.rawls.webservice

import java.util.UUID
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Route.{seal => sealRoute}
import org.broadinstitute.dsde.rawls.billing.BillingProjectOrchestrator
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.dataaccess.slick.{RawlsBillingProjectRecord, ReadAction}
import org.broadinstitute.dsde.rawls.google.MockGooglePubSubDAO
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.openam.MockUserInfoDirectives
import org.broadinstitute.dsde.rawls.spendreporting.SpendReportingService
import org.broadinstitute.dsde.rawls.{RawlsException, RawlsExceptionWithErrorReport, model}
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.joda.time.DateTime
import org.mockito.{ArgumentMatchers, Mockito}
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.scalatestplus.mockito.MockitoSugar
import spray.json.DefaultJsonProtocol._

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Random

class BillingApiServiceV2Spec extends ApiServiceSpec with MockitoSugar {
  import org.broadinstitute.dsde.rawls.model.UserAuthJsonSupport._
  import org.broadinstitute.dsde.rawls.model.SpendReportingJsonSupport._

  case class TestApiService(dataSource: SlickDataSource, gcsDAO: MockGoogleServicesDAO, gpsDAO: MockGooglePubSubDAO)(implicit override val executionContext: ExecutionContext) extends ApiServices with MockUserInfoDirectives {
    override val samDAO: SamDAO = mock[SamDAO](RETURNS_SMART_NULLS)
    when(samDAO.userHasAction(ArgumentMatchers.eq(SamResourceTypeNames.billingProject), any[String], any[SamResourceAction], any[UserInfo])).thenReturn(Future.successful(true))
    when(samDAO.addUserToPolicy(ArgumentMatchers.eq(SamResourceTypeNames.billingProject), any[String], any[SamResourcePolicyName], any[String], any[UserInfo])).thenReturn(Future.successful(()))
    when(samDAO.removeUserFromPolicy(ArgumentMatchers.eq(SamResourceTypeNames.billingProject), any[String], any[SamResourcePolicyName], any[String], any[UserInfo])).thenReturn(Future.successful(()))
  }

  case class TestApiServiceWithCustomSpendReporting(dataSource: SlickDataSource, gcsDAO: MockGoogleServicesDAO, gpsDAO: MockGooglePubSubDAO, spendReportingService: SpendReportingService)(implicit override val executionContext: ExecutionContext) extends ApiServices with MockUserInfoDirectives {
    override val spendReportingConstructor: UserInfo => SpendReportingService =
      _ => spendReportingService
  }

  def withApiServices[T](dataSource: SlickDataSource, gcsDAO: MockGoogleServicesDAO = new MockGoogleServicesDAO("test"))(testCode: TestApiService =>  T): T = {
    val apiService = TestApiService(dataSource, gcsDAO, new MockGooglePubSubDAO)
    try {
      testCode(apiService)
    } finally {
      apiService.cleanupSupervisor
    }
  }

  def withApiServicesAndCustomSpendReporting[T](dataSource: SlickDataSource, spendReportingService: SpendReportingService)(testCode: TestApiServiceWithCustomSpendReporting =>  T): T = {
    val apiService = TestApiServiceWithCustomSpendReporting(dataSource, new MockGoogleServicesDAO("test"), new MockGooglePubSubDAO, spendReportingService)
    try {
      testCode(apiService)
    } finally {
      apiService.cleanupSupervisor
    }
  }

  def withEmptyDatabaseAndApiServices[T](testCode: TestApiService =>  T): T = {
    withEmptyTestDatabase { dataSource: SlickDataSource =>
      withApiServices(dataSource)(testCode)
    }
  }

  def createProject(projectName: String): RawlsBillingProject = {
    runAndWait(rawlsBillingProjectQuery.create(billingProjectFromName(projectName)))
  }

  "PUT /billing/v2/{projectName}/members/user/{email}" should "return 200 when adding a user to a billing project" in withEmptyDatabaseAndApiServices { services =>
    val project = createProject("new_project")

    Put(s"/billing/v2/${project.projectName.value}/members/user/${testData.userWriter.userEmail.value}") ~>
      sealRoute(services.billingRoutesV2) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
      }

    Put(s"/billing/v2/${project.projectName.value}/members/owner/${testData.userWriter.userEmail.value}") ~>
      sealRoute(services.billingRoutesV2) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
      }
  }

  it should "return 403 when adding a user to a non-owned billing project" in withEmptyDatabaseAndApiServices { services =>
    val project = createProject("no_access")

    when(services.samDAO.userHasAction(ArgumentMatchers.eq(SamResourceTypeNames.billingProject), ArgumentMatchers.eq(project.projectName.value), any[SamResourceAction], any[UserInfo])).thenReturn(Future.successful(false))

    Put(s"/billing/v2/${project.projectName.value}/members/user/${testData.userReader.userEmail.value}") ~> services.sealedInstrumentedRoutes ~>
      check {
        assertResult(StatusCodes.Forbidden) {
          status
        }
      }

    Put(s"/billing/v2/${project.projectName.value}/members/owner/${testData.userReader.userEmail.value}") ~> services.sealedInstrumentedRoutes ~>
      check {
        assertResult(StatusCodes.Forbidden) {
          status
        }
      }
  }

  it should "return 400 when adding a nonexistent user to a billing project" in withEmptyDatabaseAndApiServices { services =>
    val project = createProject("no_access")

    when(services.samDAO.addUserToPolicy(ArgumentMatchers.eq(SamResourceTypeNames.billingProject), ArgumentMatchers.eq(project.projectName.value), any[SamResourcePolicyName], ArgumentMatchers.eq("nobody"), any[UserInfo])).thenReturn(
      Future.failed(new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.BadRequest, "user not found"))))

    Put(s"/billing/v2/${project.projectName.value}/members/user/nobody") ~>
      sealRoute(services.billingRoutesV2) ~>
      check {
        assertResult(StatusCodes.BadRequest) {
          status
        }
      }
  }

  it should "return 403 when adding a user to a nonexistent project" in withEmptyDatabaseAndApiServices { services =>
    when(services.samDAO.userHasAction(ArgumentMatchers.eq(SamResourceTypeNames.billingProject), ArgumentMatchers.eq("missing_project"), any[SamResourceAction], any[UserInfo])).thenReturn(Future.successful(false))

    Put(s"/billing/v2/missing_project/members/user/${testData.userOwner.userEmail.value}") ~>
      sealRoute(services.billingRoutesV2) ~>
      check {
        assertResult(StatusCodes.Forbidden) {
          status
        }
      }
  }

  "DELETE /billing/v2/{projectName}/members/user/{email}" should "return 200 when removing a user from a billing project" in withEmptyDatabaseAndApiServices { services =>
    val project = createProject("new_project")

    Delete(s"/billing/v2/${project.projectName.value}/members/user/${testData.userWriter.userEmail.value}") ~> services.sealedInstrumentedRoutes ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
      }
  }

  it should "return 403 when removing a user from a non-owned billing project" in withEmptyDatabaseAndApiServices { services =>
    val project = createProject("no_access")
    when(services.samDAO.userHasAction(ArgumentMatchers.eq(SamResourceTypeNames.billingProject), ArgumentMatchers.eq(project.projectName.value), any[SamResourceAction], any[UserInfo])).thenReturn(Future.successful(false))

    Delete(s"/billing/v2/${project.projectName.value}/members/owner/${testData.userWriter.userEmail.value}") ~>
      sealRoute(services.billingRoutesV2) ~>
      check {
        assertResult(StatusCodes.Forbidden) {
          status
        }
      }
  }

  it should "return 400 when removing a nonexistent user from a billing project" in withEmptyDatabaseAndApiServices { services =>
    val project = createProject("test_good")
    when(services.samDAO.removeUserFromPolicy(ArgumentMatchers.eq(SamResourceTypeNames.billingProject), ArgumentMatchers.eq(project.projectName.value), any[SamResourcePolicyName], ArgumentMatchers.eq("nobody"), any[UserInfo])).thenReturn(Future.failed(new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.BadRequest, ""))))

    Delete(s"/billing/v2/${project.projectName.value}/members/user/nobody") ~>
      sealRoute(services.billingRoutesV2) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
      }
  }

  it should "return 403 when removing a user from a nonexistent billing project" in withEmptyDatabaseAndApiServices { services =>
    when(services.samDAO.userHasAction(ArgumentMatchers.eq(SamResourceTypeNames.billingProject), ArgumentMatchers.eq("missing_project"), any[SamResourceAction], any[UserInfo])).thenReturn(Future.successful(false))
    Delete(s"/billing/v2/missing_project/members/user/${testData.userOwner.userEmail.value}") ~>
      sealRoute(services.billingRoutesV2) ~>
      check {
        assertResult(StatusCodes.Forbidden) {
          status
        }
      }
  }

  "POST /billing/v2" should "return 204 when creating a project with accessible billing account" in withEmptyDatabaseAndApiServices { services =>
    val projectName = RawlsBillingProjectName("test_good")

    mockPositiveBillingProjectCreation(services, projectName)

    Post("/billing/v2", CreateRawlsV2BillingProjectFullRequest(projectName, Some(services.gcsDAO.accessibleBillingAccountName), None, None)) ~>
      sealRoute(services.billingRoutesV2) ~>
      check {
        assertResult(StatusCodes.Created, responseAs[String]) {
          status
        }

        import driver.api._
        val query: ReadAction[Seq[RawlsBillingProjectRecord]] = rawlsBillingProjectQuery.filter(_.projectName === projectName.value).result
        val projects: Seq[RawlsBillingProjectRecord] = runAndWait(query)
        projects match {
          case Seq() => fail("project does not exist in db")
          case Seq(_) =>
          case _ => fail("too many projects")
        }
      }
  }

  it should "return 400 when creating a project with inaccessible to firecloud billing account" in withEmptyDatabaseAndApiServices { services =>
    Post("/billing/v2", CreateRawlsV2BillingProjectFullRequest(RawlsBillingProjectName("test_bad1"), Some(services.gcsDAO.inaccessibleBillingAccountName), None, None)) ~>
      sealRoute(services.billingRoutesV2) ~>
      check {
        assertResult(StatusCodes.BadRequest, responseAs[String]) {
          status
        }
      }
  }

  it should "return 400 when creating a project with a name that is too short" in withEmptyDatabaseAndApiServices { services =>
    Post("/billing/v2", CreateRawlsBillingProjectFullRequest(RawlsBillingProjectName("short"), services.gcsDAO.accessibleBillingAccountName, None, None, None, None)) ~>
      sealRoute(services.billingRoutesV2) ~>
      check {
        assertResult(StatusCodes.BadRequest) {
          status
        }
      }
  }

  it should "return 400 when creating a project with a name that is too long" in withEmptyDatabaseAndApiServices { services =>
    Post("/billing/v2", CreateRawlsBillingProjectFullRequest(RawlsBillingProjectName("longlonglonglonglonglonglonglonglonglong"), services.gcsDAO.accessibleBillingAccountName, None, None, None, None)) ~>
      sealRoute(services.billingRoutesV2) ~>
      check {
        assertResult(StatusCodes.BadRequest) {
          status
        }
      }
  }

  it should "return 400 when creating a project with a name that contains invalid characters" in withEmptyDatabaseAndApiServices { services =>
    Post("/billing/v2", CreateRawlsBillingProjectFullRequest(RawlsBillingProjectName("!@#$%^&*()=+,. "), services.gcsDAO.accessibleBillingAccountName, None, None, None, None)) ~>
      sealRoute(services.billingRoutesV2) ~>
      check {
        assertResult(StatusCodes.BadRequest) {
          status
        }
      }
  }

  private def mockPositiveBillingProjectCreation(services: TestApiService, projectName: RawlsBillingProjectName): Unit = {
    val policies = BillingProjectOrchestrator.defaultBillingProjectPolicies(userInfo)
    when(services.samDAO.createResourceFull(
      ArgumentMatchers.eq(SamResourceTypeNames.billingProject),
      ArgumentMatchers.eq(projectName.value),
      ArgumentMatchers.eq(policies),
      ArgumentMatchers.eq(Set.empty),
      any[UserInfo],
      ArgumentMatchers.eq(None)
    )).
      thenReturn(Future.successful(SamCreateResourceResponse(
        SamResourceTypeNames.billingProject.value,
        projectName.value,
        Set.empty,
        policies.keySet.map(p => SamCreateResourcePolicyResponse(SamCreateResourceAccessPolicyIdResponse(p.value, SamFullyQualifiedResourceId(projectName.value, SamResourceTypeNames.billingProject.value)), s"${p.value}@foo.com")))))

    when(services.samDAO.syncPolicyToGoogle(
      ArgumentMatchers.eq(SamResourceTypeNames.billingProject),
      ArgumentMatchers.eq(projectName.value),
      ArgumentMatchers.eq(SamBillingProjectPolicyNames.owner)
    )).thenReturn(Future.successful(Map(WorkbenchEmail("owner-policy@google.group") -> Seq())))
  }

  "GET /billing/v2/{projectName}/members" should "return 200 when listing billing project members as owner" in withEmptyDatabaseAndApiServices { services =>
    val project = createProject("test_good")

    when(services.samDAO.listUserActionsForResource(ArgumentMatchers.eq(SamResourceTypeNames.billingProject), ArgumentMatchers.eq(project.projectName.value), any[UserInfo])).thenReturn(Future.successful(Set(SamBillingProjectActions.readPolicies)))

    when(services.samDAO.listPoliciesForResource(ArgumentMatchers.eq(SamResourceTypeNames.billingProject), ArgumentMatchers.eq(project.projectName.value), any[UserInfo])).thenReturn(Future.successful(
      Set(
        model.SamPolicyWithNameAndEmail(SamBillingProjectPolicyNames.owner, SamPolicy(Set(WorkbenchEmail(testData.userOwner.userEmail.value)), Set.empty, Set.empty), WorkbenchEmail("")),
        model.SamPolicyWithNameAndEmail(SamBillingProjectPolicyNames.workspaceCreator, SamPolicy(Set.empty, Set.empty, Set.empty), WorkbenchEmail(""))
      )))

    Get(s"/billing/v2/${project.projectName.value}/members") ~>
      sealRoute(services.billingRoutesV2) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
        assertResult(Seq(RawlsBillingProjectMember(testData.userOwner.userEmail, ProjectRoles.Owner))) {
          responseAs[Seq[RawlsBillingProjectMember]]
        }
      }
  }

  it should "return 200 when listing billing project members as non-owner" in withEmptyDatabaseAndApiServices { services =>
    val project = createProject("no_access")

    when(services.samDAO.listUserActionsForResource(ArgumentMatchers.eq(SamResourceTypeNames.billingProject), ArgumentMatchers.eq(project.projectName.value), any[UserInfo])).thenReturn(Future.successful(Set(SamBillingProjectActions.readPolicy(SamBillingProjectPolicyNames.owner))))
    when(services.samDAO.userHasAction(ArgumentMatchers.eq(SamResourceTypeNames.billingProject), ArgumentMatchers.eq(project.projectName.value), any[SamResourceAction], any[UserInfo])).thenReturn(Future.successful(false))
    when(services.samDAO.getPolicy(ArgumentMatchers.eq(SamResourceTypeNames.billingProject), ArgumentMatchers.eq(project.projectName.value), ArgumentMatchers.eq(SamBillingProjectPolicyNames.owner), any[UserInfo])).thenReturn(Future.successful(SamPolicy(Set(WorkbenchEmail(testData.userOwner.userEmail.value)), Set.empty, Set.empty)))

    Get(s"/billing/v2/${project.projectName.value}/members") ~>
      sealRoute(services.billingRoutesV2) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
        assertResult(Seq(RawlsBillingProjectMember(testData.userOwner.userEmail, ProjectRoles.Owner))) {
          responseAs[Seq[RawlsBillingProjectMember]]
        }
      }
  }

  "GET /billing/v2/{projectName}" should "return 200 with owner role" in withEmptyDatabaseAndApiServices { services =>
    val project = createProject("project")
    when(services.samDAO.listUserRolesForResource(SamResourceTypeNames.billingProject, project.projectName.value, userInfo)).thenReturn(Future.successful(Set(
      SamBillingProjectRoles.workspaceCreator, SamBillingProjectRoles.owner
    )))

    Get(s"/billing/v2/${project.projectName.value}") ~>
      sealRoute(services.billingRoutesV2) ~>
      check {
        assertResult(StatusCodes.OK, responseAs[String]) {
          status
        }
        responseAs[RawlsBillingProjectResponse] shouldEqual RawlsBillingProjectResponse(
          project.projectName,
          project.billingAccount,
          project.servicePerimeter,
          project.invalidBillingAccount,
          Set(ProjectRoles.Owner, ProjectRoles.User),
          project.status,
          project.message,
          project.azureManagedAppCoordinates
        )
      }
  }

  it should "return 200 with user role" in withEmptyDatabaseAndApiServices { services =>
    val project = createProject("project")
    when(services.samDAO.listUserRolesForResource(SamResourceTypeNames.billingProject, project.projectName.value, userInfo)).thenReturn(Future.successful(Set(
      SamBillingProjectRoles.workspaceCreator
    )))

    Get(s"/billing/v2/${project.projectName.value}") ~>
      sealRoute(services.billingRoutesV2) ~>
      check {
        assertResult(StatusCodes.OK, responseAs[String]) {
          status
        }
        responseAs[RawlsBillingProjectResponse] shouldEqual RawlsBillingProjectResponse(
          project.projectName,
          project.billingAccount,
          project.servicePerimeter,
          project.invalidBillingAccount,
          Set(ProjectRoles.User),
          project.status,
          project.message,
          project.azureManagedAppCoordinates
        )
      }
  }

  it should "return 404 if project does not exist" in withEmptyDatabaseAndApiServices { services =>
    val projectName = "does_not_exist"
    when(services.samDAO.listUserRolesForResource(SamResourceTypeNames.billingProject, projectName, userInfo)).thenReturn(Future.successful(Set.empty[SamResourceRole]))

    Get(s"/billing/v2/$projectName") ~>
      sealRoute(services.billingRoutesV2) ~>
      check {
        assertResult(StatusCodes.NotFound, responseAs[String]) {
          status
        }
      }
  }

  it should "return 404 if user has no access" in withEmptyDatabaseAndApiServices { services =>
    val project = createProject("project")
    when(services.samDAO.listUserRolesForResource(SamResourceTypeNames.billingProject, project.projectName.value, userInfo)).thenReturn(Future.successful(Set.empty[SamResourceRole]))

    Get(s"/billing/v2/${project.projectName.value}") ~>
      sealRoute(services.billingRoutesV2) ~>
      check {
        assertResult(StatusCodes.NotFound, responseAs[String]) {
          status
        }
      }
  }

  "DELETE /billing/v2/{projectName}" should "return 204 - deleting google project" in withEmptyDatabaseAndApiServices { services =>
    val project = createProject("project")
    // wow there are a lot of sam calls in delete billing project
    when(services.samDAO.userHasAction(SamResourceTypeNames.billingProject, project.projectName.value, SamBillingProjectActions.deleteBillingProject, userInfo)).thenReturn(Future.successful(true))
    when(services.samDAO.listAllResourceMemberIds(SamResourceTypeNames.billingProject, project.projectName.value, userInfo)).thenReturn(Future.successful(Set(UserIdInfo(userInfo.userSubjectId.value, userInfo.userEmail.value, None))))
    when(services.samDAO.getPetServiceAccountKeyForUser(project.googleProjectId, userInfo.userEmail)).thenReturn(Future.successful("petSAJson"))
    when(services.samDAO.listResourceChildren(SamResourceTypeNames.billingProject, project.projectName.value, userInfo)).thenReturn(Future.successful(Seq(SamFullyQualifiedResourceId(project.googleProjectId.value, SamResourceTypeNames.googleProject.value))))
    when(services.samDAO.deleteUserPetServiceAccount(ArgumentMatchers.eq(project.googleProjectId), any[UserInfo])).thenReturn(Future.successful())
    when(services.samDAO.deleteResource(SamResourceTypeNames.billingProject, project.projectName.value, userInfo)).thenReturn(Future.successful())
    when(services.samDAO.deleteResource(SamResourceTypeNames.googleProject, project.googleProjectId.value, userInfo)).thenReturn(Future.successful())

    Delete(s"/billing/v2/${project.projectName.value}") ~>
      sealRoute(services.billingRoutesV2) ~>
      check {
        assertResult(StatusCodes.NoContent, responseAs[String]) {
          status
        }
      }

    verify(services.samDAO).deleteUserPetServiceAccount(ArgumentMatchers.eq(project.googleProjectId), any[UserInfo])
    verify(services.samDAO).deleteResource(SamResourceTypeNames.billingProject, project.projectName.value, userInfo)
    verify(services.samDAO).deleteResource(SamResourceTypeNames.googleProject, project.googleProjectId.value, userInfo)
  }
  it should "return 204 - without google project" in withEmptyDatabaseAndApiServices { services =>
    val project = createProject("project")
    when(services.samDAO.userHasAction(SamResourceTypeNames.billingProject, project.projectName.value, SamBillingProjectActions.deleteBillingProject, userInfo)).thenReturn(Future.successful(true))
    when(services.samDAO.listResourceChildren(SamResourceTypeNames.billingProject, project.projectName.value, userInfo)).thenReturn(Future.successful(Seq.empty[SamFullyQualifiedResourceId]))
    when(services.samDAO.deleteResource(SamResourceTypeNames.billingProject, project.projectName.value, userInfo)).thenReturn(Future.successful())

    Delete(s"/billing/v2/${project.projectName.value}") ~>
      sealRoute(services.billingRoutesV2) ~>
      check {
        assertResult(StatusCodes.NoContent, responseAs[String]) {
          status
        }
      }

    verify(services.samDAO).deleteResource(SamResourceTypeNames.billingProject, project.projectName.value, userInfo)
  }

  it should "return 400 if workspaces exist" in withEmptyDatabaseAndApiServices { services =>
    val project = createProject("project")
    runAndWait(workspaceQuery.createOrUpdate(Workspace(project.projectName.value, "workspace", UUID.randomUUID().toString, "", None, new DateTime(), new DateTime(), "", Map.empty)))

    when(services.samDAO.userHasAction(SamResourceTypeNames.billingProject, project.projectName.value, SamBillingProjectActions.deleteBillingProject, userInfo)).thenReturn(Future.successful(true))
    when(services.samDAO.listResourceChildren(SamResourceTypeNames.billingProject, project.projectName.value, userInfo)).thenReturn(Future.successful(Seq.empty[SamFullyQualifiedResourceId]))
    when(services.samDAO.deleteResource(SamResourceTypeNames.billingProject, project.projectName.value, userInfo)).thenReturn(Future.successful())

    Delete(s"/billing/v2/${project.projectName.value}") ~>
      sealRoute(services.billingRoutesV2) ~>
      check {
        assertResult(StatusCodes.BadRequest, responseAs[String]) {
          status
        }
      }
  }

  it should "return 403 if user does not have access" in withEmptyDatabaseAndApiServices { services =>
    val project = billingProjectFromName("no_access")
    when(services.samDAO.userHasAction(SamResourceTypeNames.billingProject, project.projectName.value, SamBillingProjectActions.deleteBillingProject, userInfo)).thenReturn(Future.successful(false))

    Delete(s"/billing/v2/${project.projectName.value}") ~>
      sealRoute(services.billingRoutesV2) ~>
      check {
        assertResult(StatusCodes.Forbidden, responseAs[String]) {
          status
        }
      }
  }

  "GET /billing/v2" should "list all my projects with workspaces" in withEmptyDatabaseAndApiServices { services =>
    val projects = List.fill(20) { createProject(UUID.randomUUID().toString) }
    val possibleRoles = List(Option(SamBillingProjectRoles.workspaceCreator), Option(SamBillingProjectRoles.owner), None)
    val samUserResources = projects.flatMap { p =>
      // randomly select a subset of possible roles
      val roles = Random.shuffle(possibleRoles).take(Random.nextInt(possibleRoles.size)).flatten.toSet
      if (roles.isEmpty) {
        None
      } else {
        Option(SamUserResource(
          p.projectName.value,
          SamRolesAndActions(roles, Set.empty),
          SamRolesAndActions(Set.empty, Set.empty),
          SamRolesAndActions(Set.empty, Set.empty),
          Set.empty,
          Set.empty))
      }
    }
    val workspaces = projects.flatMap(project => {
      val workspaceWithValidBillingAccount = new EmptyWorkspaceWithProjectAndBillingAccount(project, project.billingAccount)
      val workspaceTwoWithValidBillingAccount = new EmptyWorkspaceWithProjectAndBillingAccount(project, project.billingAccount)
      val workspaceWithInvalidBillingAccount = new EmptyWorkspaceWithProjectAndBillingAccount(project, Some(RawlsBillingAccountName("invalid-billing-account")))
      val workspaceTwoWithInvalidBillingAccount = new EmptyWorkspaceWithProjectAndBillingAccount(project, Some(RawlsBillingAccountName("invalid-billing-account")))
      List(workspaceWithValidBillingAccount, workspaceTwoWithValidBillingAccount, workspaceWithInvalidBillingAccount, workspaceTwoWithInvalidBillingAccount)
    })
    workspaces.foreach(workspace => runAndWait(workspace.save()))
    val samWorkspaceUserResources = workspaces.flatMap { w =>
      Option(SamUserResource(
        w.wsName.toString,
        SamRolesAndActions(Set(SamWorkspaceRoles.owner), Set.empty),
        SamRolesAndActions(Set.empty, Set.empty),
        SamRolesAndActions(Set.empty, Set.empty),
        Set.empty,
        Set.empty))
    }

    when(services.samDAO.listUserResources(SamResourceTypeNames.billingProject, userInfo)).thenReturn(Future.successful(samUserResources))
    when(services.samDAO.listUserResources(SamResourceTypeNames.workspace, userInfo)).thenReturn(Future.successful(samWorkspaceUserResources))

    val expected = projects.flatMap { p =>
      samUserResources.find(_.resourceId == p.projectName.value).map { samResource =>
        RawlsBillingProjectResponse(
          p.projectName,
          p.billingAccount,
          p.servicePerimeter,
          p.invalidBillingAccount,
          samResource.direct.roles.collect {
            case SamBillingProjectRoles.owner => ProjectRoles.Owner
            case SamBillingProjectRoles.workspaceCreator => ProjectRoles.User
          },
          p.status,
          p.message,
          p.azureManagedAppCoordinates
        )
      }
    }
    Get(s"/billing/v2") ~>
      sealRoute(services.billingRoutesV2) ~>
      check {
        assertResult(StatusCodes.OK, responseAs[String]) {
          status
        }

        responseAs[Seq[RawlsBillingProjectResponse]] should contain theSameElementsAs expected
      }
  }

  "PUT /billing/v2/{projectName}/billingAccount" should "update the billing account" in withEmptyDatabaseAndApiServices { services =>
    val project = createProject("project")
    when(services.samDAO.listUserRolesForResource(SamResourceTypeNames.billingProject, project.projectName.value, userInfo)).thenReturn(Future.successful(Set(
      SamBillingProjectRoles.workspaceCreator, SamBillingProjectRoles.owner
    )))
    Put(s"/billing/v2/${project.projectName.value}/billingAccount", UpdateRawlsBillingAccountRequest(services.gcsDAO.accessibleBillingAccountName)) ~>
      sealRoute(services.billingRoutesV2) ~>
      check {
        assertResult(StatusCodes.OK, responseAs[String]) {
          status
        }
      }
  }

  it should "fail to update if given inaccessible billing account" in withEmptyDatabaseAndApiServices { services =>
    val project = createProject("project")
    when(services.samDAO.listUserRolesForResource(SamResourceTypeNames.billingProject, project.projectName.value, userInfo)).thenReturn(Future.successful(Set(
      SamBillingProjectRoles.workspaceCreator, SamBillingProjectRoles.owner
    )))
    Put(s"/billing/v2/${project.projectName.value}/billingAccount", UpdateRawlsBillingAccountRequest(services.gcsDAO.inaccessibleBillingAccountName)) ~>
      sealRoute(services.billingRoutesV2) ~>
      check {
        assertResult(StatusCodes.BadRequest, responseAs[String]) {
          status
        }
      }
  }

  "DELETE /billing/v2/{projectName}/billingAccount" should "clear the billing account field" in withEmptyDatabaseAndApiServices { services =>
    val project = createProject("project")
    when(services.samDAO.listUserRolesForResource(SamResourceTypeNames.billingProject, project.projectName.value, userInfo)).thenReturn(Future.successful(Set(
      SamBillingProjectRoles.workspaceCreator, SamBillingProjectRoles.owner
    )))
    Delete(s"/billing/v2/${project.projectName.value}/billingAccount") ~>
      sealRoute(services.billingRoutesV2) ~>
      check {
        assertResult(StatusCodes.OK, responseAs[String]) {
          status
        }
      }
  }

  "GET /billing/v2/{projectName}/spendReport" should "200 and return only summary information when aggregation key is not present" in withEmptyTestDatabase { dataSource: SlickDataSource =>
    val mockSpendReportingService = mock[SpendReportingService](RETURNS_SMART_NULLS)
    // if the API service does not parse parameters and call the spendReportingService correctly, test will fail
    when(mockSpendReportingService.getSpendForBillingProject(any[RawlsBillingProjectName], any[DateTime], any[DateTime], any[Set[SpendReportingAggregationKeyWithSub]]))
      .thenReturn(Future.failed(new RawlsException("parameters were not parsed correctly")))
    when(mockSpendReportingService.getSpendForBillingProject(any[RawlsBillingProjectName], any[DateTime], any[DateTime], ArgumentMatchers.eq(Set.empty)))
      .thenReturn(Future.successful(SpendReportingResults(Seq.empty, SpendReportingForDateRange("0.0", "0.0", "USD", Option(DateTime.now()), Option(DateTime.now())))))

    withApiServicesAndCustomSpendReporting(dataSource, mockSpendReportingService) { services =>
      val project = createProject("project")

      Get(s"/billing/v2/${project.projectName.value}/spendReport?startDate=2022-03-06&endDate=2022-03-07") ~>
        sealRoute(services.billingRoutesV2) ~>
        check {
          assertResult(StatusCodes.OK, responseAs[String]) {
            status
          }

          verify(mockSpendReportingService, times(1)).getSpendForBillingProject(any[RawlsBillingProjectName], any[DateTime], any[DateTime], ArgumentMatchers.eq(Set.empty))
        }
    }
  }

  it should "200 and parse aggregation keys" in withEmptyTestDatabase { dataSource: SlickDataSource =>
    val mockSpendReportingService = mock[SpendReportingService](RETURNS_SMART_NULLS)
    val expectedAggregationKeys = Set(
      SpendReportingAggregationKeyWithSub(SpendReportingAggregationKeys.Workspace, Option(SpendReportingAggregationKeys.Daily)),
      SpendReportingAggregationKeyWithSub(SpendReportingAggregationKeys.Category),
      SpendReportingAggregationKeyWithSub(SpendReportingAggregationKeys.Category, Option(SpendReportingAggregationKeys.Category))
    )
    // if the API service does not parse parameters and call the spendReportingService correctly, test will fail
    when(mockSpendReportingService.getSpendForBillingProject(any[RawlsBillingProjectName], any[DateTime], any[DateTime], any[Set[SpendReportingAggregationKeyWithSub]]))
      .thenReturn(Future.failed(new RawlsException("parameters were not parsed correctly")))
    // we don't care about what it actually returns in this test, just that the mocked service is called correctly
    when(mockSpendReportingService.getSpendForBillingProject(any[RawlsBillingProjectName], any[DateTime], any[DateTime], ArgumentMatchers.eq(expectedAggregationKeys)))
      .thenReturn(Future.successful(SpendReportingResults(Seq.empty, SpendReportingForDateRange("0.0", "0.0", "USD", Option(DateTime.now()), Option(DateTime.now())))))

    withApiServicesAndCustomSpendReporting(dataSource, mockSpendReportingService) { services =>
      val project = createProject("project")

      Get(s"/billing/v2/${project.projectName.value}/spendReport?startDate=2022-03-06&endDate=2022-03-07&aggregationKey=Workspace~Daily&aggregationKey=Category&aggregationKey=Category~Category") ~>
        sealRoute(services.billingRoutesV2) ~>
        check {
          assertResult(StatusCodes.OK, responseAs[String]) {
            status
          }

          verify(mockSpendReportingService, times(1)).getSpendForBillingProject(any[RawlsBillingProjectName], any[DateTime], any[DateTime], ArgumentMatchers.eq(expectedAggregationKeys))
        }
    }
  }

  it should "400 when invalid dates are given" in withEmptyDatabaseAndApiServices { services =>
    val project = createProject("project")

    Get(s"/billing/v2/${project.projectName.value}/spendReport?startDate=nothing&endDate=20-020-123556") ~>
      sealRoute(services.billingRoutesV2) ~>
      check {
        assertResult(StatusCodes.BadRequest, responseAs[String]) {
          status
        }
      }
  }

  it should "400 when unsupported aggregation keys are given" in withEmptyDatabaseAndApiServices { services =>
    val project = createProject("project")

    Get(s"/billing/v2/${project.projectName.value}/spendReport?startDate=2022-02-03&endDate=2022-02-04&aggregationKey=Fake&aggregationKey=Workspace~bad") ~>
      sealRoute(services.billingRoutesV2) ~>
      check {
        assertResult(StatusCodes.BadRequest, responseAs[String]) {
          status
        }
      }
  }

  it should "400 when incorrectly formatted aggregation keys are given" in withEmptyDatabaseAndApiServices { services =>
    val project = createProject("project")

    Get(s"/billing/v2/${project.projectName.value}/spendReport?startDate=2022-02-03&endDate=2022-02-04&aggregationKey=Category-Workspace") ~>
      sealRoute(services.billingRoutesV2) ~>
      check {
        assertResult(StatusCodes.BadRequest, responseAs[String]) {
          status
        }
      }
  }

  it should "400 when too many sub aggregations are provided" in withEmptyDatabaseAndApiServices { services =>
    val project = createProject("project")

    Get(s"/billing/v2/${project.projectName.value}/spendReport?startDate=2022-02-03&endDate=2022-02-04&aggregationKey=aggregationKey=Category~Workspace~Daily") ~>
      sealRoute(services.billingRoutesV2) ~>
      check {
        assertResult(StatusCodes.BadRequest, responseAs[String]) {
          status
        }
      }
  }

  it should "404 when no dates are given" in withEmptyDatabaseAndApiServices { services =>
    val project = createProject("project")

    Get(s"/billing/v2/${project.projectName.value}/spendReport") ~>
      sealRoute(services.billingRoutesV2) ~>
      check {
        assertResult(StatusCodes.NotFound, responseAs[String]) {
          status
        }
      }
  }
}
