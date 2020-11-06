package org.broadinstitute.dsde.rawls.webservice

import java.util.UUID

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Route.{seal => sealRoute}
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.dataaccess.slick.{RawlsBillingProjectRecord, ReadAction}
import org.broadinstitute.dsde.rawls.google.MockGooglePubSubDAO
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.openam.MockUserInfoDirectives
import org.broadinstitute.dsde.rawls.{RawlsExceptionWithErrorReport, model}
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.joda.time.DateTime
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.scalatestplus.mockito.MockitoSugar
import spray.json.DefaultJsonProtocol._

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Random

class BillingApiServiceV2Spec extends ApiServiceSpec with MockitoSugar {
  import org.broadinstitute.dsde.rawls.model.UserAuthJsonSupport._

  case class TestApiService(dataSource: SlickDataSource, gcsDAO: MockGoogleServicesDAO, gpsDAO: MockGooglePubSubDAO)(implicit override val executionContext: ExecutionContext) extends ApiServices with MockUserInfoDirectives {
    override val samDAO: SamDAO = mock[SamDAO](RETURNS_SMART_NULLS)
    when(samDAO.userHasAction(ArgumentMatchers.eq(SamResourceTypeNames.billingProject), any[String], any[SamResourceAction], any[UserInfo])).thenReturn(Future.successful(true))
    when(samDAO.addUserToPolicy(ArgumentMatchers.eq(SamResourceTypeNames.billingProject), any[String], any[SamResourcePolicyName], any[String], any[UserInfo])).thenReturn(Future.successful(()))
    when(samDAO.removeUserFromPolicy(ArgumentMatchers.eq(SamResourceTypeNames.billingProject), any[String], any[SamResourcePolicyName], any[String], any[UserInfo])).thenReturn(Future.successful(()))
  }

  def withApiServices[T](dataSource: SlickDataSource, gcsDAO: MockGoogleServicesDAO = new MockGoogleServicesDAO("test"))(testCode: TestApiService =>  T): T = {
    val apiService = new TestApiService(dataSource, gcsDAO, new MockGooglePubSubDAO)
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

    Post("/billing/v2", CreateRawlsBillingProjectFullRequest(projectName, services.gcsDAO.accessibleBillingAccountName, None, None, None, None)) ~>
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
          case Seq(project) =>
          case _ => fail("too many projects")
        }
      }
  }

  it should "return 400 when creating a project with inaccessible to firecloud billing account" in withEmptyDatabaseAndApiServices { services =>
    Post("/billing/v2", CreateRawlsBillingProjectFullRequest(RawlsBillingProjectName("test_bad1"), services.gcsDAO.inaccessibleBillingAccountName, None, None, None, None)) ~>
      sealRoute(services.billingRoutesV2) ~>
      check {
        assertResult(StatusCodes.BadRequest, responseAs[String]) {
          status
        }
      }
  }

  it should "return 400 when creating a project with enableFlowLogs but not highSecurityNetwork" in withEmptyDatabaseAndApiServices { services =>
    Post("/billing/v2", CreateRawlsBillingProjectFullRequest(RawlsBillingProjectName("test_good"), services.gcsDAO.accessibleBillingAccountName, highSecurityNetwork = Some(false), enableFlowLogs = Some(true), None, None)) ~>
      sealRoute(services.billingRoutesV2) ~>
      check {
        assertResult(StatusCodes.BadRequest) {
          status
        }
      }
  }

  it should "return 400 when creating a project with privateIpGoogleAccess but not highSecurityNetwork" in withEmptyDatabaseAndApiServices { services =>
    Post("/billing/v2", CreateRawlsBillingProjectFullRequest(RawlsBillingProjectName("test_good"), services.gcsDAO.accessibleBillingAccountName, highSecurityNetwork = Some(false), None, privateIpGoogleAccess = Some(true), None)) ~>
      sealRoute(services.billingRoutesV2) ~>
      check {
        assertResult(StatusCodes.BadRequest) {
          status
        }
      }
  }

  private def mockPositiveBillingProjectCreation(services: TestApiService, projectName: RawlsBillingProjectName): Unit = {
    val policies = services.userServiceConstructor(userInfo).defaultBillingProjectPolicies
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
  }

  it should "return 201 when creating a project with a highSecurityNetwork" in withEmptyDatabaseAndApiServices { services =>
    val projectName = RawlsBillingProjectName("test_good")
    mockPositiveBillingProjectCreation(services, projectName)

    Post("/billing/v2", CreateRawlsBillingProjectFullRequest(projectName, services.gcsDAO.accessibleBillingAccountName, highSecurityNetwork = Some(true), None, None, None)) ~>
      sealRoute(services.billingRoutesV2) ~>
      check {
        assertResult(StatusCodes.Created) {
          status
        }
      }
  }

  it should "return 201 when creating a project with a highSecurityNetwork and enableFlowLogs turned on" in withEmptyDatabaseAndApiServices { services =>
    val projectName = RawlsBillingProjectName("test_good")
    mockPositiveBillingProjectCreation(services, projectName)

    Post("/billing/v2", CreateRawlsBillingProjectFullRequest(projectName, services.gcsDAO.accessibleBillingAccountName, highSecurityNetwork = Some(true), enableFlowLogs = Some(true), None, None)) ~>
      sealRoute(services.billingRoutesV2) ~>
      check {
        assertResult(StatusCodes.Created) {
          status
        }
      }
  }

  it should "return 201 when creating a project with a highSecurityNetwork with privateIpGoogleAccess turned on" in withEmptyDatabaseAndApiServices { services =>
    val projectName = RawlsBillingProjectName("test_good")
    mockPositiveBillingProjectCreation(services, projectName)

    Post("/billing/v2", CreateRawlsBillingProjectFullRequest(projectName, services.gcsDAO.accessibleBillingAccountName, highSecurityNetwork = Some(true), None, privateIpGoogleAccess = Some(true), None)) ~>
      sealRoute(services.billingRoutesV2) ~>
      check {
        assertResult(StatusCodes.Created) {
          status
        }
      }
  }

  it should "return 201 when creating a project with a highSecurityNetwork with enableFlowLogs and privateIpGoogleAccess turned on" in withEmptyDatabaseAndApiServices { services =>
    val projectName = RawlsBillingProjectName("test_good")
    mockPositiveBillingProjectCreation(services, projectName)

    Post("/billing/v2", CreateRawlsBillingProjectFullRequest(projectName, services.gcsDAO.accessibleBillingAccountName, highSecurityNetwork = Some(true), enableFlowLogs = Some(true), privateIpGoogleAccess = Some(true), None)) ~>
      sealRoute(services.billingRoutesV2) ~>
      check {
        assertResult(StatusCodes.Created) {
          status
        }
      }
  }

  "GET /billing/v2/{projectName}/members" should "return 200 when listing billing project members as owner" in withEmptyDatabaseAndApiServices { services =>
    val project = createProject("test_good")

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

  it should "return 403 when listing billing project members as non-owner" in withEmptyDatabaseAndApiServices { services =>
    val project = createProject("no_access")

    when(services.samDAO.userHasAction(ArgumentMatchers.eq(SamResourceTypeNames.billingProject), ArgumentMatchers.eq(project.projectName.value), any[SamResourceAction], any[UserInfo])).thenReturn(Future.successful(false))

    Get(s"/billing/v2/${project.projectName.value}/members") ~>
      sealRoute(services.billingRoutesV2) ~>
      check {
        assertResult(StatusCodes.Forbidden) {
          status
        }
      }
  }

  "GET /billing/v2/{projectName}" should "return 200 with owner role" in withEmptyDatabaseAndApiServices { services =>
    val project = createProject("project")
    when(services.samDAO.listUserRolesForResource(SamResourceTypeNames.billingProject, project.projectName.value, userInfo)).thenReturn(Future.successful(Set(
      SamProjectRoles.workspaceCreator, SamProjectRoles.owner
    )))

    Get(s"/billing/v2/${project.projectName.value}") ~>
      sealRoute(services.billingRoutesV2) ~>
      check {
        assertResult(StatusCodes.OK, responseAs[String]) {
          status
        }
        responseAs[RawlsBillingProjectResponse] shouldEqual RawlsBillingProjectResponse(project.projectName, project.billingAccount, project.servicePerimeter, project.invalidBillingAccount, ProjectRoles.Owner)
      }
  }

  it should "return 200 with user role" in withEmptyDatabaseAndApiServices { services =>
    val project = createProject("project")
    when(services.samDAO.listUserRolesForResource(SamResourceTypeNames.billingProject, project.projectName.value, userInfo)).thenReturn(Future.successful(Set(
      SamProjectRoles.workspaceCreator
    )))

    Get(s"/billing/v2/${project.projectName.value}") ~>
      sealRoute(services.billingRoutesV2) ~>
      check {
        assertResult(StatusCodes.OK, responseAs[String]) {
          status
        }
        responseAs[RawlsBillingProjectResponse] shouldEqual RawlsBillingProjectResponse(project.projectName, project.billingAccount, project.servicePerimeter, project.invalidBillingAccount, ProjectRoles.User)
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
    runAndWait(workspaceQuery.save(Workspace(project.projectName.value, "workspace", UUID.randomUUID().toString, "", None, new DateTime(), new DateTime(), "", Map.empty)))

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

  "GET /billing/v2" should "list all my projects" in withEmptyDatabaseAndApiServices { services =>
    val projects = List.fill(10) { createProject(UUID.randomUUID().toString) }
    val policies = projects.flatMap { p =>
      // randomly select a policy name or none to test role mapping and no access
      val maybeRole = Random.shuffle(List(Option(SamBillingProjectPolicyNames.workspaceCreator), Option(SamBillingProjectPolicyNames.owner), None)).head
      maybeRole.map { role =>
        SamResourceIdWithPolicyName(
          p.projectName.value,
          role,
          Set.empty,
          Set.empty,
          false)
      }
    }.toSet

    when(services.samDAO.getPoliciesForType(SamResourceTypeNames.billingProject, userInfo)).thenReturn(Future.successful(policies))

    val expected = projects.flatMap { p =>
      policies.find(_.resourceId == p.projectName.value).map { policy =>
        RawlsBillingProjectResponse(
          p.projectName,
          p.billingAccount,
          p.servicePerimeter,
          p.invalidBillingAccount,
          policy.accessPolicyName match {
            case SamBillingProjectPolicyNames.owner => ProjectRoles.Owner
            case SamBillingProjectPolicyNames.workspaceCreator => ProjectRoles.User
            case _ => throw new Exception("this should not happen")
          }
        )
      }
    }
    Get(s"/billing/v2") ~>
      sealRoute(services.billingRoutesV2) ~>
      check {
        assertResult(StatusCodes.OK, responseAs[String]) {
          status
        }

        responseAs[Seq[RawlsBillingProjectResponse]] should contain theSameElementsAs(expected)
      }
  }
}
