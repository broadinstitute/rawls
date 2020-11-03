package org.broadinstitute.dsde.rawls.webservice

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Route.{seal => sealRoute}
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.dataaccess.slick.{RawlsBillingProjectRecord, ReadAction}
import org.broadinstitute.dsde.rawls.google.MockGooglePubSubDAO
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.openam.MockUserInfoDirectives
import org.broadinstitute.dsde.rawls.{RawlsExceptionWithErrorReport, model}
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.scalatest.mockito.MockitoSugar
import spray.json.DefaultJsonProtocol._

import scala.concurrent.{ExecutionContext, Future}

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

  def withTestDataApiServices[T](testCode: TestApiService =>  T): T = {
    withDefaultTestDatabase { dataSource: SlickDataSource =>
      withApiServices(dataSource)(testCode)
    }
  }

  def createProject(project: RawlsBillingProject, owner: RawlsUser = testData.userOwner): Unit = {
    val projectWithOwner = project.copy()

    runAndWait(rawlsBillingProjectQuery.create(projectWithOwner))
  }

  "BillingApiServiceV2" should "return 200 when adding a user to a billing project" in withTestDataApiServices { services =>
    val project = billingProjectFromName("new_project")

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

  it should "return 403 when adding a user to a non-owned billing project" in withTestDataApiServices { services =>
    val project = billingProjectFromName("no_access")

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

  it should "return 400 when adding a nonexistent user to a billing project" in withTestDataApiServices { services =>
    val project = billingProjectFromName("no_access")

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

  it should "return 403 when adding a user to a nonexistent project" in withTestDataApiServices { services =>
    when(services.samDAO.userHasAction(ArgumentMatchers.eq(SamResourceTypeNames.billingProject), ArgumentMatchers.eq("missing_project"), any[SamResourceAction], any[UserInfo])).thenReturn(Future.successful(false))

    Put(s"/billing/v2/missing_project/members/user/${testData.userOwner.userEmail.value}") ~>
      sealRoute(services.billingRoutesV2) ~>
      check {
        assertResult(StatusCodes.Forbidden) {
          status
        }
      }
  }

  it should "return 200 when removing a user from a billing project" in withTestDataApiServices { services =>
    val project = billingProjectFromName("new_project")

    Delete(s"/billing/v2/${project.projectName.value}/members/user/${testData.userWriter.userEmail.value}") ~> services.sealedInstrumentedRoutes ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
      }
  }

  it should "return 403 when removing a user from a non-owned billing project" in withTestDataApiServices { services =>
    val project = billingProjectFromName("no_access")
    when(services.samDAO.userHasAction(ArgumentMatchers.eq(SamResourceTypeNames.billingProject), ArgumentMatchers.eq(project.projectName.value), any[SamResourceAction], any[UserInfo])).thenReturn(Future.successful(false))

    Delete(s"/billing/v2/${project.projectName.value}/members/owner/${testData.userWriter.userEmail.value}") ~>
      sealRoute(services.billingRoutesV2) ~>
      check {
        assertResult(StatusCodes.Forbidden) {
          status
        }
      }
  }

  it should "return 400 when removing a nonexistent user from a billing project" in withTestDataApiServices { services =>
    val project = billingProjectFromName("test_good")
    when(services.samDAO.removeUserFromPolicy(ArgumentMatchers.eq(SamResourceTypeNames.billingProject), ArgumentMatchers.eq(project.projectName.value), any[SamResourcePolicyName], ArgumentMatchers.eq("nobody"), any[UserInfo])).thenReturn(Future.failed(new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.BadRequest, ""))))

    Delete(s"/billing/v2/${project.projectName.value}/members/user/nobody") ~>
      sealRoute(services.billingRoutesV2) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
      }
  }

  it should "return 403 when removing a user from a nonexistent billing project" in withTestDataApiServices { services =>
    when(services.samDAO.userHasAction(ArgumentMatchers.eq(SamResourceTypeNames.billingProject), ArgumentMatchers.eq("missing_project"), any[SamResourceAction], any[UserInfo])).thenReturn(Future.successful(false))
    Delete(s"/billing/v2/missing_project/members/user/${testData.userOwner.userEmail.value}") ~>
      sealRoute(services.billingRoutesV2) ~>
      check {
        assertResult(StatusCodes.Forbidden) {
          status
        }
      }
  }

  it should "return 204 when creating a project with accessible billing account" in withTestDataApiServices { services =>
    val projectName = RawlsBillingProjectName("test_good")

    mockPositiveBillingProjectCreation(services, projectName)

    Post("/billing/v2", CreateRawlsBillingProjectFullRequest(projectName, services.gcsDAO.accessibleBillingAccountName, None, None, None, None)) ~>
      sealRoute(services.billingRoutesV2) ~>
      check {
        assertResult(StatusCodes.Created) {
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

  it should "return 400 when creating a project with inaccessible to firecloud billing account" in withTestDataApiServices { services =>
    Post("/billing/v2", CreateRawlsBillingProjectFullRequest(RawlsBillingProjectName("test_bad1"), services.gcsDAO.inaccessibleBillingAccountName, None, None, None, None)) ~>
      sealRoute(services.billingRoutesV2) ~>
      check {
        assertResult(StatusCodes.BadRequest, responseAs[String]) {
          status
        }
      }
  }

  it should "return 400 when creating a project with enableFlowLogs but not highSecurityNetwork" in withTestDataApiServices { services =>
    Post("/billing/v2", CreateRawlsBillingProjectFullRequest(RawlsBillingProjectName("test_good"), services.gcsDAO.accessibleBillingAccountName, highSecurityNetwork = Some(false), enableFlowLogs = Some(true), None, None)) ~>
      sealRoute(services.billingRoutesV2) ~>
      check {
        assertResult(StatusCodes.BadRequest) {
          status
        }
      }
  }

  it should "return 400 when creating a project with privateIpGoogleAccess but not highSecurityNetwork" in withTestDataApiServices { services =>
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
      any[UserInfo])).
      thenReturn(Future.successful(SamCreateResourceResponse(
        SamResourceTypeNames.billingProject.value,
        projectName.value,
        Set.empty,
        policies.keySet.map(p => SamCreateResourcePolicyResponse(SamCreateResourceAccessPolicyIdResponse(p.value, SamFullyQualifiesResourceId(projectName.value, SamResourceTypeNames.billingProject.value)), s"${p.value}@foo.com")))))
  }

  it should "return 201 when creating a project with a highSecurityNetwork" in withTestDataApiServices { services =>
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

  it should "return 201 when creating a project with a highSecurityNetwork and enableFlowLogs turned on" in withTestDataApiServices { services =>
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

  it should "return 201 when creating a project with a highSecurityNetwork with privateIpGoogleAccess turned on" in withTestDataApiServices { services =>
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

  it should "return 201 when creating a project with a highSecurityNetwork with enableFlowLogs and privateIpGoogleAccess turned on" in withTestDataApiServices { services =>
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

  it should "return 200 when listing billing project members as owner" in withTestDataApiServices { services =>
    val project = billingProjectFromName("test_good")

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

  it should "return 403 when listing billing project members as non-owner" in withTestDataApiServices { services =>
    val project = billingProjectFromName("no_access")

    when(services.samDAO.userHasAction(ArgumentMatchers.eq(SamResourceTypeNames.billingProject), ArgumentMatchers.eq(project.projectName.value), any[SamResourceAction], any[UserInfo])).thenReturn(Future.successful(false))

    Get(s"/billing/v2/${project.projectName.value}/members") ~>
      sealRoute(services.billingRoutesV2) ~>
      check {
        assertResult(StatusCodes.Forbidden) {
          status
        }
      }
  }
}
