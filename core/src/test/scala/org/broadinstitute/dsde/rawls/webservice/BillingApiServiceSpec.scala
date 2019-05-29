package org.broadinstitute.dsde.rawls.webservice

import java.net.URLEncoder
import java.nio.charset.StandardCharsets.UTF_8
import java.util.UUID

import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.dataaccess.slick.{RawlsBillingProjectOperationRecord, RawlsBillingProjectRecord, ReadAction}
import org.broadinstitute.dsde.rawls.google.MockGooglePubSubDAO
import org.broadinstitute.dsde.rawls.model.ExecutionJsonSupport.ActiveSubmissionFormat
import org.broadinstitute.dsde.rawls.model.UserJsonSupport._
import org.broadinstitute.dsde.rawls.model.WorkspaceJsonSupport._
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.openam.MockUserInfoDirectives
import org.broadinstitute.dsde.rawls.user.UserService
import org.scalatest.path
import akka.http.scaladsl.model.{HttpMethods, StatusCodes, Uri}
import spray.json.DefaultJsonProtocol._
import akka.http.scaladsl.server.Route.{seal => sealRoute}
import org.broadinstitute.dsde.rawls.{RawlsExceptionWithErrorReport, model}
import org.broadinstitute.dsde.rawls.mock.{CustomizableMockSamDAO, MockSamDAO}
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.mockito.ArgumentMatchers

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import org.mockito.Mockito._
import org.mockito.ArgumentMatchers._
import org.scalatest.mockito.MockitoSugar

class BillingApiServiceSpec extends ApiServiceSpec with MockitoSugar {
  import org.broadinstitute.dsde.rawls.model.UserAuthJsonSupport._

  case class TestApiService(dataSource: SlickDataSource, gcsDAO: MockGoogleServicesDAO, gpsDAO: MockGooglePubSubDAO)(implicit override val executionContext: ExecutionContext) extends ApiServices with MockUserInfoDirectives {
    override val samDAO: SamDAO = mock[SamDAO]
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
    import driver.api._
    val projectWithOwner = project.copy()

    runAndWait(rawlsBillingProjectQuery.create(projectWithOwner))
  }

  "BillingApiService" should "return 200 when adding a user to a billing project" in withTestDataApiServices { services =>
    val project = billingProjectFromName("new_project")

    Put(s"/billing/${project.projectName.value}/user/${testData.userWriter.userEmail.value}") ~>
      sealRoute(services.billingRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
      }

    Put(s"/billing/${project.projectName.value}/owner/${testData.userWriter.userEmail.value}") ~>
      sealRoute(services.billingRoutes) ~>
      check {
        assertResult(StatusCodes.OK) {
          status
        }
      }
  }

  it should "return 403 when adding a user to a non-owned billing project" in withTestDataApiServices { services =>
    val project = billingProjectFromName("no_access")

    when(services.samDAO.userHasAction(ArgumentMatchers.eq(SamResourceTypeNames.billingProject), ArgumentMatchers.eq(project.projectName.value), any[SamResourceAction], any[UserInfo])).thenReturn(Future.successful(false))

    withStatsD {
      Put(s"/billing/${project.projectName.value}/user/${testData.userReader.userEmail.value}") ~> services.sealedInstrumentedRoutes ~>
        check {
          assertResult(StatusCodes.Forbidden) {
            status
          }
        }

      Put(s"/billing/${project.projectName.value}/owner/${testData.userReader.userEmail.value}") ~> services.sealedInstrumentedRoutes ~>
        check {
          assertResult(StatusCodes.Forbidden) {
            status
          }
        }
    } { capturedMetrics =>
      val expected = expectedHttpRequestMetrics("put", s"billing.redacted.redacted.redacted", StatusCodes.Forbidden.intValue, 2)
      assertSubsetOf(expected, capturedMetrics)
    }
  }

  it should "return 404 when adding a nonexistent user to a billing project" in withTestDataApiServices { services =>
    val project = billingProjectFromName("no_access")

    Put(s"/billing/${project.projectName.value}/nobody") ~>
      sealRoute(services.billingRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
      }
  }

  it should "return 403 when adding a user to a nonexistent project" in withTestDataApiServices { services =>
    when(services.samDAO.userHasAction(ArgumentMatchers.eq(SamResourceTypeNames.billingProject), ArgumentMatchers.eq("missing_project"), any[SamResourceAction], any[UserInfo])).thenReturn(Future.successful(false))

    Put(s"/billing/missing_project/user/${testData.userOwner.userEmail.value}") ~>
      sealRoute(services.billingRoutes) ~>
      check {
        assertResult(StatusCodes.Forbidden) {
          status
        }
      }
  }

  it should "return 200 when removing a user from a billing project" in withTestDataApiServices { services =>
    val project = billingProjectFromName("new_project")

    withStatsD {
      Delete(s"/billing/${project.projectName.value}/user/${testData.userWriter.userEmail.value}") ~> services.sealedInstrumentedRoutes ~>
        check {
          assertResult(StatusCodes.OK) {
            status
          }
        }
    } { capturedMetrics =>
      val expected = expectedHttpRequestMetrics("delete", s"billing.redacted.redacted.redacted", StatusCodes.OK.intValue, 1)
      assertSubsetOf(expected, capturedMetrics)
    }
  }

  it should "return 403 when removing a user from a non-owned billing project" in withTestDataApiServices { services =>
    val project = billingProjectFromName("no_access")
    when(services.samDAO.userHasAction(ArgumentMatchers.eq(SamResourceTypeNames.billingProject), ArgumentMatchers.eq(project.projectName.value), any[SamResourceAction], any[UserInfo])).thenReturn(Future.successful(false))

    Delete(s"/billing/${project.projectName.value}/owner/${testData.userWriter.userEmail.value}") ~>
      sealRoute(services.billingRoutes) ~>
      check {
        assertResult(StatusCodes.Forbidden) {
          status
        }
      }
  }

  it should "return 400 when removing a nonexistent user from a billing project" in withTestDataApiServices { services =>
    val project = billingProjectFromName("test_good")
    when(services.samDAO.removeUserFromPolicy(ArgumentMatchers.eq(SamResourceTypeNames.billingProject), ArgumentMatchers.eq(project.projectName.value), any[SamResourcePolicyName], ArgumentMatchers.eq("nobody"), any[UserInfo])).thenReturn(Future.failed(new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.BadRequest, ""))))

    Delete(s"/billing/${project.projectName.value}/user/nobody") ~>
      sealRoute(services.billingRoutes) ~>
      check {
        assertResult(StatusCodes.NotFound) {
          status
        }
      }
  }

  it should "return 403 when removing a user from a nonexistent billing project" in withTestDataApiServices { services =>
    when(services.samDAO.userHasAction(ArgumentMatchers.eq(SamResourceTypeNames.billingProject), ArgumentMatchers.eq("missing_project"), any[SamResourceAction], any[UserInfo])).thenReturn(Future.successful(false))
    Delete(s"/billing/missing_project/user/${testData.userOwner.userEmail.value}") ~>
      sealRoute(services.billingRoutes) ~>
      check {
        assertResult(StatusCodes.Forbidden) {
          status
        }
      }
  }

  it should "return 204 when creating a project with accessible billing account" in withTestDataApiServices { services =>
    val projectName = RawlsBillingProjectName("test_good")

    when(services.samDAO.createResource(ArgumentMatchers.eq(SamResourceTypeNames.billingProject), ArgumentMatchers.eq(projectName.value), any[UserInfo])).thenReturn(Future.successful(()))
    when(services.samDAO.overwritePolicy(ArgumentMatchers.eq(SamResourceTypeNames.billingProject), ArgumentMatchers.eq(projectName.value), ArgumentMatchers.eq(SamBillingProjectPolicyNames.workspaceCreator), ArgumentMatchers.eq(SamPolicy(Set.empty, Set.empty, Set(SamProjectRoles.workspaceCreator))), any[UserInfo])).thenReturn(Future.successful(()))
    when(services.samDAO.overwritePolicy(ArgumentMatchers.eq(SamResourceTypeNames.billingProject), ArgumentMatchers.eq(projectName.value), ArgumentMatchers.eq(SamBillingProjectPolicyNames.canComputeUser), ArgumentMatchers.eq(SamPolicy(Set.empty, Set.empty, Set(SamProjectRoles.batchComputeUser, SamProjectRoles.notebookUser))), any[UserInfo])).thenReturn(Future.successful(()))
    when(services.samDAO.syncPolicyToGoogle(ArgumentMatchers.eq(SamResourceTypeNames.billingProject), ArgumentMatchers.eq(projectName.value), ArgumentMatchers.eq(SamBillingProjectPolicyNames.owner))).thenReturn(Future.successful(Map(WorkbenchEmail("owner-policy@google.group") -> Seq())))
    when(services.samDAO.syncPolicyToGoogle(ArgumentMatchers.eq(SamResourceTypeNames.billingProject), ArgumentMatchers.eq(projectName.value), ArgumentMatchers.eq(SamBillingProjectPolicyNames.canComputeUser))).thenReturn(Future.successful(Map(WorkbenchEmail("can-compute-policy@google.group") -> Seq())))

    Post("/billing", CreateRawlsBillingProjectFullRequest(projectName, services.gcsDAO.accessibleBillingAccountName, None)) ~>
      sealRoute(services.billingRoutes) ~>
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
            assertResult("gs://" + services.gcsDAO.getCromwellAuthBucketName(projectName)) {
              project.cromwellAuthBucketUrl
            }
          case _ => fail("too many projects")
        }
      }
  }

  it should "rollback billing project inserts when there is a google error" in withDefaultTestDatabase { dataSource: SlickDataSource =>
    withApiServices(dataSource, new MockGoogleServicesDAO("test") {
      override def createProject(projectName: RawlsBillingProjectName, billingAccount: RawlsBillingAccount, dmTemplatePath: String, requesterPaysRole: String, ownerGroupEmail: WorkbenchEmail, computeUserGroupEmail: WorkbenchEmail, projectTemplate: ProjectTemplate): Future[RawlsBillingProjectOperationRecord] = {
        Future.failed(new Exception("test exception"))
      }
    }) { services =>
      val projectName = RawlsBillingProjectName("test_good2")

      Post("/billing", CreateRawlsBillingProjectFullRequest(projectName, services.gcsDAO.accessibleBillingAccountName, None)) ~>
        sealRoute(services.billingRoutes) ~>
        check {
          assertResult(StatusCodes.InternalServerError) {
            status
          }
          runAndWait(rawlsBillingProjectQuery.load(projectName)) map { p => fail("did not rollback project inserts: " + p) }
        }
    }
  }

  it should "return 400 when creating a project with inaccessible to firecloud billing account" in withTestDataApiServices { services =>
    Post("/billing", CreateRawlsBillingProjectFullRequest(RawlsBillingProjectName("test_bad1"), services.gcsDAO.inaccessibleBillingAccountName, None)) ~>
      sealRoute(services.billingRoutes) ~>
      check {
        assertResult(StatusCodes.BadRequest) {
          status
        }
      }
  }

  it should "return 403 when creating a project with inaccessible to user billing account" in withTestDataApiServices { services =>
    Post("/billing", CreateRawlsBillingProjectFullRequest(RawlsBillingProjectName("test_bad1"), RawlsBillingAccountName("this does not exist"), None)) ~>
      sealRoute(services.billingRoutes) ~>
      check {
        assertResult(StatusCodes.Forbidden) {
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

    Get(s"/billing/${project.projectName.value}/members") ~>
      sealRoute(services.billingRoutes) ~>
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

    Get(s"/billing/${project.projectName.value}/members") ~>
      sealRoute(services.billingRoutes) ~>
      check {
        assertResult(StatusCodes.Forbidden) {
          status
        }
      }
  }

  it should "return 202 when adding a billing project to a service perimeter with all the right permissions" in withTestDataApiServices { services =>
    val projectName = testData.billingProject.projectName
    val servicePerimeterName = ServicePerimeterName("accessPolicies/123/servicePerimeters/service_perimeter")
    val encodedServicePerimeterName = URLEncoder.encode(servicePerimeterName.value, UTF_8.name)

    when(services.samDAO.userHasAction(SamResourceTypeNames.servicePerimeter, encodedServicePerimeterName, SamServicePerimeterActions.addProject, userInfo)).thenReturn(Future.successful(true))

    Put(s"/servicePerimeters/${encodedServicePerimeterName}/projects/${projectName.value}") ~>
      sealRoute(services.servicePerimeterRoutes) ~>
      check {
        assertResult(StatusCodes.Accepted) {
          status
        }
      }
  }

}
