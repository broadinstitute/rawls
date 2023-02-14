package org.broadinstitute.dsde.rawls.webservice

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Route.{seal => sealRoute}
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.dataaccess.slick.{
  RawlsBillingProjectOperationRecord,
  RawlsBillingProjectRecord,
  ReadAction
}
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.openam.MockUserInfoDirectives
import org.broadinstitute.dsde.rawls.{model, RawlsExceptionWithErrorReport}
import org.broadinstitute.dsde.workbench.google.mock.MockGooglePubSubDAO
import org.broadinstitute.dsde.workbench.model.WorkbenchEmail
import org.mockito.{ArgumentMatcher, ArgumentMatchers}
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.scalatestplus.mockito.MockitoSugar
import spray.json.DefaultJsonProtocol._

import java.net.URLEncoder
import java.nio.charset.StandardCharsets.UTF_8
import scala.concurrent.{ExecutionContext, Future}

class BillingApiServiceSpec extends ApiServiceSpec with MockitoSugar {
  import org.broadinstitute.dsde.rawls.model.UserAuthJsonSupport._

  case class TestApiService(dataSource: SlickDataSource, gcsDAO: MockGoogleServicesDAO, gpsDAO: MockGooglePubSubDAO)(
    implicit override val executionContext: ExecutionContext
  ) extends ApiServices
      with MockUserInfoDirectives {
    override val samDAO: SamDAO = mock[SamDAO](RETURNS_SMART_NULLS)
    when(
      samDAO.userHasAction(ArgumentMatchers.eq(SamResourceTypeNames.billingProject),
                           any[String],
                           any[SamResourceAction],
                           any[RawlsRequestContext]
      )
    ).thenReturn(Future.successful(true))
    when(
      samDAO.addUserToPolicy(ArgumentMatchers.eq(SamResourceTypeNames.billingProject),
                             any[String],
                             any[SamResourcePolicyName],
                             any[String],
                             any[RawlsRequestContext]
      )
    ).thenReturn(Future.successful(()))
    when(
      samDAO.removeUserFromPolicy(ArgumentMatchers.eq(SamResourceTypeNames.billingProject),
                                  any[String],
                                  any[SamResourcePolicyName],
                                  any[String],
                                  any[RawlsRequestContext]
      )
    ).thenReturn(Future.successful(()))
  }

  def withApiServices[T](dataSource: SlickDataSource,
                         gcsDAO: MockGoogleServicesDAO = new MockGoogleServicesDAO("test")
  )(testCode: TestApiService => T): T = {
    val apiService = new TestApiService(dataSource, gcsDAO, new MockGooglePubSubDAO)
    try
      testCode(apiService)
    finally
      apiService.cleanupSupervisor
  }

  def withTestDataApiServices[T](testCode: TestApiService => T): T =
    withDefaultTestDatabase { dataSource: SlickDataSource =>
      withApiServices(dataSource)(testCode)
    }

  def createProject(project: RawlsBillingProject, owner: RawlsUser = testData.userOwner): Unit = {
    val projectWithOwner = project.copy()

    runAndWait(rawlsBillingProjectQuery.create(projectWithOwner))
  }

  "BillingApiService" should "return 200 when adding a user to a billing project" in withTestDataApiServices {
    services =>
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

    when(
      services.samDAO.userHasAction(
        ArgumentMatchers.eq(SamResourceTypeNames.billingProject),
        ArgumentMatchers.eq(project.projectName.value),
        any[SamResourceAction],
        any[RawlsRequestContext]
      )
    ).thenReturn(Future.successful(false))

    withStatsD {
      Put(
        s"/billing/${project.projectName.value}/user/${testData.userReader.userEmail.value}"
      ) ~> services.sealedInstrumentedRoutes ~>
        check {
          assertResult(StatusCodes.Forbidden) {
            status
          }
        }

      Put(
        s"/billing/${project.projectName.value}/owner/${testData.userReader.userEmail.value}"
      ) ~> services.sealedInstrumentedRoutes ~>
        check {
          assertResult(StatusCodes.Forbidden) {
            status
          }
        }
    } { capturedMetrics =>
      val expected =
        expectedHttpRequestMetrics("put", s"billing.redacted.redacted.redacted", StatusCodes.Forbidden.intValue, 2)
      assertSubsetOf(expected, capturedMetrics)
    }
  }

  it should "return 400 when adding a nonexistent user to a billing project" in withTestDataApiServices { services =>
    val project = billingProjectFromName("no_access")

    when(
      services.samDAO.addUserToPolicy(
        ArgumentMatchers.eq(SamResourceTypeNames.billingProject),
        ArgumentMatchers.eq(project.projectName.value),
        any[SamResourcePolicyName],
        ArgumentMatchers.eq("nobody"),
        any[RawlsRequestContext]
      )
    ).thenReturn(
      Future.failed(new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.BadRequest, "user not found")))
    )

    Put(s"/billing/${project.projectName.value}/user/nobody") ~>
      sealRoute(services.billingRoutes) ~>
      check {
        assertResult(StatusCodes.BadRequest) {
          status
        }
      }
  }

  it should "return 403 when adding a user to a nonexistent project" in withTestDataApiServices { services =>
    when(
      services.samDAO.userHasAction(ArgumentMatchers.eq(SamResourceTypeNames.billingProject),
                                    ArgumentMatchers.eq("missing_project"),
                                    any[SamResourceAction],
                                    any[RawlsRequestContext]
      )
    ).thenReturn(Future.successful(false))

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
      Delete(
        s"/billing/${project.projectName.value}/user/${testData.userWriter.userEmail.value}"
      ) ~> services.sealedInstrumentedRoutes ~>
        check {
          assertResult(StatusCodes.OK) {
            status
          }
        }
    } { capturedMetrics =>
      val expected =
        expectedHttpRequestMetrics("delete", s"billing.redacted.redacted.redacted", StatusCodes.OK.intValue, 1)
      assertSubsetOf(expected, capturedMetrics)
    }
  }

  it should "return 403 when removing a user from a non-owned billing project" in withTestDataApiServices { services =>
    val project = billingProjectFromName("no_access")
    when(
      services.samDAO.userHasAction(
        ArgumentMatchers.eq(SamResourceTypeNames.billingProject),
        ArgumentMatchers.eq(project.projectName.value),
        any[SamResourceAction],
        any[RawlsRequestContext]
      )
    ).thenReturn(Future.successful(false))

    Delete(s"/billing/${project.projectName.value}/owner/${testData.userWriter.userEmail.value}") ~>
      sealRoute(services.billingRoutes) ~>
      check {
        assertResult(StatusCodes.Forbidden) {
          status
        }
      }
  }

  it should "return 400 when removing a nonexistent user from a billing project" in withTestDataApiServices {
    services =>
      val project = billingProjectFromName("test_good")
      when(
        services.samDAO.removeUserFromPolicy(
          ArgumentMatchers.eq(SamResourceTypeNames.billingProject),
          ArgumentMatchers.eq(project.projectName.value),
          any[SamResourcePolicyName],
          ArgumentMatchers.eq("nobody"),
          any[RawlsRequestContext]
        )
      ).thenReturn(Future.failed(new RawlsExceptionWithErrorReport(ErrorReport(StatusCodes.BadRequest, ""))))

      Delete(s"/billing/${project.projectName.value}/user/nobody") ~>
        sealRoute(services.billingRoutes) ~>
        check {
          assertResult(StatusCodes.NotFound) {
            status
          }
        }
  }

  it should "return 403 when removing a user from a nonexistent billing project" in withTestDataApiServices {
    services =>
      when(
        services.samDAO.userHasAction(ArgumentMatchers.eq(SamResourceTypeNames.billingProject),
                                      ArgumentMatchers.eq("missing_project"),
                                      any[SamResourceAction],
                                      any[RawlsRequestContext]
        )
      ).thenReturn(Future.successful(false))
      Delete(s"/billing/missing_project/user/${testData.userOwner.userEmail.value}") ~>
        sealRoute(services.billingRoutes) ~>
        check {
          assertResult(StatusCodes.Forbidden) {
            status
          }
        }
  }

  it should "return 204 when creating a project with accessible billing account" in withTestDataApiServices {
    services =>
      val projectName = RawlsBillingProjectName("test_good")

      mockPositiveBillingProjectCreation(services, projectName)

      Post("/billing",
           CreateRawlsBillingProjectFullRequest(projectName,
                                                services.gcsDAO.accessibleBillingAccountName,
                                                None,
                                                None,
                                                None,
                                                None
           )
      ) ~>
        sealRoute(services.billingRoutes) ~>
        check {
          assertResult(StatusCodes.Created) {
            status
          }

          import driver.api._
          val query: ReadAction[Seq[RawlsBillingProjectRecord]] =
            rawlsBillingProjectQuery.filter(_.projectName === projectName.value).result
          val projects: Seq[RawlsBillingProjectRecord] = runAndWait(query)
          projects match {
            case Seq()        => fail("project does not exist in db")
            case Seq(project) =>
            case _            => fail("too many projects")
          }
        }
  }

  it should "rollback billing project inserts when there is a google error" in withDefaultTestDatabase {
    dataSource: SlickDataSource =>
      withApiServices(
        dataSource,
        new MockGoogleServicesDAO("test") {
          override def createProject(projectName: GoogleProjectId,
                                     billingAccount: RawlsBillingAccount,
                                     dmTemplatePath: String,
                                     highSecurityNetwork: Boolean,
                                     enableFlowLogs: Boolean,
                                     privateIpGoogleAccess: Boolean,
                                     requesterPaysRole: String,
                                     ownerGroupEmail: WorkbenchEmail,
                                     computeUserGroupEmail: WorkbenchEmail,
                                     projectTemplate: ProjectTemplate,
                                     parentFolderId: Option[String]
          ): Future[RawlsBillingProjectOperationRecord] =
            Future.failed(new Exception("test exception"))
        }
      ) { services =>
        val projectName = RawlsBillingProjectName("test_good2")

        Post("/billing",
             CreateRawlsBillingProjectFullRequest(projectName,
                                                  services.gcsDAO.accessibleBillingAccountName,
                                                  None,
                                                  None,
                                                  None,
                                                  None
             )
        ) ~>
          sealRoute(services.billingRoutes) ~>
          check {
            assertResult(StatusCodes.InternalServerError) {
              status
            }
            runAndWait(rawlsBillingProjectQuery.load(projectName)) map { p =>
              fail("did not rollback project inserts: " + p)
            }
          }
      }
  }

  it should "return 400 when creating a project with inaccessible to firecloud billing account" in withTestDataApiServices {
    services =>
      Post(
        "/billing",
        CreateRawlsBillingProjectFullRequest(RawlsBillingProjectName("test_bad1"),
                                             services.gcsDAO.inaccessibleBillingAccountName,
                                             None,
                                             None,
                                             None,
                                             None
        )
      ) ~>
        sealRoute(services.billingRoutes) ~>
        check {
          assertResult(StatusCodes.BadRequest) {
            status
          }
        }
  }

  it should "return 403 when creating a project with inaccessible to user billing account" in withTestDataApiServices {
    services =>
      Post(
        "/billing",
        CreateRawlsBillingProjectFullRequest(RawlsBillingProjectName("test_bad1"),
                                             RawlsBillingAccountName("this does not exist"),
                                             None,
                                             None,
                                             None,
                                             None
        )
      ) ~>
        sealRoute(services.billingRoutes) ~>
        check {
          assertResult(StatusCodes.Forbidden) {
            status
          }
        }
  }

  it should "return 400 when creating a project with enableFlowLogs but not highSecurityNetwork" in withTestDataApiServices {
    services =>
      Post(
        "/billing",
        CreateRawlsBillingProjectFullRequest(RawlsBillingProjectName("test_good"),
                                             services.gcsDAO.accessibleBillingAccountName,
                                             highSecurityNetwork = Some(false),
                                             enableFlowLogs = Some(true),
                                             None,
                                             None
        )
      ) ~>
        sealRoute(services.billingRoutes) ~>
        check {
          assertResult(StatusCodes.BadRequest) {
            status
          }
        }
  }

  it should "return 400 when creating a project with privateIpGoogleAccess but not highSecurityNetwork" in withTestDataApiServices {
    services =>
      Post(
        "/billing",
        CreateRawlsBillingProjectFullRequest(
          RawlsBillingProjectName("test_good"),
          services.gcsDAO.accessibleBillingAccountName,
          highSecurityNetwork = Some(false),
          None,
          privateIpGoogleAccess = Some(true),
          None
        )
      ) ~>
        sealRoute(services.billingRoutes) ~>
        check {
          assertResult(StatusCodes.BadRequest) {
            status
          }
        }
  }

  it should "return 400 when creating a project with a name that is too short" in withTestDataApiServices { services =>
    Post("/billing",
         CreateRawlsBillingProjectFullRequest(RawlsBillingProjectName("short"),
                                              services.gcsDAO.accessibleBillingAccountName,
                                              None,
                                              None,
                                              None,
                                              None
         )
    ) ~>
      sealRoute(services.billingRoutes) ~>
      check {
        assertResult(StatusCodes.BadRequest) {
          status
        }
      }
  }

  it should "return 400 when creating a project with a name that is too long" in withTestDataApiServices { services =>
    Post(
      "/billing",
      CreateRawlsBillingProjectFullRequest(RawlsBillingProjectName("longlonglonglonglonglonglonglonglonglong"),
                                           services.gcsDAO.accessibleBillingAccountName,
                                           None,
                                           None,
                                           None,
                                           None
      )
    ) ~>
      sealRoute(services.billingRoutes) ~>
      check {
        assertResult(StatusCodes.BadRequest) {
          status
        }
      }
  }

  it should "return 400 when creating a project with a name that contains invalid characters" in withTestDataApiServices {
    services =>
      Post(
        "/billing",
        CreateRawlsBillingProjectFullRequest(RawlsBillingProjectName("!@#$%^&*()=+,. "),
                                             services.gcsDAO.accessibleBillingAccountName,
                                             None,
                                             None,
                                             None,
                                             None
        )
      ) ~>
        sealRoute(services.billingRoutes) ~>
        check {
          assertResult(StatusCodes.BadRequest) {
            status
          }
        }
  }

  private def mockPositiveBillingProjectCreation(services: TestApiService,
                                                 projectName: RawlsBillingProjectName
  ): Unit = {
    when(
      services.samDAO.createResource(ArgumentMatchers.eq(SamResourceTypeNames.billingProject),
                                     ArgumentMatchers.eq(projectName.value),
                                     any[RawlsRequestContext]
      )
    ).thenReturn(Future.successful(()))
    when(
      services.samDAO.createResourceFull(
        ArgumentMatchers.eq(SamResourceTypeNames.googleProject),
        ArgumentMatchers.eq(projectName.value),
        ArgumentMatchers.eq(Map.empty),
        ArgumentMatchers.eq(Set.empty),
        any[RawlsRequestContext],
        ArgumentMatchers.eq(
          Option(SamFullyQualifiedResourceId(projectName.value, SamResourceTypeNames.billingProject.value))
        )
      )
    ).thenReturn(
      Future.successful(
        SamCreateResourceResponse(SamResourceTypeNames.billingProject.value, projectName.value, Set.empty, Set.empty)
      )
    )

    when(
      services.samDAO.overwritePolicy(
        ArgumentMatchers.eq(SamResourceTypeNames.billingProject),
        ArgumentMatchers.eq(projectName.value),
        ArgumentMatchers.eq(SamBillingProjectPolicyNames.workspaceCreator),
        ArgumentMatchers.eq(SamPolicy(Set.empty, Set.empty, Set(SamBillingProjectRoles.workspaceCreator))),
        any[RawlsRequestContext]
      )
    ).thenReturn(Future.successful(()))
    when(
      services.samDAO.overwritePolicy(
        ArgumentMatchers.eq(SamResourceTypeNames.billingProject),
        ArgumentMatchers.eq(projectName.value),
        ArgumentMatchers.eq(SamBillingProjectPolicyNames.canComputeUser),
        ArgumentMatchers.eq(
          SamPolicy(Set.empty,
                    Set.empty,
                    Set(SamBillingProjectRoles.batchComputeUser, SamBillingProjectRoles.notebookUser)
          )
        ),
        any[RawlsRequestContext]
      )
    ).thenReturn(Future.successful(()))
    when(
      services.samDAO.syncPolicyToGoogle(
        ArgumentMatchers.eq(SamResourceTypeNames.billingProject),
        ArgumentMatchers.eq(projectName.value),
        ArgumentMatchers.eq(SamBillingProjectPolicyNames.owner)
      )
    ).thenReturn(Future.successful(Map(WorkbenchEmail("owner-policy@google.group") -> Seq())))
    when(
      services.samDAO.syncPolicyToGoogle(
        ArgumentMatchers.eq(SamResourceTypeNames.billingProject),
        ArgumentMatchers.eq(projectName.value),
        ArgumentMatchers.eq(SamBillingProjectPolicyNames.canComputeUser)
      )
    ).thenReturn(Future.successful(Map(WorkbenchEmail("can-compute-policy@google.group") -> Seq())))
  }

  it should "return 201 when creating a project with a highSecurityNetwork" in withTestDataApiServices { services =>
    val projectName = RawlsBillingProjectName("test_good")
    mockPositiveBillingProjectCreation(services, projectName)

    Post(
      "/billing",
      CreateRawlsBillingProjectFullRequest(projectName,
                                           services.gcsDAO.accessibleBillingAccountName,
                                           highSecurityNetwork = Some(true),
                                           None,
                                           None,
                                           None
      )
    ) ~>
      sealRoute(services.billingRoutes) ~>
      check {
        assertResult(StatusCodes.Created) {
          status
        }
      }
  }

  it should "return 201 when creating a project with a highSecurityNetwork and enableFlowLogs turned on" in withTestDataApiServices {
    services =>
      val projectName = RawlsBillingProjectName("test_good")
      mockPositiveBillingProjectCreation(services, projectName)

      Post(
        "/billing",
        CreateRawlsBillingProjectFullRequest(projectName,
                                             services.gcsDAO.accessibleBillingAccountName,
                                             highSecurityNetwork = Some(true),
                                             enableFlowLogs = Some(true),
                                             None,
                                             None
        )
      ) ~>
        sealRoute(services.billingRoutes) ~>
        check {
          assertResult(StatusCodes.Created) {
            status
          }
        }
  }

  it should "return 201 when creating a project with a highSecurityNetwork with privateIpGoogleAccess turned on" in withTestDataApiServices {
    services =>
      val projectName = RawlsBillingProjectName("test_good")
      mockPositiveBillingProjectCreation(services, projectName)

      Post(
        "/billing",
        CreateRawlsBillingProjectFullRequest(projectName,
                                             services.gcsDAO.accessibleBillingAccountName,
                                             highSecurityNetwork = Some(true),
                                             None,
                                             privateIpGoogleAccess = Some(true),
                                             None
        )
      ) ~>
        sealRoute(services.billingRoutes) ~>
        check {
          assertResult(StatusCodes.Created) {
            status
          }
        }
  }

  it should "return 201 when creating a project with a highSecurityNetwork with enableFlowLogs and privateIpGoogleAccess turned on" in withTestDataApiServices {
    services =>
      val projectName = RawlsBillingProjectName("test_good")
      mockPositiveBillingProjectCreation(services, projectName)

      Post(
        "/billing",
        CreateRawlsBillingProjectFullRequest(
          projectName,
          services.gcsDAO.accessibleBillingAccountName,
          highSecurityNetwork = Some(true),
          enableFlowLogs = Some(true),
          privateIpGoogleAccess = Some(true),
          None
        )
      ) ~>
        sealRoute(services.billingRoutes) ~>
        check {
          assertResult(StatusCodes.Created) {
            status
          }
        }
  }

  it should "return 200 when listing billing project members as owner" in withTestDataApiServices { services =>
    val project = billingProjectFromName("test_good")

    when(
      services.samDAO.listUserActionsForResource(ArgumentMatchers.eq(SamResourceTypeNames.billingProject),
                                                 ArgumentMatchers.eq(project.projectName.value),
                                                 any[RawlsRequestContext]
      )
    ).thenReturn(Future.successful(Set(SamBillingProjectActions.readPolicies)))

    when(
      services.samDAO.listPoliciesForResource(ArgumentMatchers.eq(SamResourceTypeNames.billingProject),
                                              ArgumentMatchers.eq(project.projectName.value),
                                              any[RawlsRequestContext]
      )
    ).thenReturn(
      Future.successful(
        Set(
          model.SamPolicyWithNameAndEmail(SamBillingProjectPolicyNames.owner,
                                          SamPolicy(Set(WorkbenchEmail(testData.userOwner.userEmail.value)),
                                                    Set.empty,
                                                    Set.empty
                                          ),
                                          WorkbenchEmail("")
          ),
          model.SamPolicyWithNameAndEmail(SamBillingProjectPolicyNames.workspaceCreator,
                                          SamPolicy(Set.empty, Set.empty, Set.empty),
                                          WorkbenchEmail("")
          )
        )
      )
    )

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

  it should "return 200 when listing billing project members as non-owner" in withTestDataApiServices { services =>
    val project = billingProjectFromName("no_access")

    when(
      services.samDAO.listUserActionsForResource(ArgumentMatchers.eq(SamResourceTypeNames.billingProject),
                                                 ArgumentMatchers.eq(project.projectName.value),
                                                 any[RawlsRequestContext]
      )
    ).thenReturn(Future.successful(Set(SamBillingProjectActions.readPolicy(SamBillingProjectPolicyNames.owner))))
    when(
      services.samDAO.userHasAction(
        ArgumentMatchers.eq(SamResourceTypeNames.billingProject),
        ArgumentMatchers.eq(project.projectName.value),
        any[SamResourceAction],
        any[RawlsRequestContext]
      )
    ).thenReturn(Future.successful(false))
    when(
      services.samDAO.getPolicy(
        ArgumentMatchers.eq(SamResourceTypeNames.billingProject),
        ArgumentMatchers.eq(project.projectName.value),
        ArgumentMatchers.eq(SamBillingProjectPolicyNames.owner),
        any[RawlsRequestContext]
      )
    ).thenReturn(
      Future.successful(SamPolicy(Set(WorkbenchEmail(testData.userOwner.userEmail.value)), Set.empty, Set.empty))
    )

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

  it should "return 204 when adding a billing project to a service perimeter with all the right permissions" in withTestDataApiServices {
    services =>
      val projectName = testData.billingProject.projectName
      val servicePerimeterName = ServicePerimeterName("accessPolicies/123/servicePerimeters/service_perimeter")
      val encodedServicePerimeterName = URLEncoder.encode(servicePerimeterName.value, UTF_8.name)

      when(
        services.samDAO.userHasAction(
          ArgumentMatchers.eq(SamResourceTypeNames.servicePerimeter),
          ArgumentMatchers.eq(encodedServicePerimeterName),
          ArgumentMatchers.eq(SamServicePerimeterActions.addProject),
          ArgumentMatchers.argThat(userInfoEq(testContext))
        )
      ).thenReturn(Future.successful(true))

      Put(s"/servicePerimeters/${encodedServicePerimeterName}/projects/${projectName.value}") ~>
        sealRoute(services.servicePerimeterRoutes) ~>
        check {
          assertResult(StatusCodes.NoContent) {
            status
          }
        }
  }

  it should "return 403 when adding a billing project to a service perimeter without the right permission on the billing project" in withTestDataApiServices {
    services =>
      val projectName = testData.billingProject.projectName
      val servicePerimeterName = ServicePerimeterName("accessPolicies/123/servicePerimeters/service_perimeter")
      val encodedServicePerimeterName = URLEncoder.encode(servicePerimeterName.value, UTF_8.name)

      when(
        services.samDAO.userHasAction(
          ArgumentMatchers.eq(SamResourceTypeNames.servicePerimeter),
          ArgumentMatchers.eq(encodedServicePerimeterName),
          ArgumentMatchers.eq(SamServicePerimeterActions.addProject),
          ArgumentMatchers.argThat(userInfoEq(testContext))
        )
      ).thenReturn(Future.successful(true))
      when(
        services.samDAO.userHasAction(
          ArgumentMatchers.eq(SamResourceTypeNames.billingProject),
          ArgumentMatchers.eq(projectName.value),
          ArgumentMatchers.eq(SamBillingProjectActions.addToServicePerimeter),
          ArgumentMatchers.argThat(userInfoEq(testContext))
        )
      ).thenReturn(Future.successful(false))

      Put(s"/servicePerimeters/${encodedServicePerimeterName}/projects/${projectName.value}") ~>
        sealRoute(services.servicePerimeterRoutes) ~>
        check {
          assertResult(StatusCodes.Forbidden) {
            status
          }
        }
  }

  it should "return 404 when adding a billing project to a service perimeter without the right permission on the service perimeter" in withTestDataApiServices {
    services =>
      val projectName = testData.billingProject.projectName
      val servicePerimeterName = ServicePerimeterName("accessPolicies/123/servicePerimeters/service_perimeter")
      val encodedServicePerimeterName = URLEncoder.encode(servicePerimeterName.value, UTF_8.name)

      when(
        services.samDAO.userHasAction(
          ArgumentMatchers.eq(SamResourceTypeNames.servicePerimeter),
          ArgumentMatchers.eq(encodedServicePerimeterName),
          ArgumentMatchers.eq(SamServicePerimeterActions.addProject),
          ArgumentMatchers.argThat(userInfoEq(testContext))
        )
      ).thenReturn(Future.successful(false))

      Put(s"/servicePerimeters/${encodedServicePerimeterName}/projects/${projectName.value}") ~>
        sealRoute(services.servicePerimeterRoutes) ~>
        check {
          assertResult(StatusCodes.NotFound) {
            status
          }
        }
  }
}
