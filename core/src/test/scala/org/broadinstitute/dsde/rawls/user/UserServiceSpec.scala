package org.broadinstitute.dsde.rawls.user

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import bio.terra.profile.client.ApiException
import bio.terra.profile.model.{BpmApiPolicyInput, BpmApiPolicyInputs, CloudPlatform => BPMCloudPlatform, ProfileModel}
import com.google.api.client.http.{HttpHeaders, HttpResponseException}
import com.google.api.services.cloudresourcemanager.model.Project
import com.typesafe.config.{Config, ConfigFactory}
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.billing.BillingProfileManagerDAO.ProfilePolicy
import org.broadinstitute.dsde.rawls.billing.{BillingProfileManagerDAO, BillingRepository}
import org.broadinstitute.dsde.rawls.dataaccess._
import org.broadinstitute.dsde.rawls.dataaccess.slick.TestDriverComponent
import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.WorkspaceManagerDAO
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.serviceperimeter.ServicePerimeterService
import org.broadinstitute.dsde.workbench.dataaccess.NotificationDAO
import org.broadinstitute.dsde.workbench.model.google.{BigQueryDatasetName, BigQueryTableName, GoogleProject}
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

import java.net.URLEncoder
import java.nio.charset.StandardCharsets.UTF_8
import java.util.UUID
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.jdk.CollectionConverters._

class UserServiceSpec
    extends AnyFlatSpecLike
    with TestDriverComponent
    with MockitoSugar
    with BeforeAndAfterAll
    with Matchers
    with ScalaFutures {
  import driver.api._

  val defaultServicePerimeterName: ServicePerimeterName = ServicePerimeterName(
    "accessPolicies/policyName/servicePerimeters/servicePerimeterName"
  )
  val urlEncodedDefaultServicePerimeterName: String = URLEncoder.encode(defaultServicePerimeterName.value, UTF_8.name)
  val defaultGoogleProjectNumber: GoogleProjectNumber = GoogleProjectNumber("42")
  val defaultBillingProjectName: RawlsBillingProjectName = RawlsBillingProjectName("test-bp")
  val defaultBillingProject: RawlsBillingProject = RawlsBillingProject(defaultBillingProjectName,
                                                                       CreationStatuses.Ready,
                                                                       None,
                                                                       None,
                                                                       googleProjectNumber =
                                                                         Option(defaultGoogleProjectNumber)
  )
  val defaultMockSamDAO: SamDAO = mock[SamDAO](RETURNS_SMART_NULLS)
  val defaultMockGcsDAO: GoogleServicesDAO = new MockGoogleServicesDAO("test")
  val defaultMockServicePerimeterService: ServicePerimeterService = mock[ServicePerimeterService](RETURNS_SMART_NULLS)
  val defaultBillingProfileManagerDAO: BillingProfileManagerDAO = mock[BillingProfileManagerDAO](RETURNS_SMART_NULLS)
  val testConf: Config = ConfigFactory.load()

  implicit override val patienceConfig: PatienceConfig = PatienceConfig(1.second)

  override def beforeAll(): Unit = {
    when(
      defaultMockSamDAO.userHasAction(SamResourceTypeNames.servicePerimeter,
                                      urlEncodedDefaultServicePerimeterName,
                                      SamServicePerimeterActions.addProject,
                                      testContext
      )
    ).thenReturn(Future.successful(true))
    when(
      defaultMockSamDAO.userHasAction(SamResourceTypeNames.billingProject,
                                      defaultBillingProjectName.value,
                                      SamBillingProjectActions.addToServicePerimeter,
                                      testContext
      )
    ).thenReturn(Future.successful(true))
  }

  def getUserService(dataSource: SlickDataSource = mock[SlickDataSource],
                     samDAO: SamDAO = defaultMockSamDAO,
                     gcsDAO: GoogleServicesDAO = defaultMockGcsDAO,
                     servicePerimeterService: ServicePerimeterService = defaultMockServicePerimeterService,
                     adminRegisterBillingAccountId: RawlsBillingAccountName = RawlsBillingAccountName(
                       "billingAccounts/ABCDE-FGHIJ-KLMNO"
                     ),
                     bpmDAO: BillingProfileManagerDAO = defaultBillingProfileManagerDAO,
                     workspaceManagerDao: WorkspaceManagerDAO = mock[WorkspaceManagerDAO](RETURNS_SMART_NULLS),
                     billingRepository: Option[BillingRepository] = None,
                     workspaceMonitorRecordDao: Option[WorkspaceManagerResourceMonitorRecordDao] = None
  ): UserService =
    new UserService(
      testContext,
      dataSource,
      gcsDAO,
      samDAO,
      MockBigQueryServiceFactory.ioFactory(),
      testConf.getString("gcs.pathToCredentialJson"),
      "",
      servicePerimeterService,
      adminRegisterBillingAccountId: RawlsBillingAccountName,
      workspaceManagerDao,
      bpmDAO,
      billingRepository.getOrElse(new BillingRepository(dataSource)),
      workspaceMonitorRecordDao.getOrElse(new WorkspaceManagerResourceMonitorRecordDao(dataSource)),
      mock[NotificationDAO]
    )

  // 204 when project exists without perimeter and user is owner of project and has right permissions on service-perimeter
  "UserService" should "add a service perimeter field for an existing project when user has correct permissions" in {
    withEmptyTestDatabase { dataSource: SlickDataSource =>
      val project = defaultBillingProject
      runAndWait(rawlsBillingProjectQuery.create(project))

      val mockServicePerimeterService = mock[ServicePerimeterService](RETURNS_SMART_NULLS)
      when(
        mockServicePerimeterService.overwriteGoogleProjectsInPerimeter(defaultServicePerimeterName,
                                                                       dataSource.dataAccess
        )
      ).thenReturn(DBIO.successful(()))

      val userService = getUserService(dataSource, servicePerimeterService = mockServicePerimeterService)

      val actual =
        userService.addProjectToServicePerimeter(defaultServicePerimeterName, project.projectName).futureValue
      val expected = ()
      actual shouldEqual expected
      verify(mockServicePerimeterService).overwriteGoogleProjectsInPerimeter(defaultServicePerimeterName,
                                                                             dataSource.dataAccess
      )

      val updatedProject = dataSource
        .inTransaction { dataAccess =>
          dataAccess.rawlsBillingProjectQuery.load(project.projectName)
        }
        .futureValue
        .getOrElse(fail(s"Project ${project.projectName} not found"))

      updatedProject.servicePerimeter shouldBe Option(defaultServicePerimeterName)
    }
  }

  // 204 when all of the above even if project doesn't have a google project number
  it should "add a service perimeter field and update the status for an existing project when user has correct permissions even if there isn't a project number already" in {
    withEmptyTestDatabase { dataSource: SlickDataSource =>
      val project = defaultBillingProject.copy(googleProjectNumber = None)

      runAndWait(rawlsBillingProjectQuery.create(project))

      val mockGcsDAO = mock[GoogleServicesDAO](RETURNS_SMART_NULLS)
      val googleProjectNumber = GoogleProjectNumber("42")
      val googleProject = new Project().setProjectNumber(googleProjectNumber.value.toLong)
      when(mockGcsDAO.getGoogleProject(project.googleProjectId)).thenReturn(Future.successful(googleProject))
      val folderId = "folders/1234567"
      when(mockGcsDAO.getFolderId(defaultServicePerimeterName.value.split("/").last))
        .thenReturn(Future.successful(Option(folderId)))
      when(mockGcsDAO.addProjectToFolder(project.googleProjectId, folderId)).thenReturn(Future.successful(()))
      when(mockGcsDAO.getGoogleProjectNumber(googleProject)).thenReturn(googleProjectNumber)

      val mockServicePerimeterService = mock[ServicePerimeterService](RETURNS_SMART_NULLS)
      when(
        mockServicePerimeterService.overwriteGoogleProjectsInPerimeter(defaultServicePerimeterName,
                                                                       dataSource.dataAccess
        )
      ).thenReturn(DBIO.successful(()))

      val userService =
        getUserService(dataSource, gcsDAO = mockGcsDAO, servicePerimeterService = mockServicePerimeterService)

      val actual =
        userService.addProjectToServicePerimeter(defaultServicePerimeterName, project.projectName).futureValue
      val expected = ()
      actual shouldEqual expected
      verify(mockServicePerimeterService).overwriteGoogleProjectsInPerimeter(defaultServicePerimeterName,
                                                                             dataSource.dataAccess
      )

      val updatedProject = dataSource
        .inTransaction { dataAccess =>
          dataAccess.rawlsBillingProjectQuery.load(project.projectName)
        }
        .futureValue
        .getOrElse(fail(s"Project ${project.projectName} not found"))

      updatedProject.servicePerimeter shouldBe Option(defaultServicePerimeterName)
      updatedProject.googleProjectNumber shouldBe Option(googleProjectNumber)
    }
  }

  // 400 when project has a perimeter already
  it should "fail with a 400 when the project already has a perimeter" in {
    withEmptyTestDatabase { dataSource: SlickDataSource =>
      val project = defaultBillingProject.copy(servicePerimeter =
        Option(ServicePerimeterName("accessPolicies/123/servicePerimeters/other_perimeter"))
      )
      runAndWait(rawlsBillingProjectQuery.create(project))

      val userService = getUserService(dataSource)

      val actual =
        userService.addProjectToServicePerimeter(defaultServicePerimeterName, project.projectName).failed.futureValue
      assert(actual.isInstanceOf[RawlsExceptionWithErrorReport])
      actual.asInstanceOf[RawlsExceptionWithErrorReport].errorReport.statusCode shouldEqual Option(
        StatusCodes.BadRequest
      )
    }
  }

  // 400 when project is not 'Ready'
  it should "fail with a 400 when the project's status is not 'Ready'" in {
    withEmptyTestDatabase { dataSource: SlickDataSource =>
      val project = defaultBillingProject.copy(status = CreationStatuses.Creating)
      runAndWait(rawlsBillingProjectQuery.create(project))

      val userService = getUserService(dataSource)

      val actual =
        userService.addProjectToServicePerimeter(defaultServicePerimeterName, project.projectName).failed.futureValue
      assert(actual.isInstanceOf[RawlsExceptionWithErrorReport])
      actual.asInstanceOf[RawlsExceptionWithErrorReport].errorReport.statusCode shouldEqual Option(
        StatusCodes.BadRequest
      )
    }
  }

  // 403 when user isn't owner of project or project dne
  it should "fail with a 403 when Sam says the user does not have permission on the billing project" in {
    withEmptyTestDatabase { dataSource: SlickDataSource =>
      val project = defaultBillingProject
      runAndWait(rawlsBillingProjectQuery.create(project))

      val mockSamDAO = mock[SamDAO](RETURNS_SMART_NULLS)
      when(
        mockSamDAO.userHasAction(SamResourceTypeNames.servicePerimeter,
                                 urlEncodedDefaultServicePerimeterName,
                                 SamServicePerimeterActions.addProject,
                                 testContext
        )
      ).thenReturn(Future.successful(true))
      when(
        mockSamDAO.userHasAction(SamResourceTypeNames.billingProject,
                                 project.projectName.value,
                                 SamBillingProjectActions.addToServicePerimeter,
                                 testContext
        )
      ).thenReturn(Future.successful(false))

      val userService = getUserService(dataSource, mockSamDAO)

      val actual =
        userService.addProjectToServicePerimeter(defaultServicePerimeterName, project.projectName).failed.futureValue
      assert(actual.isInstanceOf[RawlsExceptionWithErrorReport])
      actual.asInstanceOf[RawlsExceptionWithErrorReport].errorReport.statusCode shouldEqual Option(
        StatusCodes.Forbidden
      )
    }
  }

  // 404 when user doesn't have permissions on service-perimeter or s-p dne
  it should "fail with a 404 when Sam says the user does not have permission on the service perimeter" in {
    withEmptyTestDatabase { dataSource: SlickDataSource =>
      val project = defaultBillingProject
      runAndWait(rawlsBillingProjectQuery.create(project))

      val mockSamDAO = mock[SamDAO](RETURNS_SMART_NULLS)
      when(
        mockSamDAO.userHasAction(SamResourceTypeNames.servicePerimeter,
                                 urlEncodedDefaultServicePerimeterName,
                                 SamServicePerimeterActions.addProject,
                                 testContext
        )
      ).thenReturn(Future.successful(false))
      when(
        mockSamDAO.userHasAction(SamResourceTypeNames.billingProject,
                                 project.projectName.value,
                                 SamBillingProjectActions.addToServicePerimeter,
                                 testContext
        )
      ).thenReturn(Future.successful(true))

      val userService = getUserService(dataSource, mockSamDAO)

      val actual =
        userService.addProjectToServicePerimeter(defaultServicePerimeterName, project.projectName).failed.futureValue
      assert(actual.isInstanceOf[RawlsExceptionWithErrorReport])
      actual.asInstanceOf[RawlsExceptionWithErrorReport].errorReport.statusCode shouldEqual Option(StatusCodes.NotFound)
    }
  }

  // 200 when billing project is deleted
  it should "Successfully to delete a billing project" in {
    withEmptyTestDatabase { dataSource: SlickDataSource =>
      val project = defaultBillingProject
      val userIdInfo = UserIdInfo(userInfo.userSubjectId.value, userInfo.userEmail.value, Option("googleSubId"))
      val petSAJson = "petJson"
      runAndWait(rawlsBillingProjectQuery.create(project))

      val mockSamDAO = mock[SamDAO](RETURNS_SMART_NULLS)
      when(
        mockSamDAO.userHasAction(SamResourceTypeNames.billingProject,
                                 project.projectName.value,
                                 SamBillingProjectActions.deleteBillingProject,
                                 testContext
        )
      ).thenReturn(Future.successful(true))
      when(
        mockSamDAO.listAllResourceMemberIds(SamResourceTypeNames.billingProject, project.projectName.value, testContext)
      ).thenReturn(Future.successful(Set(userIdInfo)))
      when(mockSamDAO.getPetServiceAccountKeyForUser(project.googleProjectId, userInfo.userEmail))
        .thenReturn(Future.successful(petSAJson))
      when(mockSamDAO.listResourceChildren(SamResourceTypeNames.billingProject, project.projectName.value, testContext))
        .thenReturn(
          Future.successful(
            Seq(SamFullyQualifiedResourceId(project.googleProjectId.value, SamResourceTypeNames.googleProject.value))
          )
        )
      when(mockSamDAO.deleteUserPetServiceAccount(project.googleProjectId, testContext)).thenReturn(Future.successful())
      when(mockSamDAO.deleteResource(SamResourceTypeNames.billingProject, project.projectName.value, testContext))
        .thenReturn(Future.successful())
      when(mockSamDAO.deleteResource(SamResourceTypeNames.googleProject, project.googleProjectId.value, testContext))
        .thenReturn(Future.successful())

      val mockGcsDAO = mock[GoogleServicesDAO](RETURNS_SMART_NULLS)
      when(mockGcsDAO.getUserInfoUsingJson(petSAJson)).thenReturn(Future.successful(userInfo))
      when(mockGcsDAO.deleteV1Project(project.googleProjectId)).thenReturn(Future.successful())
      when(mockGcsDAO.getGoogleProject(project.googleProjectId)).thenReturn(Future.successful(new Project()))

      val userService = getUserService(dataSource, mockSamDAO, gcsDAO = mockGcsDAO)
      val actual = userService.deleteBillingProject(defaultBillingProjectName).futureValue

      verify(mockSamDAO).deleteUserPetServiceAccount(project.googleProjectId, testContext)
      verify(mockSamDAO).deleteResource(SamResourceTypeNames.billingProject, project.projectName.value, testContext)
      verify(mockSamDAO).deleteResource(SamResourceTypeNames.googleProject, project.googleProjectId.value, testContext)
      verify(mockGcsDAO).deleteV1Project(project.googleProjectId)

      runAndWait(rawlsBillingProjectQuery.load(defaultBillingProjectName)) shouldBe empty
      actual shouldEqual ()
    }
  }

  it should "Successfully to delete a billing project when the google project does not exist on GCP" in {
    withEmptyTestDatabase { dataSource: SlickDataSource =>
      val project = defaultBillingProject
      val userIdInfo = UserIdInfo(userInfo.userSubjectId.value, userInfo.userEmail.value, Option("googleSubId"))
      val petSAJson = "petJson"
      runAndWait(rawlsBillingProjectQuery.create(project))

      val mockSamDAO = mock[SamDAO](RETURNS_SMART_NULLS)
      when(
        mockSamDAO.userHasAction(SamResourceTypeNames.billingProject,
                                 project.projectName.value,
                                 SamBillingProjectActions.deleteBillingProject,
                                 testContext
        )
      ).thenReturn(Future.successful(true))
      when(
        mockSamDAO.listAllResourceMemberIds(SamResourceTypeNames.billingProject, project.projectName.value, testContext)
      ).thenReturn(Future.successful(Set(userIdInfo)))
      when(mockSamDAO.listResourceChildren(SamResourceTypeNames.billingProject, project.projectName.value, testContext))
        .thenReturn(
          Future.successful(
            Seq(SamFullyQualifiedResourceId(project.googleProjectId.value, SamResourceTypeNames.googleProject.value))
          )
        )
      when(mockSamDAO.deleteResource(SamResourceTypeNames.billingProject, project.projectName.value, testContext))
        .thenReturn(Future.successful())
      when(mockSamDAO.deleteResource(SamResourceTypeNames.googleProject, project.googleProjectId.value, testContext))
        .thenReturn(Future.successful())

      val mockGcsDAO = mock[GoogleServicesDAO](RETURNS_SMART_NULLS)
      when(mockGcsDAO.getGoogleProject(project.googleProjectId)).thenReturn(
        Future.failed(
          new HttpResponseException.Builder(404, "project not found", new HttpHeaders())
            .build()
        )
      )

      val userService = getUserService(dataSource, mockSamDAO, gcsDAO = mockGcsDAO)
      val actual = userService.deleteBillingProject(defaultBillingProjectName).futureValue

      verify(mockSamDAO, never()).deleteUserPetServiceAccount(project.googleProjectId, testContext)
      verify(mockSamDAO).deleteResource(SamResourceTypeNames.billingProject, project.projectName.value, testContext)
      verify(mockSamDAO).deleteResource(SamResourceTypeNames.googleProject, project.googleProjectId.value, testContext)
      verify(mockGcsDAO, never()).deleteV1Project(project.googleProjectId)

      runAndWait(rawlsBillingProjectQuery.load(defaultBillingProjectName)) shouldBe empty
      actual shouldEqual ()
    }
  }

  it should "fail with a 400 when workspace exists in this billing project to be deleted" in {
    withEmptyTestDatabase { dataSource: SlickDataSource =>
      val project = defaultBillingProject
      // A workspace with the which namespaceName equals to defaultBillingProject's billing project name.
      val workspace = Workspace(
        defaultBillingProjectName.value,
        testData.wsName.name,
        UUID.randomUUID().toString,
        "aBucket",
        Some("workflow-collection"),
        currentTime(),
        currentTime(),
        "test",
        Map.empty
      )

      runAndWait(rawlsBillingProjectQuery.create(project))
      runAndWait(workspaceQuery.createOrUpdate(workspace))

      val mockSamDAO = mock[SamDAO](RETURNS_SMART_NULLS)
      when(
        mockSamDAO.userHasAction(SamResourceTypeNames.billingProject,
                                 project.projectName.value,
                                 SamBillingProjectActions.deleteBillingProject,
                                 testContext
        )
      ).thenReturn(Future.successful(true))

      val userService = getUserService(dataSource, mockSamDAO)

      val actual = intercept[RawlsExceptionWithErrorReport] {
        Await.result(userService.deleteBillingProject(defaultBillingProjectName), Duration.Inf)
      }
      actual.errorReport.statusCode shouldEqual Option(StatusCodes.BadRequest)
    }
  }

  it should "fail with a 403 when Sam says the user does not have permission to delete billing project" in {
    withEmptyTestDatabase { dataSource: SlickDataSource =>
      val project = defaultBillingProject
      runAndWait(rawlsBillingProjectQuery.create(project))

      val mockSamDAO = mock[SamDAO](RETURNS_SMART_NULLS)
      when(
        mockSamDAO.userHasAction(SamResourceTypeNames.billingProject,
                                 defaultBillingProjectName.value,
                                 SamBillingProjectActions.deleteBillingProject,
                                 testContext
        )
      ).thenReturn(Future.successful(false))

      val userService = getUserService(dataSource, mockSamDAO)

      val actual = intercept[RawlsExceptionWithErrorReport] {
        Await.result(userService.deleteBillingProject(defaultBillingProjectName), Duration.Inf)
      }
      actual.errorReport.statusCode shouldEqual Option(StatusCodes.Forbidden)
    }
  }

  it should "not delete the Google project when unregistering a Billing project" in {
    withEmptyTestDatabase { dataSource: SlickDataSource =>
      val project = defaultBillingProject
      val ownerInfoMap = Map("newOwnerEmail" -> userInfo.userEmail.value, "newOwnerToken" -> userInfo.accessToken.value)
      val ownerIdInfo = UserIdInfo(userInfo.userSubjectId.value, userInfo.userEmail.value, Option("googleSubId"))
      val ownerUserInfo = UserInfo(RawlsUserEmail(ownerInfoMap("newOwnerEmail")),
                                   OAuth2BearerToken(ownerInfoMap("newOwnerToken")),
                                   3600,
                                   RawlsUserSubjectId("0")
      )
      val petSAJson = "petJson"
      runAndWait(rawlsBillingProjectQuery.create(project))

      val mockSamDAO = mock[SamDAO](RETURNS_SMART_NULLS)
      when(
        mockSamDAO.listResourceChildren(ArgumentMatchers.eq(SamResourceTypeNames.billingProject),
                                        ArgumentMatchers.eq(project.projectName.value),
                                        any[RawlsRequestContext]
        )
      ).thenReturn(
        Future.successful(
          Seq(SamFullyQualifiedResourceId(project.googleProjectId.value, SamResourceTypeNames.googleProject.value))
        )
      )
      when(
        mockSamDAO.listAllResourceMemberIds(ArgumentMatchers.eq(SamResourceTypeNames.billingProject),
                                            ArgumentMatchers.eq(project.projectName.value),
                                            any[RawlsRequestContext]
        )
      ).thenReturn(Future.successful(Set(ownerIdInfo)))
      when(mockSamDAO.getPetServiceAccountKeyForUser(project.googleProjectId, ownerUserInfo.userEmail))
        .thenReturn(Future.successful(petSAJson))
      when(mockSamDAO.deleteUserPetServiceAccount(project.googleProjectId, testContext.copy(userInfo = ownerUserInfo)))
        .thenReturn(Future.successful())
      when(
        mockSamDAO.deleteResource(ArgumentMatchers.eq(SamResourceTypeNames.googleProject),
                                  ArgumentMatchers.eq(project.googleProjectId.value),
                                  any[RawlsRequestContext]
        )
      ).thenReturn(Future.successful())
      when(
        mockSamDAO.deleteResource(ArgumentMatchers.eq(SamResourceTypeNames.billingProject),
                                  ArgumentMatchers.eq(project.projectName.value),
                                  any[RawlsRequestContext]
        )
      ).thenReturn(Future.successful())

      val mockGcsDAO = mock[GoogleServicesDAO](RETURNS_SMART_NULLS)
      when(mockGcsDAO.isAdmin(any[String])).thenReturn(Future.successful(true))
      when(mockGcsDAO.getUserInfoUsingJson(petSAJson)).thenReturn(Future.successful(ownerUserInfo))
      when(mockGcsDAO.getGoogleProject(project.googleProjectId)).thenReturn(Future.successful(new Project()))
      when(mockGcsDAO.deleteV1Project(project.googleProjectId)).thenReturn(Future.successful())

      val userService = getUserService(dataSource, mockSamDAO, gcsDAO = mockGcsDAO)
      val actual = userService.adminUnregisterBillingProjectWithOwnerInfo(project.projectName, ownerInfoMap).futureValue

      verify(mockSamDAO).deleteUserPetServiceAccount(project.googleProjectId,
                                                     testContext.copy(userInfo = ownerUserInfo)
      )
      verify(mockSamDAO).deleteResource(SamResourceTypeNames.billingProject,
                                        project.projectName.value,
                                        RawlsRequestContext(ownerUserInfo)
      )
      verify(mockSamDAO).deleteResource(SamResourceTypeNames.googleProject,
                                        project.googleProjectId.value,
                                        RawlsRequestContext(ownerUserInfo)
      )
      verify(mockGcsDAO, never()).deleteV1Project(project.googleProjectId)

      runAndWait(rawlsBillingProjectQuery.load(defaultBillingProjectName)) shouldBe empty
      actual shouldEqual ()
    }
  }

  it should "set the spend configuration of a billing project when the user has permission" in {
    withMinimalTestDatabase { dataSource: SlickDataSource =>
      val billingProject = minimalTestData.billingProject
      val spendReportDatasetName = BigQueryDatasetName("test_dataset")
      val spendReportGoogleProject = GoogleProject("some_other_google_project")
      val spendReportConfiguration = BillingProjectSpendConfiguration(spendReportGoogleProject, spendReportDatasetName)

      val mockSamDAO = mock[SamDAO](RETURNS_SMART_NULLS)
      when(
        mockSamDAO.userHasAction(SamResourceTypeNames.billingProject,
                                 billingProject.projectName.value,
                                 SamBillingProjectActions.alterSpendReportConfiguration,
                                 testContext
        )
      ).thenReturn(Future.successful(true))

      val userService = getUserService(dataSource, mockSamDAO)

      Await.result(
        userService.setBillingProjectSpendConfiguration(billingProject.projectName, spendReportConfiguration),
        Duration.Inf
      ) shouldEqual 1

      val spendReportConfigInDb = runAndWait(
        dataSource.dataAccess.rawlsBillingProjectQuery
          .filter(_.projectName === billingProject.projectName.value)
          .map(row => (row.spendReportDataset, row.spendReportTable))
          .result
      )

      val spendReportTableName =
        s"gcp_billing_export_v1_${billingProject.billingAccount.get.value.stripPrefix("billingAccounts/").replace("-", "_")}"
      spendReportConfigInDb.head shouldEqual (Some(spendReportDatasetName.value), Some(spendReportTableName))
    }
  }

  it should "not set the spend configuration of a billing project when the user doesn't have permission" in {
    withMinimalTestDatabase { dataSource: SlickDataSource =>
      val billingProject = minimalTestData.billingProject
      val spendReportDatasetName = BigQueryDatasetName("test_dataset")
      val spendReportGoogleProject = GoogleProject("some_other_google_project")
      val spendReportConfiguration = BillingProjectSpendConfiguration(spendReportGoogleProject, spendReportDatasetName)

      val mockSamDAO = mock[SamDAO](RETURNS_SMART_NULLS)
      when(
        mockSamDAO.userHasAction(SamResourceTypeNames.billingProject,
                                 billingProject.projectName.value,
                                 SamBillingProjectActions.alterSpendReportConfiguration,
                                 testContext
        )
      ).thenReturn(Future.successful(false))

      val userService = getUserService(dataSource, mockSamDAO)

      val actual = intercept[RawlsExceptionWithErrorReport] {
        Await.result(
          userService.setBillingProjectSpendConfiguration(billingProject.projectName, spendReportConfiguration),
          Duration.Inf
        )
      }

      // assert that the entire action was forbidden
      actual.errorReport.statusCode.get shouldEqual StatusCodes.Forbidden

      val spendReportConfigInDb = runAndWait(
        dataSource.dataAccess.rawlsBillingProjectQuery
          .filter(_.projectName === billingProject.projectName.value)
          .map(row => (row.spendReportDataset, row.spendReportTable))
          .result
      )

      // assert that no change was made to the spend configuration
      spendReportConfigInDb.head shouldEqual (None, None)
    }
  }

  it should "clear the spend configuration of a billing project" in {
    withMinimalTestDatabase { dataSource: SlickDataSource =>
      val billingProject = minimalTestData.billingProject
      val spendReportDatasetName = BigQueryDatasetName("test_dataset")

      val mockSamDAO = mock[SamDAO](RETURNS_SMART_NULLS)
      when(
        mockSamDAO.userHasAction(SamResourceTypeNames.billingProject,
                                 billingProject.projectName.value,
                                 SamBillingProjectActions.alterSpendReportConfiguration,
                                 testContext
        )
      ).thenReturn(Future.successful(true))

      val userService = getUserService(dataSource, mockSamDAO)

      Await.result(userService.clearBillingProjectSpendConfiguration(billingProject.projectName),
                   Duration.Inf
      ) shouldEqual 1

      val spendReportConfigInDb = runAndWait(
        dataSource.dataAccess.rawlsBillingProjectQuery
          .filter(_.projectName === billingProject.projectName.value)
          .map(row => (row.spendReportDataset, row.spendReportTable))
          .result
      )

      spendReportConfigInDb.head shouldEqual (None, None)
    }
  }

  it should "not clear the spend configuration of a billing project when the user doesn't have permission" in {
    withMinimalTestDatabase { dataSource: SlickDataSource =>
      val billingProject = minimalTestData.billingProject
      val spendReportDatasetName = BigQueryDatasetName("should_not_clear_dataset")
      val spendReportTableName = BigQueryTableName("should_not_clear_table")
      val spendReportGoogleProject = GoogleProject("some_other_google_project")

      // first, directly set the spend configuration in the DB outside of the user's permissions, so we can assert that it wasn't cleared
      runAndWait(
        dataSource.dataAccess.rawlsBillingProjectQuery.setBillingProjectSpendConfiguration(
          billingProject.projectName,
          Some(spendReportDatasetName),
          Some(spendReportTableName),
          Some(spendReportGoogleProject)
        )
      )

      val mockSamDAO = mock[SamDAO](RETURNS_SMART_NULLS)
      when(
        mockSamDAO.userHasAction(SamResourceTypeNames.billingProject,
                                 billingProject.projectName.value,
                                 SamBillingProjectActions.alterSpendReportConfiguration,
                                 testContext
        )
      ).thenReturn(Future.successful(false))

      val userService = getUserService(dataSource, mockSamDAO)

      val actual = intercept[RawlsExceptionWithErrorReport] {
        Await.result(userService.clearBillingProjectSpendConfiguration(billingProject.projectName), Duration.Inf)
      }

      // assert that the entire action was forbidden
      actual.errorReport.statusCode.get shouldEqual StatusCodes.Forbidden

      val spendReportConfigInDb = runAndWait(
        dataSource.dataAccess.rawlsBillingProjectQuery
          .filter(_.projectName === billingProject.projectName.value)
          .map(row => (row.spendReportDataset, row.spendReportTable))
          .result
      )

      // assert that no change was made to the spend configuration
      spendReportConfigInDb.head shouldEqual (Some(spendReportDatasetName.value), Some(spendReportTableName.value))
    }
  }

  it should "throw a RawlsExceptionWithErrorReport when setting the spend configuration if the dataset does not exist or can't be accessed" in {
    withMinimalTestDatabase { dataSource: SlickDataSource =>
      val billingProject = minimalTestData.billingProject
      val spendReportDatasetName = BigQueryDatasetName("dataset_does_not_exist")
      val spendReportGoogleProject = GoogleProject("some_other_google_project")
      val spendReportConfiguration = BillingProjectSpendConfiguration(spendReportGoogleProject, spendReportDatasetName)

      val mockSamDAO = mock[SamDAO](RETURNS_SMART_NULLS)
      when(
        mockSamDAO.userHasAction(SamResourceTypeNames.billingProject,
                                 billingProject.projectName.value,
                                 SamBillingProjectActions.alterSpendReportConfiguration,
                                 testContext
        )
      ).thenReturn(Future.successful(true))

      val userService = getUserService(dataSource, mockSamDAO)

      val actual = intercept[RawlsExceptionWithErrorReport] {
        Await.result(
          userService.setBillingProjectSpendConfiguration(billingProject.projectName, spendReportConfiguration),
          Duration.Inf
        )
      }

      actual.errorReport.statusCode.get shouldEqual StatusCodes.BadRequest
    }
  }

  it should "throw a RawlsExceptionWithErrorReport when setting the spend configuration if the table does not exist or can't be accessed" in {
    withMinimalTestDatabase { dataSource: SlickDataSource =>
      val billingProject =
        RawlsBillingProject(RawlsBillingProjectName("project_without_table"), CreationStatuses.Ready, None, None)
      runAndWait(dataSource.dataAccess.rawlsBillingProjectQuery.create(billingProject))

      val spendReportDatasetName = BigQueryDatasetName("some_dataset")
      val spendReportGoogleProject = GoogleProject("some_other_google_project")
      val spendReportConfiguration = BillingProjectSpendConfiguration(spendReportGoogleProject, spendReportDatasetName)

      val mockSamDAO = mock[SamDAO](RETURNS_SMART_NULLS)
      when(
        mockSamDAO.userHasAction(SamResourceTypeNames.billingProject,
                                 billingProject.projectName.value,
                                 SamBillingProjectActions.alterSpendReportConfiguration,
                                 testContext
        )
      ).thenReturn(Future.successful(true))

      val userService = getUserService(dataSource, mockSamDAO)

      val actual = intercept[RawlsExceptionWithErrorReport] {
        Await.result(
          userService.setBillingProjectSpendConfiguration(billingProject.projectName, spendReportConfiguration),
          Duration.Inf
        )
      }

      actual.errorReport.statusCode.get shouldEqual StatusCodes.BadRequest
    }
  }

  it should "throw a RawlsExceptionWithErrorReport when setting the spend configuration if the billing project does not have a billing account associated with it" in {
    withMinimalTestDatabase { dataSource: SlickDataSource =>
      val billingProject = RawlsBillingProject(RawlsBillingProjectName("project_without_billing_account"),
                                               CreationStatuses.Ready,
                                               None,
                                               None
      )
      runAndWait(dataSource.dataAccess.rawlsBillingProjectQuery.create(billingProject))

      val spendReportDatasetName = BigQueryDatasetName("some_dataset")
      val spendReportGoogleProject = GoogleProject("some_other_google_project")
      val spendReportConfiguration = BillingProjectSpendConfiguration(spendReportGoogleProject, spendReportDatasetName)

      val mockSamDAO = mock[SamDAO](RETURNS_SMART_NULLS)
      when(
        mockSamDAO.userHasAction(SamResourceTypeNames.billingProject,
                                 billingProject.projectName.value,
                                 SamBillingProjectActions.alterSpendReportConfiguration,
                                 testContext
        )
      ).thenReturn(Future.successful(true))

      val userService = getUserService(dataSource, mockSamDAO)

      val actual = intercept[RawlsExceptionWithErrorReport] {
        Await.result(
          userService.setBillingProjectSpendConfiguration(billingProject.projectName, spendReportConfiguration),
          Duration.Inf
        )
      }

      actual.errorReport.statusCode.get shouldEqual StatusCodes.BadRequest
    }
  }

  it should "throw a RawlsExceptionWithErrorReport when setting the spend configuration if the dataset has an invalid name" in {
    withMinimalTestDatabase { dataSource: SlickDataSource =>
      val billingProject = minimalTestData.billingProject
      val spendReportDatasetName = BigQueryDatasetName("test-dataset")
      val spendReportGoogleProject = GoogleProject("some_other_google_project")
      val spendReportConfiguration = BillingProjectSpendConfiguration(spendReportGoogleProject, spendReportDatasetName)

      val mockSamDAO = mock[SamDAO](RETURNS_SMART_NULLS)
      when(
        mockSamDAO.userHasAction(SamResourceTypeNames.billingProject,
                                 billingProject.projectName.value,
                                 SamBillingProjectActions.alterSpendReportConfiguration,
                                 testContext
        )
      ).thenReturn(Future.successful(true))

      val userService = getUserService(dataSource, mockSamDAO)

      val actual = intercept[RawlsExceptionWithErrorReport] {
        Await.result(
          userService.setBillingProjectSpendConfiguration(billingProject.projectName, spendReportConfiguration),
          Duration.Inf
        )
      }

      actual.errorReport.statusCode.get shouldEqual StatusCodes.BadRequest
    }
  }

  it should "update the billing account for a billing project" in {
    withMinimalTestDatabase { dataSource: SlickDataSource =>
      val billingProject = minimalTestData.billingProject
      val billingAccountName = RawlsBillingAccountName("billingAccounts/111111-111111-111111")
      val newBillingAccountRequest = UpdateRawlsBillingAccountRequest(billingAccountName)

      runAndWait(
        dataSource.dataAccess.rawlsBillingProjectQuery.updateBillingAccountValidity(billingAccountName,
                                                                                    isInvalid = true
        )
      )

      val mockSamDAO = mock[SamDAO](RETURNS_SMART_NULLS)
      when(
        mockSamDAO.userHasAction(SamResourceTypeNames.billingProject,
                                 billingProject.projectName.value,
                                 SamBillingProjectActions.updateBillingAccount,
                                 testContext
        )
      ).thenReturn(Future.successful(true))
      when(
        mockSamDAO.listUserRolesForResource(SamResourceTypeNames.billingProject,
                                            billingProject.projectName.value,
                                            testContext
        )
      ).thenReturn(Future.successful(Set(SamBillingProjectRoles.owner)))

      val mockGcsDAO = mock[GoogleServicesDAO](RETURNS_SMART_NULLS)
      when(mockGcsDAO.testTerraAndUserBillingAccountAccess(ArgumentMatchers.eq(billingAccountName), any[UserInfo]))
        .thenReturn(Future.successful(true))

      val mockBpmDAO = mock[BillingProfileManagerDAO](RETURNS_SMART_NULLS)
      when(
        mockBpmDAO.updateBillingProfile(
          ArgumentMatchers.eq(UUID.fromString(billingProject.billingProfileId.getOrElse(fail()))),
          ArgumentMatchers.eq(newBillingAccountRequest.billingAccount),
          any()
        )
      ).thenReturn(new ProfileModel())

      val userService = getUserService(dataSource, mockSamDAO, mockGcsDAO, bpmDAO = mockBpmDAO)

      Await.result(userService.updateBillingProjectBillingAccount(billingProject.projectName, newBillingAccountRequest),
                   Duration.Inf
      )

      val spendReportDatasetInDb = runAndWait(
        dataSource.dataAccess.rawlsBillingProjectQuery
          .filter(_.projectName === billingProject.projectName.value)
          .map(row => (row.spendReportDataset, row.spendReportTable))
          .result
      )

      // assert that the spend report configuration has been fully cleared
      spendReportDatasetInDb.head shouldEqual (None, None)
      val project = runAndWait(rawlsBillingProjectQuery.load(billingProject.projectName))
        .getOrElse(fail("project not found"))
      project.billingAccount shouldEqual Option(billingAccountName)
      project.invalidBillingAccount shouldBe false

      verify(mockBpmDAO).updateBillingProfile(
        ArgumentMatchers.eq(UUID.fromString(billingProject.billingProfileId.getOrElse(fail()))),
        ArgumentMatchers.eq(newBillingAccountRequest.billingAccount),
        any()
      )
    }
  }

  it should "remove the billing account for a billing project" in {
    withMinimalTestDatabase { dataSource: SlickDataSource =>
      val billingProject = minimalTestData.billingProject

      val mockSamDAO = mock[SamDAO](RETURNS_SMART_NULLS)
      when(
        mockSamDAO.userHasAction(SamResourceTypeNames.billingProject,
                                 billingProject.projectName.value,
                                 SamBillingProjectActions.updateBillingAccount,
                                 testContext
        )
      ).thenReturn(Future.successful(true))
      when(
        mockSamDAO.listUserRolesForResource(SamResourceTypeNames.billingProject,
                                            billingProject.projectName.value,
                                            testContext
        )
      ).thenReturn(Future.successful(Set(SamBillingProjectRoles.owner)))

      val mockGcsDAO = mock[GoogleServicesDAO](RETURNS_SMART_NULLS)
      val mockBpmDAO = mock[BillingProfileManagerDAO](RETURNS_SMART_NULLS)

      val userService = getUserService(dataSource, mockSamDAO, mockGcsDAO, bpmDAO = mockBpmDAO)

      Await.result(userService.deleteBillingAccount(billingProject.projectName), Duration.Inf)

      val spendReportDatasetInDb = runAndWait(
        dataSource.dataAccess.rawlsBillingProjectQuery
          .filter(_.projectName === billingProject.projectName.value)
          .map(row => (row.spendReportDataset, row.spendReportTable))
          .result
      )

      // assert that the spend report configuration has been fully cleared
      spendReportDatasetInDb.head shouldEqual (None, None)

      runAndWait(rawlsBillingProjectQuery.load(billingProject.projectName))
        .getOrElse(fail("project not found"))
        .billingAccount shouldEqual None

      verify(mockBpmDAO).removeBillingAccountFromBillingProfile(
        ArgumentMatchers.eq(UUID.fromString(billingProject.billingProfileId.getOrElse(fail()))),
        any()
      )
    }
  }

  it should "not update the billing account for a billing project if the user does not have access to the billing project" in {
    withMinimalTestDatabase { dataSource: SlickDataSource =>
      val billingProject = minimalTestData.billingProject
      val billingAccountName = RawlsBillingAccountName("billingAccounts/111111-111111-111111")
      val newBillingAccountRequest = UpdateRawlsBillingAccountRequest(billingAccountName)

      val mockSamDAO = mock[SamDAO](RETURNS_SMART_NULLS)
      when(
        mockSamDAO.userHasAction(SamResourceTypeNames.billingProject,
                                 billingProject.projectName.value,
                                 SamBillingProjectActions.updateBillingAccount,
                                 testContext
        )
      ).thenReturn(Future.successful(false))

      val mockGcsDAO = mock[GoogleServicesDAO](RETURNS_SMART_NULLS)
      when(mockGcsDAO.testTerraAndUserBillingAccountAccess(ArgumentMatchers.eq(billingAccountName), any[UserInfo]))
        .thenReturn(Future.successful(true))

      val userService = getUserService(dataSource, mockSamDAO, mockGcsDAO)

      val actual = intercept[RawlsExceptionWithErrorReport] {
        Await.result(userService.updateBillingProjectBillingAccount(billingProject.projectName,
                                                                    newBillingAccountRequest
                     ),
                     Duration.Inf
        ) shouldEqual ()
      }

      actual.errorReport.statusCode.get shouldEqual StatusCodes.Forbidden

      // billing account should remain unchanged
      runAndWait(rawlsBillingProjectQuery.load(billingProject.projectName))
        .getOrElse(fail("project not found"))
        .billingAccount shouldEqual billingProject.billingAccount
    }
  }

  it should "not update the billing account for a billing project if the user does not have access to the billing account" in {
    withMinimalTestDatabase { dataSource: SlickDataSource =>
      val billingProject = minimalTestData.billingProject
      val billingAccountName = RawlsBillingAccountName("billingAccounts/111111-111111-111111")
      val newBillingAccountRequest = UpdateRawlsBillingAccountRequest(billingAccountName)

      val mockSamDAO = mock[SamDAO](RETURNS_SMART_NULLS)
      when(
        mockSamDAO.userHasAction(SamResourceTypeNames.billingProject,
                                 billingProject.projectName.value,
                                 SamBillingProjectActions.updateBillingAccount,
                                 testContext
        )
      ).thenReturn(Future.successful(true))

      val mockGcsDAO = mock[GoogleServicesDAO](RETURNS_SMART_NULLS)
      when(mockGcsDAO.testTerraAndUserBillingAccountAccess(ArgumentMatchers.eq(billingAccountName), any[UserInfo]))
        .thenReturn(Future.successful(false))

      val userService = getUserService(dataSource, mockSamDAO, mockGcsDAO)

      val actual = intercept[RawlsExceptionWithErrorReport] {
        Await.result(userService.updateBillingProjectBillingAccount(billingProject.projectName,
                                                                    newBillingAccountRequest
                     ),
                     Duration.Inf
        ) shouldEqual ()
      }

      actual.errorReport.statusCode.get shouldEqual StatusCodes.BadRequest

      // billing account should remain unchanged
      runAndWait(rawlsBillingProjectQuery.load(billingProject.projectName))
        .getOrElse(fail("project not found"))
        .billingAccount shouldEqual billingProject.billingAccount
    }
  }

  it should "not remove the billing account for a billing project if the user does not have access to the billing project" in {
    withMinimalTestDatabase { dataSource: SlickDataSource =>
      val billingProject = minimalTestData.billingProject
      val billingAccountName = RawlsBillingAccountName("billingAccounts/111111-111111-111111")

      val mockSamDAO = mock[SamDAO](RETURNS_SMART_NULLS)
      when(
        mockSamDAO.userHasAction(SamResourceTypeNames.billingProject,
                                 billingProject.projectName.value,
                                 SamBillingProjectActions.updateBillingAccount,
                                 testContext
        )
      ).thenReturn(Future.successful(false))

      val mockGcsDAO = mock[GoogleServicesDAO](RETURNS_SMART_NULLS)
      when(mockGcsDAO.testTerraAndUserBillingAccountAccess(ArgumentMatchers.eq(billingAccountName), any[UserInfo]))
        .thenReturn(Future.successful(true))

      val userService = getUserService(dataSource, mockSamDAO, mockGcsDAO)

      val actual = intercept[RawlsExceptionWithErrorReport] {
        Await.result(userService.deleteBillingAccount(billingProject.projectName), Duration.Inf) shouldEqual ()
      }

      actual.errorReport.statusCode.get shouldEqual StatusCodes.Forbidden

      // billing account should remain unchanged
      runAndWait(rawlsBillingProjectQuery.load(billingProject.projectName))
        .getOrElse(fail("project not found"))
        .billingAccount shouldEqual billingProject.billingAccount
    }
  }

  it should "throw a RawlsExceptionWithErrorReport when updating the billing account for a billing project and the billing account name is not valid" in {
    withMinimalTestDatabase { dataSource: SlickDataSource =>
      val billingProject = minimalTestData.billingProject
      val billingAccountName = RawlsBillingAccountName("INVALID")
      val newBillingAccountRequest = UpdateRawlsBillingAccountRequest(billingAccountName)

      val mockSamDAO = mock[SamDAO](RETURNS_SMART_NULLS)
      when(
        mockSamDAO.userHasAction(SamResourceTypeNames.billingProject,
                                 billingProject.projectName.value,
                                 SamBillingProjectActions.updateBillingAccount,
                                 testContext
        )
      ).thenReturn(Future.successful(true))

      val mockGcsDAO = mock[GoogleServicesDAO](RETURNS_SMART_NULLS)
      when(mockGcsDAO.testTerraAndUserBillingAccountAccess(ArgumentMatchers.eq(billingAccountName), any[UserInfo]))
        .thenReturn(Future.successful(true))

      val userService = getUserService(dataSource, mockSamDAO, mockGcsDAO)

      val actual = intercept[RawlsExceptionWithErrorReport] {
        Await.result(userService.updateBillingProjectBillingAccount(billingProject.projectName,
                                                                    newBillingAccountRequest
                     ),
                     Duration.Inf
        ) shouldEqual ()
      }

      actual.errorReport.statusCode.get shouldEqual StatusCodes.BadRequest

      // billing account should remain unchanged
      runAndWait(rawlsBillingProjectQuery.load(billingProject.projectName))
        .getOrElse(fail("project not found"))
        .billingAccount shouldEqual billingProject.billingAccount
    }
  }

  it should "not call BPM when updating or removing a billing account if the billing project has no billing profile" in {
    withMinimalTestDatabase { dataSource: SlickDataSource =>
      val billingProject =
        minimalTestData.billingProject.copy(projectName = RawlsBillingProjectName("noProfile"), billingProfileId = None)
      val billingAccountName = RawlsBillingAccountName("billingAccounts/111111-111111-111111")
      val newBillingAccountRequest = UpdateRawlsBillingAccountRequest(billingAccountName)

      runAndWait(
        dataSource.dataAccess.rawlsBillingProjectQuery.create(billingProject)
      )

      val mockSamDAO = mock[SamDAO](RETURNS_SMART_NULLS)
      when(
        mockSamDAO.userHasAction(SamResourceTypeNames.billingProject,
                                 billingProject.projectName.value,
                                 SamBillingProjectActions.updateBillingAccount,
                                 testContext
        )
      ).thenReturn(Future.successful(true))
      when(
        mockSamDAO.listUserRolesForResource(SamResourceTypeNames.billingProject,
                                            billingProject.projectName.value,
                                            testContext
        )
      ).thenReturn(Future.successful(Set(SamBillingProjectRoles.owner)))

      val mockGcsDAO = mock[GoogleServicesDAO](RETURNS_SMART_NULLS)
      when(mockGcsDAO.testTerraAndUserBillingAccountAccess(ArgumentMatchers.eq(billingAccountName), any[UserInfo]))
        .thenReturn(Future.successful(true))

      val mockBpmDAO = mock[BillingProfileManagerDAO](RETURNS_SMART_NULLS)

      val userService = getUserService(dataSource, mockSamDAO, mockGcsDAO, bpmDAO = mockBpmDAO)

      Await.result(userService.updateBillingProjectBillingAccount(billingProject.projectName, newBillingAccountRequest),
                   Duration.Inf
      )
      Await.result(userService.deleteBillingAccount(billingProject.projectName), Duration.Inf)

      verifyNoInteractions(mockBpmDAO)
    }
  }

  it should "not throw if BPM errors while updating the billing profile" in {
    withMinimalTestDatabase { dataSource: SlickDataSource =>
      val billingProject = minimalTestData.billingProject
      val billingAccountName = RawlsBillingAccountName("billingAccounts/111111-111111-111111")
      val newBillingAccountRequest = UpdateRawlsBillingAccountRequest(billingAccountName)

      val mockSamDAO = mock[SamDAO](RETURNS_SMART_NULLS)
      when(
        mockSamDAO.userHasAction(SamResourceTypeNames.billingProject,
                                 billingProject.projectName.value,
                                 SamBillingProjectActions.updateBillingAccount,
                                 testContext
        )
      ).thenReturn(Future.successful(true))
      when(
        mockSamDAO.listUserRolesForResource(SamResourceTypeNames.billingProject,
                                            billingProject.projectName.value,
                                            testContext
        )
      ).thenReturn(Future.successful(Set(SamBillingProjectRoles.owner)))

      val mockGcsDAO = mock[GoogleServicesDAO](RETURNS_SMART_NULLS)
      when(mockGcsDAO.testTerraAndUserBillingAccountAccess(ArgumentMatchers.eq(billingAccountName), any[UserInfo]))
        .thenReturn(Future.successful(true))

      val mockBpmDAO = mock[BillingProfileManagerDAO](RETURNS_SMART_NULLS)
      when(
        mockBpmDAO.updateBillingProfile(
          ArgumentMatchers.eq(UUID.fromString(billingProject.billingProfileId.getOrElse(fail()))),
          ArgumentMatchers.eq(newBillingAccountRequest.billingAccount),
          any()
        )
      ).thenThrow(new ApiException("oh no"))
      when(
        mockBpmDAO.removeBillingAccountFromBillingProfile(
          ArgumentMatchers.eq(UUID.fromString(billingProject.billingProfileId.getOrElse(fail()))),
          any()
        )
      ).thenThrow(new ApiException("oh no"))

      val userService = getUserService(dataSource, mockSamDAO, mockGcsDAO, bpmDAO = mockBpmDAO)

      Await.result(userService.updateBillingProjectBillingAccount(billingProject.projectName, newBillingAccountRequest),
                   Duration.Inf
      )
      Await.result(userService.deleteBillingAccount(billingProject.projectName), Duration.Inf)

      verify(mockBpmDAO).updateBillingProfile(
        ArgumentMatchers.eq(UUID.fromString(billingProject.billingProfileId.getOrElse(fail()))),
        ArgumentMatchers.eq(newBillingAccountRequest.billingAccount),
        any()
      )
      verify(mockBpmDAO).removeBillingAccountFromBillingProfile(
        ArgumentMatchers.eq(UUID.fromString(billingProject.billingProfileId.getOrElse(fail()))),
        any()
      )
    }
  }

  it should "throw a RawlsExceptionWithErrorReport when setting the spend configuration if the dataset google project has an invalid name" in {
    withMinimalTestDatabase { dataSource: SlickDataSource =>
      val billingProject = minimalTestData.billingProject
      val spendReportDatasetName = BigQueryDatasetName("test-dataset")
      val spendReportGoogleProject = GoogleProject("bad%")
      val spendReportConfiguration = BillingProjectSpendConfiguration(spendReportGoogleProject, spendReportDatasetName)

      val mockSamDAO = mock[SamDAO](RETURNS_SMART_NULLS)
      when(
        mockSamDAO.userHasAction(SamResourceTypeNames.billingProject,
                                 billingProject.projectName.value,
                                 SamBillingProjectActions.alterSpendReportConfiguration,
                                 testContext
        )
      ).thenReturn(Future.successful(true))

      val userService = getUserService(dataSource, mockSamDAO)

      val actual = intercept[RawlsExceptionWithErrorReport] {
        Await.result(
          userService.setBillingProjectSpendConfiguration(billingProject.projectName, spendReportConfiguration),
          Duration.Inf
        )
      }

      actual.errorReport.statusCode.get shouldEqual StatusCodes.BadRequest
    }
  }

  it should "get the spend report configuration of a billing project when the user has permission" in {
    withMinimalTestDatabase { dataSource: SlickDataSource =>
      val billingProject = minimalTestData.billingProject
      val spendReportDatasetName = BigQueryDatasetName("test_dataset")
      val spendReportGoogleProject = GoogleProject("some_other_google_project")
      val spendReportConfiguration = BillingProjectSpendConfiguration(spendReportGoogleProject, spendReportDatasetName)

      val mockSamDAO = mock[SamDAO](RETURNS_SMART_NULLS)
      when(
        mockSamDAO.userHasAction(SamResourceTypeNames.billingProject,
                                 billingProject.projectName.value,
                                 SamBillingProjectActions.alterSpendReportConfiguration,
                                 testContext
        )
      ).thenReturn(Future.successful(true))
      when(
        mockSamDAO.userHasAction(SamResourceTypeNames.billingProject,
                                 billingProject.projectName.value,
                                 SamBillingProjectActions.readSpendReportConfiguration,
                                 testContext
        )
      ).thenReturn(Future.successful(true))

      val userService = getUserService(dataSource, mockSamDAO)

      Await.result(
        userService.setBillingProjectSpendConfiguration(billingProject.projectName, spendReportConfiguration),
        Duration.Inf
      ) shouldEqual 1

      val result =
        Await.result(userService.getBillingProjectSpendConfiguration(billingProject.projectName), Duration.Inf)

      result shouldEqual Some(spendReportConfiguration)
    }
  }

  it should "return None when the user calls getSpendReportConfiguration but it isn't configured" in {
    withMinimalTestDatabase { dataSource: SlickDataSource =>
      val billingProject = minimalTestData.billingProject

      val mockSamDAO = mock[SamDAO](RETURNS_SMART_NULLS)
      when(
        mockSamDAO.userHasAction(SamResourceTypeNames.billingProject,
                                 billingProject.projectName.value,
                                 SamBillingProjectActions.alterSpendReportConfiguration,
                                 testContext
        )
      ).thenReturn(Future.successful(true))
      when(
        mockSamDAO.userHasAction(SamResourceTypeNames.billingProject,
                                 billingProject.projectName.value,
                                 SamBillingProjectActions.readSpendReportConfiguration,
                                 testContext
        )
      ).thenReturn(Future.successful(true))

      val userService = getUserService(dataSource, mockSamDAO)

      val result =
        Await.result(userService.getBillingProjectSpendConfiguration(billingProject.projectName), Duration.Inf)

      result shouldEqual None
    }
  }

  it should "throw a RawlsExceptionWithErrorReport when the user does not have permission to get the spend report configuration" in {
    withMinimalTestDatabase { dataSource: SlickDataSource =>
      val billingProject = minimalTestData.billingProject

      val mockSamDAO = mock[SamDAO](RETURNS_SMART_NULLS)
      when(
        mockSamDAO.userHasAction(SamResourceTypeNames.billingProject,
                                 billingProject.projectName.value,
                                 SamBillingProjectActions.readSpendReportConfiguration,
                                 testContext
        )
      ).thenReturn(Future.successful(false))

      val userService = getUserService(dataSource, mockSamDAO)

      val actual = intercept[RawlsExceptionWithErrorReport] {
        Await.result(userService.getBillingProjectSpendConfiguration(billingProject.projectName), Duration.Inf)
      }

      actual.errorReport.statusCode.get shouldEqual StatusCodes.Forbidden
    }
  }

  it should "throw a RawlsExceptionWithErrorReport getting the spend report configuration for a project does not exist" in {
    withMinimalTestDatabase { dataSource: SlickDataSource =>
      val projectName = RawlsBillingProjectName("fake-project")

      val mockSamDAO = mock[SamDAO](RETURNS_SMART_NULLS)
      when(
        mockSamDAO.userHasAction(SamResourceTypeNames.billingProject,
                                 projectName.value,
                                 SamBillingProjectActions.readSpendReportConfiguration,
                                 testContext
        )
      ).thenReturn(Future.successful(true))

      val userService = getUserService(dataSource, mockSamDAO)

      val actual = intercept[RawlsExceptionWithErrorReport] {
        Await.result(userService.getBillingProjectSpendConfiguration(projectName), Duration.Inf)
      }

      actual.errorReport.statusCode.get shouldEqual StatusCodes.NotFound
    }
  }

  behavior of "getBillingProject"

  it should "return None if the user doesn't have any roles on a billing project" in {
    val projectName = RawlsBillingProjectName(UUID.randomUUID().toString)
    val project = RawlsBillingProject(projectName, CreationStatuses.Ready, None, None)
    val repository = mock[BillingRepository]
    when(repository.getBillingProject(ArgumentMatchers.eq(projectName))).thenReturn(Future.successful(Some(project)))

    val samDAO = mock[SamDAO](RETURNS_SMART_NULLS)
    when(samDAO.listUserRolesForResource(SamResourceTypeNames.billingProject, projectName.value, testContext))
      .thenReturn(Future.successful(Set.empty))

    val userService = getUserService(samDAO = samDAO, billingRepository = Some(repository))

    Await.result(userService.getBillingProject(projectName), Duration.Inf) shouldEqual None
  }

  it should "set the project as GCP if there is no billing profile set" in {
    val projectName = RawlsBillingProjectName(UUID.randomUUID().toString)
    val project = RawlsBillingProject(projectName, CreationStatuses.Ready, None, None)
    val repository = mock[BillingRepository]
    when(repository.getBillingProject(ArgumentMatchers.eq(projectName))).thenReturn(Future.successful(Some(project)))

    val samDAO = mock[SamDAO](RETURNS_SMART_NULLS)
    when(samDAO.listUserRolesForResource(SamResourceTypeNames.billingProject, projectName.value, testContext))
      .thenReturn(Future.successful(Set(SamResourceRole(SamBillingProjectRoles.owner.value))))

    val userService = getUserService(samDAO = samDAO, billingRepository = Some(repository))

    val expected = Some(RawlsBillingProjectResponse(Set(ProjectRoles.Owner), project, CloudPlatform.GCP))

    Await.result(userService.getBillingProject(projectName), Duration.Inf) shouldEqual expected
  }

  it should "map the azure data for a bpm project" in {
    val billingProfile = new ProfileModel().id(UUID.randomUUID()).cloudPlatform(BPMCloudPlatform.AZURE)
    val projectName = RawlsBillingProjectName(UUID.randomUUID().toString)
    val project = RawlsBillingProject(
      projectName,
      CreationStatuses.Ready,
      None,
      None,
      azureManagedAppCoordinates = Some(AzureManagedAppCoordinates(null, null, null)),
      billingProfileId = Some(billingProfile.getId.toString)
    )
    val repository = mock[BillingRepository]
    when(repository.getBillingProject(ArgumentMatchers.eq(projectName))).thenReturn(Future.successful(Some(project)))

    val samDAO = mock[SamDAO](RETURNS_SMART_NULLS)
    when(samDAO.listUserRolesForResource(SamResourceTypeNames.billingProject, projectName.value, testContext))
      .thenReturn(Future.successful(Set(SamResourceRole(SamBillingProjectRoles.owner.value))))
    val bpmDAO = mock[BillingProfileManagerDAO](RETURNS_SMART_NULLS)
    when(bpmDAO.getBillingProfile(billingProfile.getId, testContext)).thenReturn(Some(billingProfile))

    val userService = getUserService(samDAO = samDAO, bpmDAO = bpmDAO, billingRepository = Some(repository))

    val expected =
      Some(RawlsBillingProjectResponse(Set(ProjectRoles.Owner), project, CloudPlatform.AZURE, Option(false)))
    Await.result(userService.getBillingProject(projectName), Duration.Inf) shouldEqual expected
  }

  it should "set the project platform to Unknown if there is no billing profile for a billing profileId" in {
    val billingProfileId = UUID.randomUUID()
    val projectName = RawlsBillingProjectName(UUID.randomUUID().toString)
    val project = RawlsBillingProject(
      projectName,
      CreationStatuses.Ready,
      None,
      None,
      azureManagedAppCoordinates = Some(AzureManagedAppCoordinates(null, null, null)),
      billingProfileId = Some(billingProfileId.toString)
    )
    val repository = mock[BillingRepository]
    when(repository.getBillingProject(ArgumentMatchers.eq(projectName))).thenReturn(Future.successful(Some(project)))

    val samDAO = mock[SamDAO](RETURNS_SMART_NULLS)
    when(samDAO.listUserRolesForResource(SamResourceTypeNames.billingProject, projectName.value, testContext))
      .thenReturn(Future.successful(Set(SamResourceRole(SamBillingProjectRoles.owner.value))))
    val bpmDAO = mock[BillingProfileManagerDAO](RETURNS_SMART_NULLS)
    when(bpmDAO.getBillingProfile(billingProfileId, testContext)).thenReturn(None)

    val userService = getUserService(samDAO = samDAO, bpmDAO = bpmDAO, billingRepository = Some(repository))

    Await
      .result(userService.getBillingProject(projectName), Duration.Inf)
      .map(_.cloudPlatform)
      .get shouldEqual CloudPlatform.UNKNOWN.toString
  }

  behavior of "listBillingProjectsV2"

  it should "map azure data for a BPM project" in {
    val billingProfile = new ProfileModel().id(UUID.randomUUID()).cloudPlatform(BPMCloudPlatform.AZURE)
    val projectName = RawlsBillingProjectName(UUID.randomUUID().toString)
    val project = RawlsBillingProject(
      projectName,
      CreationStatuses.Ready,
      None,
      None,
      billingProfileId = Some(billingProfile.getId.toString)
    )

    val repository = mock[BillingRepository]
    when(repository.getBillingProjects(ArgumentMatchers.eq(Set(projectName))))
      .thenReturn(Future.successful(Seq(project)))

    val noRole = SamRolesAndActions(Set.empty, Set.empty)
    val creatorRole =
      SamRolesAndActions(Set(SamBillingProjectRoles.workspaceCreator), Set(SamBillingProjectActions.createWorkspace))
    val billingResource = SamUserResource(projectName.value, creatorRole, noRole, noRole, Set.empty, Set.empty)

    val samDAO = mock[SamDAO](RETURNS_SMART_NULLS)
    when(samDAO.listUserResources(SamResourceTypeNames.billingProject, testContext))
      .thenReturn(Future.successful(Seq(billingResource)))
    val bpmDAO = mock[BillingProfileManagerDAO](RETURNS_SMART_NULLS)
    when(bpmDAO.getAllBillingProfiles(testContext)).thenReturn(Future.successful(Seq(billingProfile)))
    val userService = getUserService(samDAO = samDAO, bpmDAO = bpmDAO, billingRepository = Some(repository))

    val expected = Seq(
      RawlsBillingProjectResponse(
        Set(ProjectRoles.User),
        project.copy(azureManagedAppCoordinates = Some(AzureManagedAppCoordinates(null, null, null))),
        CloudPlatform.AZURE,
        Option(false)
      )
    )

    Await.result(userService.listBillingProjectsV2(), Duration.Inf) should contain theSameElementsAs expected
  }

  it should "set the cloud platform to unknown when a billing profile can't be found" in {
    val billingProfileId = UUID.randomUUID()
    val projectName = RawlsBillingProjectName(UUID.randomUUID().toString)
    val project = RawlsBillingProject(
      projectName,
      CreationStatuses.Ready,
      None,
      None,
      billingProfileId = Some(billingProfileId.toString)
    )

    val repository = mock[BillingRepository]
    when(repository.getBillingProjects(ArgumentMatchers.eq(Set(projectName))))
      .thenReturn(Future.successful(Seq(project)))

    val noRole = SamRolesAndActions(Set.empty, Set.empty)
    val creatorRole =
      SamRolesAndActions(Set(SamBillingProjectRoles.workspaceCreator), Set(SamBillingProjectActions.createWorkspace))
    val billingResource = SamUserResource(projectName.value, creatorRole, noRole, noRole, Set.empty, Set.empty)

    val samDAO = mock[SamDAO](RETURNS_SMART_NULLS)
    when(samDAO.listUserResources(SamResourceTypeNames.billingProject, testContext))
      .thenReturn(Future.successful(Seq(billingResource)))
    val bpmDAO = mock[BillingProfileManagerDAO](RETURNS_SMART_NULLS)
    when(bpmDAO.getAllBillingProfiles(testContext)).thenReturn(Future.successful(Seq()))
    val userService = getUserService(samDAO = samDAO, bpmDAO = bpmDAO, billingRepository = Some(repository))

    Await.result(userService.listBillingProjectsV2(), Duration.Inf).head.cloudPlatform shouldBe
      CloudPlatform.UNKNOWN.toString
  }

  it should "return the list of billing projects including azure data when enabled" in {
    // GCP, Rawls-only project
    val ownerProject =
      RawlsBillingProject(RawlsBillingProjectName(UUID.randomUUID().toString), CreationStatuses.Ready, None, None)

    // Azure, BPM-backed project
    val bpmBillingProfile = new ProfileModel().id(UUID.randomUUID()).cloudPlatform(BPMCloudPlatform.AZURE)
    val billingProfileBackedProject = RawlsBillingProject(
      RawlsBillingProjectName(UUID.randomUUID().toString),
      CreationStatuses.Ready,
      None,
      None,
      azureManagedAppCoordinates = Some(AzureManagedAppCoordinates(null, null, null)),
      billingProfileId = Some(bpmBillingProfile.getId.toString)
    )

    // Azure, BPM-backed, protected project
    val policies = new BpmApiPolicyInputs().inputs(
      List(new BpmApiPolicyInput().namespace("terra").name("protected-data").additionalData(List.empty.asJava)).asJava
    )
    val bpmProtectedDataBillingProfile =
      new ProfileModel().id(UUID.randomUUID()).cloudPlatform(BPMCloudPlatform.AZURE).policies(policies)
    val protectedDataBillingProfileBackedProject = RawlsBillingProject(
      RawlsBillingProjectName(UUID.randomUUID().toString),
      CreationStatuses.Ready,
      None,
      None,
      azureManagedAppCoordinates = Some(AzureManagedAppCoordinates(null, null, null)),
      billingProfileId = Some(bpmProtectedDataBillingProfile.getId.toString)
    )

    val billingProjects = Set(ownerProject, billingProfileBackedProject, protectedDataBillingProfileBackedProject)

    val repository = mock[BillingRepository]
    when(repository.getBillingProjects(ArgumentMatchers.eq(billingProjects.map(_.projectName))))
      .thenReturn(Future.successful(billingProjects.toSeq))

    // Setup mock DAOs
    val ownerRole = SamRolesAndActions(Set(SamBillingProjectRoles.owner), Set(SamBillingProjectActions.createWorkspace))
    val noRole = SamRolesAndActions(Set.empty, Set.empty)
    val creatorRole =
      SamRolesAndActions(Set(SamBillingProjectRoles.workspaceCreator), Set(SamBillingProjectActions.createWorkspace))
    val userBillingResources = Seq(
      SamUserResource(ownerProject.projectName.value, ownerRole, noRole, noRole, Set.empty, Set.empty),
      SamUserResource(billingProfileBackedProject.projectName.value, creatorRole, noRole, noRole, Set.empty, Set.empty),
      SamUserResource(protectedDataBillingProfileBackedProject.projectName.value,
                      ownerRole,
                      noRole,
                      noRole,
                      Set.empty,
                      Set.empty
      )
    )
    val samDAO = mock[SamDAO](RETURNS_SMART_NULLS)
    when(samDAO.listUserResources(SamResourceTypeNames.billingProject, testContext))
      .thenReturn(Future.successful(userBillingResources))
    val bpmDAO = mock[BillingProfileManagerDAO](RETURNS_SMART_NULLS)
    when(bpmDAO.getAllBillingProfiles(testContext))
      .thenReturn(Future.successful(Seq(bpmBillingProfile, bpmProtectedDataBillingProfile)))
    val userService = getUserService(samDAO = samDAO, bpmDAO = bpmDAO, billingRepository = Some(repository))

    val expected = Seq(
      RawlsBillingProjectResponse(Set(ProjectRoles.Owner), ownerProject, CloudPlatform.GCP),
      RawlsBillingProjectResponse(Set(ProjectRoles.User),
                                  billingProfileBackedProject,
                                  CloudPlatform.AZURE,
                                  protectedData = Option(false)
      ),
      RawlsBillingProjectResponse(Set(ProjectRoles.Owner),
                                  protectedDataBillingProfileBackedProject,
                                  CloudPlatform.AZURE,
                                  protectedData = Option(true)
      )
    )

    Await.result(userService.listBillingProjectsV2(), Duration.Inf) should contain theSameElementsAs expected
  }

  it should "return the list of billing projects to which the user has access" in {
    val ownerProject = billingProjectFromName(UUID.randomUUID().toString)
    val userProject = billingProjectFromName(UUID.randomUUID().toString)

    val repository = mock[BillingRepository]
    when(repository.getBillingProjects(ArgumentMatchers.eq(Set(ownerProject.projectName, userProject.projectName))))
      .thenReturn(Future.successful(Seq(ownerProject, userProject)))

    val noRole = SamRolesAndActions(Set.empty, Set.empty)
    val ownerRole = SamRolesAndActions(Set(SamBillingProjectRoles.owner), Set(SamBillingProjectActions.createWorkspace))
    val creatorRole =
      SamRolesAndActions(Set(SamBillingProjectRoles.workspaceCreator), Set(SamBillingProjectActions.createWorkspace))
    val userBillingResources = Seq(
      SamUserResource(ownerProject.projectName.value, ownerRole, noRole, noRole, Set.empty, Set.empty),
      SamUserResource(userProject.projectName.value, creatorRole, noRole, noRole, Set.empty, Set.empty)
    )
    val samDAO = mock[SamDAO](RETURNS_SMART_NULLS)
    when(samDAO.listUserResources(SamResourceTypeNames.billingProject, testContext))
      .thenReturn(Future.successful(userBillingResources))
    val bpmDAO = mock[BillingProfileManagerDAO](RETURNS_SMART_NULLS)
    when(bpmDAO.getAllBillingProfiles(testContext)).thenReturn(Future.successful(Seq.empty))

    val userService = getUserService(samDAO = samDAO, bpmDAO = bpmDAO, billingRepository = Some(repository))

    val expected = Seq(
      RawlsBillingProjectResponse(Set(ProjectRoles.User), userProject, CloudPlatform.GCP),
      RawlsBillingProjectResponse(Set(ProjectRoles.Owner), ownerProject, CloudPlatform.GCP)
    )

    Await.result(userService.listBillingProjectsV2(), Duration.Inf) should contain theSameElementsAs expected
    verify(repository).getBillingProjects(ArgumentMatchers.eq(Set(ownerProject.projectName, userProject.projectName)))
  }

  // TODO: This can be removed, along with the filter for no roles, once we've cleaned up old billing projects in WOR-658
  it should "exclude billing projects with no roles" in {
    val ownerProject = billingProjectFromName(UUID.randomUUID().toString)
    val userProject = billingProjectFromName(UUID.randomUUID().toString)

    val repository = mock[BillingRepository]
    when(repository.getBillingProjects(ArgumentMatchers.eq(Set(ownerProject.projectName, userProject.projectName))))
      .thenReturn(Future.successful(Seq(ownerProject, userProject)))

    val noRole = SamRolesAndActions(Set.empty, Set.empty)
    val ownerRole = SamRolesAndActions(Set(SamBillingProjectRoles.owner), Set(SamBillingProjectActions.createWorkspace))
    val creatorRole =
      SamRolesAndActions(Set(SamBillingProjectRoles.workspaceCreator), Set(SamBillingProjectActions.createWorkspace))
    val userBillingResources = Seq(
      SamUserResource(ownerProject.projectName.value, noRole, noRole, noRole, Set.empty, Set.empty),
      SamUserResource(userProject.projectName.value, creatorRole, noRole, noRole, Set.empty, Set.empty)
    )
    val samDAO = mock[SamDAO](RETURNS_SMART_NULLS)
    when(samDAO.listUserResources(SamResourceTypeNames.billingProject, testContext))
      .thenReturn(Future.successful(userBillingResources))
    val bpmDAO = mock[BillingProfileManagerDAO](RETURNS_SMART_NULLS)
    when(bpmDAO.getAllBillingProfiles(testContext)).thenReturn(Future.successful(Seq.empty))

    val userService = getUserService(samDAO = samDAO, bpmDAO = bpmDAO, billingRepository = Some(repository))

    val expected = Seq(
      RawlsBillingProjectResponse(Set(ProjectRoles.User), userProject, CloudPlatform.GCP)
    )

    Await.result(userService.listBillingProjectsV2(), Duration.Inf) should contain theSameElementsAs expected
    verify(repository).getBillingProjects(ArgumentMatchers.eq(Set(ownerProject.projectName, userProject.projectName)))
  }

  behavior of "addUserToBillingProjectV2"

  it should "update billing profile record when a user is added to a billing project" in {
    withMinimalTestDatabase { dataSource =>
      val billingProfileId = UUID.randomUUID()
      val ownerProject = billingProjectFromName(UUID.randomUUID().toString, billingProfileId)
      runAndWait(rawlsBillingProjectQuery.create(ownerProject))

      val userEmail = "newUser@test.com"
      val samDAO = mock[SamDAO](RETURNS_SMART_NULLS)
      when(
        samDAO.userHasAction(SamResourceTypeNames.billingProject,
                             ownerProject.projectName.value,
                             SamBillingProjectActions.alterPolicies,
                             testContext
        )
      ).thenReturn(Future.successful(true))
      when(
        samDAO.addUserToPolicy(SamResourceTypeNames.billingProject,
                               ownerProject.projectName.value,
                               SamBillingProjectPolicyNames.workspaceCreator,
                               userEmail,
                               testContext
        )
      ).thenReturn(Future.successful())

      val bpmDAO = mock[BillingProfileManagerDAO](RETURNS_SMART_NULLS)
      val userService = getUserService(dataSource, samDAO, bpmDAO = bpmDAO)

      Await.result(userService.addUserToBillingProjectV2(
                     ownerProject.projectName,
                     ProjectAccessUpdate(userEmail, ProjectRoles.User)
                   ),
                   Duration.Inf
      )
      // Expect BPM mock to have been called
      verify(bpmDAO).addProfilePolicyMember(
        billingProfileId,
        ProfilePolicy.User,
        userEmail,
        testContext
      )
      // Expect Sam mock to be called
      verify(samDAO).addUserToPolicy(SamResourceTypeNames.billingProject,
                                     ownerProject.projectName.value,
                                     SamBillingProjectPolicyNames.workspaceCreator,
                                     userEmail,
                                     testContext
      )
    }
  }

  it should "update billing profile record when an owner is added to a billing project" in {
    withMinimalTestDatabase { dataSource =>
      val billingProfileId = UUID.randomUUID()
      val ownerProject = billingProjectFromName(UUID.randomUUID().toString, billingProfileId)
      runAndWait(rawlsBillingProjectQuery.create(ownerProject))

      val ownerEmail = "newOwner@test.com"
      val samDAO = mock[SamDAO](RETURNS_SMART_NULLS)
      when(
        samDAO.userHasAction(SamResourceTypeNames.billingProject,
                             ownerProject.projectName.value,
                             SamBillingProjectActions.alterPolicies,
                             testContext
        )
      ).thenReturn(Future.successful(true))
      when(
        samDAO.addUserToPolicy(SamResourceTypeNames.billingProject,
                               ownerProject.projectName.value,
                               SamBillingProjectPolicyNames.owner,
                               ownerEmail,
                               testContext
        )
      ).thenReturn(Future.successful())

      val bpmDAO = mock[BillingProfileManagerDAO](RETURNS_SMART_NULLS)
      val userService = getUserService(dataSource, samDAO, bpmDAO = bpmDAO)

      Await.result(userService.addUserToBillingProjectV2(
                     ownerProject.projectName,
                     ProjectAccessUpdate(ownerEmail, ProjectRoles.Owner)
                   ),
                   Duration.Inf
      )
      // Expect BPM mock to have been called
      verify(bpmDAO).addProfilePolicyMember(
        billingProfileId,
        ProfilePolicy.Owner,
        ownerEmail,
        testContext
      )
      // Expect Sam mock to be called
      verify(samDAO).addUserToPolicy(SamResourceTypeNames.billingProject,
                                     ownerProject.projectName.value,
                                     SamBillingProjectPolicyNames.owner,
                                     ownerEmail,
                                     testContext
      )
    }
  }

  it should "not update Sam permissions if billing profile member addition fails" in {
    withMinimalTestDatabase { dataSource =>
      val billingProfileId = UUID.randomUUID()
      val ownerProject = billingProjectFromName(UUID.randomUUID().toString, billingProfileId)
      runAndWait(rawlsBillingProjectQuery.create(ownerProject))

      val ownerEmail = "newOwner@test.com"
      val samDAO = mock[SamDAO](RETURNS_SMART_NULLS)
      when(
        samDAO.userHasAction(SamResourceTypeNames.billingProject,
                             ownerProject.projectName.value,
                             SamBillingProjectActions.alterPolicies,
                             testContext
        )
      ).thenReturn(Future.successful(true))

      val bpmDAO = mock[BillingProfileManagerDAO](RETURNS_SMART_NULLS)
      when(
        bpmDAO.addProfilePolicyMember(
          billingProfileId,
          ProfilePolicy.Owner,
          ownerEmail,
          testContext
        )
      ).thenThrow(new RuntimeException("Mock Error"))
      val userService = getUserService(dataSource, samDAO, bpmDAO = bpmDAO)

      intercept[RuntimeException] {
        Await.result(userService.addUserToBillingProjectV2(
                       ownerProject.projectName,
                       ProjectAccessUpdate(ownerEmail, ProjectRoles.Owner)
                     ),
                     Duration.Inf
        )
      }
      // Sam mock should not be called because BPM update failed
      verify(samDAO, never()).addUserToPolicy(SamResourceTypeNames.billingProject,
                                              ownerProject.projectName.value,
                                              SamBillingProjectPolicyNames.owner,
                                              ownerEmail,
                                              testContext
      )
    }
  }

  behavior of "removeUserFromBillingProjectV2"

  it should "update billing profile record when a user is removed from a billing project" in {
    withMinimalTestDatabase { dataSource =>
      val billingProfileId = UUID.randomUUID()
      val ownerProject = billingProjectFromName(UUID.randomUUID().toString, billingProfileId)
      runAndWait(rawlsBillingProjectQuery.create(ownerProject))

      val userEmail = "leavingUser@test.com"
      val samDAO = mock[SamDAO](RETURNS_SMART_NULLS)
      when(
        samDAO.userHasAction(SamResourceTypeNames.billingProject,
                             ownerProject.projectName.value,
                             SamBillingProjectActions.alterPolicies,
                             testContext
        )
      ).thenReturn(Future.successful(true))
      when(
        samDAO.removeUserFromPolicy(SamResourceTypeNames.billingProject,
                                    ownerProject.projectName.value,
                                    SamBillingProjectPolicyNames.workspaceCreator,
                                    userEmail,
                                    testContext
        )
      ).thenReturn(Future.successful())

      val bpmDAO = mock[BillingProfileManagerDAO](RETURNS_SMART_NULLS)
      val userService = getUserService(dataSource, samDAO, bpmDAO = bpmDAO)

      Await.result(userService.removeUserFromBillingProjectV2(
                     ownerProject.projectName,
                     ProjectAccessUpdate(userEmail, ProjectRoles.User)
                   ),
                   Duration.Inf
      )
      // Expect BPM mock to have been called
      verify(bpmDAO).deleteProfilePolicyMember(
        billingProfileId,
        ProfilePolicy.User,
        userEmail,
        testContext
      )
      // Expect Sam mock to be called
      verify(samDAO).removeUserFromPolicy(SamResourceTypeNames.billingProject,
                                          ownerProject.projectName.value,
                                          SamBillingProjectPolicyNames.workspaceCreator,
                                          userEmail,
                                          testContext
      )
    }
  }

  it should "update billing profile record when an owner is removed from a billing project" in {
    withMinimalTestDatabase { dataSource =>
      val billingProfileId = UUID.randomUUID()
      val ownerProject = billingProjectFromName(UUID.randomUUID().toString, billingProfileId)
      runAndWait(rawlsBillingProjectQuery.create(ownerProject))

      val ownerEmail = "leavingOwner@test.com"
      val samDAO = mock[SamDAO](RETURNS_SMART_NULLS)
      when(
        samDAO.userHasAction(SamResourceTypeNames.billingProject,
                             ownerProject.projectName.value,
                             SamBillingProjectActions.alterPolicies,
                             testContext
        )
      ).thenReturn(Future.successful(true))
      when(
        samDAO.removeUserFromPolicy(SamResourceTypeNames.billingProject,
                                    ownerProject.projectName.value,
                                    SamBillingProjectPolicyNames.owner,
                                    ownerEmail,
                                    testContext
        )
      ).thenReturn(Future.successful())

      val bpmDAO = mock[BillingProfileManagerDAO](RETURNS_SMART_NULLS)
      val userService = getUserService(dataSource, samDAO, bpmDAO = bpmDAO)

      Await.result(userService.removeUserFromBillingProjectV2(
                     ownerProject.projectName,
                     ProjectAccessUpdate(ownerEmail, ProjectRoles.Owner)
                   ),
                   Duration.Inf
      )
      // Expect BPM mock to have been called
      verify(bpmDAO).deleteProfilePolicyMember(
        billingProfileId,
        ProfilePolicy.Owner,
        ownerEmail,
        testContext
      )
      // Expect Sam mock to be called
      verify(samDAO).removeUserFromPolicy(SamResourceTypeNames.billingProject,
                                          ownerProject.projectName.value,
                                          SamBillingProjectPolicyNames.owner,
                                          ownerEmail,
                                          testContext
      )
    }
  }

  it should "not update Sam permissions if billing profile member deletion fails" in {
    withMinimalTestDatabase { dataSource =>
      val billingProfileId = UUID.randomUUID()
      val ownerProject = billingProjectFromName(UUID.randomUUID().toString, billingProfileId)
      runAndWait(rawlsBillingProjectQuery.create(ownerProject))

      val ownerEmail = "leavingOwner@test.com"
      val samDAO = mock[SamDAO](RETURNS_SMART_NULLS)
      when(
        samDAO.userHasAction(SamResourceTypeNames.billingProject,
                             ownerProject.projectName.value,
                             SamBillingProjectActions.alterPolicies,
                             testContext
        )
      ).thenReturn(Future.successful(true))

      val bpmDAO = mock[BillingProfileManagerDAO](RETURNS_SMART_NULLS)
      when(
        bpmDAO.deleteProfilePolicyMember(
          billingProfileId,
          ProfilePolicy.Owner,
          ownerEmail,
          testContext
        )
      ).thenThrow(new RuntimeException("Mock Error"))
      val userService = getUserService(dataSource, samDAO, bpmDAO = bpmDAO)

      intercept[RuntimeException] {
        Await.result(userService.removeUserFromBillingProjectV2(
                       ownerProject.projectName,
                       ProjectAccessUpdate(ownerEmail, ProjectRoles.Owner)
                     ),
                     Duration.Inf
        )
      }
      // Sam mock should not be called because BPM update failed
      verify(samDAO, never()).removeUserFromPolicy(SamResourceTypeNames.billingProject,
                                                   ownerProject.projectName.value,
                                                   SamBillingProjectPolicyNames.owner,
                                                   ownerEmail,
                                                   testContext
      )
    }
  }
}
