package org.broadinstitute.dsde.rawls.user

import java.net.URLEncoder
import java.nio.charset.StandardCharsets.UTF_8
import java.util.UUID
import akka.http.scaladsl.model.StatusCodes
import com.google.api.services.cloudresourcemanager.model.Project
import com.typesafe.config.{Config, ConfigFactory}
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.config.DeploymentManagerConfig
import org.broadinstitute.dsde.rawls.dataaccess.{GoogleServicesDAO, MockBigQueryServiceFactory, MockGoogleServicesDAO, SamDAO, SlickDataSource}
import org.broadinstitute.dsde.rawls.dataaccess.slick.TestDriverComponent
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.webservice.PerRequest.RequestComplete
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatestplus.mockito.MockitoSugar
import org.mockito.Mockito._

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

class UserServiceSpec extends AnyFlatSpecLike with TestDriverComponent with MockitoSugar with BeforeAndAfterAll with Matchers with ScalaFutures {
  import driver.api._

  val defaultServicePerimeterName: ServicePerimeterName = ServicePerimeterName("accessPolicies/policyName/servicePerimeters/servicePerimeterName")
  val urlEncodedDefaultServicePerimeterName: String = URLEncoder.encode(defaultServicePerimeterName.value, UTF_8.name)
  val defaultGoogleProjectNumber: GoogleProjectNumber = GoogleProjectNumber("42")
  val defaultBillingProjectName: RawlsBillingProjectName = RawlsBillingProjectName("test-bp")
  val defaultBillingProject: RawlsBillingProject = RawlsBillingProject(defaultBillingProjectName, CreationStatuses.Ready, None, None, googleProjectNumber = Option(defaultGoogleProjectNumber))
  val defaultMockSamDAO: SamDAO = mock[SamDAO](RETURNS_SMART_NULLS)
  val defaultMockGcsDAO: GoogleServicesDAO = new MockGoogleServicesDAO("test")
  val testConf: Config = ConfigFactory.load()

  override implicit val patienceConfig: PatienceConfig = PatienceConfig(1.second)

  override def beforeAll(): Unit = {
    when(defaultMockSamDAO.userHasAction(SamResourceTypeNames.servicePerimeter, urlEncodedDefaultServicePerimeterName, SamServicePerimeterActions.addProject, userInfo)).thenReturn(Future.successful(true))
    when(defaultMockSamDAO.userHasAction(SamResourceTypeNames.billingProject, defaultBillingProjectName.value, SamBillingProjectActions.addToServicePerimeter, userInfo)).thenReturn(Future.successful(true))
  }

  def getUserService(dataSource: SlickDataSource, samDAO: SamDAO = defaultMockSamDAO, gcsDAO: GoogleServicesDAO = defaultMockGcsDAO): UserService = {
    new UserService(
      userInfo,
      dataSource,
      gcsDAO,
      null,
      samDAO,
      MockBigQueryServiceFactory.ioFactory(),
      testConf.getString("gcs.pathToCredentialJson"),
      "",
      DeploymentManagerConfig(testConf.getConfig("gcs.deploymentManager")),
      null
    )
  }

  // 202 when project exists without perimeter and user is owner of project and has right permissions on service-perimeter
  "UserService" should "add a service perimeter field and update the status for an existing project when user has correct permissions" in {
    withEmptyTestDatabase { dataSource: SlickDataSource =>
      val project = defaultBillingProject
      runAndWait(rawlsBillingProjectQuery.create(project))

      val userService = getUserService(dataSource)

      val actual = userService.addProjectToServicePerimeter(defaultServicePerimeterName, project.projectName).futureValue
      val expected = RequestComplete(StatusCodes.Accepted)
      actual shouldEqual expected
    }
  }

  // 202 when all of the above even if project doesn't have a google project number
  it should "add a service perimeter field and update the status for an existing project when user has correct permissions even if there isn't a project number already" in {
    withEmptyTestDatabase { dataSource: SlickDataSource =>
      val project = defaultBillingProject.copy(googleProjectNumber = None)

      runAndWait(rawlsBillingProjectQuery.create(project))

      val mockGcsDAO = mock[GoogleServicesDAO](RETURNS_SMART_NULLS)
      when(mockGcsDAO.getGoogleProject(project.googleProjectId)).thenReturn(Future.successful(new Project().setProjectNumber(42L)))
      val folderId = "folders/1234567"
      when(mockGcsDAO.getFolderId(defaultServicePerimeterName.value.split("/").last)).thenReturn(Future.successful(Option(folderId)))
      when(mockGcsDAO.addProjectToFolder(project.googleProjectId, folderId)).thenReturn(Future.successful(()))

      val userService = getUserService(dataSource, gcsDAO = mockGcsDAO)

      val actual = userService.addProjectToServicePerimeter(defaultServicePerimeterName, project.projectName).futureValue
      val expected = RequestComplete(StatusCodes.Accepted)
      actual shouldEqual expected
    }
  }

  // 400 when project has a perimeter already
  it should "fail with a 400 when the project already has a perimeter" in {
    withEmptyTestDatabase { dataSource: SlickDataSource =>
      val project = defaultBillingProject.copy(servicePerimeter = Option(ServicePerimeterName("accessPolicies/123/servicePerimeters/other_perimeter")))
      runAndWait(rawlsBillingProjectQuery.create(project))

      val userService = getUserService(dataSource)

      val actual = userService.addProjectToServicePerimeter(defaultServicePerimeterName, project.projectName).failed.futureValue
      assert(actual.isInstanceOf[RawlsExceptionWithErrorReport])
      actual.asInstanceOf[RawlsExceptionWithErrorReport].errorReport.statusCode shouldEqual Option(StatusCodes.BadRequest)
    }
  }

  // 400 when project is not 'Ready'
  it should "fail with a 400 when the project's status is not 'Ready'" in {
    withEmptyTestDatabase { dataSource: SlickDataSource =>
      val project = defaultBillingProject.copy(status = CreationStatuses.Creating)
      runAndWait(rawlsBillingProjectQuery.create(project))

      val userService = getUserService(dataSource)

      val actual = userService.addProjectToServicePerimeter(defaultServicePerimeterName, project.projectName).failed.futureValue
      assert(actual.isInstanceOf[RawlsExceptionWithErrorReport])
      actual.asInstanceOf[RawlsExceptionWithErrorReport].errorReport.statusCode shouldEqual Option(StatusCodes.BadRequest)
    }
  }

  // 403 when user isn't owner of project or project dne
  it should "fail with a 403 when Sam says the user does not have permission on the billing project" in {
    withEmptyTestDatabase { dataSource: SlickDataSource =>
      val project = defaultBillingProject
      runAndWait(rawlsBillingProjectQuery.create(project))

      val mockSamDAO = mock[SamDAO](RETURNS_SMART_NULLS)
      when(mockSamDAO.userHasAction(SamResourceTypeNames.servicePerimeter, urlEncodedDefaultServicePerimeterName, SamServicePerimeterActions.addProject, userInfo)).thenReturn(Future.successful(true))
      when(mockSamDAO.userHasAction(SamResourceTypeNames.billingProject, project.projectName.value, SamBillingProjectActions.addToServicePerimeter, userInfo)).thenReturn(Future.successful(false))

      val userService = getUserService(dataSource, mockSamDAO)

      val actual = userService.addProjectToServicePerimeter(defaultServicePerimeterName, project.projectName).failed.futureValue
      assert(actual.isInstanceOf[RawlsExceptionWithErrorReport])
      actual.asInstanceOf[RawlsExceptionWithErrorReport].errorReport.statusCode shouldEqual Option(StatusCodes.Forbidden)
    }
  }

  // 404 when user doesn't have permissions on service-perimeter or s-p dne
  it should "fail with a 404 when Sam says the user does not have permission on the service perimeter" in {
    withEmptyTestDatabase { dataSource: SlickDataSource =>
      val project = defaultBillingProject
      runAndWait(rawlsBillingProjectQuery.create(project))

      val mockSamDAO = mock[SamDAO](RETURNS_SMART_NULLS)
      when(mockSamDAO.userHasAction(SamResourceTypeNames.servicePerimeter, urlEncodedDefaultServicePerimeterName, SamServicePerimeterActions.addProject, userInfo)).thenReturn(Future.successful(false))
      when(mockSamDAO.userHasAction(SamResourceTypeNames.billingProject, project.projectName.value, SamBillingProjectActions.addToServicePerimeter, userInfo)).thenReturn(Future.successful(true))

      val userService = getUserService(dataSource, mockSamDAO)

      val actual = userService.addProjectToServicePerimeter(defaultServicePerimeterName, project.projectName).failed.futureValue
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
      when(mockSamDAO.userHasAction(SamResourceTypeNames.billingProject, project.projectName.value, SamBillingProjectActions.deleteBillingProject, userInfo)).thenReturn(Future.successful(true))
      when(mockSamDAO.listAllResourceMemberIds(SamResourceTypeNames.billingProject, project.projectName.value, userInfo)).thenReturn(Future.successful(Set(userIdInfo)))
      when(mockSamDAO.getPetServiceAccountKeyForUser(project.googleProjectId, userInfo.userEmail)).thenReturn(Future.successful(petSAJson))
      when(mockSamDAO.listResourceChildren(SamResourceTypeNames.billingProject, project.projectName.value, userInfo)).thenReturn(Future.successful(Seq(SamFullyQualifiedResourceId(project.googleProjectId.value, SamResourceTypeNames.googleProject.value))))
      when(mockSamDAO.deleteUserPetServiceAccount(project.googleProjectId, userInfo)).thenReturn(Future.successful())
      when(mockSamDAO.deleteResource(SamResourceTypeNames.billingProject, project.projectName.value, userInfo)).thenReturn(Future.successful())
      when(mockSamDAO.deleteResource(SamResourceTypeNames.googleProject, project.googleProjectId.value, userInfo)).thenReturn(Future.successful())

      val mockGcsDAO = mock[GoogleServicesDAO](RETURNS_SMART_NULLS)
      when(mockGcsDAO.getUserInfoUsingJson(petSAJson)).thenReturn(Future.successful(userInfo))
      when(mockGcsDAO.deleteProject(project.googleProjectId)).thenReturn(Future.successful())

      val userService = getUserService(dataSource, mockSamDAO, gcsDAO = mockGcsDAO)
      val actual = userService.deleteBillingProject(defaultBillingProjectName).futureValue

      verify(mockSamDAO).deleteUserPetServiceAccount(project.googleProjectId, userInfo)
      verify(mockSamDAO).deleteResource(SamResourceTypeNames.billingProject, project.projectName.value, userInfo)
      verify(mockSamDAO).deleteResource(SamResourceTypeNames.googleProject, project.googleProjectId.value, userInfo)
      verify(mockGcsDAO).deleteProject(project.googleProjectId)

      runAndWait(rawlsBillingProjectQuery.load(defaultBillingProjectName)) shouldBe empty
      actual shouldEqual RequestComplete(StatusCodes.NoContent)
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
      runAndWait(workspaceQuery.save(workspace))

      val mockSamDAO = mock[SamDAO](RETURNS_SMART_NULLS)
      when(mockSamDAO.userHasAction(SamResourceTypeNames.billingProject, project.projectName.value, SamBillingProjectActions.deleteBillingProject, userInfo)).thenReturn(Future.successful(true))

      val userService = getUserService(dataSource, mockSamDAO)

      val actual = intercept[RawlsExceptionWithErrorReport] {
        Await.result(userService.deleteBillingProject(defaultBillingProjectName), Duration.Inf).asInstanceOf[RequestComplete[ErrorReport]]
      }
      actual.errorReport.statusCode shouldEqual Option(StatusCodes.BadRequest)
    }
  }

  it should "fail with a 403 when Sam says the user does not have permission to delete billing project" in {
    withEmptyTestDatabase { dataSource: SlickDataSource =>
      val project = defaultBillingProject
      runAndWait(rawlsBillingProjectQuery.create(project))

      val mockSamDAO = mock[SamDAO](RETURNS_SMART_NULLS)
      when(mockSamDAO.userHasAction(SamResourceTypeNames.billingProject, defaultBillingProjectName.value, SamBillingProjectActions.deleteBillingProject, userInfo)).thenReturn(Future.successful(false))

      val userService = getUserService(dataSource, mockSamDAO)

      val actual = intercept[RawlsExceptionWithErrorReport] {
        Await.result(userService.deleteBillingProject(defaultBillingProjectName), Duration.Inf).asInstanceOf[RequestComplete[ErrorReport]]
      }
      actual.errorReport.statusCode shouldEqual Option(StatusCodes.Forbidden)
    }
  }

  it should "set the spend configuration of a billing project when the user has permission" in {
    withMinimalTestDatabase { dataSource: SlickDataSource =>
      val billingProject = minimalTestData.billingProject
      val spendReportDatasetName = "test_dataset"

      val mockSamDAO = mock[SamDAO](RETURNS_SMART_NULLS)
      when(mockSamDAO.userHasAction(SamResourceTypeNames.billingProject, billingProject.projectName.value, SamBillingProjectActions.alterSpendReportConfiguration, userInfo)).thenReturn(Future.successful(true))

      val userService = getUserService(dataSource, mockSamDAO)

      Await.result(userService.setBillingProjectSpendConfiguration(billingProject.projectName, spendReportDatasetName), Duration.Inf) shouldEqual 1

      val spendReportConfigInDb = runAndWait(dataSource.dataAccess.rawlsBillingProjectQuery.filter(_.projectName === billingProject.projectName.value).map(row => (row.spendReportDataset, row.spendReportTable)).result)

      spendReportConfigInDb.head shouldEqual (Some(spendReportDatasetName), Some("gcp_billing_export_v1_some-billing-account"))
    }
  }

  it should "not set the spend configuration of a billing project when the user doesn't have permission" in {
    withMinimalTestDatabase { dataSource: SlickDataSource =>
      val billingProject = minimalTestData.billingProject
      val spendReportDatasetName = "test_dataset"

      val mockSamDAO = mock[SamDAO](RETURNS_SMART_NULLS)
      when(mockSamDAO.userHasAction(SamResourceTypeNames.billingProject, billingProject.projectName.value, SamBillingProjectActions.alterSpendReportConfiguration, userInfo)).thenReturn(Future.successful(false))

      val userService = getUserService(dataSource, mockSamDAO)

      val actual = intercept[RawlsExceptionWithErrorReport] {
        Await.result(userService.setBillingProjectSpendConfiguration(billingProject.projectName, spendReportDatasetName), Duration.Inf)
      }

      //assert that the entire action was forbidden
      actual.errorReport.statusCode.get shouldEqual StatusCodes.Forbidden

      val spendReportConfigInDb = runAndWait(dataSource.dataAccess.rawlsBillingProjectQuery.filter(_.projectName === billingProject.projectName.value).map(row => (row.spendReportDataset, row.spendReportTable)).result)

      //assert that no change was made to the spend configuration
      spendReportConfigInDb.head shouldEqual (None, None)
    }
  }

  it should "clear the spend configuration of a billing project" in {
    withMinimalTestDatabase { dataSource: SlickDataSource =>
      val billingProject = minimalTestData.billingProject
      val spendReportDatasetName = "test_dataset"

      val mockSamDAO = mock[SamDAO](RETURNS_SMART_NULLS)
      when(mockSamDAO.userHasAction(SamResourceTypeNames.billingProject, billingProject.projectName.value, SamBillingProjectActions.alterSpendReportConfiguration, userInfo)).thenReturn(Future.successful(true))

      val userService = getUserService(dataSource, mockSamDAO)

      Await.result(userService.clearBillingProjectSpendConfiguration(billingProject.projectName), Duration.Inf) shouldEqual 1

      val spendReportConfigInDb = runAndWait(dataSource.dataAccess.rawlsBillingProjectQuery.filter(_.projectName === billingProject.projectName.value).map(row => (row.spendReportDataset, row.spendReportTable)).result)

      spendReportConfigInDb.head shouldEqual (None, None)
    }
  }

  it should "not clear the spend configuration of a billing project when the user doesn't have permission" in {
    withMinimalTestDatabase { dataSource: SlickDataSource =>
      val billingProject = minimalTestData.billingProject
      val spendReportDatasetName = "should_not_clear_dataset"
      val spendReportTableName = "should_not_clear_table"

      //first, directly set the spend configuration in the DB outside of the user's permissions, so we can assert that it wasn't cleared
      runAndWait(dataSource.dataAccess.rawlsBillingProjectQuery.setBillingProjectSpendConfiguration(billingProject.projectName, Some(spendReportDatasetName), Some(spendReportTableName)))

      val mockSamDAO = mock[SamDAO](RETURNS_SMART_NULLS)
      when(mockSamDAO.userHasAction(SamResourceTypeNames.billingProject, billingProject.projectName.value, SamBillingProjectActions.alterSpendReportConfiguration, userInfo)).thenReturn(Future.successful(false))

      val userService = getUserService(dataSource, mockSamDAO)

      val actual = intercept[RawlsExceptionWithErrorReport] {
        Await.result(userService.clearBillingProjectSpendConfiguration(billingProject.projectName), Duration.Inf)
      }

      //assert that the entire action was forbidden
      actual.errorReport.statusCode.get shouldEqual StatusCodes.Forbidden

      val spendReportConfigInDb = runAndWait(dataSource.dataAccess.rawlsBillingProjectQuery.filter(_.projectName === billingProject.projectName.value).map(row => (row.spendReportDataset, row.spendReportTable)).result)

      //assert that no change was made to the spend configuration
      spendReportConfigInDb.head shouldEqual (Some(spendReportDatasetName), Some(spendReportTableName))
    }
  }

  it should "throw a RawlsExceptionWithErrorReport when setting the spend configuration if the dataset does not exist or can't be accessed" in {
    withMinimalTestDatabase { dataSource: SlickDataSource =>
      val billingProject = minimalTestData.billingProject
      val spendReportDatasetName = "dataset_does_not_exist"

      val mockSamDAO = mock[SamDAO](RETURNS_SMART_NULLS)
      when(mockSamDAO.userHasAction(SamResourceTypeNames.billingProject, billingProject.projectName.value, SamBillingProjectActions.alterSpendReportConfiguration, userInfo)).thenReturn(Future.successful(true))

      val userService = getUserService(dataSource, mockSamDAO)

      val actual = intercept[RawlsExceptionWithErrorReport] {
        Await.result(userService.setBillingProjectSpendConfiguration(billingProject.projectName, spendReportDatasetName), Duration.Inf)
      }

      actual.errorReport.statusCode.get shouldEqual StatusCodes.BadRequest
    }
  }

  it should "throw a RawlsExceptionWithErrorReport when setting the spend configuration if the table does not exist or can't be accessed" in {
    withMinimalTestDatabase { dataSource: SlickDataSource =>
      val billingProject = RawlsBillingProject(RawlsBillingProjectName("project_without_table"), CreationStatuses.Ready, None, None)
      runAndWait(dataSource.dataAccess.rawlsBillingProjectQuery.create(billingProject))

      val spendReportDatasetName = "some_dataset"

      val mockSamDAO = mock[SamDAO](RETURNS_SMART_NULLS)
      when(mockSamDAO.userHasAction(SamResourceTypeNames.billingProject, billingProject.projectName.value, SamBillingProjectActions.alterSpendReportConfiguration, userInfo)).thenReturn(Future.successful(true))

      val userService = getUserService(dataSource, mockSamDAO)

      val actual = intercept[RawlsExceptionWithErrorReport] {
        Await.result(userService.setBillingProjectSpendConfiguration(billingProject.projectName, spendReportDatasetName), Duration.Inf)
      }

      actual.errorReport.statusCode.get shouldEqual StatusCodes.BadRequest
    }
  }

  it should "throw a RawlsExceptionWithErrorReport when setting the spend configuration if the billing project does not have a billing account associated with it" in {
    withMinimalTestDatabase { dataSource: SlickDataSource =>
      val billingProject = RawlsBillingProject(RawlsBillingProjectName("project_without_billing_account"), CreationStatuses.Ready, None, None)
      runAndWait(dataSource.dataAccess.rawlsBillingProjectQuery.create(billingProject))

      val spendReportDatasetName = "some_dataset"

      val mockSamDAO = mock[SamDAO](RETURNS_SMART_NULLS)
      when(mockSamDAO.userHasAction(SamResourceTypeNames.billingProject, billingProject.projectName.value, SamBillingProjectActions.alterSpendReportConfiguration, userInfo)).thenReturn(Future.successful(true))

      val userService = getUserService(dataSource, mockSamDAO)

      val actual = intercept[RawlsExceptionWithErrorReport] {
        Await.result(userService.setBillingProjectSpendConfiguration(billingProject.projectName, spendReportDatasetName), Duration.Inf)
      }

      actual.errorReport.statusCode.get shouldEqual StatusCodes.BadRequest
    }
  }

  it should "throw a RawlsExceptionWithErrorReport when setting the spend configuration if the dataset has an invalid name" in {
    withMinimalTestDatabase { dataSource: SlickDataSource =>
      val billingProject = minimalTestData.billingProject
      val spendReportDatasetName = "test-dataset"

      val mockSamDAO = mock[SamDAO](RETURNS_SMART_NULLS)
      when(mockSamDAO.userHasAction(SamResourceTypeNames.billingProject, billingProject.projectName.value, SamBillingProjectActions.alterSpendReportConfiguration, userInfo)).thenReturn(Future.successful(true))

      val userService = getUserService(dataSource, mockSamDAO)

      val actual = intercept[RawlsExceptionWithErrorReport] {
        Await.result(userService.setBillingProjectSpendConfiguration(billingProject.projectName, spendReportDatasetName), Duration.Inf)
      }

      actual.errorReport.statusCode.get shouldEqual StatusCodes.BadRequest
    }
  }

}
