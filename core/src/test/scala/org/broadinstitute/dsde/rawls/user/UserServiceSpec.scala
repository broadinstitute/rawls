package org.broadinstitute.dsde.rawls.user

import java.net.URLEncoder
import java.nio.charset.StandardCharsets.UTF_8
import java.util.UUID

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import com.google.api.services.cloudresourcemanager.model.Project
import com.typesafe.config.{Config, ConfigFactory}
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.config.DeploymentManagerConfig
import org.broadinstitute.dsde.rawls.dataaccess.slick.TestDriverComponent
import org.broadinstitute.dsde.rawls.dataaccess.{GoogleServicesDAO, MockGoogleServicesDAO, SamDAO, SlickDataSource}
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.serviceperimeter.ServicePerimeterService
import org.broadinstitute.dsde.rawls.webservice.PerRequest.RequestComplete
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

class UserServiceSpec extends AnyFlatSpecLike with TestDriverComponent with MockitoSugar with BeforeAndAfterAll with Matchers with ScalaFutures {
  val defaultServicePerimeterName: ServicePerimeterName = ServicePerimeterName("accessPolicies/policyName/servicePerimeters/servicePerimeterName")
  val urlEncodedDefaultServicePerimeterName: String = URLEncoder.encode(defaultServicePerimeterName.value, UTF_8.name)
  val defaultGoogleProjectNumber: GoogleProjectNumber = GoogleProjectNumber("42")
  val defaultBillingProjectName: RawlsBillingProjectName = RawlsBillingProjectName("test-bp")
  val defaultBillingProject: RawlsBillingProject = RawlsBillingProject(defaultBillingProjectName, CreationStatuses.Ready, None, None, googleProjectNumber = Option(defaultGoogleProjectNumber))
  val defaultMockSamDAO: SamDAO = mock[SamDAO](RETURNS_SMART_NULLS)
  val defaultMockGcsDAO: GoogleServicesDAO = new MockGoogleServicesDAO("test")
  val defaultMockServicePerimeterService: ServicePerimeterService = mock[ServicePerimeterService](RETURNS_SMART_NULLS)
  val testConf: Config = ConfigFactory.load()

  override implicit val patienceConfig: PatienceConfig = PatienceConfig(1.second)

  override def beforeAll(): Unit = {
    when(defaultMockSamDAO.userHasAction(SamResourceTypeNames.servicePerimeter, urlEncodedDefaultServicePerimeterName, SamServicePerimeterActions.addProject, userInfo)).thenReturn(Future.successful(true))
    when(defaultMockSamDAO.userHasAction(SamResourceTypeNames.billingProject, defaultBillingProjectName.value, SamBillingProjectActions.addToServicePerimeter, userInfo)).thenReturn(Future.successful(true))
  }

  def getUserService(dataSource: SlickDataSource, samDAO: SamDAO = defaultMockSamDAO, gcsDAO: GoogleServicesDAO = defaultMockGcsDAO, servicePerimeterService: ServicePerimeterService = defaultMockServicePerimeterService, adminRegisterBillingAccountId: RawlsBillingAccountName = RawlsBillingAccountName("billingAccounts/ABCDE-FGHIJ-KLMNO")): UserService = {
    new UserService(
      userInfo,
      dataSource,
      gcsDAO,
      null,
      samDAO,
      "",
      DeploymentManagerConfig(testConf.getConfig("gcs.deploymentManager")),
      null,
      servicePerimeterService,
      adminRegisterBillingAccountId: RawlsBillingAccountName
    )
  }

  // 204 when project exists without perimeter and user is owner of project and has right permissions on service-perimeter
  "UserService" should "add a service perimeter field for an existing project when user has correct permissions" in {
    withEmptyTestDatabase { dataSource: SlickDataSource =>
      val project = defaultBillingProject
      runAndWait(rawlsBillingProjectQuery.create(project))

      val mockServicePerimeterService = mock[ServicePerimeterService](RETURNS_SMART_NULLS)
      when(mockServicePerimeterService.overwriteGoogleProjectsInPerimeter(defaultServicePerimeterName)).thenReturn(Future.successful(()))

      val userService = getUserService(dataSource, servicePerimeterService = mockServicePerimeterService)

      val actual = userService.addProjectToServicePerimeter(defaultServicePerimeterName, project.projectName).futureValue
      val expected = RequestComplete(StatusCodes.NoContent)
      actual shouldEqual expected
      verify(mockServicePerimeterService).overwriteGoogleProjectsInPerimeter(defaultServicePerimeterName)

      val updatedProject = dataSource.inTransaction { dataAccess =>
        dataAccess.rawlsBillingProjectQuery.load(project.projectName)
      }.futureValue.getOrElse(fail(s"Project ${project.projectName} not found"))

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
      when(mockGcsDAO.getFolderId(defaultServicePerimeterName.value.split("/").last)).thenReturn(Future.successful(Option(folderId)))
      when(mockGcsDAO.addProjectToFolder(project.googleProjectId, folderId)).thenReturn(Future.successful(()))
      when(mockGcsDAO.getGoogleProjectNumber(googleProject)).thenReturn(googleProjectNumber)

      val mockServicePerimeterService = mock[ServicePerimeterService](RETURNS_SMART_NULLS)
      when(mockServicePerimeterService.overwriteGoogleProjectsInPerimeter(defaultServicePerimeterName)).thenReturn(Future.successful(()))

      val userService = getUserService(dataSource, gcsDAO = mockGcsDAO, servicePerimeterService = mockServicePerimeterService)

      val actual = userService.addProjectToServicePerimeter(defaultServicePerimeterName, project.projectName).futureValue
      val expected = RequestComplete(StatusCodes.NoContent)
      actual shouldEqual expected
      verify(mockServicePerimeterService).overwriteGoogleProjectsInPerimeter(defaultServicePerimeterName)

      val updatedProject = dataSource.inTransaction { dataAccess =>
        dataAccess.rawlsBillingProjectQuery.load(project.projectName)
      }.futureValue.getOrElse(fail(s"Project ${project.projectName} not found"))

      updatedProject.servicePerimeter shouldBe Option(defaultServicePerimeterName)
      updatedProject.googleProjectNumber shouldBe Option(googleProjectNumber)
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
      when(mockGcsDAO.deleteV1Project(project.googleProjectId)).thenReturn(Future.successful())

      val userService = getUserService(dataSource, mockSamDAO, gcsDAO = mockGcsDAO)
      val actual = userService.deleteBillingProject(defaultBillingProjectName).futureValue

      verify(mockSamDAO).deleteUserPetServiceAccount(project.googleProjectId, userInfo)
      verify(mockSamDAO).deleteResource(SamResourceTypeNames.billingProject, project.projectName.value, userInfo)
      verify(mockSamDAO).deleteResource(SamResourceTypeNames.googleProject, project.googleProjectId.value, userInfo)
      verify(mockGcsDAO).deleteV1Project(project.googleProjectId)

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
      runAndWait(workspaceQuery.createOrUpdate(workspace))

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

  it should "not delete the Google project when unregistering a Billing project" in {
    withEmptyTestDatabase { dataSource: SlickDataSource =>
      val project = defaultBillingProject
      val ownerInfoMap = Map("newOwnerEmail" -> userInfo.userEmail.value, "newOwnerToken" ->  userInfo.accessToken.value)
      val ownerIdInfo = UserIdInfo(userInfo.userSubjectId.value, userInfo.userEmail.value, Option("googleSubId"))
      val ownerUserInfo = UserInfo(RawlsUserEmail(ownerInfoMap("newOwnerEmail")), OAuth2BearerToken(ownerInfoMap("newOwnerToken")), 3600, RawlsUserSubjectId("0"))
      val petSAJson = "petJson"
      runAndWait(rawlsBillingProjectQuery.create(project))

      val mockSamDAO = mock[SamDAO](RETURNS_SMART_NULLS)
      when(mockSamDAO.listResourceChildren(ArgumentMatchers.eq(SamResourceTypeNames.billingProject), ArgumentMatchers.eq(project.projectName.value), any[UserInfo])).thenReturn(Future.successful(Seq(SamFullyQualifiedResourceId(project.googleProjectId.value, SamResourceTypeNames.googleProject.value))))
      when(mockSamDAO.listAllResourceMemberIds(ArgumentMatchers.eq(SamResourceTypeNames.billingProject), ArgumentMatchers.eq(project.projectName.value), any[UserInfo])).thenReturn(Future.successful(Set(ownerIdInfo)))
      when(mockSamDAO.getPetServiceAccountKeyForUser(project.googleProjectId, ownerUserInfo.userEmail)).thenReturn(Future.successful(petSAJson))
      when(mockSamDAO.deleteUserPetServiceAccount(project.googleProjectId, ownerUserInfo)).thenReturn(Future.successful())
      when(mockSamDAO.deleteResource(ArgumentMatchers.eq(SamResourceTypeNames.googleProject), ArgumentMatchers.eq(project.googleProjectId.value), any[UserInfo])).thenReturn(Future.successful())
      when(mockSamDAO.deleteResource(ArgumentMatchers.eq(SamResourceTypeNames.billingProject), ArgumentMatchers.eq(project.projectName.value), any[UserInfo])).thenReturn(Future.successful())

      val mockGcsDAO = mock[GoogleServicesDAO](RETURNS_SMART_NULLS)
      when(mockGcsDAO.isAdmin(any[String])).thenReturn(Future.successful(true))
      when(mockGcsDAO.getUserInfoUsingJson(petSAJson)).thenReturn(Future.successful(ownerUserInfo))
      when(mockGcsDAO.deleteV1Project(project.googleProjectId)).thenReturn(Future.successful())

      val userService = getUserService(dataSource, mockSamDAO, gcsDAO = mockGcsDAO)
      val actual = userService.adminUnregisterBillingProjectWithOwnerInfo(project.projectName, ownerInfoMap).futureValue

      verify(mockSamDAO).deleteUserPetServiceAccount(project.googleProjectId, ownerUserInfo)
      verify(mockSamDAO).deleteResource(SamResourceTypeNames.billingProject, project.projectName.value, ownerUserInfo)
      verify(mockSamDAO).deleteResource(SamResourceTypeNames.googleProject, project.googleProjectId.value, ownerUserInfo)
      verify(mockGcsDAO, never()).deleteV1Project(project.googleProjectId)

      runAndWait(rawlsBillingProjectQuery.load(defaultBillingProjectName)) shouldBe empty
      actual shouldEqual RequestComplete(StatusCodes.NoContent)
    }
  }
}
