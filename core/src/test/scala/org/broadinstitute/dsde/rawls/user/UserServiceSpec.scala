package org.broadinstitute.dsde.rawls.user

import java.net.URLEncoder
import java.nio.charset.StandardCharsets.UTF_8

import akka.http.scaladsl.model.StatusCodes
import com.google.api.services.cloudresourcemanager.model.Project
import com.typesafe.config.{Config, ConfigFactory}
import org.broadinstitute.dsde.rawls.RawlsExceptionWithErrorReport
import org.broadinstitute.dsde.rawls.dataaccess.{GoogleServicesDAO, MockGoogleServicesDAO, SamDAO, SlickDataSource}
import org.broadinstitute.dsde.rawls.dataaccess.slick.TestDriverComponent
import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.webservice.PerRequest.RequestComplete
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.mockito.MockitoSugar
import org.mockito.Mockito._

import scala.concurrent.Future
import scala.concurrent.duration._

class UserServiceSpec extends FlatSpecLike with TestDriverComponent with MockitoSugar with BeforeAndAfterAll with Matchers with ScalaFutures {
  val defaultServicePerimeterName: ServicePerimeterName = ServicePerimeterName("accessPolicies/policyName/servicePerimeters/servicePerimeterName")
  val urlEncodedDefaultServicePerimeterName: String = URLEncoder.encode(defaultServicePerimeterName.value, UTF_8.name)
  val defaultGoogleProjectNumber: GoogleProjectNumber = GoogleProjectNumber("42")
  val defaultCromwellBucketUrl: String = "bucket-url"
  val defaultBillingProjectName: RawlsBillingProjectName = RawlsBillingProjectName("test-bp")
  val defaultBillingProject: RawlsBillingProject = RawlsBillingProject(defaultBillingProjectName, defaultCromwellBucketUrl, CreationStatuses.Ready, None, None, googleProjectNumber = Option(defaultGoogleProjectNumber))
  val defaultMockSamDAO: SamDAO = mock[SamDAO]
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
      Seq.empty,
      "",
      testConf.getConfig("gcs.deploymentManager"),
      null
    )
  }

  // 202 when project exists without perimeter and user is owner of project and has right permissions on service-perimeter
  "UserService" should "add a service perimeter field and update the status for an existing project when user has correct permissions" in {
    withEmptyTestDatabase { dataSource: SlickDataSource =>
      val project = defaultBillingProject
      runAndWait(rawlsBillingProjectQuery.create(project))

      val userService = getUserService(dataSource)

      val actual = userService.AddProjectToServicePerimeter(defaultServicePerimeterName, project.projectName).futureValue
      val expected = RequestComplete(StatusCodes.Accepted)
      actual shouldEqual expected
    }
  }

  // 202 when all of the above even if project doesn't have a google project number
  it should "add a service perimeter field and update the status for an existing project when user has correct permissions even if there isn't a project number already" in {
    withEmptyTestDatabase { dataSource: SlickDataSource =>
      val project = defaultBillingProject.copy(googleProjectNumber = None)
      runAndWait(rawlsBillingProjectQuery.create(project))

      val mockGcsDAO = mock[GoogleServicesDAO]
      when(mockGcsDAO.getGoogleProject(project.projectName)).thenReturn(Future.successful(new Project().setProjectNumber(42L)))

      val userService = getUserService(dataSource, gcsDAO = mockGcsDAO)

      val actual = userService.AddProjectToServicePerimeter(defaultServicePerimeterName, project.projectName).futureValue
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

      val actual = userService.AddProjectToServicePerimeter(defaultServicePerimeterName, project.projectName).failed.futureValue
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

      val actual = userService.AddProjectToServicePerimeter(defaultServicePerimeterName, project.projectName).failed.futureValue
      assert(actual.isInstanceOf[RawlsExceptionWithErrorReport])
      actual.asInstanceOf[RawlsExceptionWithErrorReport].errorReport.statusCode shouldEqual Option(StatusCodes.BadRequest)
    }
  }

  // 403 when user isn't owner of project or project dne
  it should "fail with a 403 when Sam says the user does not have permission on the billing project" in {
    withEmptyTestDatabase { dataSource: SlickDataSource =>
      val project = defaultBillingProject
      runAndWait(rawlsBillingProjectQuery.create(project))

      val mockSamDAO = mock[SamDAO]
      when(mockSamDAO.userHasAction(SamResourceTypeNames.servicePerimeter, urlEncodedDefaultServicePerimeterName, SamServicePerimeterActions.addProject, userInfo)).thenReturn(Future.successful(true))
      when(mockSamDAO.userHasAction(SamResourceTypeNames.billingProject, project.projectName.value, SamBillingProjectActions.addToServicePerimeter, userInfo)).thenReturn(Future.successful(false))

      val userService = getUserService(dataSource, mockSamDAO)

      val actual = userService.AddProjectToServicePerimeter(defaultServicePerimeterName, project.projectName).failed.futureValue
      assert(actual.isInstanceOf[RawlsExceptionWithErrorReport])
      actual.asInstanceOf[RawlsExceptionWithErrorReport].errorReport.statusCode shouldEqual Option(StatusCodes.Forbidden)
    }
  }

  // 404 when user doesn't have permissions on service-perimeter or s-p dne
  it should "fail with a 404 when Sam says the user does not have permission on the service perimeter" in {
    withEmptyTestDatabase { dataSource: SlickDataSource =>
      val project = defaultBillingProject
      runAndWait(rawlsBillingProjectQuery.create(project))

      val mockSamDAO = mock[SamDAO]
      when(mockSamDAO.userHasAction(SamResourceTypeNames.servicePerimeter, urlEncodedDefaultServicePerimeterName, SamServicePerimeterActions.addProject, userInfo)).thenReturn(Future.successful(false))
      when(mockSamDAO.userHasAction(SamResourceTypeNames.billingProject, project.projectName.value, SamBillingProjectActions.addToServicePerimeter, userInfo)).thenReturn(Future.successful(true))

      val userService = getUserService(dataSource, mockSamDAO)

      val actual = userService.AddProjectToServicePerimeter(defaultServicePerimeterName, project.projectName).failed.futureValue
      assert(actual.isInstanceOf[RawlsExceptionWithErrorReport])
      actual.asInstanceOf[RawlsExceptionWithErrorReport].errorReport.statusCode shouldEqual Option(StatusCodes.NotFound)
    }
  }
}
