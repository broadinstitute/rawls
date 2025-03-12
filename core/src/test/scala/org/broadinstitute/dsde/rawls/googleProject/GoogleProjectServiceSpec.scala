package org.broadinstitute.dsde.rawls.googleProject

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.testkit.ScalatestRouteTest
import org.broadinstitute.dsde.rawls.billing.BillingRepository
import org.broadinstitute.dsde.rawls.{RawlsExceptionWithErrorReport, TestExecutionContext}
import org.broadinstitute.dsde.rawls.dataaccess.{SamDAO, SlickDataSource}
import org.broadinstitute.dsde.rawls.mock.RemoteServicesMockServer
import org.broadinstitute.dsde.rawls.model.{
  CreationStatuses,
  RawlsBillingAccountName,
  RawlsBillingProject,
  RawlsBillingProjectName,
  RawlsGoogleProject,
  RawlsRequestContext,
  SamBillingProjectActions,
  SamGoogleProjectActions,
  SamResourceAction,
  SamResourceTypeName,
  SamResourceTypeNames
}
import org.mockito.Mockito.{verify, when}
import org.scalatest.flatspec.AnyFlatSpec
import org.broadinstitute.dsde.rawls.util.MockitoTestUtils
import org.mockito.ArgumentCaptor
import org.scalatest.matchers.should.Matchers
import org.mockito.ArgumentMatchers.{eq => mockitoEq, _}

import java.util.UUID
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

class GoogleProjectServiceSpec extends AnyFlatSpec with ScalatestRouteTest with Matchers with MockitoTestUtils {

  implicit val executionContext: TestExecutionContext = TestExecutionContext.testExecutionContext

  val mockServer: RemoteServicesMockServer = RemoteServicesMockServer()

  override def beforeAll(): Unit = {
    super.beforeAll()
    mockServer.startServer()
  }

  override def afterAll(): Unit = {
    mockServer.stopServer
    super.afterAll()
  }
  // Returns 201 on success
  "GoogleProjectService" should "create a Google project" in {
    val mockDataSource = mock[SlickDataSource]
    val mockSamDAO = mock[SamDAO]
    val mockGoogleProjectRepository = mock[GoogleProjectRepository]
    val mockContext = mock[RawlsRequestContext]
    val mockBillingRepository = mock[BillingRepository]

    val billingProjectId = RawlsBillingProjectName("billing-project-id")

    val billingProject: RawlsBillingProject = RawlsBillingProject(UUID.randomUUID(),
                                                                  billingProjectId,
                                                                  CreationStatuses.Ready,
                                                                  Option(RawlsBillingAccountName("billing-account")),
                                                                  None
    )

    when(mockBillingRepository.getBillingProject(mockitoEq(billingProjectId)))
      .thenReturn(Future.successful(Some(billingProject)))

    when(
      mockSamDAO.userHasAction(any[SamResourceTypeName], any[String], any[SamResourceAction], any[RawlsRequestContext])
    ).thenReturn(Future.successful(true))

    when(
      mockSamDAO.listUserActionsForResource(mockitoEq(SamResourceTypeNames.billingProject),
                                            any[String],
                                            any[RawlsRequestContext]
      )
    ).thenReturn(
      Future.successful(Set(SamBillingProjectActions.link, SamBillingProjectActions.own))
    )

    val googleProjectService =
      GoogleProjectService.constructor(mockDataSource, mockSamDAO, mockGoogleProjectRepository, mockBillingRepository)(
        mockContext
      )

    val testProject = RawlsGoogleProject("test-project", Some("billing-account"), Some("message"), billingProjectId)
    when(mockGoogleProjectRepository.createGoogleProject(any[RawlsGoogleProject]))
      .thenReturn(Future.successful(testProject))

    val result = Await.result(googleProjectService.createGoogleProject(testProject), Duration.Inf)
    assertResult(testProject) {
      result
    }
  }

  it should "set the billing account" in {
    val mockDataSource = mock[SlickDataSource]
    val mockSamDAO = mock[SamDAO]
    val mockGoogleProjectRepository = mock[GoogleProjectRepository]
    val mockContext = mock[RawlsRequestContext]
    val mockBillingRepository = mock[BillingRepository]

    val billingProjectId = RawlsBillingProjectName("billing-project-id")

    val billingProject: RawlsBillingProject = RawlsBillingProject(UUID.randomUUID(),
                                                                  billingProjectId,
                                                                  CreationStatuses.Ready,
                                                                  Option(RawlsBillingAccountName("billing-account")),
                                                                  None
    )

    when(mockBillingRepository.getBillingProject(mockitoEq(billingProjectId)))
      .thenReturn(Future.successful(Some(billingProject)))

    when(
      mockSamDAO.userHasAction(any[SamResourceTypeName], any[String], any[SamResourceAction], any[RawlsRequestContext])
    ).thenReturn(Future.successful(true))

    when(
      mockSamDAO.listUserActionsForResource(mockitoEq(SamResourceTypeNames.billingProject),
                                            any[String],
                                            any[RawlsRequestContext]
      )
    ).thenReturn(
      Future.successful(Set(SamBillingProjectActions.link, SamBillingProjectActions.own))
    )

    val googleProjectService =
      GoogleProjectService.constructor(mockDataSource, mockSamDAO, mockGoogleProjectRepository, mockBillingRepository)(
        mockContext
      )

    val testProject = RawlsGoogleProject("test-project", None, Some("message"), billingProjectId)
    val expectedProject = testProject.copy(billingAccount = Some("billing-account"))
    when(mockGoogleProjectRepository.createGoogleProject(any[RawlsGoogleProject]))
      .thenReturn(Future.successful(expectedProject))

    val result = Await.result(googleProjectService.createGoogleProject(testProject), Duration.Inf)
    val captor: ArgumentCaptor[RawlsGoogleProject] = ArgumentCaptor.forClass(classOf[RawlsGoogleProject])
    verify(mockGoogleProjectRepository).createGoogleProject(captor.capture())
    val capturedProject = captor.getValue
    assert(capturedProject.billingAccount.get.equals("billing-account"))

    assertResult(expectedProject) {
      result
    }
  }

  // Fails if no link action on billing-project resource in Sam and/or no link action on google-project resource in Sam
  it should "fail if no link action on billing project resource" in {
    val mockDataSource = mock[SlickDataSource]
    val mockSamDAO = mock[SamDAO]
    val mockGoogleProjectRepository = mock[GoogleProjectRepository]
    val mockContext = mock[RawlsRequestContext]
    val mockBillingRepository = mock[BillingRepository]

    val billingProjectId = RawlsBillingProjectName("billing-project-id")

    val billingProject: RawlsBillingProject = RawlsBillingProject(UUID.randomUUID(),
                                                                  billingProjectId,
                                                                  CreationStatuses.Ready,
                                                                  Option(RawlsBillingAccountName("billing-account")),
                                                                  None
    )

    when(mockBillingRepository.getBillingProject(mockitoEq(billingProjectId)))
      .thenReturn(Future.successful(Some(billingProject)))

    when(
      mockSamDAO.listUserActionsForResource(mockitoEq(SamResourceTypeNames.billingProject),
                                            any[String],
                                            any[RawlsRequestContext]
      )
    ).thenReturn(
      Future.successful(Set(SamBillingProjectActions.readPolicies, SamBillingProjectActions.launchBatchCompute))
    )

    when(
      mockSamDAO.userHasAction(mockitoEq(SamResourceTypeNames.googleProject),
                               any[String],
                               mockitoEq(SamGoogleProjectActions.link),
                               any[RawlsRequestContext]
      )
    ).thenReturn(Future.successful(true))

    val googleProjectService =
      GoogleProjectService.constructor(mockDataSource, mockSamDAO, mockGoogleProjectRepository, mockBillingRepository)(
        mockContext
      )
    val testProject = RawlsGoogleProject("test-project", Some("billing-account"), Some("message"), billingProjectId)

    val e = intercept[RawlsExceptionWithErrorReport] {
      Await.result(
        googleProjectService.createGoogleProject(testProject),
        Duration.Inf
      )
    }

    e.errorReport.statusCode shouldBe Option(StatusCodes.Forbidden)

  }

  it should "fail if no link action on google project resource" in {
    val mockDataSource = mock[SlickDataSource]
    val mockSamDAO = mock[SamDAO]
    val mockGoogleProjectRepository = mock[GoogleProjectRepository]
    val mockContext = mock[RawlsRequestContext]
    val mockBillingRepository = mock[BillingRepository]

    val billingProjectId = RawlsBillingProjectName("billing-project-id")

    val billingProject: RawlsBillingProject = RawlsBillingProject(UUID.randomUUID(),
                                                                  billingProjectId,
                                                                  CreationStatuses.Ready,
                                                                  Option(RawlsBillingAccountName("billing-account")),
                                                                  None
    )

    when(mockBillingRepository.getBillingProject(mockitoEq(billingProjectId)))
      .thenReturn(Future.successful(Some(billingProject)))

    when(
      mockSamDAO.userHasAction(mockitoEq(SamResourceTypeNames.googleProject),
                               any[String],
                               mockitoEq(SamGoogleProjectActions.link),
                               any[RawlsRequestContext]
      )
    ).thenReturn(Future.successful(false))

    when(
      mockSamDAO.listUserActionsForResource(mockitoEq(SamResourceTypeNames.billingProject),
                                            any[String],
                                            any[RawlsRequestContext]
      )
    ).thenReturn(
      Future.successful(Set(SamBillingProjectActions.link, SamBillingProjectActions.own))
    )

    val googleProjectService =
      GoogleProjectService.constructor(mockDataSource, mockSamDAO, mockGoogleProjectRepository, mockBillingRepository)(
        mockContext
      )
    val testProject = RawlsGoogleProject("test-project", Some("billing-account"), Some("message"), billingProjectId)

    val e = intercept[RawlsExceptionWithErrorReport] {
      Await.result(
        googleProjectService.createGoogleProject(testProject),
        Duration.Inf
      )
    }

    e.errorReport.statusCode shouldBe Option(StatusCodes.Forbidden)

  }

  it should "fail if the billing project does not exist" in {
    val mockDataSource = mock[SlickDataSource]
    val mockSamDAO = mock[SamDAO]
    val mockGoogleProjectRepository = mock[GoogleProjectRepository]
    val mockContext = mock[RawlsRequestContext]
    val mockBillingRepository = mock[BillingRepository]

    val billingProjectId = RawlsBillingProjectName("billing-project-id")

    when(mockBillingRepository.getBillingProject(mockitoEq(billingProjectId)))
      .thenReturn(Future.successful(None))

    when(
      mockSamDAO.userHasAction(mockitoEq(SamResourceTypeNames.googleProject),
                               any[String],
                               mockitoEq(SamGoogleProjectActions.link),
                               any[RawlsRequestContext]
      )
    ).thenReturn(Future.successful(true))

    when(
      mockSamDAO.listUserActionsForResource(mockitoEq(SamResourceTypeNames.billingProject),
                                            any[String],
                                            any[RawlsRequestContext]
      )
    ).thenReturn(
      Future.successful(Set(SamBillingProjectActions.link, SamBillingProjectActions.own))
    )

    val googleProjectService =
      GoogleProjectService.constructor(mockDataSource, mockSamDAO, mockGoogleProjectRepository, mockBillingRepository)(
        mockContext
      )
    val testProject = RawlsGoogleProject("test-project", Some("billing-account"), Some("message"), billingProjectId)

    val e = intercept[RawlsExceptionWithErrorReport] {
      Await.result(
        googleProjectService.createGoogleProject(testProject),
        Duration.Inf
      )
    }

    e.errorReport.statusCode shouldBe Option(StatusCodes.Forbidden)

  }

}
