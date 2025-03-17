package org.broadinstitute.dsde.rawls.googleProject

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.testkit.ScalatestRouteTest
import com.google.api.services.cloudbilling.model.ProjectBillingInfo
import org.broadinstitute.dsde.rawls.billing.BillingRepository
import org.broadinstitute.dsde.rawls.{RawlsExceptionWithErrorReport, TestExecutionContext}
import org.broadinstitute.dsde.rawls.dataaccess.{GoogleServicesDAO, SamDAO}
import org.broadinstitute.dsde.rawls.model.{
  CreationStatuses,
  GoogleProjectId,
  GoogleProjectRegistration,
  RawlsBillingAccountName,
  RawlsBillingProject,
  RawlsBillingProjectName,
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

class GoogleProjectRegistrationServiceSpec
    extends AnyFlatSpec
    with ScalatestRouteTest
    with Matchers
    with MockitoTestUtils {

  implicit val executionContext: TestExecutionContext = TestExecutionContext.testExecutionContext

  // Returns 201 on success
  "GoogleProjectRegistrationService" should "register a Google project" in {
    val mockSamDAO = mock[SamDAO]
    val mockGoogleProjectRegRepo = mock[GoogleProjectRegistrationRepository]
    val mockContext = mock[RawlsRequestContext]
    val mockBillingRepository = mock[BillingRepository]
    val mockGoogleServicesDAO = mock[GoogleServicesDAO]

    when(mockGoogleServicesDAO.setBillingAccountName(any[GoogleProjectId], any[RawlsBillingAccountName], any()))
      .thenReturn(Future.successful(new ProjectBillingInfo()))

    when(mockGoogleProjectRegRepo.getGoogleProjectRegistration(any[GoogleProjectId]))
      .thenReturn(Future.successful(None))

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

    val googleProjectRegService =
      GoogleProjectRegistrationService.constructor(
        mockSamDAO,
        mockGoogleProjectRegRepo,
        mockBillingRepository,
        mockGoogleServicesDAO
      )(
        mockContext
      )

    val testProject = GoogleProjectRegistration(GoogleProjectId("test-project"),
                                                Some(RawlsBillingAccountName("billing-account")),
                                                None,
                                                billingProjectId
    )
    when(mockGoogleProjectRegRepo.registerGoogleProject(any[GoogleProjectRegistration]))
      .thenReturn(Future.successful(testProject))

    val result = Await.result(googleProjectRegService.registerGoogleProject(testProject), Duration.Inf)
    assertResult(testProject) {
      result
    }
  }

  // Should set the billing account both in the rawls database and in real life
  it should "set the billing account" in {
    val mockSamDAO = mock[SamDAO]
    val mockGoogleProjectRegRepo = mock[GoogleProjectRegistrationRepository]
    val mockContext = mock[RawlsRequestContext]
    val mockBillingRepository = mock[BillingRepository]
    val mockGoogleServicesDAO = mock[GoogleServicesDAO]

    when(mockGoogleServicesDAO.setBillingAccountName(any[GoogleProjectId], any[RawlsBillingAccountName], any()))
      .thenReturn(Future.successful(new ProjectBillingInfo()))

    when(mockGoogleProjectRegRepo.getGoogleProjectRegistration(any[GoogleProjectId]))
      .thenReturn(Future.successful(None))

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

    val googleProjectRegService =
      GoogleProjectRegistrationService.constructor(
        mockSamDAO,
        mockGoogleProjectRegRepo,
        mockBillingRepository,
        mockGoogleServicesDAO
      )(
        mockContext
      )

    val testProject = GoogleProjectRegistration(GoogleProjectId("test-project"), None, None, billingProjectId)
    val expectedProject = testProject.copy(billingAccount = Some(RawlsBillingAccountName("billing-account")))
    when(mockGoogleProjectRegRepo.registerGoogleProject(any[GoogleProjectRegistration]))
      .thenReturn(Future.successful(expectedProject))

    val result = Await.result(googleProjectRegService.registerGoogleProject(testProject), Duration.Inf)
    val captor: ArgumentCaptor[GoogleProjectRegistration] = ArgumentCaptor.forClass(classOf[GoogleProjectRegistration])
    verify(mockGoogleProjectRegRepo).registerGoogleProject(captor.capture())
    val capturedProject = captor.getValue
    assert(capturedProject.billingAccount.get.equals(RawlsBillingAccountName("billing-account")))
    val googleProjectIdCaptor: ArgumentCaptor[GoogleProjectId] = ArgumentCaptor.forClass(classOf[GoogleProjectId])
    val billingAccountNameCaptor: ArgumentCaptor[RawlsBillingAccountName] =
      ArgumentCaptor.forClass(classOf[RawlsBillingAccountName])
    verify(mockGoogleServicesDAO).setBillingAccountName(googleProjectIdCaptor.capture(),
                                                        billingAccountNameCaptor.capture(),
                                                        any()
    )
    val capturedGoogleProjectId = googleProjectIdCaptor.getValue
    assert(capturedGoogleProjectId.equals(GoogleProjectId("test-project")))
    val capturedBillingAccountName = billingAccountNameCaptor.getValue
    assert(capturedBillingAccountName.equals(RawlsBillingAccountName("billing-account")))

    assertResult(expectedProject) {
      result
    }
  }

  // Fails if no link action on billing-project resource in Sam and/or no link action on google-project resource in Sam
  it should "fail if no link action on billing project resource" in {
    val mockSamDAO = mock[SamDAO]
    val mockGoogleProjectRegRepo = mock[GoogleProjectRegistrationRepository]
    val mockContext = mock[RawlsRequestContext]
    val mockBillingRepository = mock[BillingRepository]
    val mockGoogleServicesDAO = mock[GoogleServicesDAO]

    when(mockGoogleProjectRegRepo.getGoogleProjectRegistration(any[GoogleProjectId]))
      .thenReturn(Future.successful(None))

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

    val googleProjectRegService =
      GoogleProjectRegistrationService.constructor(
        mockSamDAO,
        mockGoogleProjectRegRepo,
        mockBillingRepository,
        mockGoogleServicesDAO
      )(
        mockContext
      )
    val testProject =
      GoogleProjectRegistration(GoogleProjectId("test-project"),
                                Some(RawlsBillingAccountName("billing-account")),
                                None,
                                billingProjectId
      )

    val e = intercept[RawlsExceptionWithErrorReport] {
      Await.result(
        googleProjectRegService.registerGoogleProject(testProject),
        Duration.Inf
      )
    }

    e.errorReport.statusCode shouldBe Option(StatusCodes.Forbidden)

  }

  it should "fail if no link action on google project resource" in {
    val mockSamDAO = mock[SamDAO]
    val mockGoogleProjectRegRepo = mock[GoogleProjectRegistrationRepository]
    val mockContext = mock[RawlsRequestContext]
    val mockBillingRepository = mock[BillingRepository]
    val mockGoogleServicesDAO = mock[GoogleServicesDAO]

    when(mockGoogleProjectRegRepo.getGoogleProjectRegistration(any[GoogleProjectId]))
      .thenReturn(Future.successful(None))

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

    val googleProjectRegService =
      GoogleProjectRegistrationService.constructor(
        mockSamDAO,
        mockGoogleProjectRegRepo,
        mockBillingRepository,
        mockGoogleServicesDAO
      )(
        mockContext
      )
    val testProject = GoogleProjectRegistration(GoogleProjectId("test-project"),
                                                Some(RawlsBillingAccountName("billing-account")),
                                                None,
                                                billingProjectId
    )

    val e = intercept[RawlsExceptionWithErrorReport] {
      Await.result(
        googleProjectRegService.registerGoogleProject(testProject),
        Duration.Inf
      )
    }

    e.errorReport.statusCode shouldBe Option(StatusCodes.Forbidden)

  }

  it should "fail if the billing project does not exist" in {
    val mockSamDAO = mock[SamDAO]
    val mockGoogleProjectRegRepo = mock[GoogleProjectRegistrationRepository]
    val mockContext = mock[RawlsRequestContext]
    val mockBillingRepository = mock[BillingRepository]
    val mockGoogleServicesDAO = mock[GoogleServicesDAO]

    when(mockGoogleProjectRegRepo.getGoogleProjectRegistration(any[GoogleProjectId]))
      .thenReturn(Future.successful(None))

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

    val googleProjectRegService =
      GoogleProjectRegistrationService.constructor(
        mockSamDAO,
        mockGoogleProjectRegRepo,
        mockBillingRepository,
        mockGoogleServicesDAO
      )(
        mockContext
      )
    val testProject = GoogleProjectRegistration(GoogleProjectId("test-project"),
                                                Some(RawlsBillingAccountName("billing-account")),
                                                None,
                                                billingProjectId
    )

    val e = intercept[RawlsExceptionWithErrorReport] {
      Await.result(
        googleProjectRegService.registerGoogleProject(testProject),
        Duration.Inf
      )
    }

    e.errorReport.statusCode shouldBe Option(StatusCodes.NotFound)

  }

  it should "do nothing if the google project is already registered with this billing project" in {
    val mockSamDAO = mock[SamDAO]
    val mockGoogleProjectRegRepo = mock[GoogleProjectRegistrationRepository]
    val mockContext = mock[RawlsRequestContext]
    val mockBillingRepository = mock[BillingRepository]
    val mockGoogleServicesDAO = mock[GoogleServicesDAO]

    when(mockGoogleServicesDAO.setBillingAccountName(any[GoogleProjectId], any[RawlsBillingAccountName], any()))
      .thenReturn(Future.successful(new ProjectBillingInfo()))

    val billingProjectId = RawlsBillingProjectName("billing-project-id")

    val billingProject: RawlsBillingProject = RawlsBillingProject(UUID.randomUUID(),
                                                                  billingProjectId,
                                                                  CreationStatuses.Ready,
                                                                  Option(RawlsBillingAccountName("billing-account")),
                                                                  None
    )

    when(mockBillingRepository.getBillingProject(mockitoEq(billingProjectId)))
      .thenReturn(Future.successful(Some(billingProject)))

    val testProject = GoogleProjectRegistration(GoogleProjectId("test-project"),
                                                Some(RawlsBillingAccountName("billing-account")),
                                                None,
                                                billingProjectId
    )

    when(mockGoogleProjectRegRepo.getGoogleProjectRegistration(any[GoogleProjectId]))
      .thenReturn(Future.successful(Some(testProject)))

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

    val googleProjectRegService =
      GoogleProjectRegistrationService.constructor(
        mockSamDAO,
        mockGoogleProjectRegRepo,
        mockBillingRepository,
        mockGoogleServicesDAO
      )(
        mockContext
      )

    when(mockGoogleProjectRegRepo.registerGoogleProject(any[GoogleProjectRegistration]))
      .thenReturn(Future.successful(testProject))

    val result = Await.result(googleProjectRegService.registerGoogleProject(testProject), Duration.Inf)
    assertResult(testProject) {
      result
    }
  }

  it should "throw an error if the google project is already registered with a different billing project" in {
    val mockSamDAO = mock[SamDAO]
    val mockGoogleProjectRegRepo = mock[GoogleProjectRegistrationRepository]
    val mockContext = mock[RawlsRequestContext]
    val mockBillingRepository = mock[BillingRepository]
    val mockGoogleServicesDAO = mock[GoogleServicesDAO]

    when(mockGoogleServicesDAO.setBillingAccountName(any[GoogleProjectId], any[RawlsBillingAccountName], any()))
      .thenReturn(Future.successful(new ProjectBillingInfo()))

    val billingProjectId1 = RawlsBillingProjectName("billing-project-id1")

    val billingProjectId2 = RawlsBillingProjectName("billing-project-id2")

    val billingProject1: RawlsBillingProject = RawlsBillingProject(UUID.randomUUID(),
                                                                   billingProjectId1,
                                                                   CreationStatuses.Ready,
                                                                   Option(RawlsBillingAccountName("billing-account")),
                                                                   None
    )

    when(mockBillingRepository.getBillingProject(mockitoEq(billingProjectId1)))
      .thenReturn(Future.successful(Some(billingProject1)))

    val existingProject = GoogleProjectRegistration(GoogleProjectId("test-project"),
                                                    Some(RawlsBillingAccountName("billing-account")),
                                                    None,
                                                    billingProjectId2
    )

    when(mockGoogleProjectRegRepo.getGoogleProjectRegistration(any[GoogleProjectId]))
      .thenReturn(Future.successful(Some(existingProject)))

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

    val googleProjectRegService =
      GoogleProjectRegistrationService.constructor(
        mockSamDAO,
        mockGoogleProjectRegRepo,
        mockBillingRepository,
        mockGoogleServicesDAO
      )(
        mockContext
      )

    val newProject = GoogleProjectRegistration(GoogleProjectId("test-project"),
                                               Some(RawlsBillingAccountName("billing-account")),
                                               None,
                                               billingProjectId1
    )

    when(mockGoogleProjectRegRepo.registerGoogleProject(any[GoogleProjectRegistration]))
      .thenReturn(Future.successful(newProject))

    val e = intercept[RawlsExceptionWithErrorReport] {
      Await.result(
        googleProjectRegService.registerGoogleProject(newProject),
        Duration.Inf
      )
    }

    e.errorReport.statusCode shouldBe Option(StatusCodes.Conflict)

  }

  it should "throw an error if updating the billing account fails" in {
    val mockSamDAO = mock[SamDAO]
    val mockGoogleProjectRegRepo = mock[GoogleProjectRegistrationRepository]
    val mockContext = mock[RawlsRequestContext]
    val mockBillingRepository = mock[BillingRepository]
    val mockGoogleServicesDAO = mock[GoogleServicesDAO]

    when(mockGoogleServicesDAO.setBillingAccountName(any[GoogleProjectId], any[RawlsBillingAccountName], any()))
      .thenThrow(new RuntimeException("Something has gone wrong in Google"))

    when(mockGoogleProjectRegRepo.getGoogleProjectRegistration(any[GoogleProjectId]))
      .thenReturn(Future.successful(None))

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

    val googleProjectRegService =
      GoogleProjectRegistrationService.constructor(
        mockSamDAO,
        mockGoogleProjectRegRepo,
        mockBillingRepository,
        mockGoogleServicesDAO
      )(
        mockContext
      )

    val testProject = GoogleProjectRegistration(GoogleProjectId("test-project"),
                                                Some(RawlsBillingAccountName("billing-account")),
                                                None,
                                                billingProjectId
    )
    when(mockGoogleProjectRegRepo.registerGoogleProject(any[GoogleProjectRegistration]))
      .thenReturn(Future.successful(testProject))

    val e = intercept[RuntimeException] {
      Await.result(
        googleProjectRegService.registerGoogleProject(testProject),
        Duration.Inf
      )
    }

    e.getMessage shouldBe "Something has gone wrong in Google"

  }

}
