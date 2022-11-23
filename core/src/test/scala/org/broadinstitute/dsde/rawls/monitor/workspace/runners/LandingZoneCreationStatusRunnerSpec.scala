package org.broadinstitute.dsde.rawls.monitor.workspace.runners

import bio.terra.workspace.model.{AzureLandingZoneDetails, AzureLandingZoneResult, JobReport}
import org.broadinstitute.dsde.rawls.TestExecutionContext
import org.broadinstitute.dsde.rawls.billing.BillingRepository
import org.broadinstitute.dsde.rawls.dataaccess.{GoogleServicesDAO, SamDAO}
import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkspaceManagerResourceMonitorRecord
import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.WorkspaceManagerDAO
import org.broadinstitute.dsde.rawls.model.{CreationStatuses, RawlsBillingProjectName, RawlsRequestContext}
import org.broadinstitute.dsde.rawls.monitor.workspace.runners.LandingZoneCreationStatusRunnerSpec._
import org.mockito.ArgumentMatchers
import org.mockito.Mockito.{doReturn, spy, verify, when}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

import java.sql.Timestamp
import java.time.Instant
import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}
import scala.language.postfixOps

object LandingZoneCreationStatusRunnerSpec {
  val userEmail = "user@email.com"
  val billingProjectName: RawlsBillingProjectName = RawlsBillingProjectName(UUID.randomUUID().toString)
  val monitorRecord: WorkspaceManagerResourceMonitorRecord = WorkspaceManagerResourceMonitorRecord(
    UUID.randomUUID(),
    WorkspaceManagerResourceMonitorRecord.JobType.AzureLandingZoneResult,
    None,
    Some(billingProjectName.value),
    Some(userEmail),
    new Timestamp(Instant.now().toEpochMilli)
  )
}

class LandingZoneCreationStatusRunnerSpec extends AnyFlatSpecLike with MockitoSugar with Matchers with ScalaFutures {

  implicit val executionContext: ExecutionContext = TestExecutionContext.testExecutionContext

  behavior of "initial setup and basic requirements updating billing project status"

  it should "return a completed status if the billing project name is None" in {
    val runner = new LandingZoneCreationStatusRunner(
      mock[SamDAO],
      mock[WorkspaceManagerDAO],
      mock[BillingRepository],
      mock[GoogleServicesDAO]
    )
    whenReady(runner.run(monitorRecord.copy(billingProjectId = None)))(
      _ shouldBe WorkspaceManagerResourceMonitorRecord.Complete
    )
  }

  it should "set an error on the billing project and return a completed status if the user email is None" in {
    val billingRepository = mock[BillingRepository]
    when(
      billingRepository.updateCreationStatus(
        ArgumentMatchers.eq(billingProjectName),
        ArgumentMatchers.eq(CreationStatuses.Error),
        ArgumentMatchers.any[Some[String]]()
      )
    ).thenAnswer { invocation =>
      val message: Option[String] = invocation.getArgument(2)
      assert(message.get.contains(billingProjectName.value))
      assert(message.get.toLowerCase.contains("no user email"))
      Future.successful(1)
    }
    val runner = new LandingZoneCreationStatusRunner(
      mock[SamDAO],
      mock[WorkspaceManagerDAO],
      billingRepository,
      mock[GoogleServicesDAO]
    )
    whenReady(runner.run(monitorRecord.copy(userEmail = None)))(
      _ shouldBe WorkspaceManagerResourceMonitorRecord.Complete
    )

    verify(billingRepository).updateCreationStatus(
      ArgumentMatchers.eq(billingProjectName),
      ArgumentMatchers.eq(CreationStatuses.Error),
      ArgumentMatchers.any[Some[String]]()
    )

  }

  it should "set an error in the billing project and return job as incomplete if the user context cannot be created" in {
    val billingRepository = mock[BillingRepository]
    when(
      billingRepository.updateCreationStatus(
        ArgumentMatchers.eq(billingProjectName),
        ArgumentMatchers.eq(CreationStatuses.Error),
        ArgumentMatchers.any[Some[String]]()
      )
    ).thenAnswer { invocation =>
      val message: Option[String] = invocation.getArgument(2)
      assert(message.get.toLowerCase.contains("request context"))
      assert(message.get.contains(userEmail))
      Future.successful(1)
    }
    val runner =
      spy(
        new LandingZoneCreationStatusRunner(
          mock[SamDAO],
          mock[WorkspaceManagerDAO],
          billingRepository,
          mock[GoogleServicesDAO]
        )
      )
    doReturn(Future.failed(new org.broadinstitute.dsde.workbench.client.sam.ApiException()))
      .when(runner)
      .getUserCtx(ArgumentMatchers.eq(userEmail))(ArgumentMatchers.any())

    whenReady(runner.run(monitorRecord))(_ shouldBe WorkspaceManagerResourceMonitorRecord.Incomplete)
    verify(billingRepository).updateCreationStatus(
      ArgumentMatchers.eq(billingProjectName),
      ArgumentMatchers.eq(CreationStatuses.Error),
      ArgumentMatchers.any[Some[String]]()
    )
  }

  it should "set an error status and message on the project and return the job status as incomplete if the call to get the landing zone job results fail" in {
    val ctx = mock[RawlsRequestContext]
    val wsmDao = mock[WorkspaceManagerDAO]
    val wsmExceptionMessage = "looking for this to be reported in the billing project message"
    when(
      wsmDao.getCreateAzureLandingZoneResult(
        ArgumentMatchers.eq(monitorRecord.jobControlId.toString),
        ArgumentMatchers.any()
      )
    ).thenAnswer(_ => throw new bio.terra.workspace.client.ApiException(404, wsmExceptionMessage))
    val billingRepository = mock[BillingRepository]
    when(
      billingRepository.updateCreationStatus(
        ArgumentMatchers.eq(billingProjectName),
        ArgumentMatchers.eq(CreationStatuses.Error),
        ArgumentMatchers.any[Some[String]]()
      )
    ).thenAnswer { invocation =>
      val message: Option[String] = invocation.getArgument(2)
      assert(message.get.contains(wsmExceptionMessage))
      Future.successful(1)
    }
    val runner =
      spy(new LandingZoneCreationStatusRunner(mock[SamDAO], wsmDao, billingRepository, mock[GoogleServicesDAO]))
    doReturn(Future.successful(ctx)).when(runner).getUserCtx(ArgumentMatchers.eq(userEmail))(ArgumentMatchers.any())

    whenReady(runner.run(monitorRecord))(_ shouldBe WorkspaceManagerResourceMonitorRecord.Incomplete)
    verify(billingRepository).updateCreationStatus(
      ArgumentMatchers.eq(billingProjectName),
      ArgumentMatchers.eq(CreationStatuses.Error),
      ArgumentMatchers.any[Some[String]]()
    )
  }

  behavior of "updating billing project status when a job report is present in the landing zone result"

  it should "update the project and return a completed status when the landing zone job report is marked as successful" in {
    val ctx = mock[RawlsRequestContext]
    val lzId = UUID.randomUUID()
    val landingZoneResult = new AzureLandingZoneResult()
      .landingZone(new AzureLandingZoneDetails().id(lzId))
      .jobReport(new JobReport().status(JobReport.StatusEnum.SUCCEEDED))
    val wsmDao = mock[WorkspaceManagerDAO]
    when(
      wsmDao.getCreateAzureLandingZoneResult(
        ArgumentMatchers.eq(monitorRecord.jobControlId.toString),
        ArgumentMatchers.any()
      )
    ).thenReturn(landingZoneResult)
    val billingRepository = mock[BillingRepository]
    when(
      billingRepository.updateCreationStatus(
        ArgumentMatchers.eq(billingProjectName),
        ArgumentMatchers.eq(CreationStatuses.Ready),
        ArgumentMatchers.eq(None)
      )
    ).thenReturn(Future.successful(1))
    val runner =
      spy(new LandingZoneCreationStatusRunner(mock[SamDAO], wsmDao, billingRepository, mock[GoogleServicesDAO]))
    doReturn(Future.successful(ctx)).when(runner).getUserCtx(ArgumentMatchers.eq(userEmail))(ArgumentMatchers.any())

    whenReady(runner.run(monitorRecord))(_ shouldBe WorkspaceManagerResourceMonitorRecord.Complete)

    verify(billingRepository).updateCreationStatus(
      ArgumentMatchers.eq(billingProjectName),
      ArgumentMatchers.eq(CreationStatuses.Ready),
      ArgumentMatchers.eq(None)
    )
  }

  it should "return incomplete job status when the landing zone job is still running" in {
    val ctx = mock[RawlsRequestContext]
    val wsmDao = mock[WorkspaceManagerDAO]
    val lzId = UUID.randomUUID()
    val landingZoneResult = new AzureLandingZoneResult()
      .landingZone(new AzureLandingZoneDetails().id(lzId))
      .jobReport(new JobReport().status(JobReport.StatusEnum.RUNNING))
    when(
      wsmDao.getCreateAzureLandingZoneResult(
        ArgumentMatchers.eq(monitorRecord.jobControlId.toString),
        ArgumentMatchers.any()
      )
    ).thenReturn(landingZoneResult)
    val runner =
      spy(new LandingZoneCreationStatusRunner(mock[SamDAO], wsmDao, mock[BillingRepository], mock[GoogleServicesDAO]))
    doReturn(Future.successful(ctx)).when(runner).getUserCtx(ArgumentMatchers.eq(userEmail))(ArgumentMatchers.any())

    // since no methods are defined on the mock billing repository, any calls will throw an exception
    whenReady(runner.run(monitorRecord))(_ shouldBe WorkspaceManagerResourceMonitorRecord.Incomplete)
  }

  it should "update the project as Errored and return a completed status when the job is marked as failed" in {
    val ctx = mock[RawlsRequestContext]
    val failureMessage = "this is a very specific failure message"
    val wsmDao = mock[WorkspaceManagerDAO]
    val landingZoneResult = new AzureLandingZoneResult()
      .jobReport(new JobReport().status(JobReport.StatusEnum.FAILED))
      .errorReport(new bio.terra.workspace.model.ErrorReport().message(failureMessage))
    when(
      wsmDao.getCreateAzureLandingZoneResult(
        ArgumentMatchers.eq(monitorRecord.jobControlId.toString),
        ArgumentMatchers.any()
      )
    ).thenReturn(landingZoneResult)
    val billingRepository = mock[BillingRepository]
    when(
      billingRepository.updateCreationStatus(
        ArgumentMatchers.eq(billingProjectName),
        ArgumentMatchers.eq(CreationStatuses.Error),
        ArgumentMatchers.any[Some[String]]()
      )
    ).thenAnswer { invocation =>
      val message: Option[String] = invocation.getArgument(2)
      assert(message.get.contains(failureMessage))
      Future.successful(1)
    }

    val runner =
      spy(new LandingZoneCreationStatusRunner(mock[SamDAO], wsmDao, billingRepository, mock[GoogleServicesDAO]))
    doReturn(Future.successful(ctx)).when(runner).getUserCtx(ArgumentMatchers.eq(userEmail))(ArgumentMatchers.any())

    whenReady(runner.run(monitorRecord))(_ shouldBe WorkspaceManagerResourceMonitorRecord.Complete)

    verify(billingRepository).updateCreationStatus(
      ArgumentMatchers.eq(billingProjectName),
      ArgumentMatchers.eq(CreationStatuses.Error),
      ArgumentMatchers.any[Some[String]]()
    )
  }

  behavior of "updating billing project status when the job report is missing in the landing zone result"

  it should "update the project with an error state if there is no job report or errors" in {
    val ctx = mock[RawlsRequestContext]
    val wsmDao = mock[WorkspaceManagerDAO]
    val lzId = UUID.randomUUID()
    val landingZoneResult = new AzureLandingZoneResult().landingZone(new AzureLandingZoneDetails().id(lzId))
    when(
      wsmDao.getCreateAzureLandingZoneResult(
        ArgumentMatchers.eq(monitorRecord.jobControlId.toString),
        ArgumentMatchers.any()
      )
    ).thenReturn(landingZoneResult)
    val billingRepository = mock[BillingRepository]
    when(
      billingRepository.updateCreationStatus(
        ArgumentMatchers.eq(billingProjectName),
        ArgumentMatchers.eq(CreationStatuses.Error),
        ArgumentMatchers.any[Some[String]]()
      )
    ).thenReturn(Future.successful(1))
    val runner =
      spy(new LandingZoneCreationStatusRunner(mock[SamDAO], wsmDao, billingRepository, mock[GoogleServicesDAO]))
    doReturn(Future.successful(ctx)).when(runner).getUserCtx(ArgumentMatchers.eq(userEmail))(ArgumentMatchers.any())

    whenReady(runner.run(monitorRecord))(_ shouldBe WorkspaceManagerResourceMonitorRecord.Complete)
    verify(billingRepository).updateCreationStatus(
      ArgumentMatchers.eq(billingProjectName),
      ArgumentMatchers.eq(CreationStatuses.Error),
      ArgumentMatchers.any[Some[String]]()
    )
  }

  it should "set an error in the billing project and return complete status when there are errors but no job report" in {
    val wsmDao = mock[WorkspaceManagerDAO]
    val failureMessage = "this is a very specific failure message"
    val landingZoneResult = new AzureLandingZoneResult()
      .errorReport(new bio.terra.workspace.model.ErrorReport().message(failureMessage))
    when(
      wsmDao.getCreateAzureLandingZoneResult(
        ArgumentMatchers.eq(monitorRecord.jobControlId.toString),
        ArgumentMatchers.any()
      )
    ).thenReturn(landingZoneResult)
    val billingRepository = mock[BillingRepository]
    when(
      billingRepository.updateCreationStatus(
        ArgumentMatchers.eq(billingProjectName),
        ArgumentMatchers.eq(CreationStatuses.Error),
        ArgumentMatchers.any[Some[String]]()
      )
    ).thenAnswer { invocation =>
      val message: Option[String] = invocation.getArgument(2)
      assert(message.get.contains(failureMessage))
      Future.successful(1)
    }
    val runner =
      spy(new LandingZoneCreationStatusRunner(mock[SamDAO], wsmDao, billingRepository, mock[GoogleServicesDAO]))
    val ctx = mock[RawlsRequestContext]
    doReturn(Future.successful(ctx)).when(runner).getUserCtx(ArgumentMatchers.eq(userEmail))(ArgumentMatchers.any())

    whenReady(runner.run(monitorRecord))(_ shouldBe WorkspaceManagerResourceMonitorRecord.Complete)

    verify(billingRepository).updateCreationStatus(
      ArgumentMatchers.eq(billingProjectName),
      ArgumentMatchers.eq(CreationStatuses.Error),
      ArgumentMatchers.any[Some[String]]()
    )
  }

}
