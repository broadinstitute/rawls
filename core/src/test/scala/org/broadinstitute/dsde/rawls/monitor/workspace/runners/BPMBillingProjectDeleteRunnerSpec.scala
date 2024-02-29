package org.broadinstitute.dsde.rawls.monitor.workspace.runners

import bio.terra.workspace.model.{DeleteAzureLandingZoneJobResult, ErrorReport, JobReport}
import org.broadinstitute.dsde.rawls.TestExecutionContext
import org.broadinstitute.dsde.rawls.billing.{BillingProjectDeletion, BillingRepository}
import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkspaceManagerResourceMonitorRecord
import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkspaceManagerResourceMonitorRecord.JobType
import org.broadinstitute.dsde.rawls.dataaccess.workspacemanager.WorkspaceManagerDAO
import org.broadinstitute.dsde.rawls.dataaccess.{GoogleServicesDAO, SamDAO}
import org.broadinstitute.dsde.rawls.model.CreationStatuses.{Deleting, DeletionFailed}
import org.broadinstitute.dsde.rawls.model.{RawlsBillingProjectName, RawlsRequestContext, RawlsUserEmail}
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{doReturn, spy, verify, when}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

import java.sql.Timestamp
import java.time.Instant
import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

class BPMBillingProjectDeleteRunnerSpec extends AnyFlatSpec with MockitoSugar with Matchers with ScalaFutures {
  implicit val executionContext: ExecutionContext = TestExecutionContext.testExecutionContext

  behavior of "initial setup and basic requirements updating billing project status"

  val userEmail = "user@email.com"
  val billingProjectName: RawlsBillingProjectName = RawlsBillingProjectName("fake_name")

  it should "return a completed status if no billing project is set" in {
    val monitorRecord: WorkspaceManagerResourceMonitorRecord =
      WorkspaceManagerResourceMonitorRecord.forAzureLandingZoneCreate(
        UUID.randomUUID(),
        billingProjectName,
        RawlsUserEmail(userEmail)
      )
    val runner = new BPMBillingProjectDeleteRunner(
      mock[SamDAO],
      mock[GoogleServicesDAO],
      mock[WorkspaceManagerDAO],
      mock[BillingRepository],
      mock[BillingProjectDeletion]
    )
    whenReady(runner(monitorRecord.copy(billingProjectId = None)))(
      _ shouldBe WorkspaceManagerResourceMonitorRecord.Complete
    )
  }

  it should "set an error on the billing project and return a completed status if the user email is None" in {
    val billingRepository = mock[BillingRepository]
    when(
      billingRepository.updateCreationStatus(
        ArgumentMatchers.eq(billingProjectName),
        ArgumentMatchers.eq(DeletionFailed),
        ArgumentMatchers.any[Some[String]]()
      )
    ).thenAnswer { invocation =>
      val message: Option[String] = invocation.getArgument(2)
      assert(message.get.contains(billingProjectName.value))
      assert(message.get.toLowerCase.contains("no user email"))
      Future.successful(1)
    }
    val runner = new BPMBillingProjectDeleteRunner(
      mock[SamDAO],
      mock[GoogleServicesDAO],
      mock[WorkspaceManagerDAO],
      billingRepository,
      mock[BillingProjectDeletion]
    )
    val monitorRecord: WorkspaceManagerResourceMonitorRecord =
      WorkspaceManagerResourceMonitorRecord(UUID.randomUUID(),
                                            JobType.BpmBillingProjectDelete,
                                            None,
                                            Some(billingProjectName.value),
                                            None,
                                            Timestamp.from(Instant.now())
      )

    whenReady(runner(monitorRecord))(_ shouldBe WorkspaceManagerResourceMonitorRecord.Complete)

    verify(billingRepository).updateCreationStatus(
      ArgumentMatchers.eq(billingProjectName),
      ArgumentMatchers.eq(DeletionFailed),
      ArgumentMatchers.any[Some[String]]()
    )

  }

  it should "set an error in the billing project and return job as incomplete if the user context cannot be created" in {
    val billingRepository = mock[BillingRepository]
    when(
      billingRepository.updateCreationStatus(
        ArgumentMatchers.eq(billingProjectName),
        ArgumentMatchers.eq(Deleting),
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
        new BPMBillingProjectDeleteRunner(
          mock[SamDAO],
          mock[GoogleServicesDAO],
          mock[WorkspaceManagerDAO],
          billingRepository,
          mock[BillingProjectDeletion]
        )
      )
    doReturn(Future.failed(new org.broadinstitute.dsde.workbench.client.sam.ApiException()))
      .when(runner)
      .getUserCtx(ArgumentMatchers.eq(userEmail))(ArgumentMatchers.any())
    val monitorRecord: WorkspaceManagerResourceMonitorRecord =
      WorkspaceManagerResourceMonitorRecord(UUID.randomUUID(),
                                            JobType.BpmBillingProjectDelete,
                                            None,
                                            Some(billingProjectName.value),
                                            Some(userEmail),
                                            Timestamp.from(Instant.now())
      )

    whenReady(runner(monitorRecord))(_ shouldBe WorkspaceManagerResourceMonitorRecord.Incomplete)
    verify(billingRepository).updateCreationStatus(
      ArgumentMatchers.eq(billingProjectName),
      ArgumentMatchers.eq(Deleting),
      ArgumentMatchers.any[Some[String]]()
    )
  }

  it should "set an error status and message on the project and return incomplete when landing zone call returns 500" in {
    val ctx = mock[RawlsRequestContext]
    val wsmDao = mock[WorkspaceManagerDAO]
    val wsmExceptionMessage = "looking for this to be reported in the billing project message"
    val landingZoneId = "7db89d7c-ef8b-4eaa-aef8-a62bd66cb095"
    val monitorRecord: WorkspaceManagerResourceMonitorRecord = WorkspaceManagerResourceMonitorRecord(
      UUID.randomUUID(),
      JobType.BpmBillingProjectDelete,
      None,
      Some(billingProjectName.value),
      Some(userEmail),
      Timestamp.from(Instant.now())
    )

    when(
      wsmDao.getDeleteLandingZoneResult(
        ArgumentMatchers.eq(monitorRecord.jobControlId.toString),
        ArgumentMatchers.eq(UUID.fromString(landingZoneId)),
        ArgumentMatchers.any()
      )
    )
      .thenAnswer(_ => throw new bio.terra.workspace.client.ApiException(500, wsmExceptionMessage))
    val billingRepository = mock[BillingRepository]
    when(billingRepository.getLandingZoneId(ArgumentMatchers.eq(billingProjectName))(any()))
      .thenReturn(Future.successful(Some(landingZoneId)))
    when(
      billingRepository.updateCreationStatus(
        ArgumentMatchers.eq(billingProjectName),
        ArgumentMatchers.eq(Deleting),
        ArgumentMatchers.any[Some[String]]()
      )
    ).thenAnswer { invocation =>
      val message: Option[String] = invocation.getArgument(2)
      assert(message.get.contains(wsmExceptionMessage))
      Future.successful(1)
    }
    val runner = spy(
      new BPMBillingProjectDeleteRunner(
        mock[SamDAO],
        mock[GoogleServicesDAO],
        wsmDao,
        billingRepository,
        mock[BillingProjectDeletion]
      )
    )
    doReturn(Future.successful(ctx)).when(runner).getUserCtx(ArgumentMatchers.eq(userEmail))(ArgumentMatchers.any())

    whenReady(runner(monitorRecord))(_ shouldBe WorkspaceManagerResourceMonitorRecord.Incomplete)

    verify(billingRepository).updateCreationStatus(
      ArgumentMatchers.eq(billingProjectName),
      ArgumentMatchers.eq(Deleting),
      ArgumentMatchers.any[Some[String]]()
    )
  }

  it should "set an error status and message from the error report for failed jobs" in {
    val ctx = mock[RawlsRequestContext]
    val wsmDao = mock[WorkspaceManagerDAO]
    val wsmExceptionMessage = "looking for this to be reported in the billing project message"
    val landingZoneId = UUID.randomUUID()
    val monitorRecord: WorkspaceManagerResourceMonitorRecord = WorkspaceManagerResourceMonitorRecord(
      UUID.randomUUID(),
      JobType.BpmBillingProjectDelete,
      None,
      Some(billingProjectName.value),
      Some(userEmail),
      Timestamp.from(Instant.now())
    )

    val jobReport = new DeleteAzureLandingZoneJobResult()
      .jobReport(new JobReport().status(JobReport.StatusEnum.FAILED))
      .errorReport(new ErrorReport().message(wsmExceptionMessage))

    when(
      wsmDao.getDeleteLandingZoneResult(
        ArgumentMatchers.eq(monitorRecord.jobControlId.toString),
        ArgumentMatchers.eq(landingZoneId),
        ArgumentMatchers.any()
      )
    ).thenReturn(jobReport)

    val billingRepository = mock[BillingRepository]
    when(billingRepository.getLandingZoneId(ArgumentMatchers.eq(billingProjectName))(any()))
      .thenReturn(Future.successful(Some(landingZoneId.toString)))
    when(
      billingRepository.updateCreationStatus(
        ArgumentMatchers.eq(billingProjectName),
        ArgumentMatchers.eq(DeletionFailed),
        ArgumentMatchers.any[Some[String]]()
      )
    ).thenAnswer { invocation =>
      val message: Option[String] = invocation.getArgument(2)
      assert(message.get.contains(wsmExceptionMessage))
      Future.successful(1)
    }
    val runner = spy(
      new BPMBillingProjectDeleteRunner(
        mock[SamDAO],
        mock[GoogleServicesDAO],
        wsmDao,
        billingRepository,
        mock[BillingProjectDeletion]
      )
    )
    doReturn(Future.successful(ctx)).when(runner).getUserCtx(ArgumentMatchers.eq(userEmail))(ArgumentMatchers.any())

    whenReady(runner(monitorRecord))(_ shouldBe WorkspaceManagerResourceMonitorRecord.Complete)

    verify(billingRepository).updateCreationStatus(
      ArgumentMatchers.eq(billingProjectName),
      ArgumentMatchers.eq(DeletionFailed),
      ArgumentMatchers.any[Some[String]]()
    )
  }

  it should "set an error status and message from the error report jobs with no job report" in {
    val ctx = mock[RawlsRequestContext]
    val wsmDao = mock[WorkspaceManagerDAO]
    val wsmExceptionMessage = "looking for this to be reported in the billing project message"
    val landingZoneId = UUID.randomUUID()
    val monitorRecord: WorkspaceManagerResourceMonitorRecord = WorkspaceManagerResourceMonitorRecord(
      UUID.randomUUID(),
      JobType.BpmBillingProjectDelete,
      None,
      Some(billingProjectName.value),
      Some(userEmail),
      Timestamp.from(Instant.now())
    )

    val jobReport = new DeleteAzureLandingZoneJobResult().errorReport(new ErrorReport().message(wsmExceptionMessage))

    when(
      wsmDao.getDeleteLandingZoneResult(
        ArgumentMatchers.eq(monitorRecord.jobControlId.toString),
        ArgumentMatchers.eq(landingZoneId),
        ArgumentMatchers.any()
      )
    ).thenReturn(jobReport)

    val billingRepository = mock[BillingRepository]
    when(billingRepository.getLandingZoneId(ArgumentMatchers.eq(billingProjectName))(any()))
      .thenReturn(Future.successful(Some(landingZoneId.toString)))
    when(
      billingRepository.updateCreationStatus(
        ArgumentMatchers.eq(billingProjectName),
        ArgumentMatchers.eq(DeletionFailed),
        ArgumentMatchers.any[Some[String]]()
      )
    ).thenAnswer { invocation =>
      val message: Option[String] = invocation.getArgument(2)
      assert(message.get.contains(wsmExceptionMessage))
      Future.successful(1)
    }
    val runner = spy(
      new BPMBillingProjectDeleteRunner(
        mock[SamDAO],
        mock[GoogleServicesDAO],
        wsmDao,
        billingRepository,
        mock[BillingProjectDeletion]
      )
    )
    doReturn(Future.successful(ctx)).when(runner).getUserCtx(ArgumentMatchers.eq(userEmail))(ArgumentMatchers.any())

    whenReady(runner(monitorRecord))(_ shouldBe WorkspaceManagerResourceMonitorRecord.Complete)

    verify(billingRepository).updateCreationStatus(
      ArgumentMatchers.eq(billingProjectName),
      ArgumentMatchers.eq(DeletionFailed),
      ArgumentMatchers.any[Some[String]]()
    )
  }

  it should "reports a sensible message when no message is set in the error report" in {
    val ctx = mock[RawlsRequestContext]
    val wsmDao = mock[WorkspaceManagerDAO]
    val landingZoneId = UUID.randomUUID()
    val monitorRecord: WorkspaceManagerResourceMonitorRecord = WorkspaceManagerResourceMonitorRecord(
      UUID.randomUUID(),
      JobType.BpmBillingProjectDelete,
      None,
      Some(billingProjectName.value),
      Some(userEmail),
      Timestamp.from(Instant.now())
    )

    val jobReport = new DeleteAzureLandingZoneJobResult()

    when(
      wsmDao.getDeleteLandingZoneResult(
        ArgumentMatchers.eq(monitorRecord.jobControlId.toString),
        ArgumentMatchers.eq(landingZoneId),
        ArgumentMatchers.any()
      )
    ).thenReturn(jobReport)

    val billingRepository = mock[BillingRepository]
    when(billingRepository.getLandingZoneId(ArgumentMatchers.eq(billingProjectName))(any()))
      .thenReturn(Future.successful(Some(landingZoneId.toString)))
    when(
      billingRepository.updateCreationStatus(
        ArgumentMatchers.eq(billingProjectName),
        ArgumentMatchers.eq(DeletionFailed),
        ArgumentMatchers.any[Some[String]]()
      )
    ).thenAnswer { invocation =>
      val message: Option[String] = invocation.getArgument(2)
      assert(message.get.contains("deletion failed"))
      assert(message.get.contains("no error"))
      Future.successful(1)
    }
    val runner = spy(
      new BPMBillingProjectDeleteRunner(
        mock[SamDAO],
        mock[GoogleServicesDAO],
        wsmDao,
        billingRepository,
        mock[BillingProjectDeletion]
      )
    )
    doReturn(Future.successful(ctx)).when(runner).getUserCtx(ArgumentMatchers.eq(userEmail))(ArgumentMatchers.any())

    whenReady(runner(monitorRecord))(_ shouldBe WorkspaceManagerResourceMonitorRecord.Complete)

    verify(billingRepository).updateCreationStatus(
      ArgumentMatchers.eq(billingProjectName),
      ArgumentMatchers.eq(DeletionFailed),
      ArgumentMatchers.any[Some[String]]()
    )
  }

  it should "continue running when the landing zone delete job is still running" in {
    val ctx = mock[RawlsRequestContext]
    val wsmDao = mock[WorkspaceManagerDAO]
    val landingZoneId = UUID.randomUUID()
    val monitorRecord: WorkspaceManagerResourceMonitorRecord = WorkspaceManagerResourceMonitorRecord(
      UUID.randomUUID(),
      JobType.BpmBillingProjectDelete,
      None,
      Some(billingProjectName.value),
      Some(userEmail),
      Timestamp.from(Instant.now())
    )
    when(
      wsmDao.getDeleteLandingZoneResult(
        ArgumentMatchers.eq(monitorRecord.jobControlId.toString),
        ArgumentMatchers.eq(landingZoneId),
        ArgumentMatchers.any()
      )
    ).thenReturn(new DeleteAzureLandingZoneJobResult().jobReport(new JobReport().status(JobReport.StatusEnum.RUNNING)))

    val billingRepository = mock[BillingRepository]
    when(billingRepository.getLandingZoneId(ArgumentMatchers.eq(billingProjectName))(any()))
      .thenReturn(Future.successful(Some(landingZoneId.toString)))

    val runner = spy(
      new BPMBillingProjectDeleteRunner(
        mock[SamDAO],
        mock[GoogleServicesDAO],
        wsmDao,
        billingRepository,
        mock[BillingProjectDeletion]
      )
    )
    doReturn(Future.successful(ctx)).when(runner).getUserCtx(ArgumentMatchers.eq(userEmail))(ArgumentMatchers.any())

    whenReady(runner(monitorRecord))(_ shouldBe WorkspaceManagerResourceMonitorRecord.Incomplete)
  }

  it should "finalize the deletion using the billing project lifecycle when the landing zone delete job has completed" in {
    val ctx = mock[RawlsRequestContext]
    val wsmDao = mock[WorkspaceManagerDAO]
    val landingZoneId = UUID.randomUUID()
    val monitorRecord: WorkspaceManagerResourceMonitorRecord = WorkspaceManagerResourceMonitorRecord(
      UUID.randomUUID(),
      JobType.BpmBillingProjectDelete,
      None,
      Some(billingProjectName.value),
      Some(userEmail),
      Timestamp.from(Instant.now())
    )

    when(
      wsmDao.getDeleteLandingZoneResult(
        ArgumentMatchers.eq(monitorRecord.jobControlId.toString),
        ArgumentMatchers.eq(landingZoneId),
        ArgumentMatchers.any()
      )
    ).thenReturn(
      new DeleteAzureLandingZoneJobResult()
        .jobReport(new JobReport().status(JobReport.StatusEnum.SUCCEEDED))
    )

    val billingRepository = mock[BillingRepository]
    when(billingRepository.getLandingZoneId(ArgumentMatchers.eq(billingProjectName))(any()))
      .thenReturn(Future.successful(Some(landingZoneId.toString)))

    val billingProjectDeletion = mock[BillingProjectDeletion]
    when(
      billingProjectDeletion.finalizeDelete(
        ArgumentMatchers.eq(billingProjectName),
        ArgumentMatchers.any(),
        ArgumentMatchers.any()
      )(ArgumentMatchers.any())
    ).thenReturn(Future.successful())

    val runner = spy(
      new BPMBillingProjectDeleteRunner(
        mock[SamDAO],
        mock[GoogleServicesDAO],
        wsmDao,
        billingRepository,
        billingProjectDeletion
      )
    )
    doReturn(Future.successful(ctx)).when(runner).getUserCtx(ArgumentMatchers.eq(userEmail))(ArgumentMatchers.any())

    whenReady(runner(monitorRecord))(_ shouldBe WorkspaceManagerResourceMonitorRecord.Complete)

    verify(billingProjectDeletion).finalizeDelete(
      ArgumentMatchers.eq(billingProjectName),
      ArgumentMatchers.any(),
      ArgumentMatchers.any()
    )(ArgumentMatchers.any())
  }

  it should "not attempt to retrieve the landing zone delete result for BPM projects without a landing zone" in {
    val ctx = mock[RawlsRequestContext]
    val monitorRecord: WorkspaceManagerResourceMonitorRecord = WorkspaceManagerResourceMonitorRecord(
      UUID.randomUUID(),
      JobType.BpmBillingProjectDelete,
      None,
      Some(billingProjectName.value),
      Some(userEmail),
      Timestamp.from(Instant.now())
    )

    val billingProjectDeletion = mock[BillingProjectDeletion]
    when(
      billingProjectDeletion.finalizeDelete(
        ArgumentMatchers.eq(billingProjectName),
        ArgumentMatchers.any(),
        ArgumentMatchers.any()
      )(ArgumentMatchers.any())
    ).thenReturn(Future.successful())

    val repo = mock[BillingRepository]
    when(repo.getLandingZoneId(ArgumentMatchers.eq(billingProjectName))(ArgumentMatchers.any()))
      .thenReturn(Future.successful(None))

    val runner = spy(
      new BPMBillingProjectDeleteRunner(
        mock[SamDAO],
        mock[GoogleServicesDAO],
        mock[WorkspaceManagerDAO],
        repo,
        billingProjectDeletion
      )
    )

    doReturn(Future.successful(ctx)).when(runner).getUserCtx(ArgumentMatchers.eq(userEmail))(ArgumentMatchers.any())

    whenReady(runner(monitorRecord))(_ shouldBe WorkspaceManagerResourceMonitorRecord.Complete)

    verify(billingProjectDeletion).finalizeDelete(
      ArgumentMatchers.eq(billingProjectName),
      ArgumentMatchers.any(),
      ArgumentMatchers.any()
    )(ArgumentMatchers.any())
  }

}
