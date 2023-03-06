package org.broadinstitute.dsde.rawls.monitor.workspace.runners

import org.scalatest.flatspec.AnyFlatSpec

class GoogleBillingProjectDeleteRunnerSpec extends AnyFlatSpec {

  import org.broadinstitute.dsde.rawls.TestExecutionContext
  import org.broadinstitute.dsde.rawls.billing.{BillingProjectLifecycle, BillingRepository}
  import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkspaceManagerResourceMonitorRecord
  import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkspaceManagerResourceMonitorRecord.JobType
  import org.broadinstitute.dsde.rawls.dataaccess.{GoogleServicesDAO, SamDAO}
  import org.broadinstitute.dsde.rawls.model.CreationStatuses.DeletionFailed
  import org.broadinstitute.dsde.rawls.model.{RawlsBillingProjectName, RawlsUserEmail}
  import org.mockito.ArgumentMatchers
  import org.mockito.Mockito.{doReturn, spy, verify, when}
  import org.scalatest.concurrent.ScalaFutures
  import org.scalatest.flatspec.AnyFlatSpec
  import org.scalatest.matchers.should.Matchers
  import org.scalatestplus.mockito.MockitoSugar

  import java.sql.Timestamp
  import java.time.Instant
  import java.util.UUID
  import scala.concurrent.{ExecutionContext, Future}

  class GoogleBillingProjectDeleteRunnerSpec extends AnyFlatSpec with MockitoSugar with Matchers with ScalaFutures {
    implicit val executionContext: ExecutionContext = TestExecutionContext.testExecutionContext

    behavior of "initial setup and basic requirements updating billing project status"

    val userEmail = "user@email.com"
    val billingProjectName = RawlsBillingProjectName("fake_name")

    it should "return a completed status if the billing project name is None" in {
      val monitorRecord: WorkspaceManagerResourceMonitorRecord =
        WorkspaceManagerResourceMonitorRecord.forAzureLandingZoneCreate(
          UUID.randomUUID(),
          billingProjectName,
          RawlsUserEmail(userEmail)
        )
      val runner = new GoogleBillingProjectDeleteRunner(
        mock[SamDAO],
        mock[GoogleServicesDAO],
        mock[BillingRepository],
        mock[BillingProjectLifecycle]
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
      val runner = new GoogleBillingProjectDeleteRunner(
        mock[SamDAO],
        mock[GoogleServicesDAO],
        billingRepository,
        mock[BillingProjectLifecycle]
      )
      val monitorRecord: WorkspaceManagerResourceMonitorRecord =
        WorkspaceManagerResourceMonitorRecord(UUID.randomUUID(),
                                              JobType.AzureBillingProjectDelete,
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
          ArgumentMatchers.eq(DeletionFailed),
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
          new GoogleBillingProjectDeleteRunner(
            mock[SamDAO],
            mock[GoogleServicesDAO],
            billingRepository,
            mock[BillingProjectLifecycle]
          )
        )
      doReturn(Future.failed(new org.broadinstitute.dsde.workbench.client.sam.ApiException()))
        .when(runner)
        .getUserCtx(ArgumentMatchers.eq(userEmail))(ArgumentMatchers.any())
      val monitorRecord: WorkspaceManagerResourceMonitorRecord =
        WorkspaceManagerResourceMonitorRecord(UUID.randomUUID(),
                                              JobType.AzureBillingProjectDelete,
                                              None,
                                              Some(billingProjectName.value),
                                              Some(userEmail),
                                              Timestamp.from(Instant.now())
        )

      whenReady(runner(monitorRecord))(_ shouldBe WorkspaceManagerResourceMonitorRecord.Incomplete)
      verify(billingRepository).updateCreationStatus(
        ArgumentMatchers.eq(billingProjectName),
        ArgumentMatchers.eq(DeletionFailed),
        ArgumentMatchers.any[Some[String]]()
      )
    }

  }

}
