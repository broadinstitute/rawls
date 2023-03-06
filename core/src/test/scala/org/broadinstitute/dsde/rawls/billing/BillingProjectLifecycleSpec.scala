package org.broadinstitute.dsde.rawls.billing

import org.broadinstitute.dsde.rawls.TestExecutionContext
import org.broadinstitute.dsde.rawls.config.MultiCloudWorkspaceConfig
import org.broadinstitute.dsde.rawls.dataaccess.SamDAO
import org.broadinstitute.dsde.rawls.dataaccess.slick.WorkspaceManagerResourceMonitorRecord.JobType.JobType
import org.broadinstitute.dsde.rawls.model.{
  CreateRawlsV2BillingProjectFullRequest,
  CreationStatuses,
  RawlsBillingProjectName,
  RawlsRequestContext,
  SamBillingProjectActions,
  SamResourceTypeNames
}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatestplus.mockito.MockitoSugar.mock
import org.mockito.Mockito.{verify, when, RETURNS_SMART_NULLS}

import java.util.UUID
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
// so we don't have to define a new type in order to call test methods in our instantiations of the interface
import scala.language.reflectiveCalls

class BillingProjectLifecycleSpec extends AnyFlatSpec {
  implicit val executionContext: ExecutionContext = TestExecutionContext.testExecutionContext

  behavior of "unregisterBillingProject"

  it should "delete the project even if Sam deleteResource fails" in {
    val billingProjectName = RawlsBillingProjectName("fake_billing_account_name")
    val testContext = mock[RawlsRequestContext]

    val samDAOMock = mock[SamDAO](RETURNS_SMART_NULLS)
    when(
      samDAOMock.userHasAction(SamResourceTypeNames.billingProject,
                               billingProjectName.value,
                               SamBillingProjectActions.deleteBillingProject,
                               testContext
      )
    ).thenReturn(Future.successful(true))

    when(samDAOMock.deleteResource(SamResourceTypeNames.billingProject, billingProjectName.value, testContext))
      .thenReturn(Future.failed(new Throwable("Sam failed")))

    val repo = mock[BillingRepository]
    when(repo.failUnlessHasNoWorkspaces(billingProjectName)(executionContext)).thenReturn(Future.successful())
    when(repo.deleteBillingProject(billingProjectName)).thenReturn(Future.successful(true))
    when(repo.getBillingProfileId(billingProjectName)(executionContext)).thenReturn(Future.successful(None))

    val billingLifecycle = new BillingProjectLifecycle {
      override val samDAO: SamDAO = samDAOMock
      override val billingRepository: BillingRepository = repo

      override def validateBillingProjectCreationRequest(
        createProjectRequest: CreateRawlsV2BillingProjectFullRequest,
        ctx: RawlsRequestContext
      ): Future[Unit] = ???
      override def postCreationSteps(createProjectRequest: CreateRawlsV2BillingProjectFullRequest,
                                     config: MultiCloudWorkspaceConfig,
                                     ctx: RawlsRequestContext
      ): Future[CreationStatuses.CreationStatus] = ???
      override def initiateDelete(projectName: RawlsBillingProjectName, ctx: RawlsRequestContext)(implicit
        executionContext: ExecutionContext
      ): Future[(UUID, JobType)] = ???
      override def finalizeDelete(projectName: RawlsBillingProjectName, ctx: RawlsRequestContext)(implicit
        executionContext: ExecutionContext
      ): Future[Unit] = ???

      def exposeUnregisterBillingProject(projectName: RawlsBillingProjectName, ctx: RawlsRequestContext)(implicit
        executionContext: ExecutionContext
      ): Future[Unit] = unregisterBillingProject(projectName, ctx)
    }

    intercept[Throwable] {
      Await.result(billingLifecycle.exposeUnregisterBillingProject(billingProjectName, testContext), Duration.Inf)
    }

    verify(repo).deleteBillingProject(billingProjectName)

  }

}
