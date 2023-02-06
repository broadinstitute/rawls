package org.broadinstitute.dsde.rawls.billing
import org.broadinstitute.dsde.rawls.config.MultiCloudWorkspaceConfig
import org.broadinstitute.dsde.rawls.model.{
  CreateRawlsV2BillingProjectFullRequest,
  CreationStatuses,
  RawlsBillingProjectName,
  RawlsRequestContext
}

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

/**
  * This is a "noop" BP lifecycle that does only sets up internal Rawls billing project state to make it aware
  * of the provided, previously-created landing information. No external calls are made.
  */
class StaticMrgBillingProjectLifecycle(billingRepository: BillingRepository,
                                       staticLandingZoneId: UUID,
                                       staticBillingProfileId: UUID
)(implicit val executionContext: ExecutionContext)
    extends BillingProjectLifecycle {
  override def validateBillingProjectCreationRequest(createProjectRequest: CreateRawlsV2BillingProjectFullRequest,
                                                     ctx: RawlsRequestContext
  ): Future[Unit] =
    Future.successful()

  override def postCreationSteps(createProjectRequest: CreateRawlsV2BillingProjectFullRequest,
                                 config: MultiCloudWorkspaceConfig,
                                 ctx: RawlsRequestContext
  ): Future[CreationStatuses.CreationStatus] =
    for {
      _ <- billingRepository.updateLandingZoneId(createProjectRequest.projectName, staticLandingZoneId)
      _ <- billingRepository.setBillingProfileId(createProjectRequest.projectName, staticBillingProfileId)
    } yield {
      CreationStatuses.Ready
    }

  override def preDeletionSteps(projectName: RawlsBillingProjectName, ctx: RawlsRequestContext): Future[Unit] =
    Future.successful()
}
