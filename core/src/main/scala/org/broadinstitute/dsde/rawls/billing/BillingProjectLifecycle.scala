package org.broadinstitute.dsde.rawls.billing

import org.broadinstitute.dsde.rawls.config.MultiCloudWorkspaceConfig
import org.broadinstitute.dsde.rawls.model.CreationStatuses.CreationStatus
import org.broadinstitute.dsde.rawls.model.{
  CreateRawlsV2BillingProjectFullRequest,
  RawlsBillingProjectName,
  RawlsRequestContext
}

import scala.concurrent.Future

/**
 * Handles provisioning and deleting billing projects with external providers. Implementors of this trait are not concerned
 * with internal Rawls state (db records, etc.), but rather ensuring that
 * a) the creation request is valid for the given cloud provider
 * b) external state is valid after rawls internal state is updated (i.e, syncing groups, etc.)
 */
trait BillingProjectLifecycle {
  def validateBillingProjectCreationRequest(createProjectRequest: CreateRawlsV2BillingProjectFullRequest,
                                            ctx: RawlsRequestContext
  ): Future[Unit]
  def postCreationSteps(createProjectRequest: CreateRawlsV2BillingProjectFullRequest,
                        config: MultiCloudWorkspaceConfig,
                        ctx: RawlsRequestContext
  ): Future[CreationStatus]

  def preDeletionSteps(projectName: RawlsBillingProjectName, ctx: RawlsRequestContext): Future[Unit]
}
