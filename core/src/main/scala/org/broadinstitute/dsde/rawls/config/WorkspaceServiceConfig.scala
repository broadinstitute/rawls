package org.broadinstitute.dsde.rawls.config

import com.typesafe.config.Config

import java.util.concurrent.TimeUnit
import scala.concurrent.duration._

final case class WorkspaceServiceConfig(trackDetailedSubmissionMetrics: Boolean, workspaceBucketNamePrefix: String)

case object WorkspaceServiceConfig {
  def apply[T <: WorkspaceServiceConfig](conf: Config): WorkspaceServiceConfig = {
    val gcsConfig = conf.getConfig("gcs")

    new WorkspaceServiceConfig(
      conf.getBoolean("submissionmonitor.trackDetailedSubmissionMetrics"),
      gcsConfig.getString("groupsPrefix")
    )
  }
}


// TODO this data will be pulled from the spend profile service, hardcoding in conf until that svc is ready
final case class MultiCloudWorkspaceConfig(multiCloudWorkspacesEnabled: Boolean,
                                           cloudContextPollTimeout: FiniteDuration,
                                           spendProfileId: String,
                                           azureTenantId: String,
                                           azureSubscriptionId: String,
                                           azureResourceGroupId: String)


case object MultiCloudWorkspaceConfig {
  def apply[T <: MultiCloudWorkspaceConfig](conf: Config): MultiCloudWorkspaceConfig = {
    new MultiCloudWorkspaceConfig(
      conf.getBoolean("multiCloudWorkspaces.enabled"),
      FiniteDuration(Duration(conf.getInt("multiCloudWorkspaces.cloudContextPollTimeoutSeconds"), TimeUnit.SECONDS).toSeconds, SECONDS),
      conf.getString("multiCloudWorkspaces.spendProfileId"),
      conf.getString("multiCloudWorkspaces.tenantId"),
      conf.getString("multiCloudWorkspaces.subscriptionId"),
      conf.getString("multiCloudWorkspaces.resourceGroupId")
    )
  }
}
