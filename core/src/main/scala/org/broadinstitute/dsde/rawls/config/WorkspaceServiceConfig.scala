package org.broadinstitute.dsde.rawls.config

import com.typesafe.config.Config

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
                                           spendProfileId: String,
                                           azureTenantId: String,
                                           azureSubscriptionId: String,
                                           azureResourceGroupId: String)


case object MultiCloudWorkspaceConfig {
  def apply[T <: MultiCloudWorkspaceConfig](conf: Config): MultiCloudWorkspaceConfig = {
    new MultiCloudWorkspaceConfig(
      conf.getBoolean("multiCloudWorkspaces.enabled"),
      conf.getString("multiCloudWorkspaces.spendProfileId"),
      conf.getString("tenantId"),
      conf.getString("subscriptionId"),
      conf.getString("resourceGroupId")
    )
  }
}