package org.broadinstitute.dsde.rawls.config

import com.typesafe.config.Config

final case class WorkspaceServiceConfig(trackDetailedSubmissionMetrics: Boolean, workspaceBucketNamePrefix: String, spendProfileId: String, azureConfig: AzureConfig, multiCloudWorkspacesEnabled: Boolean)

// TODO this data will be pulled from the spend profile service, hardcoding in conf until that svc is ready
final case class AzureConfig(tenantId: String, subscriptionId: String, resourceGroupId: String)

case object WorkspaceServiceConfig {
  def apply[T <: WorkspaceServiceConfig](conf: Config): WorkspaceServiceConfig = {
    val gcsConfig = conf.getConfig("gcs")
    val azureConfig = conf.getConfig("multiCloudWorkspaces.azureConfig")

    new WorkspaceServiceConfig(
      conf.getBoolean("submissionmonitor.trackDetailedSubmissionMetrics"),
      gcsConfig.getString("groupsPrefix"),
      conf.getString("multiCloudWorkspaces.spendProfileId"),
      AzureConfig(
        azureConfig.getString("tenantId"),
        azureConfig.getString("subscriptionId"),
        azureConfig.getString("resourceGroupId")
      ),
      conf.getBoolean("multiCloudWorkspaces.enabled")
    )
  }
}