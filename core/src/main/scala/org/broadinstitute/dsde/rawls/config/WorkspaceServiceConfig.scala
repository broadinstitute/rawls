package org.broadinstitute.dsde.rawls.config

import com.typesafe.config.Config

final case class WorkspaceServiceConfig(trackDetailedSubmissionMetrics: Boolean, workspaceBucketNamePrefix: String, spendProfileConfig: SpendProfileConfig, mcWorkspaceEnabled: Boolean)
final case class SpendProfileConfig(tenantId: String, subscriptionId: String, resourceGroupId: String)

case object WorkspaceServiceConfig {
  def apply[T <: WorkspaceServiceConfig](conf: Config): WorkspaceServiceConfig = {
    val gcsConfig = conf.getConfig("gcs")
    val spendProfileConfig = conf.getConfig("mcWorkspaces.spendProfile")

    new WorkspaceServiceConfig(
      conf.getBoolean("submissionmonitor.trackDetailedSubmissionMetrics"),
      gcsConfig.getString("groupsPrefix"),
      SpendProfileConfig(
        spendProfileConfig.getString("tenantId"),
        spendProfileConfig.getString("subscriptionId"),
        spendProfileConfig.getString("resourceGroupId")
      ),
      conf.getBoolean("mcWorkspaces.mcWorkspacesEnabled")
    )
  }
}
