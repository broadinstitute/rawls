package org.broadinstitute.dsde.rawls.config

import com.typesafe.config.Config

final case class WorkspaceServiceConfig(trackDetailedSubmissionMetrics: Boolean,
                                        workspaceBucketNamePrefix: String,
                                        defaultLocation: String
)

case object WorkspaceServiceConfig {
  def apply[T <: WorkspaceServiceConfig](appConfigManager: MultiCloudAppConfigManager): WorkspaceServiceConfig = {

    new WorkspaceServiceConfig(
      appConfigManager.conf.getBoolean("submissionmonitor.trackDetailedSubmissionMetrics"),
      appConfigManager.gcsConfig.map(_.getString("groupsPrefix")).getOrElse("unsupported"),
      appConfigManager.gcsConfig.map(_.getString("defaultLocation")).getOrElse("unsupported")
    )
  }
}
