package org.broadinstitute.dsde.rawls.config

import com.typesafe.config.Config

final case class WorkspaceServiceConfig(trackDetailedSubmissionMetrics: Boolean,
                                        workspaceBucketNamePrefix: String,
                                        defaultLocation: String
)

case object WorkspaceServiceConfig {
  def apply[T <: WorkspaceServiceConfig](conf: Config): WorkspaceServiceConfig = {
    val gcsConfig = conf.getConfig("gcs")

    new WorkspaceServiceConfig(
      conf.getBoolean("submissionmonitor.trackDetailedSubmissionMetrics"),
      gcsConfig.getString("groupsPrefix"),
      gcsConfig.getString("defaultLocation")
    )

  }
}
