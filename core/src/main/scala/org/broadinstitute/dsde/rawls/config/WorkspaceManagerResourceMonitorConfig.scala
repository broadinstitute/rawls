package org.broadinstitute.dsde.rawls.config

import com.typesafe.config.Config



case class WorkspaceManagerResourceMonitorConfig(
  defaultRetrySeconds: Int,
  retryUncompletedJobsSeconds: Int
)

case object WorkspaceManagerResourceMonitorConfig {
  val defaultRetrySecondsPath = "workspaceManagerResourceMonitor.defaultRetrySeconds"
  val retryUncompletedJobsSecondsPath = "workspaceManagerResourceMonitor.retryUncompletedJobsSeconds"

  def apply(conf: Config): WorkspaceManagerResourceMonitorConfig = WorkspaceManagerResourceMonitorConfig(
    conf.getInt(defaultRetrySecondsPath),
    conf.getInt(retryUncompletedJobsSecondsPath)
  )
}
