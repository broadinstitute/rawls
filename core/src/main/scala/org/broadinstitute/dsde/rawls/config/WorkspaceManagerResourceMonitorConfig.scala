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
    if (conf.hasPath("defaultRetrySecondsPath")) conf.getInt(defaultRetrySecondsPath) else 60,
    if (conf.hasPath("retryUncompletedJobsSecondsPath")) conf.getInt(retryUncompletedJobsSecondsPath) else 5
  )
}
