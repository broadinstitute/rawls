package org.broadinstitute.dsde.test.api

import org.broadinstitute.dsde.workbench.config.CommonConfig

object RawlsConfig extends CommonConfig {
  private val gcs = config.getConfig("gcs")
  val pathToQAJson = gcs.getString("qaJsonFile")

  private val fireCloud = config.getConfig("fireCloud")
  // This value is only available when tests are run through the rawls-swat Github action (in a bee).
  val wsmUrl = if (fireCloud.hasPath("workspaceManagerApiUrl")) {
    fireCloud.getString("workspaceManagerApiUrl")
  } else {
    "ERROR: workspaceManagerApiUrl unspecified! "
  }

  // This value is only available when tests are run through the rawls-swat Github action (in a bee).
  val leoUrl = if (fireCloud.hasPath("leonardoApiUrl")) {
    fireCloud.getString("leonardoApiUrl")
  } else {
    "ERROR: leonardoApiUrl unspecified! "
  }
}
