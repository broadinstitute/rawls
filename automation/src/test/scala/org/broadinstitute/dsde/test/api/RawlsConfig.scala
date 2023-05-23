package org.broadinstitute.dsde.test.api

import org.broadinstitute.dsde.workbench.config.CommonConfig

object RawlsConfig extends CommonConfig {
  private val gcs = config.getConfig("gcs")
  val pathToQAJson = gcs.getString("qaJsonFile")

  val wsmUrl = config.getConfig("fireCloud").getString("workspaceManagerApiUrl")
}
