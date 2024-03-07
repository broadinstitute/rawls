package org.broadinstitute.dsde.rawls.config

import com.typesafe.config.{Config, ConfigFactory}
import org.broadinstitute.dsde.rawls.model.WorkspaceCloudPlatform
import org.broadinstitute.dsde.rawls.model.WorkspaceCloudPlatform.{Gcp, WorkspaceCloudPlatform}

class MultiCloudAppConfigManager {
  val conf = ConfigFactory.parseResources("version.conf").withFallback(ConfigFactory.load())
  val cloudProvider = getCloudProvider(conf)
  val gcsConfig = cloudProvider match {
    case Gcp => Option(conf.getConfig("gcs"))
    case _   => None
  }

  private def getCloudProvider(config: Config): WorkspaceCloudPlatform =
    if (config.hasPath("cloudProvider")) {
      WorkspaceCloudPlatform.withName(config.getString("cloudProvider"))
    } else {
      Gcp
    }
}
