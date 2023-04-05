package org.broadinstitute.dsde.rawls.config

import com.typesafe.config.Config
import org.broadinstitute.dsde.rawls.model.ProjectPoolId

import java.time.Duration

case class FastPassConfig(
  enabled: Boolean,
  grantPeriod: Duration
)

object FastPassConfig {
  def apply(conf: Config): FastPassConfig = {
    val fastPassConfig = conf.getConfig("fastPass")
    FastPassConfig(
      fastPassConfig.getBoolean("enabled"),
      fastPassConfig.getDuration("grantPeriod")
    )
  }
}
