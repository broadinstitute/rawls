package org.broadinstitute.dsde.rawls.config

import com.typesafe.config.Config

import java.time.Duration

case class FastPassConfig(
  enabled: Boolean,
  grantPeriod: Duration,
  monitorCleanupPeriod: Duration
)

object FastPassConfig {
  def apply(conf: Config): FastPassConfig = {
    val fastPassConfig = conf.getConfig("fastPass")
    FastPassConfig(
      fastPassConfig.getBoolean("enabled"),
      fastPassConfig.getDuration("grantPeriod"),
      fastPassConfig.getDuration("monitorCleanupPeriod")
    )
  }
}
