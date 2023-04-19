package org.broadinstitute.dsde.rawls.config

import com.typesafe.config.Config

final case class PrometheusConfig(endpointPort: Int)

object PrometheusConfig {
  def apply(conf: Config): PrometheusConfig = {
    val prometheusConfig = conf.getConfig("prometheus")
    PrometheusConfig(
      prometheusConfig.getInt("endpointPort")
    )
  }
}
