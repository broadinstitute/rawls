package org.broadinstitute.dsde.rawls.config

import com.typesafe.config.Config

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration

case class WDLParserConfig(cacheMaxSize: Int,
                           cacheTTLSuccessSeconds: Duration,
                           cacheTTLFailureSeconds: Duration,
                           serverBasePath: String,
                           useCache: Boolean
)

case object WDLParserConfig {
  def apply[T <: WDLParserConfig](conf: Config): WDLParserConfig = WDLParserConfig(
    conf.getInt("cache-max-size"),
    Duration(conf.getInt("cache-ttl-success-seconds"), TimeUnit.SECONDS),
    Duration(conf.getInt("cache-ttl-failure-seconds"), TimeUnit.SECONDS),
    conf.getString("server"),
    conf.getBoolean("useCache")
  )
}
