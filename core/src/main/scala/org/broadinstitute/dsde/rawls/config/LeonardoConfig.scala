package org.broadinstitute.dsde.rawls.config

import com.typesafe.config.Config

case class LeonardoConfig(baseUrl: String, wdsType: String)

object LeonardoConfig {
  def apply(conf: Config): LeonardoConfig = {
    val leonardoConfig = conf.getConfig("leonardo")
    LeonardoConfig(
      leonardoConfig.getString("server"),
      leonardoConfig.getString("wdsType")
    )
  }
}