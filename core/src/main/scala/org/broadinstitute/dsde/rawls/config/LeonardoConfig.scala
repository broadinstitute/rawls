package org.broadinstitute.dsde.rawls.config

import com.typesafe.config.Config

final case class LeonardoConfig(baseUrl: String)

case object LeonardoConfig {
  def apply[T <: LeonardoConfig](conf: Config): LeonardoConfig = {
    val leonardoConfig = conf.getConfig("leonardo")

    new LeonardoConfig(
      leonardoConfig.getString("server"),
    )
  }
}