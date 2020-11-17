package org.broadinstitute.dsde.rawls.config

import com.typesafe.config.Config

case class ResourceBufferConfig(
                                 url: String,
                                 regularProjectPoolId: String,
                                 servicePerimeterProjectPoolId: String
                               )

object ResourceBufferConfig{
  def apply(conf: Config): ResourceBufferConfig = {
    ResourceBufferConfig(
      conf.getString("url"),
      conf.getString("projectPool.regularProjectPoolId"),
      conf.getString("projectPool.servicePerimeterProjectPoolId")
    )
  }
}
