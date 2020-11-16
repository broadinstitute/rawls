package org.broadinstitute.dsde.rawls.config

import com.typesafe.config.Config

case class ResourceBufferConfig(
                                         // pool for projects
                                         regularProjectPoolId: String,
                                         // pool for projects that will be in service perimeters
                                         servicePerimeterProjectPoolId: String
                    )

object ResourceBufferConfig{
  def apply(conf: Config): ResourceBufferConfig = {
    ResourceBufferConfig(
      conf.getString("regularProjectPoolId"),
      conf.getString("servicePerimeterProjectPoolId")

    )
  }
}
