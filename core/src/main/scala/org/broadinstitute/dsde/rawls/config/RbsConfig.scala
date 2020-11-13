package org.broadinstitute.dsde.rawls.config

import com.typesafe.config.Config

case class RbsConfig(
                                         // pool for projects
                                         regularProjectPoolId: String,
                                         // pool for projects that will be in service perimeters
                                         servicePerimeterProjectPoolId: String
                    )

object RbsConfig{
  def apply(conf: Config): RbsConfig = {
    RbsConfig(
      conf.getString("regularProjectPoolId"),
      conf.getString("servicePerimeterProjectPoolId")

    )
  }
}