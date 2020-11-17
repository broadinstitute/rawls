package org.broadinstitute.dsde.rawls.config

import com.typesafe.config.Config
import org.broadinstitute.dsde.rawls.model.ProjectPoolId

case class ResourceBufferConfig(
                                 url: String,
                                 regularProjectPoolId: ProjectPoolId,
                                 servicePerimeterProjectPoolId: ProjectPoolId
                               )

object ResourceBufferConfig{
  def apply(conf: Config): ResourceBufferConfig = {
    ResourceBufferConfig(
      conf.getString("url"),  // todo: will startup fail if this config doesn't exist?
      ProjectPoolId(conf.getString("projectPool.regularProjectPoolId")),
      ProjectPoolId(conf.getString("projectPool.servicePerimeterProjectPoolId"))
    )
  }
}
