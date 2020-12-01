package org.broadinstitute.dsde.rawls.config

import com.typesafe.config.Config
import org.broadinstitute.dsde.rawls.model.ProjectPoolId

case class ResourceBufferConfig(
                                 url: String,
                                 regularProjectPoolId: ProjectPoolId,
                                 networkMonitoredProjectPoolId: ProjectPoolId
                               )

object ResourceBufferConfig{
  def apply(conf: Config): ResourceBufferConfig = {
    ResourceBufferConfig(
      conf.getString("url"),
      ProjectPoolId(conf.getString("projectPool.regular")),
      ProjectPoolId(conf.getString("projectPool.servicePerimeter"))
    )
  }
}
