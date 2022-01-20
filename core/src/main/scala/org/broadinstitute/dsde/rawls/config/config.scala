package org.broadinstitute.dsde.rawls

import net.ceedubs.ficus.readers.ValueReader

/**
  * Created by dvoet on 7/18/17.
  */
package object config {
  implicit val swaggerReader: ValueReader[SwaggerConfig] = ValueReader.relative { config =>
    SwaggerConfig(
      config.getString("googleClientId"),
      config.getString("realm")
    )
  }
}
