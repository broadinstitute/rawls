package org.broadinstitute.dsde.rawls

import scala.concurrent.duration.Duration

/**
 * Created by dvoet on 2/24/17.
 */
package object util {
  def toScalaDuration(javaDuration: java.time.Duration) = Duration.fromNanos(javaDuration.toNanos)
}
