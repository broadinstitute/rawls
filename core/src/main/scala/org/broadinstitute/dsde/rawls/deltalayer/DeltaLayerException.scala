package org.broadinstitute.dsde.rawls.deltalayer

import akka.http.scaladsl.model.{StatusCode, StatusCodes}
import org.broadinstitute.dsde.rawls.RawlsException

/** Exception related to working with Delta Layer.
 */
class DeltaLayerException(message: String = null, cause: Throwable = null, val code: StatusCode = StatusCodes.InternalServerError) extends RawlsException(message, cause)

