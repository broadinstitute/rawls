package org.broadinstitute.dsde.rawls.entities.exceptions

import akka.http.scaladsl.model.{StatusCode, StatusCodes}
import org.broadinstitute.dsde.rawls.RawlsException

/** Exception related to working with data entities.
 */
class DataEntityException(message: String = null,
                          cause: Throwable = null,
                          val code: StatusCode = StatusCodes.InternalServerError
) extends RawlsException(message, cause)
