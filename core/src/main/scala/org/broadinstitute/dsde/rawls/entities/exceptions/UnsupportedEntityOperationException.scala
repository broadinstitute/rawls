package org.broadinstitute.dsde.rawls.entities.exceptions

import akka.http.scaladsl.model.{StatusCode, StatusCodes}

/** Exception related to working with data entities.
 */
class UnsupportedEntityOperationException(message: String = null,
                                          cause: Throwable = null,
                                          override val code: StatusCode = StatusCodes.InternalServerError
) extends DataEntityException(message, cause, code)
