package org.broadinstitute.dsde.rawls.entities.exceptions

import akka.http.scaladsl.model.{StatusCode, StatusCodes}

class EntityNotFoundException(message: String = "Entity not found.",
                              cause: Throwable = null,
                              override val code: StatusCode = StatusCodes.NotFound
) extends DataEntityException(message, cause, code)
