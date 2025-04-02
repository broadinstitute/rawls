package org.broadinstitute.dsde.rawls.entities.exceptions

import akka.http.scaladsl.model.{StatusCode, StatusCodes}

class EntityReferenceNotFoundException(message: String = "Entity reference not found.",
                                       cause: Throwable = null,
                                       override val code: StatusCode = StatusCodes.BadRequest
) extends DataEntityException(message, cause, code)
