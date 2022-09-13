package org.broadinstitute.dsde.rawls.entities.exceptions

import akka.http.scaladsl.model.{StatusCode, StatusCodes}

class EntityTypeNotFoundException(message: String = "Entity type not found",
                                  cause: Throwable = null,
                                  code: StatusCode = StatusCodes.BadRequest,
                                  val requestedType: String
) extends DataEntityException(message, cause, code)
