package org.broadinstitute.dsde.rawls.entities.exceptions

import akka.http.scaladsl.model.StatusCodes

class EntityTypeNotFoundException(val requestedType: String)
  extends DataEntityException(code = StatusCodes.BadRequest, message = "Entity type not found")
