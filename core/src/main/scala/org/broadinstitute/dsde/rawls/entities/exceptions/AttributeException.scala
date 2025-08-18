package org.broadinstitute.dsde.rawls.entities.exceptions

import akka.http.scaladsl.model.StatusCode

class AttributeException(val message: String, cause: Throwable = null, override val code: StatusCode)
    extends DataEntityException(message, cause, code)
