package org.broadinstitute.dsde.rawls.entities.exceptions

class EntityNotFoundException(message: String = null, cause: Throwable = null)
    extends DataEntityException(message, cause)
