package org.broadinstitute.dsde.rawls.entities.exceptions

/** Exception related to working with data entities.
 */
class UnsupportedEntityOperationException(message: String = null, cause: Throwable = null)
    extends DataEntityException(message, cause)
