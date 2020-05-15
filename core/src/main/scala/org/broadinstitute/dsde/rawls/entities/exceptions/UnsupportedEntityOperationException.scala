package org.broadinstitute.dsde.rawls.entities.exceptions

import org.broadinstitute.dsde.rawls.RawlsException

/** Exception related to working with data entities.
 */
class UnsupportedEntityOperationException(message: String = null, cause: Throwable = null) extends RawlsException(message, cause)

