package org.broadinstitute.dsde.rawls.dataaccess.slick

import org.broadinstitute.dsde.rawls.RawlsException

/**
 * Created by dvoet on 3/24/16.
 */
class RawlsConcurrentModificationException(message: String = null, cause: Throwable = null)
    extends RawlsException(message, cause)
