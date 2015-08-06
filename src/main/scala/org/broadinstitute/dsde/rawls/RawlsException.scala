package org.broadinstitute.dsde.rawls

import spray.http.{StatusCodes, StatusCode}

class RawlsException(message: String = null, cause: Throwable = null) extends Exception(message, cause)

/**
 * Specifies which HTTP status code to return if/when this exception gets bubbled up.
 */
class RawlsExceptionWithStatusCode(message: String = null, cause: Throwable = null,
  statusCode: StatusCode = StatusCodes.InternalServerError) extends RawlsException(message, cause) {
  def getCode = statusCode
}