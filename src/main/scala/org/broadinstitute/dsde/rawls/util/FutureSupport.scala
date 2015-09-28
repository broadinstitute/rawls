package org.broadinstitute.dsde.rawls.util

import scala.concurrent.Future
import scala.util.{Failure, Success, Try}
import scala.concurrent.ExecutionContext.Implicits.global

/**
 * Created by dvoet on 10/5/15.
 */
trait FutureSupport {
  /**
   * Converts a Future[T] to a Future[Try[T]]. Even if the operation of the Future failed, the resulting
   * Future is considered a success and the error is available in the Try.
   *
   * @param f
   * @tparam T
   * @return
   */
  implicit def toFutureTry[T](f: Future[T]): Future[Try[T]] = f map(Success(_)) recover { case t => Failure(t) }
}
