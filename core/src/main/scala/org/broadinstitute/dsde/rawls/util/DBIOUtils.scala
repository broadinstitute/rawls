package org.broadinstitute.dsde.rawls.util

import org.broadinstitute.dsde.rawls.dataaccess.slick.ReadWriteAction
import slick.dbio.DBIO

import scala.concurrent.ExecutionContext

/**
 * Created by dvoet on 11/21/16.
 */
object DBIOUtils {

  /**
   * Given an option, do a db action on it if it is defined, the result of the action is an option
   * @param maybe
   * @param op the thing to do
   * @tparam T type of input within the option
   * @tparam R type of result
   * @return
   */
  def maybeDbAction[T, R](
    maybe: Option[T]
  )(op: T => ReadWriteAction[R])(implicit executionContext: ExecutionContext): ReadWriteAction[Option[R]] =
    maybe match {
      case None    => DBIO.successful(None)
      case Some(t) => op(t).map(r => Option(r))
    }
}
