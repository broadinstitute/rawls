package org.broadinstitute.dsde.rawls.dataaccess

import scala.annotation.tailrec
import scala.util.{Try,Success,Failure}

/**
 * Created by tsharpe on 9/21/15.
 */
trait Retry {

  def retry[T](op: => T, pred: (Throwable) => Boolean = always): T = {
    retry(backoffIntervals)(op,pred) match {
      case Failure(exception) => throw exception
      case Success(t) => t
    }
  }

  def tryRetry[T](op: => T, pred: (Throwable) => Boolean = always): Try[T] = {
    retry(backoffIntervals)(op,pred)
  }

  @tailrec
  private def retry[T](backoffIntervalMillis: Seq[Long])(op: => T, pred: (Throwable) => Boolean ): Try[T] = {
    val result = Try(op)
    if ( result.isSuccess || backoffIntervalMillis.isEmpty || !pred(result.failed.get) )
      result
    else {
      Thread.sleep(backoffIntervalMillis.head)
      retry(backoffIntervalMillis.tail)(op, pred)
    }
  }

  private def always( throwable: Throwable ) = { true }

  private val backoffIntervals = Seq(100L,1000L,3000L)
}
