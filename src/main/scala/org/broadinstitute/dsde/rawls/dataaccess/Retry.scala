package org.broadinstitute.dsde.rawls.dataaccess

import akka.actor.ActorSystem

import scala.annotation.tailrec
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Try,Success,Failure}
import scala.concurrent.duration._

import akka.pattern._

/**
 * Created by tsharpe on 9/21/15.
 */
trait Retry {
  val system: ActorSystem

  def retry[T](pred: (Throwable) => Boolean = always)(op: () => Future[T])(implicit executionContext: ExecutionContext): Future[T] = {
    retry(allBackoffIntervals)(op,pred)
  }

  def retryExponentially[T](pred: (Throwable) => Boolean = always)(op: () => Future[T])(implicit executionContext: ExecutionContext): Future[T] = {
    retry(exponentialBackOffIntervals)(op,pred)
  }

  private def retry[T](remainingBackoffIntervals: Seq[FiniteDuration])(op: => () => Future[T], pred: (Throwable) => Boolean)(implicit executionContext: ExecutionContext): Future[T] = {
    op().recoverWith {
      case t if pred(t) && !remainingBackoffIntervals.isEmpty => after(remainingBackoffIntervals.head, system.scheduler) {
        retry(remainingBackoffIntervals.tail)(op, pred)
      }
    }
  }

  private def always( throwable: Throwable ) = { true }

  private val allBackoffIntervals = Seq(100 milliseconds, 1 second, 3 seconds)

  private def exponentialBackOffIntervals: Seq[FiniteDuration] = {
    val plainIntervals = Seq(1000 milliseconds, 2000 milliseconds, 4000 milliseconds, 8000 milliseconds, 16000 milliseconds, 32000 milliseconds)
    plainIntervals.map(i => i.+(scala.util.Random.nextInt(1000) milliseconds))
  }

}
