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

  /**
   * will retry at the given interval until success or the overall timeout has passed
   * @param pred which failures to retry
   * @param interval how often to retry
   * @param timeout how long from now to give up
   * @param op what to try
   * @param executionContext
   * @tparam T
   * @return
   */
  def retryUntilSuccessOrTimeout[T](pred: (Throwable) => Boolean = always)(interval: FiniteDuration, timeout: FiniteDuration)(op: () => Future[T])(implicit executionContext: ExecutionContext): Future[T] = {
    val trialCount = Math.ceil(timeout / interval).toInt
    retry(Seq.fill(trialCount)(interval))(op,pred)
  }

  private def retry[T](remainingBackoffIntervals: Seq[FiniteDuration])(op: => () => Future[T], pred: (Throwable) => Boolean)(implicit executionContext: ExecutionContext): Future[T] = {
    op().recoverWith {
      case t if pred(t) && !remainingBackoffIntervals.isEmpty => after(remainingBackoffIntervals.head, system.scheduler) {
        retry(remainingBackoffIntervals.tail)(op, pred)
      }
    }
  }

  def always( throwable: Throwable ) = { true }

  private val allBackoffIntervals = Seq(100 milliseconds, 1 second, 3 seconds)

  private def exponentialBackOffIntervals: Seq[FiniteDuration] = {
    val plainIntervals = Seq(1000 milliseconds, 2000 milliseconds, 4000 milliseconds, 8000 milliseconds, 16000 milliseconds, 32000 milliseconds)
    plainIntervals.map(i => i.+(scala.util.Random.nextInt(1000) milliseconds))
  }

}
