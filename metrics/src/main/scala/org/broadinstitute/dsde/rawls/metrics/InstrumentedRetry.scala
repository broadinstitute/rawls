package org.broadinstitute.dsde.rawls.metrics

import com.typesafe.scalalogging.LazyLogging
import nl.grons.metrics4.scala.Histogram
import org.broadinstitute.dsde.rawls.util.Retry

import scala.concurrent.{ExecutionContext, Future}
import scala.language.implicitConversions
import scala.util.Success

/**
  * Created by rtitle on 8/3/17.
  */
trait InstrumentedRetry extends Retry {
  this: WorkbenchInstrumented with LazyLogging =>

  /**
    * Converts an RetryableFuture[A] to a Future[A].
    * Given an implicit Histogram, instruments the number of failures in the histogram.
    */
  implicit protected def retryableFutureToFutureWithHisto[A](
    af: RetryableFuture[A]
  )(implicit histo: Histogram, executionContext: ExecutionContext): Future[A] = {
    val instrumentedAf = af.andThen {
      case Success(Left(errors))       => histo += errors.toList.size
      case Success(Right((errors, _))) => histo += errors.size
    }
    super.retryableFutureToFuture(instrumentedAf)
  }
}
