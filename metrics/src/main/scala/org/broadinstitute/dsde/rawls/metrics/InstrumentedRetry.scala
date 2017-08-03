package org.broadinstitute.dsde.rawls.metrics

import com.typesafe.scalalogging.LazyLogging
import nl.grons.metrics.scala.Histogram
import org.broadinstitute.dsde.rawls.util.Retry

/**
  * Created by rtitle on 8/3/17.
  */
trait InstrumentedRetry extends Retry {
  this: WorkbenchInstrumented with LazyLogging =>

  implicit def updateRetryHistogram[A](implicit histo: Histogram): RetryCountConsumer[A] =
    (a, count) => {
      histo += count
      a
    }

}
