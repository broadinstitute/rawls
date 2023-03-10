package org.broadinstitute.dsde.rawls.metrics

import com.codahale.metrics.RatioGauge
import com.codahale.metrics.RatioGauge.Ratio
import nl.grons.metrics4.scala.Counter

/**
  * A metric to output a ratio of hits vs misses (originally intended for caches)
  */
class HitRatioGauge(val hits: Counter, val calls: Counter) extends RatioGauge {
  override def getRatio: Ratio = Ratio.of(hits.count, calls.count)

  def miss(): Unit = calls.inc()

  def hit(): Unit = {
    hits.inc()
    calls.inc()
  }

}
