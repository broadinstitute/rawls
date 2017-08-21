package com.readytalk.metrics

import com.typesafe.scalalogging.LazyLogging

/**
  * Created by rtitle on 8/21/17.
  */
class WorkbenchStatsD(host: String, port: Int) extends StatsD(host, port) with LazyLogging {
  // Filter out detailed timer metrics to prevent hitting Hosted Graphite quotas.
  // Keep: mean, p95, stddev
  val MetricSuffixesToFilter = Set("max", "min", "p50", "p75", "p98", "p99", "p999", "samples", "m1_rate", "m5_rate", "m15_rate", "mean_rate")

  override def send(name: String, value: String): Unit = {
    if (MetricSuffixesToFilter.exists(suffix => name.endsWith(suffix))) {
      logger.debug(s"Filtering metric with name [$name] and value [$value]")
    } else {
      super.send(name, value)
    }
  }
}

object WorkbenchStatsD {
  def apply(host: String, port: Int): WorkbenchStatsD = new WorkbenchStatsD(host, port)
}
