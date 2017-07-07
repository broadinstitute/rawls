package org.broadinstitute.dsde.rawls.metrics

import java.util.concurrent.TimeUnit

import com.codahale.metrics.{MetricFilter, SharedMetricRegistries}
import com.readytalk.metrics.{StatsD, StatsDReporter}
import org.broadinstitute.dsde.rawls.model.Subsystems.Subsystem
import org.broadinstitute.dsde.rawls.util.MockitoTestUtils
import org.mockito.Mockito.{atLeastOnce, inOrder => mockitoInOrder}
import org.scalatest.concurrent.Eventually

import scala.collection.JavaConverters._
import scala.concurrent.duration._

/**
  * Created by rtitle on 6/29/17.
  */
trait StatsDTestUtils { this: Eventually with MockitoTestUtils =>

  protected def workbenchMetricBaseName = "test"
  def clearRegistries(): Unit = SharedMetricRegistries.getOrCreate("default").removeMatching(MetricFilter.ALL)

  protected def withStatsD[T](testCode: => T)(verify: Seq[(String, String)] => Unit = _ => ()): T = {
    val statsD = mock[StatsD]
    clearRegistries()
    val reporter = StatsDReporter.forRegistry(SharedMetricRegistries.getOrCreate("default"))
      .convertRatesTo(TimeUnit.SECONDS)
      .convertDurationsTo(TimeUnit.MILLISECONDS)
      .build(statsD)
    reporter.start(1, TimeUnit.SECONDS)
    try {
      val result = testCode
      eventually(timeout(10 seconds)) {
        val order = mockitoInOrder(statsD)
        order.verify(statsD).connect()
        val metricCaptor = captor[String]
        val valueCaptor = captor[String]
        order.verify(statsD, atLeastOnce).send(metricCaptor.capture, valueCaptor.capture)
        order.verify(statsD).close()
        verify(metricCaptor.getAllValues.asScala.zip(valueCaptor.getAllValues.asScala))
      }
      result
    } finally {
      reporter.stop()
      clearRegistries()
    }
  }

  protected def expectedHttpRequestMetrics(method: String, path: String, statusCode: Int, expectedTimes: Int, subsystem: Option[Subsystem] = None): Set[(String, String)] =
    Set(
      (s"test.${subsystem.map(s => s"subsystem.${s.toString}.").getOrElse("")}httpRequestMethod.$method.httpRequestUri.$path.httpResponseStatusCode.$statusCode.request", expectedTimes.toString),
      (s"test.${subsystem.map(s => s"subsystem.${s.toString}.").getOrElse("")}httpRequestMethod.$method.httpRequestUri.$path.httpResponseStatusCode.$statusCode.latency.samples", expectedTimes.toString)
    )
}
