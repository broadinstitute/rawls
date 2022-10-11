package org.broadinstitute.dsde.rawls.metrics

import com.codahale.metrics.{MetricFilter, SharedMetricRegistries}
import com.readytalk.metrics.{StatsD, StatsDReporter}
import org.broadinstitute.dsde.rawls.util.MockitoTestUtils
import org.mockito.Mockito.{atLeastOnce, inOrder => mockitoInOrder, RETURNS_SMART_NULLS}
import org.scalatest.concurrent.Eventually

import java.util.concurrent.TimeUnit
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.language.postfixOps

/**
  * Created by rtitle on 6/29/17.
  */
trait StatsDTestUtils { this: Eventually with MockitoTestUtils =>

  protected val workbenchMetricBaseName = "test"
  def clearRegistries(): Unit = SharedMetricRegistries.getOrCreate("default").removeMatching(MetricFilter.ALL)

  protected def withStatsD[T](testCode: => T)(verify: Seq[(String, String)] => Unit = _ => ()): T = {
    val statsD = mock[StatsD](RETURNS_SMART_NULLS)
    clearRegistries()
    val reporter = StatsDReporter
      .forRegistry(SharedMetricRegistries.getOrCreate("default"))
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
        verify(metricCaptor.getAllValues.asScala.toList.zip(valueCaptor.getAllValues.asScala.toList))
      }
      result
    } finally {
      reporter.stop()
      clearRegistries()
    }
  }
}
