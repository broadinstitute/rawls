package org.broadinstitute.dsde.rawls.metrics

import com.codahale.metrics._
import com.codahale.metrics.health.SharedHealthCheckRegistries
import com.readytalk.metrics.{StatsD, StatsDReporter}
import org.broadinstitute.dsde.rawls.metrics.MetricsSpec.TestInstrumented
import org.mockito.ArgumentMatchers.{eq => argEq, _}
import org.mockito.Mockito.{inOrder => mockitoInOrder, _}
import org.mockito.{ArgumentMatcher, InOrder}
import org.scalatest.BeforeAndAfter
import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Seconds, Span}
import org.scalatestplus.mockito.MockitoSugar

import java.util.concurrent.TimeUnit
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.Try

/**
  * Created by rtitle on 5/31/17.
  */
class MetricsSpec extends AnyFlatSpec with Matchers with BeforeAndAfter with Eventually with MockitoSugar {
  var statsD: StatsD = _
  var reporter: StatsDReporter = _
  var test: TestInstrumented = _

  implicit override val patienceConfig = PatienceConfig(timeout = scaled(Span(10, Seconds)))

  before {
    test = new TestInstrumented
    statsD = mock[StatsD](RETURNS_SMART_NULLS)
    reporter = StatsDReporter
      .forRegistry(SharedMetricRegistries.getOrCreate("default"))
      .convertRatesTo(TimeUnit.SECONDS)
      .convertDurationsTo(TimeUnit.MILLISECONDS)
      .build(statsD)
    reporter.start(1, TimeUnit.SECONDS)
  }

  after {
    reporter.stop()
    SharedMetricRegistries.clear()
    SharedHealthCheckRegistries.clear()
  }

  "DropWizard metrics" should "increment counters in statsd" in {
    for (_ <- 0 until 100) test.increment

    // counter value should be 100
    verifyStatsD { order =>
      order.verify(statsD).send(argEq("test.a.counter.count"), argEq("100"))
    }
  }

  it should "increment transient counters in statsd" in {
    for (_ <- 0 until 100) test.increment

    // counter value should be 100
    verifyStatsD { order =>
      order.verify(statsD).send(argEq("test.transient.a.transientCounter.count"), argEq("100"))
    }
  }

  it should "update gauges in statsd" in {
    test.set(42)

    // Gauge value should be 42
    verifyStatsD { order =>
      order.verify(statsD).send(argEq("test.a.gauge.current"), argEq("42"))
    }

    test.set(88)
    test.set(99)

    // Gauge should take the most recent value
    verifyStatsD { order =>
      order.verify(statsD).send(argEq("test.a.gauge.current"), argEq("99"))
    }
  }

  it should "update cached gauges in statsd" in {
    test.set(42)

    // Gauge value should be 42
    verifyStatsD { order =>
      order.verify(statsD).send(argEq("test.a.gauge.current"), argEq("42"))
    }

    test.set(43)

    // The gauge value should not be updated immediately
    verifyStatsD { order =>
      order.verify(statsD).send(argEq("test.a.gauge.current"), argEq("42"))
    }

    // Eventually gauge value should be 43
    verifyStatsD { order =>
      order.verify(statsD).send(argEq("test.a.gauge.current"), argEq("43"))
    }
  }

  it should "throw an exception if a gauge already exists" in {
    test.set(42)

    assertThrows[IllegalArgumentException] {
      test.ExpandedMetricBuilder.expand("a", "gauge").asGauge("current")(-1)
    }

    // Gauge value should be 42
    verifyStatsD { order =>
      order.verify(statsD).send(argEq("test.a.gauge.current"), argEq("42"))
    }
  }

  it should "not throw an exception if a gauge already exists and calling asGaugeIfAbsent" in {
    test.set(42)

    // Should not throw exception, and should not affect the gauge
    test.ExpandedMetricBuilder.expand("a", "gauge").asGaugeIfAbsent("current")(-1)

    // Gauge value should be 42
    verifyStatsD { order =>
      order.verify(statsD).send(argEq("test.a.gauge.current"), argEq("42"))
    }
  }

  it should "update timers in statsd" in {
    for (_ <- 0 until 10) test.slowReset

    // Timer should have been updated
    verifyStatsD { order =>
      verifyTimer(order, "test.a.timer.latency")
      verifyMeter(order, "test.a.timer.latency", 10)
    }
  }

  it should "update timed futures in statsd" in {
    for (_ <- 0 until 10) test.slowResetFuture

    // Timer should have been updated
    verifyStatsD { order =>
      verifyTimer(order, "test.a.timer.latency")
      verifyMeter(order, "test.a.timer.latency", 10)
    }
  }

  it should "update histograms in statsd" in {
    for (_ <- 0 until 100) test.increment

    // Histogram should have been updated
    verifyStatsD { order =>
      verifyHistogram(order, "test.a.histogram.histo", 100)
    }
  }

  it should "update meters in statsd" in {
    for (_ <- 0 until 100) test.increment

    // Meter should have been updated
    verifyStatsD { order =>
      verifyMeter(order, "test.meter", 100)
    }
  }

  "DropWizard health checks" should "reflect current health" in {
    val registry = SharedHealthCheckRegistries.getOrCreate("default")
    test.set(50)

    val results = registry.runHealthChecks()

    // It should be healthy
    results should have size 1
    results should contain key "test.health"
    val result = results.get("test.health")
    result.isHealthy should be(true)
    result.getDetails should be(null)
    result.getError should be(null)
    result.getMessage should be(null)
    result.getTimestamp should not be null

    test.set(0)

    // It should be unhealthy
    val results2 = registry.runHealthChecks()
    results2 should have size 1
    results2 should contain key "test.health"
    val result2 = results2.get("test.health")
    result2.isHealthy should be(false)
    result2.getDetails should be(null)
    result2.getError should be(null)
    result2.getMessage should be("Ouch")
    result2.getTimestamp should not be null
  }

  // Helper functions

  private def verifyStatsD(inner: InOrder => Unit) =
    eventually {
      val order = mockitoInOrder(statsD)
      order.verify(statsD).connect()
      inner(order)
      order.verify(statsD).close()
    }

  private def verifyTimer(order: InOrder, prefix: String): Unit = {
    order.verify(statsD, atLeastOnce).send(argEq(s"$prefix.max"), argThat(nonZeroString))
    order.verify(statsD, atLeastOnce).send(argEq(s"$prefix.mean"), argThat(nonZeroString))
    order.verify(statsD, atLeastOnce).send(argEq(s"$prefix.min"), argThat(nonZeroString))
    order.verify(statsD, atLeastOnce).send(argEq(s"$prefix.stddev"), argThat(nonZeroString))
    order.verify(statsD, atLeastOnce).send(argEq(s"$prefix.p50"), argThat(nonZeroString))
    order.verify(statsD, atLeastOnce).send(argEq(s"$prefix.p75"), argThat(nonZeroString))
    order.verify(statsD, atLeastOnce).send(argEq(s"$prefix.p95"), argThat(nonZeroString))
    order.verify(statsD, atLeastOnce).send(argEq(s"$prefix.p98"), argThat(nonZeroString))
    order.verify(statsD, atLeastOnce).send(argEq(s"$prefix.p99"), argThat(nonZeroString))
    order.verify(statsD, atLeastOnce).send(argEq(s"$prefix.p999"), argThat(nonZeroString))
  }

  private def verifyHistogram(order: InOrder, prefix: String, samples: Int): Unit = {
    order.verify(statsD, atLeastOnce).send(argEq(s"$prefix.samples"), argEq(samples.toString))
    verifyTimer(order, prefix)
  }

  private def verifyMeter(order: InOrder, prefix: String, samples: Int): Unit = {
    order.verify(statsD, atLeastOnce).send(argEq(s"$prefix.samples"), argEq(samples.toString))
    order.verify(statsD, atLeastOnce).send(argEq(s"$prefix.m1_rate"), argThat(nonZeroString))
    order.verify(statsD, atLeastOnce).send(argEq(s"$prefix.m5_rate"), argThat(nonZeroString))
    order.verify(statsD, atLeastOnce).send(argEq(s"$prefix.m15_rate"), argThat(nonZeroString))
    order.verify(statsD, atLeastOnce).send(argEq(s"$prefix.mean_rate"), argThat(nonZeroString))
  }

  private def nonZeroString: ArgumentMatcher[String] = new ArgumentMatcher[String] {
    override def matches(argument: String): Boolean =
      Try(argument.toDouble).toOption.exists(_ != 0)
  }

  private def zeroString: ArgumentMatcher[String] = new ArgumentMatcher[String] {
    override def matches(argument: String): Boolean =
      Try(argument.toDouble).toOption.exists(_ == 0)
  }
}

object MetricsSpec {

  /**
    * Test class to exercise DropWizard metrics functionality.
    */
  class TestInstrumented extends WorkbenchInstrumented {
    override val workbenchMetricBaseName = "test"
    private var n: Int = _

    // Define a counter and a timer metric
    lazy val counter = ExpandedMetricBuilder.expand("a", "counter").asCounter("count")
    lazy val aTransientTimer = ExpandedMetricBuilder.expand("a", "transientCounter").transient().asCounter("count")
    lazy val timer = ExpandedMetricBuilder.expand("a", "timer").asTimer("latency")
    lazy val histogram = ExpandedMetricBuilder.expand("a", "histogram").asHistogram("histo")
    lazy val meter = metrics.meter("meter")

    // Non-instrumented methods:
    def set(v: Int): Unit = n = v
    def get: Int = n
    private def slowResetInternal: Unit = {
      Thread.sleep(100)
      n = 0
    }

    // A health check for the value of n
    healthCheck("health", unhealthyMessage = "Ouch") {
      n > 0
    }

    // A gauge of the value of n
    ExpandedMetricBuilder.expand("a", "gauge").asGauge("current")(get)

    // A cached gauge of the value of n with a 2 second TTL
    metrics.cachedGauge("cachedGauge", 2 seconds)(get)

    // Increments the value of n; updates the counter, histogram, and meter
    def increment: Unit = {
      n = n + 1
      counter += 1
      aTransientTimer += 1
      histogram += n
      meter.mark()
    }

    // Updates a timer
    def slowReset: Unit =
      timer.time {
        slowResetInternal
      }

    // Updates a timer using a Future
    def slowResetFuture: Future[Unit] =
      timer.timeFuture {
        Future(slowResetInternal)
      }
  }
}
