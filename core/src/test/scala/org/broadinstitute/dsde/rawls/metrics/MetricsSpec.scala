package org.broadinstitute.dsde.rawls.metrics

import java.util.concurrent.TimeUnit

import akka.actor.Actor

import scala.util.Try
import scala.concurrent.Future
import com.codahale.metrics._
import com.codahale.metrics.health.HealthCheck
import com.readytalk.metrics.{StatsD, StatsDReporter}
import nl.grons.metrics.scala
import nl.grons.metrics.scala.{DefaultInstrumented, MetricName}
import org.broadinstitute.dsde.rawls.metrics.MetricsSpec.TestInstrumented
import org.mockito.{ArgumentCaptor, ArgumentMatcher}
import org.mockito.ArgumentMatchers.{eq => argEq, _}
import org.mockito.Mockito.{inOrder => mockitoInOrder, _}
import org.scalatest.concurrent.Eventually
import org.scalatest.mock.MockitoSugar
import org.scalatest.time.{Seconds, Span}
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

/**
  * Created by rtitle on 5/31/17.
  */
class MetricsSpec extends FlatSpec with Matchers with BeforeAndAfter with Eventually with MockitoSugar {

  var statsD: StatsD = _
  var reporter: StatsDReporter = _
  var test: TestInstrumented = _

  implicit override val patienceConfig = PatienceConfig(timeout = scaled(Span(5, Seconds)))

  before {
    test = new TestInstrumented
    statsD = mock[StatsD]
    reporter = StatsDReporter.forRegistry(SharedMetricRegistries.getOrCreate("default"))
      .convertRatesTo(TimeUnit.SECONDS)
      .convertDurationsTo(TimeUnit.MILLISECONDS)
      .build(statsD)
    reporter.start(1, TimeUnit.SECONDS)
    Thread.sleep(5000)
  }

  after {
    reporter.stop()
    SharedMetricRegistries.clear()
  }

  "DropWizard Metrics" should "increment counters in statsd" in {
    for (_ <- 0 until 100) {
      test.increment
    }

    eventually {
      val order = mockitoInOrder(statsD)
      order.verify(statsD).connect()
      order.verify(statsD).send(argEq("test.testCounter"), argEq("100"))
      order.verify(statsD).close()
    }
  }

  it should "update gauges in statsd" in {
    test.gauge
    test.set(42)

    eventually {
      val order = mockitoInOrder(statsD)
      order.verify(statsD).connect()
      order.verify(statsD).send(argEq("test.testGauge"), argEq("42"))
      order.verify(statsD).close()
    }

    test.set(88)
    test.set(99)

    eventually {
      val order = mockitoInOrder(statsD)
      order.verify(statsD).connect()
      order.verify(statsD).send(argEq("test.testGauge"), argEq("99"))
      order.verify(statsD).close()
    }
  }

  List(test.slowReset _, test.slowResetFuture _).foreach { fn =>
    it should "update timers in statsd" in {
      fn.apply()

      eventually {
        val order = mockitoInOrder(statsD)
        order.verify(statsD, atLeastOnce).connect()
        verify(statsD, atLeastOnce).send(argEq("test.testTimer.max"), argThat(nonZeroString))
        verify(statsD, atLeastOnce).send(argEq("test.testTimer.mean"), argThat(nonZeroString))
        verify(statsD, atLeastOnce).send(argEq("test.testTimer.min"), argThat(nonZeroString))
        // stddev should be zero for 1 sample
        verify(statsD, atLeastOnce).send(argEq("test.testTimer.stddev"), argThat(zeroString))
        verify(statsD, atLeastOnce).send(argEq("test.testTimer.p50"), argThat(nonZeroString))
        verify(statsD, atLeastOnce).send(argEq("test.testTimer.p75"), argThat(nonZeroString))
        verify(statsD, atLeastOnce).send(argEq("test.testTimer.p95"), argThat(nonZeroString))
        verify(statsD, atLeastOnce).send(argEq("test.testTimer.p98"), argThat(nonZeroString))
        verify(statsD, atLeastOnce).send(argEq("test.testTimer.p99"), argThat(nonZeroString))
        verify(statsD, atLeastOnce).send(argEq("test.testTimer.p999"), argThat(nonZeroString))
        verify(statsD, atLeastOnce).send(argEq("test.testTimer.samples"), argEq("1"))
        verify(statsD, atLeastOnce).close()
      }
    }
  }

  def nonZeroString: ArgumentMatcher[String] = new ArgumentMatcher[String] {
    override def matches(argument: String): Boolean =
      Try(argument.toDouble).toOption.exists(_ != 0)
  }

  def zeroString: ArgumentMatcher[String] = new ArgumentMatcher[String] {
    override def matches(argument: String): Boolean =
      Try(argument.toDouble).toOption.exists(_ == 0)
  }

}

object MetricsSpec {

  class TestInstrumented extends DefaultInstrumented {
    private var n: Int = _

    lazy val counter = metrics.counter("testCounter")
    lazy val gauge = metrics.gauge("testGauge")(get)
    lazy val timer = metrics.timer("testTimer")

    override lazy val metricBaseName = MetricName("test")

    def set(v: Int): Unit = n = v
    def get: Int = n

    def increment: Unit = {
      counter += 1
      n = n + 1
    }

    private def slowResetInternal: Unit = {
      Thread.sleep(100)
      n = 0
    }

    def slowReset: Unit = {
      timer.time {
        slowResetInternal
      }
    }

    def slowResetFuture: Future[Unit] = {
      timer.timeFuture {
        Future(slowResetInternal)
      }
    }

    def isAlive: HealthCheck =
      healthCheck("testHealth") {
        n > 0
      }
  }
}