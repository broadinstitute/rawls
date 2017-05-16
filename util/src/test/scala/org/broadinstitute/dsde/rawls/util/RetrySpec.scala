package org.broadinstitute.dsde.rawls.util

import akka.actor.ActorSystem
import akka.testkit.TestKit
import com.typesafe.scalalogging.{LazyLogging, Logger}
import org.mockito.Mockito._
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers._
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}
import org.scalatest.concurrent.Eventually._
import org.scalatest.mock.MockitoSugar
import org.scalatest.time.{Millis, Minutes, Seconds, Span}
import org.slf4j.{Logger => Underlying}

import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.reflect._

/**
  * Created by rtitle on 5/16/17.
  */
class RetrySpec extends TestKit(ActorSystem("MySpec")) with FlatSpecLike with BeforeAndAfterAll with Matchers with MockitoSugar {
  import system.dispatcher

  implicit val patienceConfig = PatienceConfig(timeout = scaled(Span(10, Seconds)))

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  "Retry" should "retry 3 times by default" in {
    val testable = new TestRetry(system, setUpMockLogger)

    testable.retry()(() => testable.failure)

    // retryCount should be 4 (1 initial run plus 3 retries)
    // log count should be 4 (3 retries plus 1 failure message)
    eventually {
      testable.retryCount should equal(4)
      verify(testable.underlying, times(4)).info(anyString, any[Throwable])
    }
  }

  it should "not retry upon success" in {
    val testable = new TestRetry(system, setUpMockLogger)

    testable.retry()(() => testable.success)

    // Sleep a bit to make sure we're not retrying
    Thread.sleep(3000)

    // Job should have executed once and not logged anything
    testable.retryCount should equal(1)
    verify(testable.underlying, never).info(anyString, any[Throwable])
  }

  it should "not retry if the predicate returns false" in {
    val testable = new TestRetry(system, setUpMockLogger)

    testable.retry(_ => false)(() => testable.failure)

    Thread.sleep(3000)

    // Job should have executed once and logged once
    testable.retryCount should equal(1)
    verify(testable.underlying, times(1)).info(anyString, any[Throwable])
  }

  it should "log a custom error message" in {
    val testable = new TestRetry(system, setUpMockLogger)
    val customLogMsg = "custom"

    testable.retry(failureLogMessage = customLogMsg)(() => testable.failure)

    // retryCount should be 4 (1 initial run plus 3 retries)
    // log count should be 4 (3 retries plus 1 failure message)
    eventually {
      testable.retryCount should equal(4)
      val argumentCaptor = captor[String]
      verify(testable.underlying, times(4)).info(argumentCaptor.capture, any[Throwable])
      argumentCaptor.getAllValues.asScala.foreach { msg =>
        msg should startWith(customLogMsg)
      }
    }
  }

  it should "retry exponentially 6 times" in {
    // Need to increase the patience config because exponential retries take longer
    implicit val patienceConfig = PatienceConfig(timeout = scaled(Span(2, Minutes)))

    val testable = new TestRetry(system, setUpMockLogger)

    testable.retryExponentially()(() => testable.failure)

    // retryCount should be 7 (1 initial run plus 6 retries)
    // log count should be 4 (6 retries plus 1 failure message)
    eventually {
      testable.retryCount should equal(7)
      verify(testable.underlying, times(7)).info(anyString, any[Throwable])
    }
  }

  it should "retry until a success" in {
    val testable = new TestRetry(system, setUpMockLogger)

    testable.retryUntilSuccessOrTimeout()(100 milliseconds, 1 day)(() => testable.failureNTimes(10))

    eventually {
      testable.retryCount should equal(10)
      verify(testable.underlying, times(10)).info(anyString, any[Throwable])
    }
  }

  it should "retry until a timeout" in {
    val testable = new TestRetry(system, setUpMockLogger)

    testable.retryUntilSuccessOrTimeout()(100 milliseconds, 1 second)(() => testable.failure)

    eventually {
      testable.retryCount should equal(10)
      verify(testable.underlying, times(10)).info(anyString, any[Throwable])
    }
  }

  private def setUpMockLogger: Underlying = {
    val mockLogger = mock[Underlying]
    when(mockLogger.isInfoEnabled).thenReturn(true)
    mockLogger
  }

  private def captor[T: ClassTag]: ArgumentCaptor[T] =
    ArgumentCaptor.forClass(classTag[T].runtimeClass).asInstanceOf[ArgumentCaptor[T]]

}

class TestRetry(val system: ActorSystem, val underlying: Underlying) extends Retry with LazyLogging {
  override lazy val logger: Logger = Logger(underlying)
  var retryCount: Int = _

  def increment { retryCount = retryCount + 1 }
  def success = {
    increment
    Future.successful(())
  }
  def failure = {
    increment
    Future.failed(new Exception)
  }
  def failureNTimes(n: Int) = {
    if (retryCount < n) failure
    else success
  }

}
