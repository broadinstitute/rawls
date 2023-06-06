package org.broadinstitute.dsde.rawls.util

import akka.actor.ActorSystem
import akka.testkit.TestKit
import com.typesafe.scalalogging.{LazyLogging, Logger}
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Minutes, Seconds, Span}
import org.slf4j.{Logger => SLF4JLogger}

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.language.postfixOps

/**
  * Created by rtitle on 5/16/17.
  */
class RetrySpec
    extends TestKit(ActorSystem("MySpec"))
    with AnyFlatSpecLike
    with BeforeAndAfterAll
    with Matchers
    with MockitoTestUtils
    with ScalaFutures {
  import system.dispatcher

  // This configures how long the calls to `whenReady(Future)` will wait for the Future
  // before giving up and failing the test.
  // See: http://doc.scalatest.org/2.2.4/index.html#org.scalatest.concurrent.Futures
  implicit override val patienceConfig = PatienceConfig(timeout = scaled(Span(10, Seconds)))

  override def afterAll() {
    TestKit.shutdownActorSystem(system)
  }

  "Retry" should "retry 3 times by default" in {
    val testable = new TestRetry(system, setUpMockLogger)
    import testable._

    val result: Future[Int] = testable.retry()(() => testable.failure)

    // result should be a failure
    // invocationCount should be 4 (1 initial run plus 3 retries)
    // log count should be 4 (3 retries plus 1 failure message)
    whenReady(result.failed) { ex =>
      ex shouldBe an[Exception]
      ex should have message "test exception"
      testable.invocationCount should equal(4)
      // A note on Mockito: this is basically saying "verify that my mock
      // SLF4JLogger had info() called on it 4 times with any String and
      // any Throwable as arguments."
      // For more information and examples on Mockito, see:
      // http://static.javadoc.io/org.mockito/mockito-core/2.7.22/org/mockito/Mockito.html#verification
      verify(testable.slf4jLogger, times(4)).info(anyString, any[Throwable])
    }
  }

  it should "not retry upon success" in {
    val testable = new TestRetry(system, setUpMockLogger)
    import testable._

    val result: Future[Int] = testable.retry()(() => testable.success)

    // result should a success
    // invocationCount should be 1
    // should not have logged anything
    whenReady(result) { res =>
      res should be(42)
      testable.invocationCount should equal(1)
      verify(testable.slf4jLogger, never).info(anyString, any[Throwable])
    }
  }

  it should "not retry if the predicate returns false" in {
    val testable = new TestRetry(system, setUpMockLogger)
    import testable._

    val result: Future[Int] = testable.retry(_ => false)(() => testable.failure)

    // result should be a failure
    // invocationCount should be 1
    // log count should be 1
    whenReady(result.failed) { ex =>
      ex shouldBe an[Exception]
      ex should have message "test exception"
      testable.invocationCount should equal(1)
      verify(testable.slf4jLogger, times(1)).info(anyString, any[Throwable])
    }
  }

  it should "log a custom error message" in {
    val testable = new TestRetry(system, setUpMockLogger)
    import testable._
    val customLogMsg = "custom"

    val result: Future[Int] = testable.retry(failureLogMessage = customLogMsg)(() => testable.failure)

    // result should be a failure
    // invocationCount should be 4 (1 initial run plus 3 retries)
    // log count should be 4 (3 retries plus 1 failure message)
    whenReady(result.failed) { ex =>
      ex shouldBe an[Exception]
      ex should have message "test exception"
      testable.invocationCount should equal(4)
      val argumentCaptor = captor[String]
      verify(testable.slf4jLogger, times(4)).info(argumentCaptor.capture, any[Throwable])
      argumentCaptor.getAllValues.asScala.foreach { msg =>
        msg should startWith(customLogMsg)
      }
    }
  }

  it should "retry exponentially 6 times" in {
    // Need to increase the patience config because exponential retries take longer
    implicit val patienceConfig = PatienceConfig(timeout = scaled(Span(2, Minutes)))

    val testable = new TestRetry(system, setUpMockLogger)
    import testable._

    val result: Future[Int] = testable.retryExponentially()(() => testable.failure)

    // result should be a failure
    // invocationCount should be 7 (1 initial run plus 6 retries)
    // log count should be 7 (6 retries plus 1 failure message)
    whenReady(result.failed) { ex =>
      ex shouldBe an[Exception]
      ex should have message "test exception"
      testable.invocationCount should equal(7)
      verify(testable.slf4jLogger, times(7)).info(anyString, any[Throwable])
    }
  }

  it should "retry until a success" in {
    val testable = new TestRetry(system, setUpMockLogger)
    import testable._

    val result: Future[Int] =
      testable.retryUntilSuccessOrTimeout()(100 milliseconds, 1 minute)(() => testable.failureNTimes(10))

    // result should be a success
    // invocationCount should be 11 (10 failures and 1 success)
    // log count should be 10
    whenReady(result) { res =>
      res should be(42)
      testable.invocationCount should equal(11)
      verify(testable.slf4jLogger, times(10)).info(anyString, any[Throwable])
    }
  }

  it should "retry until a timeout" in {
    val testable = new TestRetry(system, setUpMockLogger)
    import testable._

    val result: Future[Int] = testable.retryUntilSuccessOrTimeout()(100 milliseconds, 1 second)(() => testable.failure)

    // result should be a failure
    // invocationCounts should be 11
    // log count should be 11
    whenReady(result.failed) { ex =>
      ex shouldBe an[Exception]
      ex should have message "test exception"
      testable.invocationCount should equal(11)
      verify(testable.slf4jLogger, times(11)).info(anyString, any[Throwable])
    }
  }

  /**
    * Creates a mock org.sl4jf.Logger instance using Mockito which is stubbed to
    * return true on calls to isInfoEnabled().
    * This mock will be used to capture and inspect calls to logger.info() from
    * the Retry trait.
    * @return mock SLF4JLogger
    */
  private def setUpMockLogger: SLF4JLogger = {
    val mockLogger = mock[SLF4JLogger](RETURNS_SMART_NULLS)
    when(mockLogger.isInfoEnabled).thenReturn(true)
    mockLogger
  }
}

class TestRetry(val system: ActorSystem, val slf4jLogger: SLF4JLogger) extends Retry with LazyLogging {
  override lazy val logger: Logger = Logger(slf4jLogger)
  var invocationCount: Int = _

  def increment { invocationCount = invocationCount + 1 }
  def success = {
    increment
    Future.successful(42)
  }
  def failure = {
    increment
    Future.failed[Int](new Exception("test exception"))
  }
  def failureNTimes(n: Int) =
    if (invocationCount < n) failure
    else success

}
